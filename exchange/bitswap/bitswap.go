// package bitswap implements the IPFS Exchange interface with the BitSwap
// bilateral exchange protocol.
package bitswap

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	process "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/goprocess"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	blocks "github.com/ipfs/go-ipfs/blocks"
	blockstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	exchange "github.com/ipfs/go-ipfs/exchange"
	decision "github.com/ipfs/go-ipfs/exchange/bitswap/decision"
	bsmsg "github.com/ipfs/go-ipfs/exchange/bitswap/message"
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	notifications "github.com/ipfs/go-ipfs/exchange/bitswap/notifications"
	wantlist "github.com/ipfs/go-ipfs/exchange/bitswap/wantlist"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	"github.com/ipfs/go-ipfs/thirdparty/delay"
	eventlog "github.com/ipfs/go-ipfs/thirdparty/eventlog"
	u "github.com/ipfs/go-ipfs/util"
	pset "github.com/ipfs/go-ipfs/util/peerset" // TODO move this to peerstore
)

var log = eventlog.Logger("bitswap")

const (
	// maxProvidersPerRequest specifies the maximum number of providers desired
	// from the network. This value is specified because the network streams
	// results.
	// TODO: if a 'non-nice' strategy is implemented, consider increasing this value
	maxProvidersPerRequest = 3
	providerRequestTimeout = time.Second * 10
	hasBlockTimeout        = time.Second * 15
	provideTimeout         = time.Second * 15
	sizeBatchRequestChan   = 32
	// kMaxPriority is the max priority as defined by the bitswap protocol
	kMaxPriority = math.MaxInt32

	HasBlockBufferSize = 256
	provideWorkers     = 4
)

var (
	rebroadcastDelay = delay.Fixed(time.Second * 10)
)

// New initializes a BitSwap instance that communicates over the provided
// BitSwapNetwork. This function registers the returned instance as the network
// delegate.
// Runs until context is cancelled.
func New(parent context.Context, p peer.ID, network bsnet.BitSwapNetwork,
	bstore blockstore.Blockstore, nice bool) exchange.Interface {

	// important to use provided parent context (since it may include important
	// loggable data). It's probably not a good idea to allow bitswap to be
	// coupled to the concerns of the IPFS daemon in this way.
	//
	// FIXME(btc) Now that bitswap manages itself using a process, it probably
	// shouldn't accept a context anymore. Clients should probably use Close()
	// exclusively. We should probably find another way to share logging data
	ctx, cancelFunc := context.WithCancel(parent)

	notif := notifications.New()
	px := process.WithTeardown(func() error {
		notif.Shutdown()
		return nil
	})

	go func() {
		<-px.Closing() // process closes first
		cancelFunc()
	}()
	go func() {
		<-ctx.Done() // parent cancelled first
		px.Close()
	}()

	bs := &Bitswap{
		self:          p,
		blockstore:    bstore,
		notifications: notif,
		engine:        decision.NewEngine(ctx, bstore), // TODO close the engine with Close() method
		network:       network,
		wantlist:      wantlist.NewThreadSafe(),
		batchRequests: make(chan *blockRequest, sizeBatchRequestChan),
		process:       px,
		newBlocks:     make(chan *blocks.Block, HasBlockBufferSize),
		provideKeys:   make(chan u.Key),
		pm:            NewPeerManager(network),
	}
	go bs.pm.Run(ctx)
	network.SetDelegate(bs)

	// Start up bitswaps async worker routines
	bs.startWorkers(px, ctx)
	return bs
}

// Bitswap instances implement the bitswap protocol.
type Bitswap struct {

	// the ID of the peer to act on behalf of
	self peer.ID

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// the peermanager manages sending messages to peers in a way that
	// wont block bitswap operation
	pm *PeerManager

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

	notifications notifications.PubSub

	// Requests for a set of related blocks
	// the assumption is made that the same peer is likely to
	// have more than a single block in the set
	batchRequests chan *blockRequest

	engine *decision.Engine

	wantlist *wantlist.ThreadSafe

	process process.Process

	newBlocks chan *blocks.Block

	provideKeys chan u.Key

	blocksRecvd    int
	dupBlocksRecvd int
}

type blockRequest struct {
	keys []u.Key
	ctx  context.Context
}

// GetBlock attempts to retrieve a particular block from peers within the
// deadline enforced by the context.
func (bs *Bitswap) GetBlock(parent context.Context, k u.Key) (*blocks.Block, error) {

	// Any async work initiated by this function must end when this function
	// returns. To ensure this, derive a new context. Note that it is okay to
	// listen on parent in this scope, but NOT okay to pass |parent| to
	// functions called by this one. Otherwise those functions won't return
	// when this context's cancel func is executed. This is difficult to
	// enforce. May this comment keep you safe.

	ctx, cancelFunc := context.WithCancel(parent)

	ctx = eventlog.ContextWithLoggable(ctx, eventlog.Uuid("GetBlockRequest"))
	defer log.EventBegin(ctx, "GetBlockRequest", &k).Done()

	defer func() {
		cancelFunc()
	}()

	promise, err := bs.GetBlocks(ctx, []u.Key{k})
	if err != nil {
		return nil, err
	}

	select {
	case block, ok := <-promise:
		if !ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, errors.New("promise channel was closed")
			}
		}
		return block, nil
	case <-parent.Done():
		return nil, parent.Err()
	}
}

func (bs *Bitswap) WantlistForPeer(p peer.ID) []u.Key {
	var out []u.Key
	for _, e := range bs.engine.WantlistForPeer(p) {
		out = append(out, e.Key)
	}
	return out
}

// GetBlocks returns a channel where the caller may receive blocks that
// correspond to the provided |keys|. Returns an error if BitSwap is unable to
// begin this request within the deadline enforced by the context.
//
// NB: Your request remains open until the context expires. To conserve
// resources, provide a context with a reasonably short deadline (ie. not one
// that lasts throughout the lifetime of the server)
func (bs *Bitswap) GetBlocks(ctx context.Context, keys []u.Key) (<-chan *blocks.Block, error) {
	select {
	case <-bs.process.Closing():
		return nil, errors.New("bitswap is closed")
	default:
	}
	promise := bs.notifications.Subscribe(ctx, keys...)

	req := &blockRequest{
		keys: keys,
		ctx:  ctx,
	}
	select {
	case bs.batchRequests <- req:
		return promise, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HasBlock announces the existance of a block to this bitswap service. The
// service will potentially notify its peers.
func (bs *Bitswap) HasBlock(ctx context.Context, blk *blocks.Block) error {
	log.Event(ctx, "hasBlock", blk)
	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	if err := bs.blockstore.Put(blk); err != nil {
		return err
	}
	bs.wantlist.Remove(blk.Key())
	bs.notifications.Publish(blk)
	select {
	case bs.newBlocks <- blk:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (bs *Bitswap) sendWantlistMsgToPeers(ctx context.Context, m bsmsg.BitSwapMessage, peers <-chan peer.ID) error {
	set := pset.New()
	wg := sync.WaitGroup{}

loop:
	for {
		select {
		case peerToQuery, ok := <-peers:
			if !ok {
				break loop
			}

			if !set.TryAdd(peerToQuery) { //Do once per peer
				continue
			}

			wg.Add(1)
			go func(p peer.ID) {
				defer wg.Done()
				if err := bs.send(ctx, p, m); err != nil {
					log.Debug(err) // TODO remove if too verbose
				}
			}(peerToQuery)
		case <-ctx.Done():
			return nil
		}
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// NB: we may be abandoning goroutines here before they complete
		// this shouldnt be an issue because they will complete soon anyways
		// we just don't want their being slow to impact bitswap transfer speeds
	}
	return nil
}

func (bs *Bitswap) sendWantlistToPeers(ctx context.Context, peers <-chan peer.ID) error {
	message := bsmsg.New()
	message.SetFull(true)
	for _, wanted := range bs.wantlist.Entries() {
		message.AddEntry(wanted.Key, wanted.Priority)
	}
	return bs.sendWantlistMsgToPeers(ctx, message, peers)
}

func (bs *Bitswap) sendWantlistToProviders(ctx context.Context, entries []wantlist.Entry) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prepare a channel to hand off to sendWantlistToPeers
	sendToPeers := make(chan peer.ID)

	// Get providers for all entries in wantlist (could take a while)
	wg := sync.WaitGroup{}
	for _, e := range entries {
		wg.Add(1)
		go func(k u.Key) {
			defer wg.Done()

			child, cancel := context.WithTimeout(ctx, providerRequestTimeout)
			defer cancel()
			providers := bs.network.FindProvidersAsync(child, k, maxProvidersPerRequest)
			for prov := range providers {
				sendToPeers <- prov
			}
		}(e.Key)
	}

	go func() {
		wg.Wait() // make sure all our children do finish.
		close(sendToPeers)
	}()

	err := bs.sendWantlistToPeers(ctx, sendToPeers)
	if err != nil {
		log.Debugf("sendWantlistToPeers error: %s", err)
	}
}

// TODO(brian): handle errors
func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) error {
	//defer log.EventBegin(ctx, "receiveMessage", p, incoming).Done()

	// This call records changes to wantlists, blocks received,
	// and number of bytes transfered.
	bs.engine.MessageReceived(p, incoming)
	// TODO: this is bad, and could be easily abused.
	// Should only track *useful* messages in ledger

	for _, e := range incoming.Wantlist() {
		if e.Cancel {
			bs.pm.CancelBlock(p, e.Key)
		}
	}

	var keys []u.Key
	for _, block := range incoming.Blocks() {
		bs.blocksRecvd++
		if has, err := bs.blockstore.Has(block.Key()); err == nil && has {
			bs.dupBlocksRecvd++
		}
		log.Debugf("got block %s from %s", block, p)
		hasBlockCtx, cancel := context.WithTimeout(ctx, hasBlockTimeout)
		if err := bs.HasBlock(hasBlockCtx, block); err != nil {
			return fmt.Errorf("ReceiveMessage HasBlock error: %s", err)
		}
		cancel()
		keys = append(keys, block.Key())
	}

	bs.cancelBlocks(ctx, keys)
	return nil
}

// Connected/Disconnected warns bitswap about peer connections
func (bs *Bitswap) PeerConnected(p peer.ID) {
	// TODO: add to clientWorker??
	bs.pm.Connected(p)
	peers := make(chan peer.ID, 1)
	peers <- p
	close(peers)
	err := bs.sendWantlistToPeers(context.TODO(), peers)
	if err != nil {
		log.Debugf("error sending wantlist: %s", err)
	}
}

// Connected/Disconnected warns bitswap about peer connections
func (bs *Bitswap) PeerDisconnected(p peer.ID) {
	bs.pm.Disconnected(p)
	bs.engine.PeerDisconnected(p)
}

func (bs *Bitswap) cancelBlocks(ctx context.Context, bkeys []u.Key) {
	if len(bkeys) < 1 {
		return
	}
	message := bsmsg.New()
	message.SetFull(false)
	for _, k := range bkeys {
		log.Debug("cancel block: %s", k)
		message.Cancel(k)
	}

	bs.pm.Broadcast(message)
	return
}

func (bs *Bitswap) wantNewBlocks(ctx context.Context, bkeys []u.Key) {
	if len(bkeys) < 1 {
		return
	}

	message := bsmsg.New()
	message.SetFull(false)
	for i, k := range bkeys {
		message.AddEntry(k, kMaxPriority-i)
	}

	bs.pm.Broadcast(message)
}

func (bs *Bitswap) ReceiveError(err error) {
	log.Debugf("Bitswap ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

// send strives to ensure that accounting is always performed when a message is
// sent
func (bs *Bitswap) send(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) error {
	//defer log.EventBegin(ctx, "sendMessage", p, m).Done()
	bs.pm.Send(p, m)
	return bs.engine.MessageSent(p, m)
}

func (bs *Bitswap) Close() error {
	return bs.process.Close()
}

func (bs *Bitswap) GetWantlist() []u.Key {
	var out []u.Key
	for _, e := range bs.wantlist.Entries() {
		out = append(out, e.Key)
	}
	return out
}
