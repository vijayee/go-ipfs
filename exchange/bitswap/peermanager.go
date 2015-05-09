package bitswap

import (
	"sync"

	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	blocks "github.com/ipfs/go-ipfs/blocks"
	bsmsg "github.com/ipfs/go-ipfs/exchange/bitswap/message"
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	u "github.com/ipfs/go-ipfs/util"
	// TODO move this to peerstore
)

type PeerManager struct {
	receiver bsnet.Receiver

	incoming   chan *msgPair
	connect    chan peer.ID
	disconnect chan peer.ID
	cancels    chan cancellation

	peers map[peer.ID]*msgQueue

	network bsnet.BitSwapNetwork
}

func NewPeerManager(network bsnet.BitSwapNetwork) *PeerManager {
	return &PeerManager{
		incoming:   make(chan *msgPair, 10),
		connect:    make(chan peer.ID, 10),
		disconnect: make(chan peer.ID, 10),
		cancels:    make(chan cancellation, 10),
		peers:      make(map[peer.ID]*msgQueue),
		network:    network,
	}
}

type msgPair struct {
	to  peer.ID
	msg bsmsg.BitSwapMessage
}

type msgQueue struct {
	p peer.ID

	lk     sync.Mutex
	wlmsg  bsmsg.BitSwapMessage
	blocks map[u.Key]*blocks.Block

	work chan struct{}
	done chan struct{}
}

func (pm *PeerManager) runQueue(mq *msgQueue) {
	for {
		select {
		case <-mq.work: // there is work to be done
			err := pm.network.ConnectTo(context.TODO(), mq.p)
			if err != nil {
				log.Error(err)
				// TODO: cant connect, what now?
			}

			// grab messages from queue
			mq.lk.Lock()
			wlm := mq.wlmsg
			blks := mq.blocks
			mq.wlmsg = nil
			mq.blocks = make(map[u.Key]*blocks.Block)
			mq.lk.Unlock()

			if !wlm.Empty() {
				// send wantlist updates
				err = pm.network.SendMessage(context.TODO(), mq.p, wlm)
				if err != nil {
					log.Error("bitswap send error: ", err)
					// TODO: what do we do if this fails?
				}
			}

			// now send blocks
			for _, blk := range blks {
				msg := bsmsg.New()
				msg.AddBlock(blk)

				err := pm.network.SendMessage(context.TODO(), mq.p, msg)
				if err != nil {
					log.Error("bitswap send error: ", err)
					// TODO: what do we do if this fails?
				}
			}

		case <-mq.done:
			return
		}
	}
}

func (mq *msgQueue) AddMessage(msg bsmsg.BitSwapMessage) {
	mq.lk.Lock()
	defer func() {
		mq.lk.Unlock()
		select {
		case mq.work <- struct{}{}:
		default:
		}
	}()

	for _, blk := range msg.Blocks() {
		mq.blocks[blk.Key()] = blk
	}
	msg.ClearBlocks()

	if mq.wlmsg == nil || msg.Full() {
		mq.wlmsg = msg
		return
	}

	for _, e := range msg.Wantlist() {
		if e.Cancel {
			mq.wlmsg.Cancel(e.Key)
		} else {
			mq.wlmsg.AddEntry(e.Key, e.Priority)
		}
	}
}

func (pm *PeerManager) Send(to peer.ID, msg bsmsg.BitSwapMessage) {
	pm.incoming <- &msgPair{to: to, msg: msg}
}

func (pm *PeerManager) Broadcast(msg bsmsg.BitSwapMessage) {
	pm.incoming <- &msgPair{msg: msg}
}

func (pm *PeerManager) Connected(p peer.ID) {
	pm.connect <- p
}

func (pm *PeerManager) Disconnected(p peer.ID) {
	pm.disconnect <- p
}

type cancellation struct {
	who peer.ID
	blk u.Key
}

func (pm *PeerManager) CancelBlock(p peer.ID, blk u.Key) {
	pm.cancels <- cancellation{who: p, blk: blk}
}

// TODO: use goprocess here once i trust it
func (pm *PeerManager) Run(ctx context.Context) {
	for {
		select {
		case msgp := <-pm.incoming:

			// Broadcast message to all if recipient not set
			if msgp.to == "" {
				for _, p := range pm.peers {
					p.AddMessage(msgp.msg)
				}
				continue
			}

			p, ok := pm.peers[msgp.to]
			if !ok {
				//TODO: decide, drop message? or dial?
				log.Error("outgoing message to peer with no live message queue")
				pm.startPeerHandler(msgp.to)
				p = pm.peers[msgp.to]
			}

			p.AddMessage(msgp.msg)
		case p := <-pm.connect:
			pm.startPeerHandler(p)
		case p := <-pm.disconnect:
			pm.stopPeerHandler(p)
		case c := <-pm.cancels:
			p, ok := pm.peers[c.who]
			if !ok {
				// This is weird, but whatever
				continue
			}

			p.lk.Lock()
			delete(p.blocks, c.blk)
			p.lk.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func (pm *PeerManager) startPeerHandler(p peer.ID) {
	_, ok := pm.peers[p]
	if ok {
		// TODO: log an error?
		return
	}

	mq := new(msgQueue)
	mq.done = make(chan struct{})
	mq.work = make(chan struct{})
	mq.blocks = make(map[u.Key]*blocks.Block)
	mq.p = p

	pm.peers[p] = mq
	go pm.runQueue(mq)
}

func (pm *PeerManager) stopPeerHandler(p peer.ID) {
	pq, ok := pm.peers[p]
	if !ok {
		// TODO: log error?
		return
	}

	close(pq.done)
	delete(pm.peers, p)
}
