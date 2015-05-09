package bitswap

import (
	"time"

	ds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	ds_sync "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/sync"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	blockstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	tn "github.com/ipfs/go-ipfs/exchange/bitswap/testnet"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	p2ptestutil "github.com/ipfs/go-ipfs/p2p/test/util"
	delay "github.com/ipfs/go-ipfs/thirdparty/delay"
	datastore2 "github.com/ipfs/go-ipfs/util/datastore2"
	testutil "github.com/ipfs/go-ipfs/util/testutil"
)

// WARNING: this uses RandTestBogusIdentity DO NOT USE for NON TESTS!
func NewTestSessionGenerator(
	net tn.Network) SessionGenerator {
	ctx, cancel := context.WithCancel(context.TODO())
	return SessionGenerator{
		net:    net,
		seq:    0,
		ctx:    ctx, // TODO take ctx as param to Next, Instances
		cancel: cancel,
	}
}

// TODO move this SessionGenerator to the core package and export it as the core generator
type SessionGenerator struct {
	seq    int
	net    tn.Network
	ctx    context.Context
	cancel context.CancelFunc
}

func (g *SessionGenerator) Close() error {
	g.cancel()
	return nil // for Closer interface
}

func (g *SessionGenerator) Next() Instance {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	if err != nil {
		panic("FIXME") // TODO change signature
	}
	return session(g.ctx, g.net, p)
}

func (g *SessionGenerator) Instances(n int) []Instance {
	instances := make([]Instance, 0)
	for j := 0; j < n; j++ {
		inst := g.Next()
		instances = append(instances, inst)
	}
	for i, inst := range instances {
		for j, oinst := range instances {
			if i == j {
				continue
			}
			inst.Exchange.PeerConnected(oinst.Peer)
		}
	}
	return instances
}

type Instance struct {
	Peer       peer.ID
	Exchange   *Bitswap
	blockstore blockstore.Blockstore

	blockstoreDelay delay.D
}

func (i *Instance) Blockstore() blockstore.Blockstore {
	return i.blockstore
}

func (i *Instance) SetBlockstoreLatency(t time.Duration) time.Duration {
	return i.blockstoreDelay.Set(t)
}

// session creates a test bitswap session.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// sessions. To safeguard, use the SessionGenerator to generate sessions. It's
// just a much better idea.
func session(ctx context.Context, net tn.Network, p testutil.Identity) Instance {
	bsdelay := delay.Fixed(0)
	const kWriteCacheElems = 100

	adapter := net.Adapter(p)
	dstore := ds_sync.MutexWrap(datastore2.WithDelay(ds.NewMapDatastore(), bsdelay))

	bstore, err := blockstore.WriteCached(blockstore.NewBlockstore(ds_sync.MutexWrap(dstore)), kWriteCacheElems)
	if err != nil {
		panic(err.Error()) // FIXME perhaps change signature and return error.
	}

	const alwaysSendToPeer = true

	bs := New(ctx, p.ID(), adapter, bstore, alwaysSendToPeer).(*Bitswap)

	return Instance{
		Peer:            p.ID(),
		Exchange:        bs,
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
	}
}
