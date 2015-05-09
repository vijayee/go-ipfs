package bitswap

import (
	"errors"

	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	bsmsg "github.com/ipfs/go-ipfs/exchange/bitswap/message"
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	routing "github.com/ipfs/go-ipfs/routing"
	mockrouting "github.com/ipfs/go-ipfs/routing/mock"
	delay "github.com/ipfs/go-ipfs/thirdparty/delay"
	util "github.com/ipfs/go-ipfs/util"
	testutil "github.com/ipfs/go-ipfs/util/testutil"
)

func VirtualNetwork(rs mockrouting.Server, d delay.D) Network {
	return &network{
		clients:       make(map[peer.ID]bsnet.Receiver),
		delay:         d,
		routingserver: rs,
	}
}

type network struct {
	clients       map[peer.ID]bsnet.Receiver
	routingserver mockrouting.Server
	delay         delay.D
}

func (n *network) Adapter(p testutil.Identity) bsnet.BitSwapNetwork {
	client := &networkClient{
		local:   p.ID(),
		network: n,
		routing: n.routingserver.Client(p),
	}
	n.clients[p.ID()] = client
	return client
}

func (n *network) HasPeer(p peer.ID) bool {
	_, found := n.clients[p]
	return found
}

// TODO should this be completely asynchronous?
// TODO what does the network layer do with errors received from services?
func (n *network) SendMessage(
	ctx context.Context,
	from peer.ID,
	to peer.ID,
	message bsmsg.BitSwapMessage) error {

	receiver, ok := n.clients[to]
	if !ok {
		return errors.New("Cannot locate peer on network")
	}

	// nb: terminate the context since the context wouldn't actually be passed
	// over the network in a real scenario

	go n.deliver(receiver, from, message)

	return nil
}

func (n *network) deliver(
	r bsnet.Receiver, from peer.ID, message bsmsg.BitSwapMessage) error {
	if message == nil || from == "" {
		return errors.New("Invalid input")
	}

	n.delay.Wait()

	return r.ReceiveMessage(context.TODO(), from, message)
}

type networkClient struct {
	local peer.ID
	bsnet.Receiver
	network *network
	routing routing.IpfsRouting
}

func (nc *networkClient) SendMessage(
	ctx context.Context,
	to peer.ID,
	message bsmsg.BitSwapMessage) error {
	return nc.network.SendMessage(ctx, nc.local, to, message)
}

// FindProvidersAsync returns a channel of providers for the given key
func (nc *networkClient) FindProvidersAsync(ctx context.Context, k util.Key, max int) <-chan peer.ID {

	// NB: this function duplicates the PeerInfo -> ID transformation in the
	// bitswap network adapter. Not to worry. This network client will be
	// deprecated once the ipfsnet.Mock is added. The code below is only
	// temporary.

	out := make(chan peer.ID)
	go func() {
		defer close(out)
		providers := nc.routing.FindProvidersAsync(ctx, k, max)
		for info := range providers {
			select {
			case <-ctx.Done():
			case out <- info.ID:
			}
		}
	}()
	return out
}

// Provide provides the key to the network
func (nc *networkClient) Provide(ctx context.Context, k util.Key) error {
	return nc.routing.Provide(ctx, k)
}

func (nc *networkClient) SetDelegate(r bsnet.Receiver) {
	nc.Receiver = r
}

func (nc *networkClient) ConnectTo(_ context.Context, p peer.ID) error {
	if !nc.network.HasPeer(p) {
		return errors.New("no such peer in network")
	}
	nc.network.clients[p].PeerConnected(nc.local)
	nc.Receiver.PeerConnected(p)
	return nil
}
