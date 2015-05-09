package decision

import (
	"sync"
	"time"

	wantlist "github.com/ipfs/go-ipfs/exchange/bitswap/wantlist"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	pq "github.com/ipfs/go-ipfs/thirdparty/pq"
	u "github.com/ipfs/go-ipfs/util"
)

type peerRequestQueue interface {
	// Pop returns the next peerRequestTask. Returns nil if the peerRequestQueue is empty.
	Pop() *peerRequestTask
	Push(entry wantlist.Entry, to peer.ID)
	Remove(k u.Key, p peer.ID)
	// NB: cannot expose simply expose taskQueue.Len because trashed elements
	// may exist. These trashed elements should not contribute to the count.
}

func newPRQ() peerRequestQueue {
	return &prq{
		taskMap:  make(map[string]*peerRequestTask),
		partners: make(map[peer.ID]*activePartner),
		pQueue:   pq.New(partnerCompare),
	}
}

// verify interface implementation
var _ peerRequestQueue = &prq{}

// TODO: at some point, the strategy needs to plug in here
// to help decide how to sort tasks (on add) and how to select
// tasks (on getnext). For now, we are assuming a dumb/nice strategy.
type prq struct {
	lock     sync.Mutex
	pQueue   pq.PQ
	taskMap  map[string]*peerRequestTask
	partners map[peer.ID]*activePartner
}

// Push currently adds a new peerRequestTask to the end of the list
func (tl *prq) Push(entry wantlist.Entry, to peer.ID) {
	tl.lock.Lock()
	defer tl.lock.Unlock()
	partner, ok := tl.partners[to]
	if !ok {
		partner = newActivePartner()
		tl.pQueue.Push(partner)
		tl.partners[to] = partner
	}

	if task, ok := tl.taskMap[taskKey(to, entry.Key)]; ok {
		task.Entry.Priority = entry.Priority
		partner.taskQueue.Update(task.index)
		return
	}

	partner.activelk.Lock()
	defer partner.activelk.Unlock()
	_, ok = partner.activeBlocks[entry.Key]
	if ok {
		return
	}

	task := &peerRequestTask{
		Entry:   entry,
		Target:  to,
		created: time.Now(),
		Done: func() {
			partner.TaskDone(entry.Key)
			tl.lock.Lock()
			tl.pQueue.Update(partner.Index())
			tl.lock.Unlock()
		},
	}

	partner.taskQueue.Push(task)
	tl.taskMap[task.Key()] = task
	partner.requests++
	tl.pQueue.Update(partner.Index())
}

// Pop 'pops' the next task to be performed. Returns nil if no task exists.
func (tl *prq) Pop() *peerRequestTask {
	tl.lock.Lock()
	defer tl.lock.Unlock()
	if tl.pQueue.Len() == 0 {
		return nil
	}
	partner := tl.pQueue.Pop().(*activePartner)

	var out *peerRequestTask
	for partner.taskQueue.Len() > 0 {
		out = partner.taskQueue.Pop().(*peerRequestTask)
		delete(tl.taskMap, out.Key())
		if out.trash {
			out = nil
			continue // discarding tasks that have been removed
		}

		partner.StartTask(out.Entry.Key)
		partner.requests--
		break // and return |out|
	}

	tl.pQueue.Push(partner)
	return out
}

// Remove removes a task from the queue
func (tl *prq) Remove(k u.Key, p peer.ID) {
	tl.lock.Lock()
	t, ok := tl.taskMap[taskKey(p, k)]
	if ok {
		// remove the task "lazily"
		// simply mark it as trash, so it'll be dropped when popped off the
		// queue.
		t.trash = true

		// having canceled a block, we now account for that in the given partner
		tl.partners[p].requests--
	}
	tl.lock.Unlock()
}

type peerRequestTask struct {
	Entry  wantlist.Entry
	Target peer.ID

	// A callback to signal that this task has been completed
	Done func()

	// trash in a book-keeping field
	trash bool
	// created marks the time that the task was added to the queue
	created time.Time
	index   int // book-keeping field used by the pq container
}

// Key uniquely identifies a task.
func (t *peerRequestTask) Key() string {
	return taskKey(t.Target, t.Entry.Key)
}

// Index implements pq.Elem
func (t *peerRequestTask) Index() int {
	return t.index
}

// SetIndex implements pq.Elem
func (t *peerRequestTask) SetIndex(i int) {
	t.index = i
}

// taskKey returns a key that uniquely identifies a task.
func taskKey(p peer.ID, k u.Key) string {
	return string(p) + string(k)
}

// FIFO is a basic task comparator that returns tasks in the order created.
var FIFO = func(a, b *peerRequestTask) bool {
	return a.created.Before(b.created)
}

// V1 respects the target peer's wantlist priority. For tasks involving
// different peers, the oldest task is prioritized.
var V1 = func(a, b *peerRequestTask) bool {
	if a.Target == b.Target {
		return a.Entry.Priority > b.Entry.Priority
	}
	return FIFO(a, b)
}

func wrapCmp(f func(a, b *peerRequestTask) bool) func(a, b pq.Elem) bool {
	return func(a, b pq.Elem) bool {
		return f(a.(*peerRequestTask), b.(*peerRequestTask))
	}
}

type activePartner struct {

	// Active is the number of blocks this peer is currently being sent
	// active must be locked around as it will be updated externally
	activelk sync.Mutex
	active   int

	activeBlocks map[u.Key]struct{}

	// requests is the number of blocks this peer is currently requesting
	// request need not be locked around as it will only be modified under
	// the peerRequestQueue's locks
	requests int

	// for the PQ interface
	index int

	// priority queue of tasks belonging to this peer
	taskQueue pq.PQ
}

func newActivePartner() *activePartner {
	return &activePartner{
		taskQueue:    pq.New(wrapCmp(V1)),
		activeBlocks: make(map[u.Key]struct{}),
	}
}

// partnerCompare implements pq.ElemComparator
func partnerCompare(a, b pq.Elem) bool {
	pa := a.(*activePartner)
	pb := b.(*activePartner)

	// having no blocks in their wantlist means lowest priority
	// having both of these checks ensures stability of the sort
	if pa.requests == 0 {
		return false
	}
	if pb.requests == 0 {
		return true
	}
	return pa.active < pb.active
}

// StartTask signals that a task was started for this partner
func (p *activePartner) StartTask(k u.Key) {
	p.activelk.Lock()
	p.activeBlocks[k] = struct{}{}
	p.active++
	p.activelk.Unlock()
}

// TaskDone signals that a task was completed for this partner
func (p *activePartner) TaskDone(k u.Key) {
	p.activelk.Lock()
	delete(p.activeBlocks, k)
	p.active--
	if p.active < 0 {
		panic("more tasks finished than started!")
	}
	p.activelk.Unlock()
}

// Index implements pq.Elem
func (p *activePartner) Index() int {
	return p.index
}

// SetIndex implements pq.Elem
func (p *activePartner) SetIndex(i int) {
	p.index = i
}
