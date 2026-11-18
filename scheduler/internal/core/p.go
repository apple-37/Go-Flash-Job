package core

import (
	"sync"
	"sync/atomic"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
)

type delivery struct {
	task  model.Task
	claim string
}

type localQueue struct {
	mu       sync.Mutex
	items    []delivery
	reserved int // Slots promised to the one Fetcher, including its in-flight Redis read.
	wake     chan struct{}
}

func (q *localQueue) pop() (delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return delivery{}, false
	}
	d := q.items[0]
	q.items[0] = delivery{}
	q.items = q.items[1:]
	return d, true
}

// Only reservation planning and stealing share dispatchMu. The local pop path
// locks its own queue. No queue locks are held during Redis or RabbitMQ I/O.
type readyQueues struct {
	dispatchMu  sync.Mutex
	next        int
	local       []localQueue
	capacity    int
	closed      atomic.Bool
	closeOnce   sync.Once
	stop        chan struct{}
	dispatched  atomic.Int64
	batches     atomic.Int64
	steals      atomic.Int64
	stolenTasks atomic.Int64
}

func newQueues(cfg config.SchedulerConfig) *readyQueues {
	q := &readyQueues{local: make([]localQueue, cfg.Workers), capacity: cfg.LocalCapacity, stop: make(chan struct{})}
	for i := range q.local {
		q.local[i].wake = make(chan struct{}, 1)
	}
	return q
}

// A reservation is completed exactly once by the Fetcher. complete(nil) releases
// all slots after an empty read, a Redis error, or leadership cancellation.
type reservation struct {
	queues *readyQueues
	owners []int
	counts []int
}

func (q *readyQueues) reserve(limit int) *reservation {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()
	r := &reservation{queues: q, counts: make([]int, len(q.local))}
	if q.closed.Load() {
		return r
	}
	free := make([]int, len(q.local))
	total := 0
	for i := range q.local {
		p := &q.local[i]
		p.mu.Lock()
		free[i] = q.capacity - len(p.items) - p.reserved
		p.mu.Unlock()
		total += free[i]
	}
	// Consumers can only increase free space while dispatchMu excludes transfers.
	// Group assignments outside the per-P critical sections.
	next := q.next
	for n := 0; n < min(limit, total); n++ {
		for free[next] == 0 {
			next = (next + 1) % len(q.local)
		}
		owner := next
		r.owners = append(r.owners, owner)
		r.counts[owner]++
		free[owner]--
		next = (next + 1) % len(q.local)
	}
	for i, count := range r.counts {
		if count == 0 {
			continue
		}
		p := &q.local[i]
		p.mu.Lock()
		p.reserved += count
		p.mu.Unlock()
	}
	return r
}

func (r *reservation) complete(tasks []delivery) {
	q := r.queues
	if len(tasks) > len(r.owners) {
		panic("delivery batch exceeds reserved capacity")
	}
	groups := make([][]delivery, len(q.local))
	for i, d := range tasks {
		owner := r.owners[i]
		groups[owner] = append(groups[owner], d)
	}
	inserted := 0
	for i, count := range r.counts {
		if count == 0 {
			continue
		}
		p := &q.local[i]
		p.mu.Lock()
		p.reserved -= count
		if !q.closed.Load() {
			p.items = append(p.items, groups[i]...)
			inserted += len(groups[i])
		}
		p.mu.Unlock()
	}
	if inserted > 0 {
		// Advance by the actual Redis result, not the requested batch size.
		// Otherwise short reads can repeatedly assign their first task to P0.
		q.dispatchMu.Lock()
		q.next = (r.owners[len(tasks)-1] + 1) % len(q.local)
		q.dispatchMu.Unlock()
		q.dispatched.Add(int64(inserted))
		q.batches.Add(1)
		// Idle workers can also help queues whose owner is waiting for confirmation.
		for i := range q.local {
			select {
			case q.local[i].wake <- struct{}{}:
			default:
			}
		}
	}
}

func (q *readyQueues) steal(id int) (delivery, bool) {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()
	own := &q.local[id]
	own.mu.Lock()
	defer own.mu.Unlock()
	if q.closed.Load() {
		return delivery{}, false
	}
	// A producer may have filled this queue after take's first local check.
	if len(own.items) > 0 {
		d := own.items[0]
		own.items[0] = delivery{}
		own.items = own.items[1:]
		return d, true
	}
	for offset := 1; offset < len(q.local); offset++ {
		victim := &q.local[(id+offset)%len(q.local)]
		victim.mu.Lock()
		// One stolen task is returned directly; only the remainder occupies space.
		n := min((len(victim.items)+1)/2, q.capacity-own.reserved+1)
		if n == 0 {
			victim.mu.Unlock()
			continue
		}
		split := len(victim.items) - n
		d := victim.items[split]
		own.items = append(own.items, victim.items[split+1:]...)
		clear(victim.items[split:])
		victim.items = victim.items[:split]
		victim.mu.Unlock()
		q.steals.Add(1)
		q.stolenTasks.Add(int64(n))
		return d, true
	}
	return delivery{}, false
}

func (q *readyQueues) take(id int) (delivery, bool) {
	for !q.closed.Load() {
		if d, ok := q.local[id].pop(); ok {
			return d, true
		}
		if d, ok := q.steal(id); ok {
			return d, true
		}
		select {
		case <-q.stop:
			return delivery{}, false
		case <-q.local[id].wake:
		}
	}
	return delivery{}, false
}

func (q *readyQueues) close() {
	q.closeOnce.Do(func() { q.closed.Store(true); close(q.stop) })
}

func (q *readyQueues) stats() map[string]any {
	depths := make([]int, len(q.local))
	reserved := make([]int, len(q.local))
	for i := range q.local {
		p := &q.local[i]
		p.mu.Lock()
		depths[i] = len(p.items)
		reserved[i] = p.reserved
		p.mu.Unlock()
	}
	return map[string]any{
		"strategy": "round_robin_steal", "local_capacity": q.capacity,
		"local_depths": depths, "reserved_slots": reserved,
		"dispatch_batches": q.batches.Load(), "dispatched_tasks": q.dispatched.Load(),
		"steals": q.steals.Load(), "stolen_tasks": q.stolenTasks.Load(),
	}
}
