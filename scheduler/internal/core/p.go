package core

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
)

type delivery struct {
	task  model.Task
	claim string
}

// taskHeap orders prefetched tasks by their scheduled time, then submission time.
type taskHeap []delivery

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].task.TriggerTime != h[j].task.TriggerTime {
		return h[i].task.TriggerTime < h[j].task.TriggerTime
	}
	if h[i].task.EnqueueAt != h[j].task.EnqueueAt {
		return h[i].task.EnqueueAt < h[j].task.EnqueueAt
	}
	return h[i].task.ID < h[j].task.ID
}
func (h taskHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(value any) { *h = append(*h, value.(delivery)) }
func (h *taskHeap) Pop() any {
	last := len(*h) - 1
	d := (*h)[last]
	(*h)[last] = delivery{}
	*h = (*h)[:last]
	return d
}

type localQueue struct {
	mu       sync.Mutex
	items    taskHeap
	reserved int // Slots promised to the one Fetcher, including its in-flight Redis read.
	wake     chan struct{}
}

// A negative wait means empty; a positive wait is time until the heap head is due.
// Inspecting the head and popping it share one lock, including during stealing.
func (q *localQueue) popDue(now time.Time) (delivery, bool, time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return delivery{}, false, -1
	}
	wait := time.Unix(q.items[0].task.TriggerTime, 0).Sub(now)
	if wait > 0 {
		return delivery{}, false, wait
	}
	return heap.Pop(&q.items).(delivery), true, 0
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
			heap.Init(&p.items)
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

// Transfer earliest tasks using heap.Pop. The receiver still checks their due
// times in take; a successful steal never grants permission to publish early.
func (q *readyQueues) steal(id int) bool {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()
	own := &q.local[id]
	own.mu.Lock()
	defer own.mu.Unlock()
	if q.closed.Load() {
		return false
	}
	// A producer may have filled this queue after take's first local check.
	if len(own.items) > 0 {
		return true
	}
	for offset := 1; offset < len(q.local); offset++ {
		victim := &q.local[(id+offset)%len(q.local)]
		victim.mu.Lock()
		// Leave one task with the owner so a lone future timer cannot bounce
		// between otherwise empty workers. All stolen tasks occupy heap space.
		n := min(len(victim.items)/2, q.capacity-own.reserved)
		if n == 0 {
			victim.mu.Unlock()
			continue
		}
		for i := 0; i < n; i++ {
			own.items = append(own.items, heap.Pop(&victim.items).(delivery))
		}
		heap.Init(&own.items)
		victim.mu.Unlock()
		select {
		case victim.wake <- struct{}{}:
		default:
		}
		q.steals.Add(1)
		q.stolenTasks.Add(int64(n))
		return true
	}
	return false
}

func (q *readyQueues) take(id int) (delivery, bool) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for !q.closed.Load() {
		d, ok, wait := q.local[id].popDue(time.Now())
		if ok {
			return d, true
		}
		if wait < 0 && q.steal(id) {
			continue
		}
		var deadline <-chan time.Time
		if wait > 0 {
			timer.Reset(wait)
			deadline = timer.C
		}
		select {
		case <-q.stop:
			return delivery{}, false
		case <-q.local[id].wake:
		case <-deadline:
		}
		timer.Stop()
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
		"strategy": "round_robin_heap_steal", "preload_seconds": int64(preloadWindow / time.Second), "local_capacity": q.capacity,
		"local_depths": depths, "reserved_slots": reserved,
		"dispatch_batches": q.batches.Load(), "dispatched_tasks": q.dispatched.Load(),
		"steals": q.steals.Load(), "stolen_tasks": q.stolenTasks.Load(),
	}
}
