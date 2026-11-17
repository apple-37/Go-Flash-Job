package core

import (
	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
	"sync"
)

type delivery struct {
	task  model.Task
	claim string
}

type localQueue struct {
	mu    sync.Mutex
	items []delivery
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

func (q *localQueue) stealHalf() []delivery {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Keep one entry with the owner when there is no backlog.
	n := len(q.items) / 2
	if n == 0 {
		return nil
	}
	split := len(q.items) - n
	batch := q.items[split:]
	q.items = q.items[:split:split]
	return batch
}

type readyQueues struct {
	mu                           sync.Mutex
	changed                      *sync.Cond
	global                       []delivery
	local                        []localQueue
	cfg                          config.SchedulerConfig
	closed                       bool
	refills, steals, stolenTasks int64
}

func newQueues(cfg config.SchedulerConfig) *readyQueues {
	q := &readyQueues{cfg: cfg, local: make([]localQueue, cfg.Workers)}
	q.changed = sync.NewCond(&q.mu)
	return q
}

func (q *readyQueues) space() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.cfg.Capacity - len(q.global)
}

// There is exactly one producer; it reserves at most space() per Redis read.
func (q *readyQueues) push(batch []delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.global = append(q.global, batch...)
	q.changed.Broadcast()
}

func (q *readyQueues) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.changed.Broadcast()
}

func (q *readyQueues) take(id int) (delivery, bool) {
	own := &q.local[id]
	if q.cfg.Strategy != "shared" {
		if d, ok := own.pop(); ok {
			return d, true
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed {
		var batch []delivery
		if len(q.global) > 0 {
			n := 1
			if q.cfg.Strategy != "shared" {
				n = min(q.cfg.Batch, len(q.global))
			}
			q.refills++
			batch = q.global[:n:n]
			q.global = q.global[n:]
		} else if q.cfg.Strategy == "steal" {
			for offset := 1; offset < len(q.local); offset++ {
				batch = q.local[(id+offset)%len(q.local)].stealHalf()
				if len(batch) > 0 {
					q.steals++
					q.stolenTasks += int64(len(batch))
					break
				}
			}
		}
		if len(batch) > 0 {
			d := batch[0]
			batch[0] = delivery{}
			if len(batch) > 1 {
				own.mu.Lock()
				own.items = batch[1:]
				own.mu.Unlock()
				if q.cfg.Strategy == "steal" {
					q.changed.Broadcast()
				}
			}
			return d, true
		}
		q.changed.Wait()
	}
	return delivery{}, false
}

func (q *readyQueues) stats() map[string]any {
	q.mu.Lock()
	defer q.mu.Unlock()
	return map[string]any{"strategy": q.cfg.Strategy, "global_depth": len(q.global), "global_claims": q.refills, "steals": q.steals, "stolen_tasks": q.stolenTasks}
}
