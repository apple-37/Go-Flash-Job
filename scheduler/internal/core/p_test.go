package core

import (
	"fmt"
	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
	"sync"
	"sync/atomic"
	"testing"
)

func TestQueueStrategies(t *testing.T) {
	for _, strategy := range []string{"shared", "local", "steal"} {
		t.Run(strategy, func(t *testing.T) {
			q := newQueues(config.SchedulerConfig{Workers: 2, Batch: 4, Capacity: 8, Strategy: strategy})
			var input []delivery
			for i := 0; i < 8; i++ {
				input = append(input, delivery{task: model.Task{ID: fmt.Sprint(i)}})
			}
			q.push(input)
			seen := map[string]bool{}
			take := func(worker int) {
				d, ok := q.take(worker)
				if !ok || seen[d.task.ID] {
					t.Fatalf("lost or repeated delivery %v", d)
				}
				seen[d.task.ID] = true
			}
			take(0)
			if strategy == "shared" {
				if q.space() != 1 {
					t.Fatal("shared worker should claim one")
				}
				for i := 1; i < 8; i++ {
					take(i % 2)
				}
			} else {
				if q.space() != 4 {
					t.Fatal("local worker should claim a batch")
				}
				for i := 0; i < 4; i++ {
					take(1)
				}
				if strategy == "steal" {
					take(1)
				} // Only worker 0 has backlog now.
				for len(seen) < 8 {
					take(0)
				}
			}
			q.close()
			if _, ok := q.take(0); ok {
				t.Fatal("closed queue returned work")
			}
		})
	}
}

func BenchmarkReadyQueues(b *testing.B) {
	for _, strategy := range []string{"shared", "local", "steal"} {
		b.Run(strategy, func(b *testing.B) {
			const workers = 4
			q := newQueues(config.SchedulerConfig{Workers: workers, Batch: 16, Capacity: b.N, Strategy: strategy})
			// Load before timing; consumers drain the burst without broker or Redis I/O.
			input := make([]delivery, b.N)
			q.push(input)
			var done sync.WaitGroup
			done.Add(workers)
			var consumed atomic.Int64
			b.ResetTimer()
			for id := 0; id < workers; id++ {
				go func() {
					defer done.Done()
					for {
						_, ok := q.take(id)
						if !ok {
							return
						}
						if consumed.Add(1) == int64(b.N) {
							q.close()
							return
						}
					}
				}()
			}
			done.Wait()
		})
	}
}
