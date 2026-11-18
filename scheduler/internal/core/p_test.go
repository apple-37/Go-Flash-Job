package core

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
)

func deliveries(start, count int) []delivery {
	items := make([]delivery, count)
	for i := range items {
		items[i].task = model.Task{ID: fmt.Sprint(start + i)}
	}
	return items
}

func TestHeapWaitsUntilScheduledTime(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 1, LocalCapacity: 4})
	defer q.close()
	tasks := deliveries(0, 3)
	for i, due := range []int64{300, 100, 200} {
		tasks[i].task.TriggerTime = due
	}
	q.reserve(3).complete(tasks)
	if _, ok, wait := q.local[0].popDue(time.Unix(99, 500000000)); ok || wait != 500*time.Millisecond {
		t.Fatalf("early dispatch or inaccurate wait: ok=%v wait=%v", ok, wait)
	}
	for _, due := range []int64{100, 200, 300} {
		d, ok, _ := q.local[0].popDue(time.Unix(due, 0))
		if !ok || d.task.TriggerTime != due {
			t.Fatalf("heap order: %v", d)
		}
	}
}

func TestStolenFutureTasksStayInHeap(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 2, LocalCapacity: 4})
	defer q.close()
	tasks := deliveries(0, 8)
	for i := range tasks {
		tasks[i].task.TriggerTime = int64(100 + i)
	}
	q.reserve(8).complete(tasks)
	for i := 0; i < 4; i++ {
		q.local[0].popDue(time.Unix(200, 0))
	}
	if !q.steal(0) {
		t.Fatal("no transfer")
	}
	if got := q.stats()["local_depths"]; !reflect.DeepEqual(got, []int{2, 2}) {
		t.Fatal(got)
	}
	if _, ok, _ := q.local[0].popDue(time.Unix(100, 0)); ok {
		t.Fatal("stolen task published early")
	}
	for _, due := range []int64{101, 103} {
		d, ok, _ := q.local[0].popDue(time.Unix(due, 0))
		if !ok || d.task.TriggerTime != due {
			t.Fatalf("receiver heap: %v", d)
		}
	}
	for _, due := range []int64{105, 107} {
		d, ok, _ := q.local[1].popDue(time.Unix(due, 0))
		if !ok || d.task.TriggerTime != due {
			t.Fatalf("victim heap: %v", d)
		}
	}
}

func TestEarlierTaskInterruptsTimer(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 1, LocalCapacity: 4})
	defer q.close()
	future := deliveries(0, 1)
	future[0].task.TriggerTime = time.Now().Add(time.Hour).Unix()
	q.reserve(1).complete(future)
	result := make(chan delivery, 1)
	go func() {
		if d, ok := q.take(0); ok {
			result <- d
		}
	}()
	select {
	case <-result:
		t.Fatal("future task published early")
	case <-time.After(20 * time.Millisecond):
	}
	q.reserve(1).complete(deliveries(1, 1))
	select {
	case d := <-result:
		if d.task.ID != "1" {
			t.Fatalf("returned %s", d.task.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("earlier head did not interrupt timer")
	}
}

func TestRoundRobinAcrossSmallBatches(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 3, LocalCapacity: 4})
	defer q.close()
	for i := 0; i < 6; i++ {
		r := q.reserve(1)
		if !reflect.DeepEqual(r.owners, []int{i % 3}) {
			t.Fatalf("batch %d assigned to %v", i, r.owners)
		}
		r.complete(deliveries(i, 1))
	}
	for id := range q.local {
		for _, want := range []string{fmt.Sprint(id), fmt.Sprint(id + 3)} {
			got, ok, _ := q.local[id].popDue(time.Now())
			if !ok || got.task.ID != want {
				t.Fatalf("P%d got %v, want %s", id, got, want)
			}
		}
	}
}

func TestRoundRobinSkipsFullQueues(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 2, LocalCapacity: 2})
	defer q.close()
	r := q.reserve(10)
	if len(r.owners) != 4 {
		t.Fatalf("reserved %d, want 4", len(r.owners))
	}
	r.complete(deliveries(0, 4))
	if n := len(q.reserve(1).owners); n != 0 {
		t.Fatalf("full queues reserved %d", n)
	}
	q.local[1].popDue(time.Now())
	r = q.reserve(10)
	if !reflect.DeepEqual(r.owners, []int{1}) {
		t.Fatalf("unexpected assignment %v", r.owners)
	}
	r.complete(deliveries(4, 1))
	if got := q.stats()["local_depths"]; !reflect.DeepEqual(got, []int{2, 2}) {
		t.Fatal(got)
	}
}

func TestRoundRobinAdvancesByActualRead(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 4, LocalCapacity: 4})
	defer q.close()
	for i := 0; i < 8; i++ {
		empty := q.reserve(8)
		empty.complete(nil)
		r := q.reserve(8)
		if r.owners[0] != i%4 {
			t.Fatalf("short read %d starts at P%d", i, r.owners[0])
		}
		r.complete(deliveries(i, 1))
	}
	if got := q.stats()["local_depths"]; !reflect.DeepEqual(got, []int{2, 2, 2, 2}) {
		t.Fatal(got)
	}
}

func TestReservationProtectsSpaceDuringStealing(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 2, LocalCapacity: 4})
	defer q.close()
	q.reserve(8).complete(deliveries(0, 8))
	for i := 0; i < 4; i++ {
		q.local[0].popDue(time.Now())
	}
	inFlight := q.reserve(4)
	if q.steal(0) {
		t.Fatal("stealing consumed reserved heap capacity")
	}
	inFlight.complete(deliveries(8, 4))
	if got := q.stats()["local_depths"]; !reflect.DeepEqual(got, []int{4, 4}) {
		t.Fatal(got)
	}
}
func TestShortReadReleasesUnusedReservation(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 2, LocalCapacity: 4})
	defer q.close()
	r := q.reserve(8)
	r.complete(deliveries(0, 1))
	next := q.reserve(8)
	if len(next.owners) != 7 {
		t.Fatalf("unused slots leaked: %d", len(next.owners))
	}
	next.complete(nil) // Redis error or empty read
	if got := q.stats()["reserved_slots"]; !reflect.DeepEqual(got, []int{0, 0}) {
		t.Fatal(got)
	}
}

func TestWakeOnDispatchAndClose(t *testing.T) {
	q := newQueues(config.SchedulerConfig{Workers: 1, LocalCapacity: 2})
	defer q.close()
	results := make(chan bool, 2)
	go func() {
		_, ok := q.take(0)
		results <- ok
		_, ok = q.take(0)
		results <- ok
	}()
	r := q.reserve(1)
	r.complete(deliveries(0, 1))
	select {
	case ok := <-results:
		if !ok {
			t.Fatal("dispatch returned closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missed dispatch wakeup")
	}
	q.close()
	select {
	case ok := <-results:
		if ok {
			t.Fatal("closed returned work")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missed close wakeup")
	}
}

func TestConcurrentDispatchAndStealing(t *testing.T) {
	const total = 600
	q := newQueues(config.SchedulerConfig{Workers: 4, LocalCapacity: 7})
	defer q.close()
	results := make(chan string, total)
	var consumers sync.WaitGroup
	for id := range q.local {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				d, ok := q.take(id)
				if !ok {
					return
				}
				results <- d.task.ID
			}
		}()
	}
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		for offset := 0; offset < total; {
			if q.closed.Load() {
				return
			}
			r := q.reserve(min(23, total-offset))
			if len(r.owners) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			time.Sleep(time.Millisecond) // Let thieves run during an outstanding read.
			r.complete(deliveries(offset, len(r.owners)))
			offset += len(r.owners)
		}
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	defer func() { q.close(); <-produced; consumers.Wait() }()
	seen := make(map[string]bool, total)
	for len(seen) < total {
		select {
		case id := <-results:
			if seen[id] {
				t.Fatalf("duplicate %s", id)
			}
			seen[id] = true
			snapshot := q.stats()
			depths := snapshot["local_depths"].([]int)
			reserved := snapshot["reserved_slots"].([]int)
			for i, depth := range depths {
				if depth+reserved[i] > 7 {
					t.Fatalf("P%d capacity exceeded: %d queued, %d reserved", i, depth, reserved[i])
				}
			}
		case <-deadline.C:
			t.Fatalf("received %d/%d", len(seen), total)
		}
	}
	q.close()
	<-produced
	consumers.Wait()
	if len(results) != 0 {
		t.Fatal("unexpected extra deliveries")
	}
	if q.dispatched.Load() != total {
		t.Fatal("incorrect dispatch accounting")
	}
}
