package core

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	fetchInterval         = 5 * time.Second
	preloadWindow         = 10 * time.Second
	publishTimeout        = 3 * time.Second
	maxRetryBackoff       = 30 * time.Second
	recoverPendingBatch   = 1000
	recoverPendingTimeout = 2 * time.Minute
)

// Task 代表一个待执行的任务。
type Task struct {
	JobID       string
	TriggerTime int64 // 触发时间戳（秒）
	RetryCount  int
}

// TaskCommand 是推送到 MQ 的任务消息体。
type TaskCommand struct {
	JobID       string `json:"job_id"`
	TriggerTime int64  `json:"trigger_time"`
}

// TaskHeap 实现 container/heap 接口。
type TaskHeap []*Task

func (h TaskHeap) Len() int           { return len(h) }
func (h TaskHeap) Less(i, j int) bool { return h[i].TriggerTime < h[j].TriggerTime }
func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TaskHeap) Push(x interface{}) {
	*h = append(*h, x.(*Task))
}
func (h *TaskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type Dispatcher struct {
	localQueue TaskHeap
	mu         sync.Mutex
	wakeCh     chan struct{}
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		localQueue: make(TaskHeap, 0),
		wakeCh:     make(chan struct{}, 1),
	}
}

// Start 启动调度器引擎。
func (d *Dispatcher) Start(ctx context.Context) {
	heap.Init(&d.localQueue)
	d.recoverPendingTasks(ctx)

	go d.fetcherLoop(ctx)
	go d.executorLoop(ctx)

	fmt.Println("🚀 GMP 调度引擎已启动...")
}

// recoverPendingTasks 把异常退出遗留在 pending 的任务重新归还到 global，避免永久丢失。
func (d *Dispatcher) recoverPendingTasks(ctx context.Context) {
	luaScript := redis.NewScript(`
		local members = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
		for i=1,#members,2 do
			redis.call('ZADD', KEYS[2], members[i+1], members[i])
			redis.call('ZREM', KEYS[1], members[i])
		end
		return #members / 2
	`)

	res, err := luaScript.Run(ctx, database.RDB, []string{consts.JobPendingZSetKey, consts.JobZSetKey}, recoverPendingBatch-1).Int64()
	if err != nil && err != redis.Nil {
		log.Printf("⚠️ pending 任务回收失败: %v", err)
		return
	}
	if res > 0 {
		log.Printf("♻️ 已回收 %d 条 pending 任务到 global queue", res)
	}
}

func (d *Dispatcher) fetcherLoop(ctx context.Context) {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	// 原子地把任务从 global 转到 pending，避免“先删后发 MQ”导致静默丢失。
	luaScript := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
		for i=1,#items,2 do
			redis.call('ZADD', KEYS[2], items[i+1], items[i])
			redis.call('ZREM', KEYS[1], items[i])
		end
		return items
	`)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		maxScore := time.Now().Add(preloadWindow).Unix()
		result, err := luaScript.Run(ctx, database.RDB, []string{consts.JobZSetKey, consts.JobPendingZSetKey}, maxScore).StringSlice()
		if err != nil && err != redis.Nil {
			log.Printf("⚠️ Redis 拉取任务失败: %v", err)
			continue
		}

		if len(result) == 0 {
			d.recoverStalePending(ctx)
			continue
		}
		if len(result)%2 != 0 {
			log.Printf("⚠️ Redis 返回了非法任务载荷，len=%d", len(result))
			continue
		}

		count := len(result) / 2
		fmt.Printf("📦 [Fetcher] 抢到 %d 个任务，放入 Local Queue\n", count)

		d.mu.Lock()
		for i := 0; i < len(result); i += 2 {
			jobID := result[i]
			triggerTime, parseErr := strconv.ParseInt(result[i+1], 10, 64)
			if parseErr != nil {
				log.Printf("⚠️ triggerTime 解析失败, job=%s score=%s err=%v", jobID, result[i+1], parseErr)
				continue
			}
			heap.Push(&d.localQueue, &Task{
				JobID:       jobID,
				TriggerTime: triggerTime,
			})
		}
		d.mu.Unlock()
		d.notifyWake()
	}
}

// recoverStalePending 将长时间未确认的 pending 任务归还到 global。
func (d *Dispatcher) recoverStalePending(ctx context.Context) {
	deadline := time.Now().Add(-recoverPendingTimeout).Unix()
	luaScript := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
		for i=1,#items,2 do
			redis.call('ZADD', KEYS[2], items[i+1], items[i])
			redis.call('ZREM', KEYS[1], items[i])
		end
		return #items / 2
	`)

	count, err := luaScript.Run(ctx, database.RDB, []string{consts.JobPendingZSetKey, consts.JobZSetKey}, deadline).Int64()
	if err != nil && err != redis.Nil {
		log.Printf("⚠️ stale pending 回收失败: %v", err)
		return
	}
	if count > 0 {
		log.Printf("♻️ 回收 %d 条 stale pending 任务", count)
	}
}

func (d *Dispatcher) executorLoop(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		d.mu.Lock()
		if d.localQueue.Len() == 0 {
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-d.wakeCh:
			}
			continue
		}

		topTask := d.localQueue[0]
		now := time.Now().Unix()

		var waitDuration time.Duration
		if topTask.TriggerTime <= now {
			waitDuration = 0
		} else {
			waitDuration = time.Duration(topTask.TriggerTime-now) * time.Second
		}
		d.mu.Unlock()

		if waitDuration == 0 {
			d.mu.Lock()
			taskToRun := heap.Pop(&d.localQueue).(*Task)
			d.mu.Unlock()

			if err := d.publishToMQ(ctx, taskToRun.JobID, taskToRun.TriggerTime); err != nil {
				next := time.Now().Add(backoffDuration(taskToRun.RetryCount + 1)).Unix()
				taskToRun.RetryCount++
				taskToRun.TriggerTime = next

				d.mu.Lock()
				heap.Push(&d.localQueue, taskToRun)
				d.mu.Unlock()
				d.notifyWake()
				continue
			}

			if _, err := database.RDB.ZRem(ctx, consts.JobPendingZSetKey, taskToRun.JobID).Result(); err != nil {
				log.Printf("⚠️ 任务 %s 已投递 MQ 但 pending 移除失败: %v", taskToRun.JobID, err)
			}
			continue
		}

		timer.Reset(waitDuration)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		case <-d.wakeCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// publishToMQ 将到期任务发送给执行器。
func (d *Dispatcher) publishToMQ(ctx context.Context, jobID string, triggerTime int64) error {
	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	body, err := json.Marshal(TaskCommand{JobID: jobID, TriggerTime: triggerTime})
	if err != nil {
		return fmt.Errorf("marshal task command failed: %w", err)
	}

	err = mq.RabbitChannel.PublishWithContext(
		publishCtx,
		"",
		consts.TaskQueue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("❌ 任务 %s 推送 MQ 失败: %v", jobID, err)
		return err
	}

	fmt.Printf("⚡ [Dispatcher] 任务 %s 已到期，成功推入 RabbitMQ\n", jobID)
	return nil
}

func (d *Dispatcher) notifyWake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

func backoffDuration(retry int) time.Duration {
	if retry <= 0 {
		return 0
	}
	backoff := time.Second << (retry - 1)
	if backoff > maxRetryBackoff {
		return maxRetryBackoff
	}
	return backoff
}
