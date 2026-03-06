package core

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go-flash-job/pkg/consts" 
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
)

// ==========================================
// 1. 定义本地最小堆 (Local Queue / G)
// ==========================================

// Task 代表一个待执行的任务 (G)
type Task struct {
	JobID       string
	TriggerTime int64 // 触发时间的时间戳 (秒)
}

// TaskHeap 实现 container/heap 接口
type TaskHeap[]*Task

func (h TaskHeap) Len() int           { return len(h) }
func (h TaskHeap) Less(i, j int) bool { return h[i].TriggerTime < h[j].TriggerTime } // 最小堆：时间早的排在前面
func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TaskHeap) Push(x interface{}) {
	*h = append(*h, x.(*Task))
}
func (h *TaskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ==========================================
// 2. 调度引擎核心 (Dispatcher)
// ==========================================

type Dispatcher struct {
	localQueue TaskHeap
	mu         sync.Mutex
	wakeCh     chan struct{} // 用于唤醒 M (执行器) 的信号
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		localQueue: make(TaskHeap, 0),
		wakeCh:     make(chan struct{}, 1),
	}
}

// Start 启动调度器引擎
func (d *Dispatcher) Start() {
	heap.Init(&d.localQueue)

	// 启动 P (拉取器): 负责从 Global Queue 获取任务
	go d.fetcherLoop()

	// 启动 M (执行器): 负责精准触发任务
	go d.executorLoop()

	fmt.Println("🚀 GMP 调度引擎已启动...")
}

// ==========================================
// 3. P 协程: Work Stealing (从 Redis 拉取)
// ==========================================
func (d *Dispatcher) fetcherLoop() {
	ticker := time.NewTicker(5 * time.Second) // 每 5 秒拉取一次
	defer ticker.Stop()

	ctx := context.Background()

	// 简历亮点：使用 Lua 脚本保证 ZRANGE 和 ZREM 的原子性，防止分布式多节点重复拉取
	luaScript := redis.NewScript(`
		local keys = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
		if #keys > 0 then
			redis.call('ZREM', KEYS[1], unpack(keys))
		end
		return keys
	`)

	for {
		<-ticker.C
		// 预加载未来 10 秒内的任务
		maxScore := time.Now().Add(10 * time.Second).Unix()

		// 执行 Lua 脚本批量拉取
		result, err := luaScript.Run(ctx, database.RDB,[]string{consts.JobZSetKey}, maxScore).StringSlice()
		if err != nil && err != redis.Nil {
			log.Printf("⚠️ Redis 拉取任务失败: %v", err)
			continue
		}

		if len(result) == 0 {
			continue
		}

		fmt.Printf("📦 [Fetcher] 偷取到 %d 个任务，放入 Local Queue\n", len(result))

		// 加锁，将任务推入本地最小堆
		d.mu.Lock()
		for _, jobID := range result {
			heap.Push(&d.localQueue, &Task{
				JobID:       jobID,
				// 简化：真实场景需要从 DB 或缓存拿真实时间，这里假设我们直接推送到堆里尽快执行
				TriggerTime: time.Now().Unix(), 
			})
		}
		d.mu.Unlock()

		// 唤醒 M 协程去检查新的堆顶任务
		select {
		case d.wakeCh <- struct{}{}:
		default:
		}
	}
}

// ==========================================
// 4. M 协程: 无忙等待的精准触发
// ==========================================
func (d *Dispatcher) executorLoop() {
	// 创建一个定时器，初始置为休眠状态
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		d.mu.Lock()
		if d.localQueue.Len() == 0 {
			d.mu.Unlock()
			// 堆为空，永久休眠，等待 fetcher 唤醒
			<-d.wakeCh 
			continue
		}

		// 获取堆顶任务（即将要执行的任务）
		topTask := d.localQueue[0]
		now := time.Now().Unix()
		
		var waitDuration time.Duration
		if topTask.TriggerTime <= now {
			waitDuration = 0 // 已经到期，立即执行
		} else {
			waitDuration = time.Duration(topTask.TriggerTime-now) * time.Second
		}
		d.mu.Unlock()

		if waitDuration == 0 {
			// [核心动作] 任务到期，弹出并推送到 RabbitMQ
			d.mu.Lock()
			taskToRun := heap.Pop(&d.localQueue).(*Task)
			d.mu.Unlock()

			d.publishToMQ(taskToRun.JobID)
			continue // 继续检查下一个堆顶任务
		}

		// 任务没到期，重置 Timer，挂起协程 (让出 CPU)
		timer.Reset(waitDuration)

		select {
		case <-timer.C:
			// 定时器到期，自动进入下一次循环去 Pop 任务
		case <-d.wakeCh:
			// 在休眠期间，Fetcher 拉取了更早的新任务，唤醒重新计算时间
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// 将到期的任务发送给执行器 (Worker)
func (d *Dispatcher) publishToMQ(jobID string) {
	err := mq.RabbitChannel.PublishWithContext(
		context.Background(),
		"",               // exchange
		consts.TaskQueue, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:[]byte(jobID), // 将 JobID 丢进 MQ
		},
	)
	if err != nil {
		log.Printf("❌ 任务 %s 推送 MQ 失败: %v\n", jobID, err)
	} else {
		fmt.Printf("⚡ [Dispatcher] 任务 %s 已到期，成功推入 RabbitMQ\n", jobID)
	}
}