package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
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

	// P 数量（仿 GMP，校招项目简化为 2 个）
	NumP = 2
)

// Scheduler GMP 调度器
type Scheduler struct {
	ps    []*P
	fsm   *FSM
	mu    sync.Mutex
	stopCh chan struct{}
	wg    sync.WaitGroup
}

// NewScheduler 创建调度器
func NewScheduler() *Scheduler {
	s := &Scheduler{
		ps:    make([]*P, NumP),
		fsm:   NewFSM(),
		stopCh: make(chan struct{}),
	}

	// 初始化所有 P
	for i := 0; i < NumP; i++ {
		s.ps[i] = NewP(i, s)
	}

	return s
}

// Start 启动调度器
func (s *Scheduler) Start(ctx context.Context) {
	fmt.Println("🚀 GMP 调度引擎已启动...")

	// 1. 注册 FSM 钩子
	s.registerFSMHooks()

	// 2. 恢复 pending 队列中的任务（异常退出遗留）
	s.recoverPendingTasks(ctx)

	// 3. FSM 智能恢复：扫描所有任务状态，重新加入未完成的任务
	if count, err := s.fsm.RecoverStaleTasks(ctx); err != nil {
		log.Printf("⚠️ FSM 智能恢复失败: %v", err)
	} else if count > 0 {
		log.Printf("♻️ FSM 智能恢复 %d 个未完成任务", count)
	}

	// 4. 启动 fetcher 协程（拉取全局任务到本地）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.fetcherLoop(ctx)
	}()

	// 5. 启动所有 P
	for i, p := range s.ps {
		s.wg.Add(1)
		go func(idx int, pp *P) {
			defer s.wg.Done()
			pp.Run(ctx)
			log.Printf("🛑 P%d 已退出", idx)
		}(i, p)
	}

	// 6. 启动 stale pending 回收协程
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.recoverStalePendingLoop(ctx)
	}()

	// 7. 启动终态清理协程（每小时清理一次）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.terminalStateCleanerLoop(ctx)
	}()

	// 8. 启动卡死任务监控协程（每 30s 扫描一次，超 2 分钟未更新视为卡死）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.staleStateMonitorLoop(ctx)
	}()
}

// staleStateMonitorLoop 监控 DISPATCHED/RUNNING 状态卡死的任务
func (s *Scheduler) staleStateMonitorLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	const staleTimeout = 2 * time.Minute

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		count, err := s.fsm.MonitorStaleStates(ctx, staleTimeout)
		if err != nil {
			log.Printf("⚠️ 卡死任务监控失败: %v", err)
			continue
		}
		if count > 0 {
			log.Printf("⏰ 检测到 %d 个卡死任务，已重新入队", count)
		}
	}
}

// registerFSMHooks 注册状态机钩子
func (s *Scheduler) registerFSMHooks() {
	// 死信状态钩子
	s.fsm.RegisterHook(model.StatusDead, func(ctx context.Context, task *model.Task) error {
		return SaveDeadTask(ctx, task, "max retry exhausted")
	})
}

// fetcherLoop 后台拉取全局队列的任务，分发到各 P
func (s *Scheduler) fetcherLoop(ctx context.Context) {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	// Lua 脚本：原子地把任务从 global 转到 pending
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
			continue
		}
		if len(result)%2 != 0 {
			log.Printf("⚠️ Redis 返回了非法任务载荷，len=%d", len(result))
			continue
		}

		// 解析任务列表
		tasks := s.parseTasks(result)
		if len(tasks) == 0 {
			continue
		}

		fmt.Printf("📦 [Fetcher] 抢到 %d 个任务，分配到 P\n", len(tasks))

		// 负载均衡：轮询分配到各 P
		s.balanceTasks(tasks)
	}
}

// parseTasks 解析 Redis 返回的任务列表
// 任务 ID 格式: "job_<id>_<priority>" 或者完整结构 JSON
func (s *Scheduler) parseTasks(result []string) []*model.Task {
	var tasks []*model.Task

	for i := 0; i < len(result); i += 2 {
		memberStr := result[i]
		triggerTime, parseErr := strconv.ParseInt(result[i+1], 10, 64)
		if parseErr != nil {
			log.Printf("⚠️ triggerTime 解析失败, member=%s score=%s err=%v", memberStr, result[i+1], parseErr)
			continue
		}

		// 优先尝试 JSON 解析（新版任务格式）
		var task model.Task
		if err := json.Unmarshal([]byte(memberStr), &task); err == nil && task.ID != "" {
			task.TriggerTime = triggerTime
			tasks = append(tasks, &task)
			continue
		}

		// 兼容旧版格式: "job_<id>_<priority>"
		task = s.parseLegacyTask(memberStr, triggerTime)
		tasks = append(tasks, &task)
	}

	return tasks
}

// parseLegacyTask 解析旧版任务格式 "job_<id>_<priority>"
func (s *Scheduler) parseLegacyTask(memberStr string, triggerTime int64) model.Task {
	parts := strings.Split(memberStr, "_")
	priority := consts.PriorityLow
	jobID := memberStr

	if len(parts) >= 3 {
		jobID = strings.Join(parts[1:len(parts)-1], "_")
		priority = parts[len(parts)-1]
	}

	return model.Task{
		ID:          jobID,
		Name:        "legacy_" + jobID,
		FuncName:    "mock_work",
		TriggerTime: triggerTime,
		Priority:    priority,
		MaxRetry:    consts.DefaultMaxRetry,
		Timeout:     30,
	}
}

// balanceTasks 负载均衡：轮询分配任务到各 P
func (s *Scheduler) balanceTasks(tasks []*model.Task) {
	for i, task := range tasks {
		// 状态机：PENDING -> READY
		ctx := context.Background()
		_ = s.fsm.Fire(ctx, EventTrigger, task)

		// 轮询分配到 P
		pIdx := i % NumP
		s.ps[pIdx].Push(task)
	}
}

// fetchFromGlobal 从全局队列获取任务（P 调用时调用）
func (s *Scheduler) fetchFromGlobal() []*model.Task {
	// 简化版：直接通过 ZRANGEBYSCORE 拉取
	maxScore := time.Now().Add(preloadWindow).Unix()

	// Lua 脚本：原子拉取并转移到 pending
	luaScript := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
		for i=1,#items,2 do
			redis.call('ZADD', KEYS[2], items[i+1], items[i])
			redis.call('ZREM', KEYS[1], items[i])
		end
		return items
	`)

	ctx := context.Background()
	result, err := luaScript.Run(ctx, database.RDB, []string{consts.JobZSetKey, consts.JobPendingZSetKey}, maxScore).StringSlice()
	if err != nil || len(result) == 0 {
		return nil
	}

	return s.parseTasks(result)
}

// workSteal 从其他 P 偷任务
func (s *Scheduler) workSteal(thief *P) []*model.Task {
	for _, p := range s.ps {
		if p.ID == thief.ID {
			continue
		}
		if stolen := p.StealHalf(); len(stolen) > 0 {
			return stolen
		}
	}
	return nil
}

// publishToMQ 推送任务到 RabbitMQ
func (s *Scheduler) publishToMQ(ctx context.Context, task *model.Task) error {
	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	cmd := model.TaskCommand{
		ID:          task.ID,
		Name:        task.Name,
		FuncName:    task.FuncName,
		TriggerTime: task.TriggerTime,
		Priority:    task.Priority,
		RetryCount:  task.RetryCount,
		MaxRetry:    task.MaxRetry,
		Timeout:     task.Timeout,
		Payload:     task.Payload,
	}

	body, err := json.Marshal(cmd)
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
		return err
	}

	return nil
}

// removeFromPending 从 Pending 队列移除任务
func (s *Scheduler) removeFromPending(ctx context.Context, taskID string) error {
	_, err := database.RDB.ZRem(ctx, consts.JobPendingZSetKey, taskID).Result()
	return err
}

// backoff 计算重试退避时间（指数退避 + 随机抖动）
func (s *Scheduler) backoff(retry int) time.Duration {
	if retry <= 0 {
		return 0
	}
	// 指数退避：1s, 2s, 4s, 8s, 16s, max 30s
	backoff := time.Second << (retry - 1)
	if backoff > maxRetryBackoff {
		backoff = maxRetryBackoff
	}
	// 加入随机抖动（0-500ms），避免雪崩
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	return backoff + jitter
}

// recoverPendingTasks 恢复 pending 队列中的任务（启动时调用）
func (s *Scheduler) recoverPendingTasks(ctx context.Context) {
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

// recoverStalePendingLoop 定期回收超时的 pending 任务
func (s *Scheduler) recoverStalePendingLoop(ctx context.Context) {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	luaScript := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
		for i=1,#items,2 do
			redis.call('ZADD', KEYS[2], items[i+1], items[i])
			redis.call('ZREM', KEYS[1], items[i])
		end
		return #items / 2
	`)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		deadline := time.Now().Add(-recoverPendingTimeout).Unix()
		count, err := luaScript.Run(ctx, database.RDB, []string{consts.JobPendingZSetKey, consts.JobZSetKey}, deadline).Int64()
		if err != nil && err != redis.Nil {
			log.Printf("⚠️ stale pending 回收失败: %v", err)
			continue
		}
		if count > 0 {
			log.Printf("♻️ 回收 %d 条 stale pending 任务", count)
		}
	}
}

// terminalStateCleanerLoop 定期清理 1 小时前的终态任务记录（节省 Redis 内存）
func (s *Scheduler) terminalStateCleanerLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 清理 1 小时前的 SUCCESS/DEAD 状态记录
		threshold := time.Now().Add(-1 * time.Hour).Unix()
		count, err := s.fsm.CleanTerminalStates(ctx, threshold)
		if err != nil {
			log.Printf("⚠️ 终态清理失败: %v", err)
			continue
		}
		if count > 0 {
			log.Printf("🧹 清理 %d 条终态任务状态记录", count)
		}
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop() {
	close(s.stopCh)
	for _, p := range s.ps {
		p.Stop()
	}
	s.wg.Wait()
}

// GetFSM 获取状态机实例（供外部调用）
func (s *Scheduler) GetFSM() *FSM {
	return s.fsm
}
