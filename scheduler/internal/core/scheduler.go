package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
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
	// fetcherLoop tick 间隔：从 5s 降到 200ms，保证调度延迟 < 200ms
	// 配合 preloadWindow 使用，到期前 1s 就开始拉取
	fetchInterval         = 200 * time.Millisecond
	preloadWindow         = 1 * time.Second
	publishTimeout        = 3 * time.Second
	maxRetryBackoff       = 30 * time.Second
	recoverPendingBatch   = 1000
	recoverPendingTimeout = 2 * time.Minute

	// P 数量（仿 GMP，校招项目简化为 2 个）
	NumP = 2

	// 老化阈值：Low 任务在队列中等待超过 5 分钟，自动提升为 High
	// 防止低优先级任务被持续到来的高优先级任务饿死
	agingThreshold = 5 * time.Minute
)

// 包级 Lua 脚本：原子地把到期任务从 global 移到 pending
// KEYS[1]=global ZSet, KEYS[2]=pending ZSet, ARGV[1]=maxScore
var fetchExpiredScript = redis.NewScript(`
	local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES')
	for i=1,#items,2 do
		redis.call('ZADD', KEYS[2], items[i+1], items[i])
		redis.call('ZREM', KEYS[1], items[i])
	end
	return items
`)

// Scheduler GMP 调度器
type Scheduler struct {
	ps     []*P
	fsm    *FSM
	leader *LeaderElection
	mu     sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler 创建调度器
func NewScheduler() *Scheduler {
	s := &Scheduler{
		ps:     make([]*P, NumP),
		fsm:    NewFSM(),
		leader: NewLeaderElection(),
		stopCh: make(chan struct{}),
	}

	// 初始化所有 P
	for i := 0; i < NumP; i++ {
		s.ps[i] = NewP(i, s)
	}

	return s
}

// Start 启动调度器
// 选主循环：拿到锁才调度，失去锁则停所有调度协程并重新选主
// 这样多实例部署时只有主在调度，主挂了 10s 后 backup 自动接管
func (s *Scheduler) Start(ctx context.Context) {
	fmt.Println("🚀 GMP 调度引擎已启动...")

	// 1. 注册 FSM 钩子（不需要锁，任何时候都能做）
	s.registerFSMHooks()

	// 2. 选主循环
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lostCh, err := s.leader.WaitForLeadership(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️ 选主失败: %v，重试", err)
			continue
		}

		// 拿到锁，作为 Leader 运行
		// leadCtx 在 lostCh 关闭（续期失败/主失联）时 cancel，停止所有调度协程
		leadCtx, cancel := context.WithCancel(ctx)
		go func() {
			<-lostCh
			cancel()
		}()

		s.runAsLeader(leadCtx)
		log.Printf("🔄 失去 Leader 身份，重新选主...")
	}
}

// runAsLeader 作为 Leader 启动所有调度协程，阻塞直到 leadCtx 取消
// 失去锁时所有协程退出，避免和新的 Leader 同时调度
func (s *Scheduler) runAsLeader(ctx context.Context) {
	// 1. 恢复 pending 队列中的任务（异常退出遗留）
	s.recoverPendingTasks(ctx)

	// 2. FSM 智能恢复：扫描所有任务状态，重新加入未完成的任务
	if count, err := s.fsm.RecoverStaleTasks(ctx); err != nil {
		log.Printf("⚠️ FSM 智能恢复失败: %v", err)
	} else if count > 0 {
		log.Printf("♻️ FSM 智能恢复 %d 个未完成任务", count)
	}

	// 3. 启动 fetcher 协程（拉取全局任务到本地）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.fetcherLoop(ctx)
	}()

	// 4. 启动所有 P
	for i, p := range s.ps {
		s.wg.Add(1)
		go func(idx int, pp *P) {
			defer s.wg.Done()
			pp.Run(ctx)
			log.Printf("🛑 P%d 已退出", idx)
		}(i, p)
	}

	// 5. 启动 stale pending 回收协程
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.recoverStalePendingLoop(ctx)
	}()

	// 6. 启动终态清理协程（每小时清理一次）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.terminalStateCleanerLoop(ctx)
	}()

	// 7. 启动卡死任务监控协程（每 30s 扫描一次，超 2 分钟未更新视为卡死）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.staleStateMonitorLoop(ctx)
	}()

	// 等待所有调度协程退出（leadCtx cancel 后）
	s.wg.Wait()
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

// fetcherLoop 后台拉取到期任务，分发到各 P
// 职责单一：只负责 global_queue → pending → P.localQueue
// 200ms 高频 tick 保证调度延迟 < 200ms
func (s *Scheduler) fetcherLoop(ctx context.Context) {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		maxScore := time.Now().Add(preloadWindow).Unix()

		// 用 Lua 原子拉取到期任务
		result, err := fetchExpiredScript.Run(ctx, database.RDB,
			[]string{consts.JobZSetKey, consts.JobPendingZSetKey}, maxScore).StringSlice()
		if err != nil && err != redis.Nil {
			log.Printf("⚠️ Redis 拉取任务失败: %v", err)
			continue
		}
		if len(result) == 0 || len(result)%2 != 0 {
			if len(result)%2 != 0 {
				log.Printf("⚠️ 返回了非法任务载荷，len=%d", len(result))
			}
			continue
		}

		tasks := s.parseTasks(result)
		if len(tasks) == 0 {
			continue
		}

		// 老化机制：Low 任务在队列中等待超过 5 分钟，自动提升为 High
		// 防止被持续到来的高优先级任务饿死
		s.applyAging(tasks)

		s.balanceTasks(tasks)
	}
}

// applyAging 老化提升：超过阈值未执行的 Low 任务提升为 High
// M5: 持久化到 detail Hash，防止 P 崩溃后老化提升丢失
func (s *Scheduler) applyAging(tasks []*model.Task) {
	now := time.Now().Unix()
	threshold := int64(agingThreshold.Seconds())

	var promoted []*model.Task
	for _, t := range tasks {
		if t.Priority == consts.PriorityLow && t.EnqueueAt > 0 && now-t.EnqueueAt > threshold {
			t.Priority = consts.PriorityHigh
			promoted = append(promoted, t)
		}
	}

	if len(promoted) == 0 {
		return
	}

	// M5: 批量写回 detail Hash，持久化优先级提升
	ctx := context.Background()
	pipe := database.RDB.Pipeline()
	for _, t := range promoted {
		if taskJSON, err := json.Marshal(t); err == nil {
			pipe.HSet(ctx, model.DetailKey(t.ID), "data", taskJSON)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("⚠️ 老化提升持久化失败: %v", err)
	}
	log.Printf("⏫ 老化提升 %d 个 Low 任务为 High", len(promoted))
}

// parseTasks 解析 Redis 返回的 [jobID, score, ...] 列表
// member 是 jobID（不再包含 JSON），详情通过 HGET 从 task_detail Hash 读取
func (s *Scheduler) parseTasks(result []string) []*model.Task {
	if len(result) == 0 || len(result)%2 != 0 {
		return nil
	}

	ctx := context.Background()
	n := len(result) / 2

	// 收集 (jobID, score) 对
	type pair struct {
		jobID  string
		score  int64
	}
	pairs := make([]pair, 0, n)
	for i := 0; i < len(result); i += 2 {
		jobID := result[i]
		score, err := strconv.ParseInt(result[i+1], 10, 64)
		if err != nil {
			log.Printf("⚠️ triggerTime 解析失败, jobID=%s score=%s err=%v", jobID, result[i+1], err)
			continue
		}
		pairs = append(pairs, pair{jobID: jobID, score: score})
	}
	if len(pairs) == 0 {
		return nil
	}

	// pipeline 批量 HGET 任务详情，减少 RTT
	pipe := database.RDB.Pipeline()
	cmds := make([]*redis.StringCmd, len(pairs))
	for i, p := range pairs {
		cmds[i] = pipe.HGet(ctx, model.DetailKey(p.jobID), "data")
	}
	_, _ = pipe.Exec(ctx)

	var tasks []*model.Task
	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			log.Printf("⚠️ 任务详情读取失败, jobID=%s err=%v", pairs[i].jobID, err)
			continue
		}
		var task model.Task
		if err := json.Unmarshal(data, &task); err != nil {
			log.Printf("⚠️ 任务详情反序列化失败, jobID=%s err=%v", pairs[i].jobID, err)
			continue
		}
		task.TriggerTime = pairs[i].score
		tasks = append(tasks, &task)
	}
	return tasks
}

// balanceTasks 负载均衡：轮询分配任务到各 P
func (s *Scheduler) balanceTasks(tasks []*model.Task) {
	for i, task := range tasks {
		// S2: 状态机 PENDING -> READY
		// CAS 失败说明状态被并发修改（任务已被其他协程处理或 recovery 重置），
		// 跳过分发避免后续 DISPATCH 状态转换非法（PENDING --DISPATCH--> ?）
		ctx := context.Background()
		if err := s.fsm.Fire(ctx, EventTrigger, task); err != nil {
			log.Printf("⚠️ [FSM] task=%s Fire(TRIGGER) 失败: %v，跳过分发（任务已被其他协程处理）", task.ID, err)
			continue
		}

		// 轮询分配到 P
		pIdx := i % NumP
		s.ps[pIdx].Push(task)
	}
}

// workSteal 从其他 P 偷任务（P 本地空时调用）
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
		CallbackURL: task.CallbackURL,
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

	// M1: 从 Channel 池获取独立 Channel，避免多协程并发 Publish 触发 channel exception
	ch := mq.GetPublishChannel()
	err = ch.PublishWithContext(
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
// 注：executor/consumer.go 的 backoffDuration 逻辑一致，两者独立维护避免跨包依赖
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
// pending 队列是临时队列，回收时直接写回 global queue
// member 是 jobID（不是 JSON），无需反序列化
func (s *Scheduler) recoverPendingTasks(ctx context.Context) {
	// 拉取所有 pending 任务（score <= now，即所有到期或过期的），分批防止 OOM
	maxScore := time.Now().Unix()
	result, err := database.RDB.ZRangeByScore(ctx, consts.JobPendingZSetKey, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(maxScore, 10),
		Count: recoverPendingBatch, // M2: 加 LIMIT 防止一次拉取过多导致 OOM
	}).Result()
	if err != nil && err != redis.Nil {
		log.Printf("⚠️ pending 任务拉取失败: %v", err)
		return
	}

	if len(result) == 0 {
		return
	}

	// member 是 jobID，直接写回 global queue
	pipe := database.RDB.Pipeline()
	count := 0
	for _, jobID := range result {
		// 用 jobID 作为 member 写回 global queue，score=now 立即执行
		pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(maxScore), // 立即执行
			Member: jobID,
		})
		pipe.ZRem(ctx, consts.JobPendingZSetKey, jobID)
		count++
	}
	if count > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("⚠️ pending 任务回收 pipeline 失败: %v", err)
			return
		}
		log.Printf("♻️ 已回收 %d 条 pending 任务到 global queue", count)
	}
}

// recoverStalePendingLoop 定期回收超时的 pending 任务
// pending 队列中的任务如果超过 2 分钟没被消费，说明 P 挂了或任务卡死
func (s *Scheduler) recoverStalePendingLoop(ctx context.Context) {
	// 5 秒一次，比 fetcherLoop 低频
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 拉取超过 2 分钟的 stale pending 任务，分批防止 OOM
		deadline := time.Now().Add(-recoverPendingTimeout).Unix()
		result, err := database.RDB.ZRangeByScore(ctx, consts.JobPendingZSetKey, &redis.ZRangeBy{
			Min:   "0",
			Max:   strconv.FormatInt(deadline, 10),
			Count: recoverPendingBatch, // M2: 加 LIMIT
		}).Result()
		if err != nil || len(result) == 0 {
			continue
		}

		// member 是 jobID，直接写回 global queue
		pipe := database.RDB.Pipeline()
		count := 0
		now := time.Now().Unix()
		for _, jobID := range result {
			pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
				Score:  float64(now), // 立即执行
				Member: jobID,
			})
			pipe.ZRem(ctx, consts.JobPendingZSetKey, jobID)
			count++
		}
		if count > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("⚠️ stale pending 回收 pipeline 失败: %v", err)
				continue
			}
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
// S5: 加 10s 超时保护，避免某个协程卡住导致进程无法退出
// 退出时主动释放 Leader 锁，让 backup 立即接管
func (s *Scheduler) Stop() {
	close(s.stopCh)
	for _, p := range s.ps {
		p.Stop()
	}

	// 主动释放 Leader 锁（CAS 释放，避免误删别人的锁）
	// 即使释放失败，锁也会在 TTL(10s) 后过期，backup 自动接管
	s.leader.Release(context.Background())

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ 调度器已优雅退出")
	case <-time.After(10 * time.Second):
		log.Println("⚠️ 调度器优雅退出超时(10s)，强制退出")
	}
}

// GetFSM 获取状态机实例（供外部调用）
func (s *Scheduler) GetFSM() *FSM {
	return s.fsm
}
