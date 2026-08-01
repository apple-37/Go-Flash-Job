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
	"go-flash-job/pkg/metrics"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/mq"
	"go-flash-job/pkg/shard"

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
)

// 包级 Lua 脚本：原子地把到期任务从 global 分片转到 pending
// KEYS[1]=global shard ZSet, KEYS[2]=pending ZSet, ARGV[1]=maxScore
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

// fetcherLoop 后台拉取所有分片中到期任务，分发到各 P
// 职责单一：只负责 global_shard_N → pending → P.localQueue
// 200ms 高频 tick 保证调度延迟 < 200ms
// 遍历 16 个分片，用 pipeline 减少 RTT
func (s *Scheduler) fetcherLoop(ctx context.Context) {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	shardKeys := shard.AllShardKeys()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		maxScore := time.Now().Add(preloadWindow).Unix()

		// 遍历所有分片，用 Lua 原子拉取到期任务
		var allTasks []*model.Task
		for _, shardKey := range shardKeys {
			result, err := fetchExpiredScript.Run(ctx, database.RDB,
				[]string{shardKey, consts.JobPendingZSetKey}, maxScore).StringSlice()
			if err != nil && err != redis.Nil {
				log.Printf("⚠️ Redis 拉取分片 %s 失败: %v", shardKey, err)
				continue
			}
			if len(result) == 0 {
				continue
			}
			if len(result)%2 != 0 {
				log.Printf("⚠️ 分片 %s 返回了非法任务载荷，len=%d", shardKey, len(result))
				continue
			}
			tasks := s.parseTasks(result)
			allTasks = append(allTasks, tasks...)
		}

		// 埋点：更新队列大小（每 5 个 tick 采样一次，减少 Redis 压力）
		if rand.Intn(5) == 0 {
			s.updateQueueMetrics(ctx, shardKeys)
		}

		if len(allTasks) == 0 {
			continue
		}

		s.balanceTasks(allTasks)
	}
}

// updateQueueMetrics 更新队列大小指标
func (s *Scheduler) updateQueueMetrics(ctx context.Context, shardKeys []string) {
	pipe := database.RDB.Pipeline()
	cmds := make([]*redis.IntCmd, len(shardKeys))
	for i, k := range shardKeys {
		cmds[i] = pipe.ZCard(ctx, k)
	}
	_, _ = pipe.Exec(ctx)

	var total int64
	for _, cmd := range cmds {
		if n, err := cmd.Result(); err == nil {
			total += n
		}
	}
	metrics.QueueSize.WithLabelValues("global").Set(float64(total))

	if n, err := database.RDB.ZCard(ctx, consts.JobPendingZSetKey).Result(); err == nil {
		metrics.QueueSize.WithLabelValues("pending").Set(float64(n))
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

		// JSON 格式解析（任务以 model.Task 序列化存储）
		var task model.Task
		if err := json.Unmarshal([]byte(memberStr), &task); err != nil {
			log.Printf("⚠️ 任务解析失败, member=%s err=%v", memberStr, err)
			continue
		}
		if task.ID == "" {
			continue
		}
		task.TriggerTime = triggerTime
		tasks = append(tasks, &task)
	}

	return tasks
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

// workSteal 从其他 P 偷任务（P 本地空时调用）
func (s *Scheduler) workSteal(thief *P) []*model.Task {
	for _, p := range s.ps {
		if p.ID == thief.ID {
			continue
		}
		if stolen := p.StealHalf(); len(stolen) > 0 {
			metrics.WorkStealCount.WithLabelValues(strconv.Itoa(thief.ID)).Inc()
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

	// 埋点：MQ publish 耗时
	start := time.Now()
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
	metrics.MQPublishDuration.WithLabelValues().Observe(time.Since(start).Seconds())

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
// pending 队列是临时队列，回收时按 jobID 路由到对应分片
func (s *Scheduler) recoverPendingTasks(ctx context.Context) {
	// 拉取所有 pending 任务（score <= now，即所有到期或过期的）
	maxScore := time.Now().Unix()
	result, err := database.RDB.ZRangeByScore(ctx, consts.JobPendingZSetKey, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(maxScore, 10),
	}).Result()
	if err != nil && err != redis.Nil {
		log.Printf("⚠️ pending 任务拉取失败: %v", err)
		return
	}

	if len(result) == 0 {
		return
	}

	// 按分片批量写回 global
	pipe := database.RDB.Pipeline()
	count := 0
	for _, memberStr := range result {
		var task model.Task
		if err := json.Unmarshal([]byte(memberStr), &task); err != nil {
			continue
		}
		pipe.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
			Score:  float64(task.TriggerTime),
			Member: memberStr,
		})
		pipe.ZRem(ctx, consts.JobPendingZSetKey, memberStr)
		count++
	}
	if count > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("⚠️ pending 任务回收 pipeline 失败: %v", err)
			return
		}
		log.Printf("♻️ 已回收 %d 条 pending 任务到分片 global queue", count)
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

		// 拉取超过 2 分钟的 stale pending 任务
		deadline := time.Now().Add(-recoverPendingTimeout).Unix()
		result, err := database.RDB.ZRangeByScore(ctx, consts.JobPendingZSetKey, &redis.ZRangeBy{
			Min: "0", Max: strconv.FormatInt(deadline, 10),
		}).Result()
		if err != nil || len(result) == 0 {
			continue
		}

		// 按分片写回 global
		pipe := database.RDB.Pipeline()
		count := 0
		for _, memberStr := range result {
			var task model.Task
			if err := json.Unmarshal([]byte(memberStr), &task); err != nil {
				continue
			}
			pipe.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
				Score:  float64(time.Now().Unix()), // 立即执行
				Member: memberStr,
			})
			pipe.ZRem(ctx, consts.JobPendingZSetKey, memberStr)
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
