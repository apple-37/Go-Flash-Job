package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/shard"

	"github.com/redis/go-redis/v9"
)

// Event 状态机事件
type Event string

const (
	EventTrigger  Event = "TRIGGER"   // PENDING -> READY
	EventDispatch Event = "DISPATCH"  // READY -> DISPATCHED
	EventStart    Event = "START"     // DISPATCHED -> RUNNING
	EventSuccess  Event = "SUCCESS"   // RUNNING -> SUCCESS
	EventFail     Event = "FAIL"      // RUNNING -> RETRY/DEAD
	EventRetry    Event = "RETRY"     // RETRY -> READY
)

// Transition 状态转换规则
type Transition struct {
	From  model.TaskStatus
	Event Event
	To    model.TaskStatus
}

// HookFunc 状态变更钩子函数
type HookFunc func(ctx context.Context, task *model.Task) error

// FSM 有限状态机引擎
type FSM struct {
	transitions map[Transition]struct{}
	hooks       map[model.TaskStatus][]HookFunc
	mu          sync.RWMutex
}

// NewFSM 创建状态机
func NewFSM() *FSM {
	fsm := &FSM{
		transitions: make(map[Transition]struct{}),
		hooks:       make(map[model.TaskStatus][]HookFunc),
	}

	// 注册合法的状态转换
	transitions := []Transition{
		{From: model.StatusPending, Event: EventTrigger, To: model.StatusReady},
		{From: model.StatusReady, Event: EventDispatch, To: model.StatusDispatched},
		{From: model.StatusDispatched, Event: EventStart, To: model.StatusRunning},
		{From: model.StatusRunning, Event: EventSuccess, To: model.StatusSuccess},
		{From: model.StatusRunning, Event: EventFail, To: model.StatusRetry},
		{From: model.StatusRetry, Event: EventRetry, To: model.StatusReady},
		{From: model.StatusRetry, Event: EventFail, To: model.StatusDead},
	}
	for _, t := range transitions {
		fsm.transitions[t] = struct{}{}
	}

	return fsm
}

// RegisterHook 注册状态钩子
func (f *FSM) RegisterHook(state model.TaskStatus, hook HookFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hooks[state] = append(f.hooks[state], hook)
}

// Fire 触发事件，转换状态并持久化到 Redis
func (f *FSM) Fire(ctx context.Context, event Event, task *model.Task) error {
	current, err := f.GetState(ctx, task.ID)
	if err != nil {
		return fmt.Errorf("get current state failed: %w", err)
	}

	// 查找合法转换
	key := Transition{From: current, Event: event}
	target, ok := f.findTransition(current, event)
	if !ok {
		return fmt.Errorf("illegal transition: %s --%s--> ?", current, event)
	}

	// 持久化新状态
	if err := f.saveState(ctx, task, target); err != nil {
		return fmt.Errorf("save state failed: %w", err)
	}

	// 触发钩子
	f.mu.RLock()
	hooks := f.hooks[target]
	f.mu.RUnlock()
	for _, hook := range hooks {
		if err := hook(ctx, task); err != nil {
			log.Printf("⚠️ 钩子执行失败 task=%s state=%s err=%v", task.ID, target, err)
		}
	}

	log.Printf("🔄 [FSM] task=%s %s --%s--> %s", task.ID, current, event, target)
	_ = key
	return nil
}

// findTransition 查找目标状态
func (f *FSM) findTransition(from model.TaskStatus, event Event) (model.TaskStatus, bool) {
	for t := range f.transitions {
		if t.From == from && t.Event == event {
			return t.To, true
		}
	}
	return "", false
}

// GetState 从 Redis 获取任务状态
func (f *FSM) GetState(ctx context.Context, taskID string) (model.TaskStatus, error) {
	key := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, taskID)
	status, err := database.RDB.HGet(ctx, key, "status").Result()
	if err == redis.Nil {
		return model.StatusPending, nil // 默认状态
	}
	if err != nil {
		return "", err
	}
	return model.TaskStatus(status), nil
}

// saveState 保存任务状态到 Redis Hash
func (f *FSM) saveState(ctx context.Context, task *model.Task, status model.TaskStatus) error {
	key := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, task.ID)
	_, err := database.RDB.HSet(ctx, key,
		"id", task.ID,
		"name", task.Name,
		"callback_url", task.CallbackURL,
		"status", string(status),
		"retry_count", task.RetryCount,
		"max_retry", task.MaxRetry,
		"trigger_time", task.TriggerTime,
		"updated_at", time.Now().Unix(),
	).Result()
	return err
}

// SaveDeadTask 保存死信任务到 Redis ZSet
func SaveDeadTask(ctx context.Context, task *model.Task, errMsg string) error {
	// 1. 保存任务详情到 Hash
	detailKey := fmt.Sprintf("flash_job:dead_detail:%s", task.ID)
	if err := database.RDB.HSet(ctx, detailKey,
		"id", task.ID,
		"name", task.Name,
		"callback_url", task.CallbackURL,
		"retry_count", task.RetryCount,
		"error", errMsg,
		"dead_at", time.Now().Unix(),
	).Err(); err != nil {
		return err
	}

	// 2. 加入死信队列 ZSet
	_, err := database.RDB.ZAdd(ctx, consts.JobDeadZSetKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.ID,
	}).Result()
	return err
}

// RecoverStaleTasks 启动时扫描所有 Task State，根据状态智能恢复
// - PENDING/READY/RETRY: 重新加入 Global Queue 等待调度
// - DISPATCHED/RUNNING: 视为卡死，重新加入 Global Queue
// - SUCCESS/DEAD: 忽略（终态）
func (f *FSM) RecoverStaleTasks(ctx context.Context) (int, error) {
	// 扫描所有 task:state:* Hash
	pattern := fmt.Sprintf("%s:*", consts.TaskStateKeyPrefix)
	iter := database.RDB.Scan(ctx, 0, pattern, 1000).Iterator()

	recovered := 0
	now := time.Now().Unix()

	for iter.Next(ctx) {
		key := iter.Val()
		// 提取 taskID
		taskID := strings.TrimPrefix(key, consts.TaskStateKeyPrefix+":")
		if taskID == "" {
			continue
		}

		// 获取状态和触发时间
		fields, err := database.RDB.HGetAll(ctx, key).Result()
		if err != nil {
			log.Printf("⚠️ 获取任务状态失败 taskID=%s err=%v", taskID, err)
			continue
		}

		status := model.TaskStatus(fields["status"])
		updatedAt, _ := strconv.ParseInt(fields["updated_at"], 10, 64)
		triggerTime, _ := strconv.ParseInt(fields["trigger_time"], 10, 64)

		// 跳过终态
		if status == model.StatusSuccess || status == model.StatusDead {
			continue
		}

		// 计算"卡死"判定时间窗：
		// - DISPATCHED/RUNNING 超过 2 分钟未变更 → 视为卡死
		// - 其他状态立即恢复
		staleThreshold := int64(120) // 2 分钟
		if status == model.StatusDispatched || status == model.StatusRunning {
			if now-updatedAt < staleThreshold {
				continue // 还在合理执行时间内，跳过
			}
		}

		// 重新加入 Global Queue
		// 重新构造 Task 信息
		task := &model.Task{
			ID:          taskID,
			Name:        fields["name"],
			CallbackURL: fields["callback_url"],
			TriggerTime: triggerTime,
			Priority:    consts.PriorityLow, // 恢复任务统一为低优先级
			RetryCount:  0,
			MaxRetry:    consts.DefaultMaxRetry,
			Timeout:     30,
		}

		// 反序列化 triggerTime，如果 < 1e9 视为相对时间
		if triggerTime < 1000000000 {
			task.TriggerTime = now + triggerTime
		} else if triggerTime < now {
			// 触发时间已过，立即执行
			task.TriggerTime = now
		}

		taskJSON, _ := json.Marshal(task)
		_, err = database.RDB.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
			Score:  float64(task.TriggerTime),
			Member: string(taskJSON),
		}).Result()
		if err != nil {
			log.Printf("⚠️ 任务恢复加入 Global 失败 taskID=%s err=%v", taskID, err)
			continue
		}

		// 更新状态为 PENDING
		_ = f.saveState(ctx, task, model.StatusPending)
		recovered++
	}

	if err := iter.Err(); err != nil && err != redis.Nil {
		return recovered, err
	}

	return recovered, nil
}

// CleanTerminalStates 清理终态任务的状态记录（节省 Redis 内存）
func (f *FSM) CleanTerminalStates(ctx context.Context, beforeTime int64) (int, error) {
	pattern := fmt.Sprintf("%s:*", consts.TaskStateKeyPrefix)
	iter := database.RDB.Scan(ctx, 0, pattern, 1000).Iterator()

	cleaned := 0
	for iter.Next(ctx) {
		key := iter.Val()
		fields, err := database.RDB.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}

		status := model.TaskStatus(fields["status"])
		if status != model.StatusSuccess && status != model.StatusDead {
			continue
		}

		updatedAt, _ := strconv.ParseInt(fields["updated_at"], 10, 64)
		if updatedAt > beforeTime {
			continue
		}

		// 删除 1 小时前的终态记录
		if _, err := database.RDB.Del(ctx, key).Result(); err == nil {
			cleaned++
		}
	}

	return cleaned, iter.Err()
}

// MonitorStaleStates 监控卡死任务（DISPATCHED/RUNNING 超过阈值未更新）
// 调用方应在独立 goroutine 中定期执行
func (f *FSM) MonitorStaleStates(ctx context.Context, staleTimeout time.Duration) (int, error) {
	pattern := fmt.Sprintf("%s:*", consts.TaskStateKeyPrefix)
	iter := database.RDB.Scan(ctx, 0, pattern, 500).Iterator()

	recovered := 0
	now := time.Now().Unix()
	threshold := now - int64(staleTimeout.Seconds())

	for iter.Next(ctx) {
		key := iter.Val()
		fields, err := database.RDB.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}

		status := model.TaskStatus(fields["status"])
		// 只监控 DISPATCHED 和 RUNNING 状态
		if status != model.StatusDispatched && status != model.StatusRunning {
			continue
		}

		updatedAt, _ := strconv.ParseInt(fields["updated_at"], 10, 64)
		if updatedAt > threshold {
			continue // 未超时
		}

		// 任务卡死，重新加入 Global Queue 等待重新调度
		taskID := strings.TrimPrefix(key, consts.TaskStateKeyPrefix+":")
		triggerTime, _ := strconv.ParseInt(fields["trigger_time"], 10, 64)

		task := &model.Task{
			ID:          taskID,
			Name:        fields["name"],
			CallbackURL: fields["callback_url"],
			TriggerTime: triggerTime,
			Priority:    consts.PriorityLow,
			MaxRetry:    consts.DefaultMaxRetry,
			Timeout:     30,
		}

		if triggerTime < now {
			task.TriggerTime = now // 立即执行
		}

		taskJSON, _ := json.Marshal(task)
		_, err = database.RDB.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
			Score:  float64(task.TriggerTime),
			Member: string(taskJSON),
		}).Result()
		if err != nil {
			log.Printf("⚠️ 卡死任务重新入队失败 taskID=%s err=%v", taskID, err)
			continue
		}

		// 状态回退到 PENDING
		_ = f.saveState(ctx, task, model.StatusPending)
		recovered++
		log.Printf("⏰ 检测到卡死任务 taskID=%s status=%s 已重新入队", taskID, status)
	}

	return recovered, iter.Err()
}
