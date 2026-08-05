package core

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"

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

// casScript 原子 CAS 状态变更 Lua 脚本（P4: 解决读-改-写竞态）
// KEYS[1]=task_state key, KEYS[2]=active_tasks, KEYS[3]=terminal_tasks
// ARGV[1]=expected status, ARGV[2]=new status, ARGV[3..9]=task fields
// 返回 1=成功, 0=状态已变（CAS 失败）
var casScript = redis.NewScript(`
	local cur = redis.call('HGET', KEYS[1], 'status')
	if cur ~= ARGV[1] then return 0 end

	redis.call('HSET', KEYS[1],
		'id', ARGV[3], 'name', ARGV[4], 'callback_url', ARGV[5],
		'status', ARGV[2], 'retry_count', ARGV[6],
		'max_retry', ARGV[7], 'trigger_time', ARGV[8], 'updated_at', ARGV[9])

	-- 终态：从 active 移到 terminal；非终态：更新 active 时间戳
	if ARGV[2] == 'SUCCESS' or ARGV[2] == 'DEAD' then
		redis.call('ZREM', KEYS[2], ARGV[3])
		redis.call('ZADD', KEYS[3], ARGV[9], ARGV[3])
	else
		redis.call('ZADD', KEYS[2], ARGV[9], ARGV[3])
	end
	return 1
`)

// Fire 触发事件，CAS 原子转换状态并持久化到 Redis
func (f *FSM) Fire(ctx context.Context, event Event, task *model.Task) error {
	current, err := f.GetState(ctx, task.ID)
	if err != nil {
		return fmt.Errorf("get current state failed: %w", err)
	}

	// 查找合法转换
	target, ok := f.findTransition(current, event)
	if !ok {
		return fmt.Errorf("illegal transition: %s --%s--> ?", current, event)
	}

	// P4: CAS 原子写入，避免并发 Fire 互相覆盖
	stateKey := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, task.ID)
	now := time.Now().Unix()
	result, err := casScript.Run(ctx, database.RDB,
		[]string{stateKey, consts.ActiveTasksKey, consts.TerminalTasksKey},
		string(current), string(target),
		task.ID, task.Name, task.CallbackURL,
		task.RetryCount, task.MaxRetry, task.TriggerTime, now,
	).Int64()
	if err != nil {
		return fmt.Errorf("cas state change failed: %w", err)
	}
	if result == 0 {
		// CAS 失败：状态已被其他协程修改，说明并发事件冲突
		return fmt.Errorf("cas failed: task %s state changed concurrently", task.ID)
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

// saveState 直接写入状态（无 CAS，用于恢复场景，不关心并发）
func (f *FSM) saveState(ctx context.Context, task *model.Task, status model.TaskStatus) error {
	key := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, task.ID)
	now := time.Now().Unix()
	pipe := database.RDB.Pipeline()
	pipe.HSet(ctx, key,
		"id", task.ID,
		"name", task.Name,
		"callback_url", task.CallbackURL,
		"status", string(status),
		"retry_count", task.RetryCount,
		"max_retry", task.MaxRetry,
		"trigger_time", task.TriggerTime,
		"updated_at", now,
	)
	// 非终态加入活跃索引，终态加入终态索引
	if status == model.StatusSuccess || status == model.StatusDead {
		pipe.ZRem(ctx, consts.ActiveTasksKey, task.ID)
		pipe.ZAdd(ctx, consts.TerminalTasksKey, redis.Z{
			Score:  float64(now),
			Member: task.ID,
		})
	} else {
		pipe.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
			Score:  float64(now),
			Member: task.ID,
		})
	}
	_, err := pipe.Exec(ctx)
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

	// 2. 加入死信队列 ZSet（member = jobID）
	_, err := database.RDB.ZAdd(ctx, consts.JobDeadZSetKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.ID,
	}).Result()
	return err
}

// RecoverStaleTasks 启动时扫描活跃任务索引，根据状态智能恢复
// M3: 用 active_tasks ZSet 替代 SCAN，O(N) N=活跃任务数，避免全库扫描
// - PENDING/READY/RETRY: 重新加入 Global Queue 等待调度
// - DISPATCHED/RUNNING: 视为卡死，重新加入 Global Queue
// - SUCCESS/DEAD: 不在 active_tasks 中（已移到 terminal_tasks），无需处理
func (f *FSM) RecoverStaleTasks(ctx context.Context) (int, error) {
	// M3: 从 active_tasks ZSet 获取所有活跃任务（替代 SCAN）
	jobIDs, err := database.RDB.ZRangeByScore(ctx, consts.ActiveTasksKey, &redis.ZRangeBy{
		Min: "0", Max: "+inf", Count: 10000,
	}).Result()
	if err != nil {
		return 0, err
	}

	recovered := 0
	now := time.Now().Unix()

	for _, jobID := range jobIDs {
		stateKey := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, jobID)
		fields, err := database.RDB.HGetAll(ctx, stateKey).Result()
		if err != nil {
			continue
		}

		status := model.TaskStatus(fields["status"])
		updatedAt, _ := strconv.ParseInt(fields["updated_at"], 10, 64)
		triggerTime, _ := strconv.ParseInt(fields["trigger_time"], 10, 64)

		// DISPATCHED/RUNNING 超过 2 分钟未变更 → 视为卡死
		staleThreshold := int64(120)
		if status == model.StatusDispatched || status == model.StatusRunning {
			if now-updatedAt < staleThreshold {
				continue
			}
		}

		// 优先从 detail Hash 读取完整任务（含 Payload）
		task, derr := model.GetTaskDetail(ctx, database.RDB, jobID)
		if derr != nil {
			// detail 丢失，用 state Hash 的字段重建（兜底）
			task = &model.Task{
				ID:          jobID,
				Name:        fields["name"],
				CallbackURL: fields["callback_url"],
				TriggerTime: triggerTime,
				Priority:    consts.PriorityLow,
				MaxRetry:    consts.DefaultMaxRetry,
				Timeout:     30,
			}
		}

		if triggerTime < now {
			task.TriggerTime = now // 立即执行
		}

		// member = jobID（P1 修复），详情已在 Hash 中
		_, err = database.RDB.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(task.TriggerTime),
			Member: jobID,
		}).Result()
		if err != nil {
			log.Printf("⚠️ 任务恢复加入 Global 失败 taskID=%s err=%v", jobID, err)
			continue
		}

		_ = f.saveState(ctx, task, model.StatusPending)
		recovered++
	}

	return recovered, nil
}

// CleanTerminalStates 清理终态任务的状态记录（节省 Redis 内存）
// M3: 从 terminal_tasks ZSet 获取过期终态任务（替代 SCAN）
func (f *FSM) CleanTerminalStates(ctx context.Context, beforeTime int64) (int, error) {
	// M3: 从 terminal_tasks ZSet 拉取过期的终态任务
	jobIDs, err := database.RDB.ZRangeByScore(ctx, consts.TerminalTasksKey, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(beforeTime, 10), Count: 1000,
	}).Result()
	if err != nil {
		return 0, err
	}

	pipe := database.RDB.Pipeline()
	for _, jobID := range jobIDs {
		stateKey := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, jobID)
		pipe.Del(ctx, stateKey)
		pipe.Del(ctx, model.DetailKey(jobID))
		pipe.ZRem(ctx, consts.TerminalTasksKey, jobID)
	}
	if len(jobIDs) == 0 {
		return 0, nil
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return len(jobIDs), nil
}

// MonitorStaleStates 监控卡死任务（DISPATCHED/RUNNING 超过阈值未更新）
// M3: 用 active_tasks ZSet 的 score（=updated_at）直接筛选，无需遍历所有 key
func (f *FSM) MonitorStaleStates(ctx context.Context, staleTimeout time.Duration) (int, error) {
	now := time.Now().Unix()
	threshold := now - int64(staleTimeout.Seconds())

	// M3: active_tasks 的 score = updated_at，直接按 score 筛选过期任务
	jobIDs, err := database.RDB.ZRangeByScore(ctx, consts.ActiveTasksKey, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(threshold, 10), Count: 1000,
	}).Result()
	if err != nil {
		return 0, err
	}

	recovered := 0
	for _, jobID := range jobIDs {
		stateKey := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, jobID)
		fields, err := database.RDB.HGetAll(ctx, stateKey).Result()
		if err != nil {
			continue
		}

		status := model.TaskStatus(fields["status"])
		// 只监控 DISPATCHED 和 RUNNING 状态
		if status != model.StatusDispatched && status != model.StatusRunning {
			continue
		}

		// 任务卡死，从 detail Hash 读取完整任务
		task, derr := model.GetTaskDetail(ctx, database.RDB, jobID)
		if derr != nil {
			triggerTime, _ := strconv.ParseInt(fields["trigger_time"], 10, 64)
			task = &model.Task{
				ID:          jobID,
				Name:        fields["name"],
				CallbackURL: fields["callback_url"],
				TriggerTime: triggerTime,
				Priority:    consts.PriorityLow,
				MaxRetry:    consts.DefaultMaxRetry,
				Timeout:     30,
			}
		}

		if task.TriggerTime < now {
			task.TriggerTime = now // 立即执行
		}

		// member = jobID
		_, err = database.RDB.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(task.TriggerTime),
			Member: jobID,
		}).Result()
		if err != nil {
			log.Printf("⚠️ 卡死任务重新入队失败 taskID=%s err=%v", jobID, err)
			continue
		}

		_ = f.saveState(ctx, task, model.StatusPending)
		recovered++
		log.Printf("⏰ 检测到卡死任务 taskID=%s status=%s 已重新入队", jobID, status)
	}

	return recovered, nil
}
