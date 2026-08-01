package model

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-flash-job/pkg/consts"

	"github.com/redis/go-redis/v9"
)

// DetailKey 返回任务详情 Hash 的 key
func DetailKey(jobID string) string {
	return fmt.Sprintf("%s:%s", consts.TaskDetailKeyPrefix, jobID)
}

// SaveTaskDetail 保存完整任务详情到 Redis Hash
// ZSet 只存 jobID（保证重试时去重），详情存这里
func SaveTaskDetail(ctx context.Context, rdb *redis.Client, task *Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task failed: %w", err)
	}
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, DetailKey(task.ID), "data", taskJSON)
	// 同时记录到活跃任务索引（score=当前时间，供 MonitorStaleStates 快速扫描）
	pipe.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.ID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// GetTaskDetail 从 Redis Hash 读取任务详情
func GetTaskDetail(ctx context.Context, rdb *redis.Client, jobID string) (*Task, error) {
	data, err := rdb.HGet(ctx, DetailKey(jobID), "data").Bytes()
	if err != nil {
		return nil, err
	}
	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshal task failed: %w", err)
	}
	return &task, nil
}

// DelTaskDetail 删除任务详情（任务进入终态后清理）
func DelTaskDetail(ctx context.Context, rdb *redis.Client, jobID string) error {
	pipe := rdb.Pipeline()
	pipe.Del(ctx, DetailKey(jobID))
	pipe.ZRem(ctx, consts.ActiveTasksKey, jobID)
	_, err := pipe.Exec(ctx)
	return err
}

// TouchActiveTask 更新活跃任务索引的时间戳（状态变更时调用）
func TouchActiveTask(ctx context.Context, rdb *redis.Client, jobID string) error {
	return rdb.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: jobID,
	}).Err()
}
