package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/shard"

	"github.com/redis/go-redis/v9"
)

type JobService struct{}

func NewJobService() *JobService {
	return &JobService{}
}

// SubmitJob 提交单个任务到全局队列
// 业务方通过 HTTP API 调用，任务到时间后由 scheduler 调度执行
func (s *JobService) SubmitJob(ctx context.Context, task *model.Task) error {
	// 字段校验
	if task.ID == "" {
		return fmt.Errorf("job id is required")
	}
	if task.CallbackURL == "" {
		return fmt.Errorf("callback_url is required")
	}
	if task.Priority == "" {
		task.Priority = consts.PriorityLow
	}
	if task.MaxRetry == 0 {
		task.MaxRetry = consts.DefaultMaxRetry
	}
	if task.Timeout == 0 {
		task.Timeout = 30
	}

	// 触发时间处理：<=0 或相对偏移量（<1e9）当作"X 秒后"
	now := time.Now().Unix()
	if task.TriggerTime <= 0 {
		task.TriggerTime = now + 5 // 默认 5 秒后执行
	} else if task.TriggerTime < 1e9 {
		task.TriggerTime = now + task.TriggerTime
	}
	task.EnqueueAt = now

	// 序列化任务
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task failed: %w", err)
	}

	// 写入 Redis 分片 ZSet（score = 触发时间戳，member = jobID）
	// jobID 作为 member 保证重试时去重（RetryCount 变化不影响 member）
	// 完整任务详情存独立 Hash，避免 member 膨胀 + 支持原地更新
	shardKey := shard.ShardKey(task.ID)
	pipe := database.RDB.Pipeline()
	pipe.ZAdd(ctx, shardKey, redis.Z{
		Score:  float64(task.TriggerTime),
		Member: task.ID,
	})
	pipe.HSet(ctx, model.DetailKey(task.ID), "data", taskJSON)
	pipe.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
		Score:  float64(now),
		Member: task.ID,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline failed: %w", err)
	}

	log.Printf("✅ 任务已提交: ID=%s Name=%s Priority=%s TriggerAt=%s CallbackURL=%s",
		task.ID, task.Name, task.Priority,
		time.Unix(task.TriggerTime, 0).Format("15:04:05"),
		task.CallbackURL)

	return nil
}

// BatchSubmitJobs 批量提交任务（用于业务方一次性注入大量任务，如 5000 个爬虫任务）
func (s *JobService) BatchSubmitJobs(ctx context.Context, tasks []model.Task) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}

	pipe := database.RDB.Pipeline()
	now := time.Now().Unix()
	count := 0

	for i := range tasks {
		task := &tasks[i]
		if task.ID == "" || task.CallbackURL == "" {
			continue
		}
		if task.Priority == "" {
			task.Priority = consts.PriorityLow
		}
		if task.MaxRetry == 0 {
			task.MaxRetry = consts.DefaultMaxRetry
		}
		if task.Timeout == 0 {
			task.Timeout = 30
		}
		if task.TriggerTime <= 0 {
			task.TriggerTime = now + 5
		} else if task.TriggerTime < 1e9 {
			task.TriggerTime = now + task.TriggerTime
		}
		task.EnqueueAt = now

		taskJSON, _ := json.Marshal(task)
		// member = jobID（去重），详情存 Hash
		pipe.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
			Score:  float64(task.TriggerTime),
			Member: task.ID,
		})
		pipe.HSet(ctx, model.DetailKey(task.ID), "data", taskJSON)
		pipe.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
			Score:  float64(now),
			Member: task.ID,
		})
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("no valid tasks")
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("redis pipeline failed: %w", err)
	}

	log.Printf("✅ 批量提交 %d 个任务", count)
	return count, nil
}
