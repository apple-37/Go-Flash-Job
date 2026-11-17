package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"

	"github.com/redis/go-redis/v9"
)

const defaultMaxRetry = 3

type JobService struct{}

func NewJobService() *JobService {
	return &JobService{}
}

// A job ID identifies one trigger, including client retries of the submission.
const createTask = `
 if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
 redis.call('HSET', KEYS[1], 'id', ARGV[1], 'status', 'PENDING', 'generation', ARGV[5],
   'updated_at', ARGV[4], 'trigger_time', ARGV[2], 'retry_count', 0)
 redis.call('HSET', KEYS[2], 'data', ARGV[3])
 redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
 redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
 return 1
`

func submit(ctx context.Context, tasks []model.Task) (int, error) {
	now := time.Now().Unix()
	pipe := database.RDB.Pipeline()
	var results []*redis.Cmd
	for i := range tasks {
		t := &tasks[i]
		if t.ID == "" || t.CallbackURL == "" || t.Timeout < 0 || t.MaxRetry < 0 || t.LatestAt < 0 {
			return 0, fmt.Errorf("invalid task at index %d", i)
		}
		if t.TriggerTime <= 0 {
			t.TriggerTime = now + 5
		} else if t.TriggerTime < 1e9 {
			t.TriggerTime += now
		}
		if t.Timeout == 0 {
			t.Timeout = 30
		}
		if t.MaxRetry == 0 {
			t.MaxRetry = defaultMaxRetry
		}
		t.EnqueueAt = now
		t.RetryCount = 0
		t.Generation = rand.Text()
		data, err := json.Marshal(t)
		if err != nil {
			return 0, err
		}
		keys := []string{database.StateKey(t.ID), database.DetailKey(t.ID), database.JobZSetKey, database.ActiveTasksKey}
		results = append(results, pipe.Eval(ctx, createTask, keys, t.ID, t.TriggerTime, data, now, t.Generation))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	count := 0
	for _, r := range results {
		n, err := r.Int()
		if err != nil {
			return count, err
		}
		count += n
	}
	return count, nil
}

var ErrDuplicate = fmt.Errorf("job ID already exists")

func (s *JobService) SubmitJob(ctx context.Context, t *model.Task) error {
	tasks := []model.Task{*t}
	n, err := submit(ctx, tasks)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrDuplicate
	}
	*t = tasks[0]
	return nil
}

func (s *JobService) BatchSubmitJobs(ctx context.Context, tasks []model.Task) (int, error) {
	return submit(ctx, tasks)
}

func (s *JobService) ListDeadJobs(ctx context.Context, start, stop int64) ([]map[string]interface{}, error) {
	// ZREVRANGE 按时间倒序返回 jobID 列表
	jobIDs, err := database.RDB.ZRevRange(ctx, database.JobDeadZSetKey, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("query dead queue failed: %w", err)
	}
	if len(jobIDs) == 0 {
		return nil, nil
	}

	// 批量拉取死信详情
	pipe := database.RDB.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(jobIDs))
	for i, jobID := range jobIDs {
		detailKey := database.DeadDetailKey(jobID)
		cmds[i] = pipe.HGetAll(ctx, detailKey)
	}
	_, _ = pipe.Exec(ctx)

	result := make([]map[string]interface{}, 0, len(jobIDs))
	for i, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			result = append(result, map[string]interface{}{"id": jobIDs[i], "msg": "detail missing"})
			continue
		}
		item := make(map[string]interface{}, len(fields)+1)
		for k, v := range fields {
			item[k] = v
		}
		item["id"] = jobIDs[i]
		result = append(result, item)
	}
	return result, nil
}
