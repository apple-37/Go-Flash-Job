package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
)

type JobService struct{}

func NewJobService() *JobService {
	return &JobService{}
}

// SeedFakeJobs 生成海量测试任务并压入 Redis (压测专用)
func (s *JobService) SeedFakeJobs(ctx context.Context, count int) error {
	log.Printf("🔥 准备瞬间注入 %d 个任务进行压测...", count)

	// 1. 使用 Redis Pipeline 批量写入，极大减少网络 RTT 开销
	pipe := database.RDB.Pipeline()

	now := time.Now().Unix()
	
	// 生成任务：模拟未来的 1~60 秒内随机触发
	for i := 0; i < count; i++ {
		// 模拟生成 jobID
		jobID := "seed_job_" + strconv.Itoa(i)
		
		// 假定任务在未来 5 秒后触发
		triggerTime := now + 5 

		// 将任务压入 Redis ZSet (Score = 触发时间戳)
		pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(triggerTime),
			Member: jobID,
		})
	}

	// 执行 Pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("Redis Pipeline 写入失败: %v", err)
	}

	fmt.Printf("✅ 成功将 %d 个任务瞬间压入 Redis 全局队列\n", count)
	return nil
}