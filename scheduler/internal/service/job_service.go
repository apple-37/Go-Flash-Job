package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"

	"github.com/redis/go-redis/v9"
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

// LoadJobsFromFiles 从/data目录读取job文件并添加到系统中
func (s *JobService) LoadJobsFromFiles(ctx context.Context) error {
	log.Printf("📁 准备从/data目录读取job文件...")

	// 检查/data目录是否存在
	dataDir := "./data"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return fmt.Errorf("data目录不存在: %v", err)
	}

	// 读取/data目录下的所有文件
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("读取data目录失败: %v", err)
	}

	// 使用 Redis Pipeline 批量写入
	pipe := database.RDB.Pipeline()
	now := time.Now().Unix()

	// 解析每个job文件
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// 只处理.job文件
		if filepath.Ext(file.Name()) != ".job" {
			continue
		}

		// 读取文件内容
		filePath := filepath.Join(dataDir, file.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("⚠️ 读取文件 %s 失败: %v", file.Name(), err)
			continue
		}

		// 解析文件内容
		jobID, priority, err := s.parseJobFile(string(content))
		if err != nil {
			log.Printf("⚠️ 解析文件 %s 失败: %v", file.Name(), err)
			continue
		}

		// 生成任务ID
		taskID := fmt.Sprintf("job_%s", jobID)

		// 假定任务在未来 5 秒后触发
		triggerTime := now + 5

		// 将任务压入 Redis ZSet (Score = 触发时间戳)
		// 注意：这里我们将优先级信息存储在任务ID中，格式为 "job_[ID]_[Priority]"
		taskIDWithPriority := fmt.Sprintf("%s_%s", taskID, priority)
		pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(triggerTime),
			Member: taskIDWithPriority,
		})

		log.Printf("✅ 从文件 %s 读取任务: ID=%s, Priority=%s", file.Name(), jobID, priority)
	}

	// 执行 Pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("Redis Pipeline 写入失败: %v", err)
	}

	fmt.Printf("✅ 成功从/data目录加载任务到 Redis 全局队列\n")
	return nil
}

// parseJobFile 解析job文件内容，提取JobID和Priority
func (s *JobService) parseJobFile(content string) (jobID, priority string, err error) {
	lines := strings.Split(content, "\n")
	inJobID := false
	inPriority := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "[JobID]" {
			inJobID = true
			continue
		} else if line == "[Priority]" {
			inJobID = false
			inPriority = true
			continue
		} else if line == "[Created]" || line == "[Tasks]" {
			inJobID = false
			inPriority = false
			continue
		}

		if inJobID && line != "" {
			jobID = line
		} else if inPriority && line != "" {
			priority = line
		}
	}

	if jobID == "" || priority == "" {
		return "", "", fmt.Errorf("文件格式不正确，缺少JobID或Priority字段")
	}

	return jobID, priority, nil
}