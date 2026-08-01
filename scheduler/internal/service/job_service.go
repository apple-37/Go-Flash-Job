package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"

	"github.com/redis/go-redis/v9"
)

type JobService struct{}

func NewJobService() *JobService {
	return &JobService{}
}

// SeedFakeJobs 生成海量测试任务并压入 Redis (压测专用)
func (s *JobService) SeedFakeJobs(ctx context.Context, count int) error {
	log.Printf("🔥 准备瞬间注入 %d 个任务进行压测...", count)

	pipe := database.RDB.Pipeline()
	now := time.Now().Unix()

	priorities := []string{consts.PriorityHigh, consts.PriorityMedium, consts.PriorityLow}

	for i := 0; i < count; i++ {
		taskID := fmt.Sprintf("seed_job_%d", i)
		// 随机触发时间：未来 1~30 秒
		offset := 1 + randInt(30)
		triggerTime := now + int64(offset)

		// 随机优先级
		priority := priorities[i%3]

		task := model.Task{
			ID:          taskID,
			Name:        "seed_task",
			FuncName:    "mock_work",
			TriggerTime: triggerTime,
			Priority:    priority,
			MaxRetry:    consts.DefaultMaxRetry,
			Timeout:     30,
		}

		// 序列化为 JSON 存入 ZSet member
		taskJSON, _ := json.Marshal(task)
		pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(triggerTime),
			Member: string(taskJSON),
		})
	}

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

	dataDir := "./data"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return fmt.Errorf("data目录不存在: %v", err)
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("读取data目录失败: %v", err)
	}

	pipe := database.RDB.Pipeline()
	now := time.Now().Unix()
	count := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) != ".job" {
			continue
		}

		filePath := filepath.Join(dataDir, file.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("⚠️ 读取文件 %s 失败: %v", file.Name(), err)
			continue
		}

		task, err := s.parseJobFile(string(content), now)
		if err != nil {
			log.Printf("⚠️ 解析文件 %s 失败: %v", file.Name(), err)
			continue
		}

		taskJSON, _ := json.Marshal(task)
		pipe.ZAdd(ctx, consts.JobZSetKey, redis.Z{
			Score:  float64(task.TriggerTime),
			Member: string(taskJSON),
		})

		log.Printf("✅ 从文件 %s 读取任务: ID=%s, Name=%s, Priority=%s, TriggerTime=%d",
			file.Name(), task.ID, task.Name, task.Priority, task.TriggerTime)
		count++
	}

	if count == 0 {
		log.Println("⚠️ 没有找到有效的 .job 文件")
		return nil
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("Redis Pipeline 写入失败: %v", err)
	}

	fmt.Printf("✅ 成功从/data目录加载 %d 个任务到 Redis 全局队列\n", count)
	return nil
}

// parseJobFile 解析job文件内容
// 文件格式：
// [JobID]
// 0
// [Name]
// send_email
// [FuncName]
// send_email
// [TriggerTime]
// 5
// [Priority]
// High
// [MaxRetry]
// 3
// [Timeout]
// 30
// [Payload]
// {"to":"user@example.com"}
func (s *JobService) parseJobFile(content string, now int64) (*model.Task, error) {
	lines := strings.Split(content, "\n")
	fields := make(map[string]string)

	currentSection := ""
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 检查是否是 section 标记
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			continue
		}

		if currentSection != "" {
			// 多行字段（如 Payload）使用追加
			if existing, ok := fields[currentSection]; ok {
				fields[currentSection] = existing + "\n" + line
			} else {
				fields[currentSection] = line
			}
		}
	}

	// 必填字段校验
	if fields["JobID"] == "" {
		return nil, fmt.Errorf("缺少 JobID 字段")
	}

	task := &model.Task{
		ID:       fields["JobID"],
		Name:     fields["Name"],
		FuncName: fields["FuncName"],
		Priority: fields["Priority"],
	}

	// 设置默认值
	if task.Name == "" {
		task.Name = "task_" + task.ID
	}
	if task.FuncName == "" {
		task.FuncName = "mock_work"
	}
	if task.Priority == "" {
		task.Priority = consts.PriorityLow
	}

	// 解析触发时间
	if fields["TriggerTime"] != "" {
		// 如果是相对时间（小于当前时间戳的整数），则当作偏移量
		triggerOffset, err := strconv.ParseInt(fields["TriggerTime"], 10, 64)
		if err == nil {
			if triggerOffset < 1000000000 {
				// 当作偏移量
				task.TriggerTime = now + triggerOffset
			} else {
				// 当作绝对时间戳
				task.TriggerTime = triggerOffset
			}
		}
	} else {
		// 默认 5 秒后触发
		task.TriggerTime = now + 5
	}

	// 解析最大重试次数
	if fields["MaxRetry"] != "" {
		if maxRetry, err := strconv.Atoi(fields["MaxRetry"]); err == nil {
			task.MaxRetry = maxRetry
		}
	} else {
		task.MaxRetry = consts.DefaultMaxRetry
	}

	// 解析超时
	if fields["Timeout"] != "" {
		if timeout, err := strconv.Atoi(fields["Timeout"]); err == nil {
			task.Timeout = timeout
		}
	} else {
		task.Timeout = 30
	}

	// 解析 Payload
	if fields["Payload"] != "" {
		task.Payload = []byte(fields["Payload"])
	}

	return task, nil
}

// randInt 返回 [0, n) 的随机整数
func randInt(n int) int {
	if n <= 0 {
		return 0
	}
	return int(time.Now().UnixNano()) % n
}
