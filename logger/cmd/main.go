package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"go-flash-job/logger/internal/store"
	"go-flash-job/pkg/config"
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/model"

	"github.com/IBM/sarama"
)

// KafkaLogMsg 对应我们在 Executor 发出的 JSON 结构
type KafkaLogMsg struct {
	JobID     string `json:"job_id"`
	Name      string `json:"name"`
	FuncName  string `json:"func_name"`
	Status    int    `json:"status"`
	CostMs    int64  `json:"cost_ms"`
	Retry     int    `json:"retry"`
	Timestamp int64  `json:"timestamp"`
	ErrorMsg  string `json:"error_msg"`
}

func main() {
	fmt.Println("Logger 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. 加载配置
	config.InitConfig()

	// 2. 初始化文件日志存储
	store.InitLogStorage()
	defer store.StopLogStorage()

	// 3. 启动 Kafka 消费者
	brokers := config.AppConfig.Kafka.Brokers
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("❌ 创建 Kafka 消费者失败: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(consts.JobLogTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("❌ 订阅 Kafka Topic 分区失败: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("🎧 Logger 已启动，正在监听 Kafka Topic: [%s]\n", consts.JobLogTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Logger 收到退出信号")
			return
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				log.Println("⚠️ Kafka 分区通道已关闭")
				return
			}

			var logMsg KafkaLogMsg
			if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
				log.Printf("⚠️ 反序列化日志失败: %v", err)
				continue
			}

			logEntry := model.SysJobLog{
				JobID:     logMsg.JobID,
				Name:      logMsg.Name,
				FuncName:  logMsg.FuncName,
				Status:    logMsg.Status,
				CostMs:    logMsg.CostMs,
				Retry:     logMsg.Retry,
				ErrorMsg:  logMsg.ErrorMsg,
				CreatedAt: time.Unix(logMsg.Timestamp, 0),
			}

			store.AddLog(logEntry)
		}
	}
}
