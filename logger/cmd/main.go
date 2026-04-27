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
	"go-flash-job/pkg/config" // 引入 config
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"

	"github.com/IBM/sarama"
)

// KafkaLogMsg 对应我们在 Executor 发出的 JSON 结构
type KafkaLogMsg struct {
	JobID     string `json:"job_id"`
	Status    int    `json:"status"`
	CostMs    int64  `json:"cost_ms"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	fmt.Println("🚀 Go-Flash-Job Logger (日志清洗服务) 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. [重构] 加载配置
	config.InitConfig()

	// 2. [重构] 使用配置初始化数据库和缓冲器
	database.InitMySQL(config.AppConfig.MySQL.DSN)
	store.InitLogStorage()
	defer database.CloseMySQL()
	defer store.StopLogStorage()

	// 3. [重构] 使用配置启动 Kafka 消费者
	brokers := config.AppConfig.Kafka.Brokers
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("❌ 创建 Kafka 消费者失败: %v", err)
	}
	defer consumer.Close()

	// 订阅 Topic 的所有分区
	partitionConsumer, err := consumer.ConsumePartition(consts.JobLogTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("❌ 订阅 Kafka Topic 分区失败: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("🎧 Logger 已启动，正在监听 Kafka Topic: [%s]\n", consts.JobLogTopic)

	// 3. 循环处理消息
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

			// a. 反序列化 JSON 消息
			var logMsg KafkaLogMsg
			if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
				log.Printf("⚠️ 反序列化日志失败: %v", err)
				continue
			}

			// b. 转换数据结构 (DTO -> Model)
			logEntry := model.SysJobLog{
				JobID:     logMsg.JobID,
				Status:    logMsg.Status,
				CostMs:    logMsg.CostMs,
				CreatedAt: time.Unix(logMsg.Timestamp, 0),
			}

			// c. 将日志写入内存缓冲，等待批量刷盘 (非阻塞)
			store.AddLog(logEntry)
		}
	}
}