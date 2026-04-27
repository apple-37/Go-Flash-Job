// 文件: cmd/executor/main.go
package main

import (
	"context"
	"fmt"
	"go-flash-job/executor/internal/client"
	"go-flash-job/pkg/config" // 引入 config
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("🚀 Go-Flash-Job Executor (执行器) 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. [重构] 加载配置
	config.InitConfig()

	// 2. [重构] 使用配置初始化消息队列
	database.InitRedis(config.AppConfig.Redis)
	mq.InitKafka(config.AppConfig.Kafka.Brokers)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)
	defer database.CloseRedis()
	defer mq.CloseKafka()
	defer mq.CloseRabbitMQ()

	// 3. 启动消费者并阻塞主线程
	client.StartConsumer(ctx)
}