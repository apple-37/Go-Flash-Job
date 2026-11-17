package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
)

func main() {
	fmt.Println("Executor 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. 加载配置
	config.InitConfig()

	// 2. 使用配置初始化消息队列
	database.InitRedis(config.AppConfig.Redis)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)
	defer database.CloseRedis()
	defer mq.CloseRabbitMQ()

	// 3. 启动消费者并阻塞主线程
	startConsumer(ctx)
}
