// 文件: cmd/executor/main.go
package main

import (
	"fmt"
	"go-flash-job/internal/executor/client"
	"go-flash-job/pkg/config" // 引入 config
	"go-flash-job/pkg/mq"
)

func main() {
	fmt.Println("🚀 Go-Flash-Job Executor (执行器) 正在启动...")

	// 1. [重构] 加载配置
	config.InitConfig()

	// 2. [重构] 使用配置初始化消息队列
	mq.InitKafka(config.AppConfig.Kafka.Brokers)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)

	// 3. 启动消费者并阻塞主线程
	client.StartConsumer()
}