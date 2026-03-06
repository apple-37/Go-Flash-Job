// 文件: cmd/scheduler/main.go
package main

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	v1 "go-flash-job/api/http/v1"
	"go-flash-job/internal/scheduler/core"
	"go-flash-job/pkg/config" // 引入 config
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
)

func main() {
	fmt.Println("🚀 Go-Flash-Job Scheduler 正在启动...")

	// 1. [重构] 加载配置
	config.InitConfig()

	// 2. [重构] 使用配置初始化基础设施
	database.InitMySQL(config.AppConfig.MySQL.DSN)
	database.InitRedis(config.AppConfig.Redis)
	mq.InitKafka(config.AppConfig.Kafka.Brokers)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)

	// 3. 启动核心调度引擎 (不变)
	dispatcher := core.NewDispatcher()
	dispatcher.Start()

	// 4. 启动 HTTP API Server (不变)
	r := gin.Default()
	v1.RegisterRoutes(r)

	// 5. [重构] 使用配置中的端口
	port := config.AppConfig.Server.Port
	fmt.Printf("🌟 Scheduler HTTP 服务启动于 %s\n", port)
	if err := r.Run(port); err != nil {
		log.Fatalf("❌ HTTP Server 启动失败: %v", err)
	}
}