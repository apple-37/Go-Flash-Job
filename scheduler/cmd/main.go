// 文件: cmd/scheduler/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go-flash-job/pkg/config" // 引入 config
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
	"go-flash-job/scheduler/internal/api"
	"go-flash-job/scheduler/internal/core"
	"go-flash-job/scheduler/internal/service"

	"github.com/gin-gonic/gin"
)

func main() {
	fmt.Println("🚀 Go-Flash-Job Scheduler 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. [重构] 加载配置
	config.InitConfig()

	// 2. [重构] 使用配置初始化基础设施
	database.InitMySQL(config.AppConfig.MySQL.DSN)
	database.InitRedis(config.AppConfig.Redis)
	mq.InitKafka(config.AppConfig.Kafka.Brokers)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)
	defer database.CloseMySQL()
	defer database.CloseRedis()
	defer mq.CloseKafka()
	defer mq.CloseRabbitMQ()

	// 3. 启动核心调度引擎 (不变)
	dispatcher := core.NewDispatcher()
	dispatcher.Start(ctx)

	// 4. 从/data目录加载任务文件
	jobService := service.NewJobService()
	if err := jobService.LoadJobsFromFiles(ctx); err != nil {
		log.Printf("⚠️ 从/data目录加载任务文件失败: %v", err)
	} else {
		log.Println("✅ 成功从/data目录加载任务文件")
	}

	// 5. 启动 HTTP API Server (不变)
	r := gin.Default()
	api.RegisterRoutes(r)

	// 5. [重构] 使用配置中的端口
	port := config.AppConfig.Server.Port
	fmt.Printf("🌟 Scheduler HTTP 服务启动于 %s\n", port)

	srv := &http.Server{Addr: port, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ HTTP Server 启动失败: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("⚠️ HTTP Server 优雅停机失败: %v", err)
	}
	log.Println("🛑 Scheduler 已优雅停机")
}