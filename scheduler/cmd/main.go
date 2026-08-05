package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"
	"go-flash-job/scheduler/internal/api"
	"go-flash-job/scheduler/internal/core"

	"github.com/gin-gonic/gin"
)

func main() {
	fmt.Println("Scheduler 正在启动...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. 加载配置
	config.InitConfig()

	// 2. 使用配置初始化基础设施
	database.InitRedis(config.AppConfig.Redis)
	mq.InitKafka(config.AppConfig.Kafka.Brokers)
	mq.InitRabbitMQ(config.AppConfig.RabbitMQ.URL)
	defer database.CloseRedis()
	defer mq.CloseKafka()
	defer mq.CloseRabbitMQ()

	// 3. 启动 GMP 调度引擎
	// port 需在 NewScheduler 之前读取，用于构造 leader instanceID（hostname:port）
	port := config.AppConfig.Server.Port
	scheduler := core.NewScheduler(port)
	scheduler.Start(ctx)
	defer scheduler.Stop()

	// 4. 启动 HTTP API Server（业务方通过 API 提交任务）
	r := gin.Default()
	api.RegisterRoutes(r, scheduler)

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
