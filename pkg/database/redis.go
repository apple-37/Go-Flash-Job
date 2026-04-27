// 文件: pkg/database/redis.go
package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-flash-job/pkg/config" // 引入 config 包

	"github.com/redis/go-redis/v9"
)

var RDB *redis.Client
var ctx = context.Background()

// InitRedis 接收 RedisConfig 结构体作为参数
func InitRedis(cfg config.RedisConfig) {
	var err error
	for attempt := 1; attempt <= maxInitAttempts; attempt++ {
		RDB = redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		})

		err = RDB.Ping(ctx).Err()
		if err == nil {
			fmt.Println("✅ Redis 连接成功")
			return
		}

		delay := retryDelay(attempt)
		log.Printf("⚠️ Redis 连接失败(第 %d/%d 次): %v，%v 后重试", attempt, maxInitAttempts, err, delay)
		time.Sleep(delay)
	}

	log.Fatalf("❌ Redis 初始化失败，已重试 %d 次: %v", maxInitAttempts, err)
}

func CloseRedis() {
	if RDB == nil {
		return
	}
	if err := RDB.Close(); err != nil {
		log.Printf("⚠️ 关闭 Redis 连接失败: %v", err)
	}
}