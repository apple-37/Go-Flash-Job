// 文件: pkg/database/redis.go
package database

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"go-flash-job/pkg/config" // 引入 config 包
	"log"
)

var RDB *redis.Client
var ctx = context.Background()

// InitRedis 接收 RedisConfig 结构体作为参数
func InitRedis(cfg config.RedisConfig) {
	RDB = redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := RDB.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis 连接失败: %v", err)
	}
	fmt.Println("✅ Redis 连接成功")
}