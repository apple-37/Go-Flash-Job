// 文件: pkg/database/mysql.go
package database

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var DB *gorm.DB

const (
	maxInitAttempts = 8
	baseRetryDelay  = time.Second
)

// InitMySQL 接收 DSN 字符串作为参数
func InitMySQL(dsn string) {
	var (
		db  *gorm.DB
		err error
	)

	for attempt := 1; attempt <= maxInitAttempts; attempt++ {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			DB = db
			fmt.Println("✅ MySQL 连接成功")
			return
		}

		delay := retryDelay(attempt)
		log.Printf("⚠️ MySQL 连接失败(第 %d/%d 次): %v，%v 后重试", attempt, maxInitAttempts, err, delay)
		time.Sleep(delay)
	}

	log.Fatalf("❌ MySQL 初始化失败，已重试 %d 次: %v", maxInitAttempts, err)
}

func CloseMySQL() {
	if DB == nil {
		return
	}
	sqlDB, err := DB.DB()
	if err != nil {
		log.Printf("⚠️ 获取 MySQL 原生连接失败: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Printf("⚠️ 关闭 MySQL 连接失败: %v", err)
	}
}

func retryDelay(attempt int) time.Duration {
	d := baseRetryDelay * time.Duration(1<<(attempt-1))
	if d > 10*time.Second {
		return 10 * time.Second
	}
	return d
}