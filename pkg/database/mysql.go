// 文件: pkg/database/mysql.go
package database

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

var DB *gorm.DB

// InitMySQL 接收 DSN 字符串作为参数
func InitMySQL(dsn string) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("❌ MySQL 连接失败: %v", err)
	}
	DB = db
	fmt.Println("✅ MySQL 连接成功")
}