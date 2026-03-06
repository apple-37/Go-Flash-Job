package store

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
)

const (
	BatchSize     = 100             // 满 100 条刷盘一次
	FlushInterval = 2 * time.Second // 或者最多等 2 秒刷盘一次
	KeepDays      = 7               // 日志仅保留 7 天
)

type LogBuffer struct {
	buffer[]model.SysJobLog
	mu     sync.Mutex
}

var globalBuffer *LogBuffer

// InitLogStorage 初始化存储和后台任务
func InitLogStorage() {
	globalBuffer = &LogBuffer{
		buffer: make([]model.SysJobLog, 0, BatchSize),
	}

	// 1. 启动定时刷盘协程
	go globalBuffer.tickerFlush()

	// 2. 启动自动清理协程 (解决你说的磁盘爆满问题)
	go startAutoCleaner()
	
	fmt.Println("✅ 异步批量日志写入器 & 自动清理器 已启动")
}

// AddLog 外部只需调用此方法，绝不阻塞主业务
func AddLog(logEntry model.SysJobLog) {
	globalBuffer.mu.Lock()
	defer globalBuffer.mu.Unlock()

	globalBuffer.buffer = append(globalBuffer.buffer, logEntry)

	// 达到阈值，立即刷盘
	if len(globalBuffer.buffer) >= BatchSize {
		globalBuffer.flush()
	}
}

// flush 将内存数据写入 MySQL (需在加锁状态下调用)
func (b *LogBuffer) flush() {
	if len(b.buffer) == 0 {
		return
	}

	// 浅拷贝当前数据并清空原 buffer，极速释放锁
	toInsert := b.buffer
	b.buffer = make([]model.SysJobLog, 0, BatchSize)

	// 异步交由 GORM 执行批量插入
	go func(data[]model.SysJobLog) {
		err := database.DB.CreateInBatches(data, len(data)).Error
		if err != nil {
			log.Printf("❌ 批量写入 MySQL 失败: %v\n", err)
		} else {
			fmt.Printf("💾 成功将 %d 条日志批量落库\n", len(data))
		}
	}(toInsert)
}

// tickerFlush 兜底机制：即使没满 100 条，时间到了也得刷入数据库
func (b *LogBuffer) tickerFlush() {
	ticker := time.NewTicker(FlushInterval)
	for range ticker.C {
		b.mu.Lock()
		b.flush()
		b.mu.Unlock()
	}
}

// startAutoCleaner 磁盘/数据守护神
func startAutoCleaner() {
	// 生产环境通常是每晚凌晨 3 点执行，这里为了演示，每小时检查一次
	ticker := time.NewTicker(1 * time.Hour) 
	for range ticker.C {
		deadline := time.Now().AddDate(0, 0, -KeepDays)
		fmt.Printf("🧹 触发自动清理机制，删除 %v 之前的过期日志...\n", deadline.Format("2006-01-02"))

		// 物理删除老数据（企业里也可以选择导入到 S3/冷存 然后再删）
		res := database.DB.Where("created_at < ?", deadline).Delete(&model.SysJobLog{})
		if res.Error != nil {
			log.Printf("⚠️ 清理过期日志失败: %v", res.Error)
		} else {
			fmt.Printf("✅ 成功清理 %d 条过期日志，释放磁盘空间\n", res.RowsAffected)
		}
	}
}