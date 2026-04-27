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
	BatchSize      = 100             // 满 100 条刷盘一次
	FlushInterval  = 2 * time.Second // 或者最多等 2 秒刷盘一次
	KeepDays       = 7               // 日志仅保留 7 天
	FlushQueueSize = 256             // 有界队列，保护内存
	FlushWorkers   = 2               // 固定 worker 数，防止 goroutine 爆炸
)

type LogBuffer struct {
	buffer     []model.SysJobLog
	flushQueue chan []model.SysJobLog
	stopCh     chan struct{}
	mu         sync.Mutex
	wg         sync.WaitGroup
}

var globalBuffer *LogBuffer

// InitLogStorage 初始化存储和后台任务。
func InitLogStorage() {
	globalBuffer = &LogBuffer{
		buffer:     make([]model.SysJobLog, 0, BatchSize),
		flushQueue: make(chan []model.SysJobLog, FlushQueueSize),
		stopCh:     make(chan struct{}),
	}

	for i := 0; i < FlushWorkers; i++ {
		globalBuffer.wg.Add(1)
		go globalBuffer.flushWorker(i)
	}

	globalBuffer.wg.Add(1)
	go globalBuffer.tickerFlush()

	globalBuffer.wg.Add(1)
	go startAutoCleaner(globalBuffer.stopCh, &globalBuffer.wg)

	fmt.Println("✅ 异步批量日志写入器 & 自动清理器 已启动")
}

// StopLogStorage 优雅停机，确保已入队日志尽量刷盘。
func StopLogStorage() {
	if globalBuffer == nil {
		return
	}

	globalBuffer.mu.Lock()
	globalBuffer.flushLocked()
	globalBuffer.mu.Unlock()

	close(globalBuffer.stopCh)
	globalBuffer.wg.Wait()
	close(globalBuffer.flushQueue)

	for batch := range globalBuffer.flushQueue {
		globalBuffer.persist(batch)
	}
}

// AddLog 外部只需调用此方法，绝不阻塞主业务。
func AddLog(logEntry model.SysJobLog) {
	if globalBuffer == nil {
		return
	}

	globalBuffer.mu.Lock()
	defer globalBuffer.mu.Unlock()

	globalBuffer.buffer = append(globalBuffer.buffer, logEntry)
	if len(globalBuffer.buffer) >= BatchSize {
		globalBuffer.flushLocked()
	}
}

// flushLocked 将内存数据入队，需在加锁状态下调用。
func (b *LogBuffer) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	batch := make([]model.SysJobLog, len(b.buffer))
	copy(batch, b.buffer)
	b.buffer = b.buffer[:0]

	select {
	case b.flushQueue <- batch:
	default:
		// 显式丢弃策略：队列满时丢弃当前批次，防止内存无限增长。
		log.Printf("⚠️ 日志刷盘队列已满，丢弃 %d 条日志", len(batch))
	}
}

func (b *LogBuffer) flushWorker(workerID int) {
	defer b.wg.Done()

	for {
		select {
		case <-b.stopCh:
			return
		case batch := <-b.flushQueue:
			b.persist(batch)
		}
	}
}

func (b *LogBuffer) persist(data []model.SysJobLog) {
	err := database.DB.CreateInBatches(data, len(data)).Error
	if err != nil {
		log.Printf("❌ 批量写入 MySQL 失败: %v", err)
		return
	}
	fmt.Printf("💾 成功将 %d 条日志批量落库\n", len(data))
}

// tickerFlush 兜底机制：即使没满 100 条，时间到了也得刷入数据库。
func (b *LogBuffer) tickerFlush() {
	defer b.wg.Done()

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			b.flushLocked()
			b.mu.Unlock()
		}
	}
}

// startAutoCleaner 磁盘/数据守护神。
func startAutoCleaner(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	// 生产环境通常是每晚凌晨 3 点执行，这里为了演示，每小时检查一次。
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			deadline := time.Now().AddDate(0, 0, -KeepDays)
			fmt.Printf("🧹 触发自动清理机制，删除 %v 之前的过期日志...\n", deadline.Format("2006-01-02"))

			res := database.DB.Where("created_at < ?", deadline).Delete(&model.SysJobLog{})
			if res.Error != nil {
				log.Printf("⚠️ 清理过期日志失败: %v", res.Error)
			} else {
				fmt.Printf("✅ 成功清理 %d 条过期日志，释放磁盘空间\n", res.RowsAffected)
			}
		}
	}
}
