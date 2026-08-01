package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go-flash-job/pkg/config"
	"go-flash-job/pkg/model"
)

const (
	BatchSize      = 100             // 满 100 条刷盘一次
	FlushInterval  = 2 * time.Second // 或者最多等 2 秒刷盘一次
	FlushQueueSize = 256             // 有界队列，保护内存
	FlushWorkers   = 2               // 固定 worker 数，防止 goroutine 爆炸
)

type LogBuffer struct {
	buffer     []model.JobLog
	flushQueue chan []model.JobLog
	stopCh     chan struct{}
	mu         sync.Mutex
	wg         sync.WaitGroup

	// 文件轮转相关
	dir          string
	maxSizeBytes int64
	maxKeepDays  int
	currentFile  *os.File
	currentSize  int64
	fileMu       sync.Mutex
}

var globalBuffer *LogBuffer

// InitLogStorage 初始化文件日志存储
func InitLogStorage() {
	cfg := config.AppConfig.Logger
	dir := cfg.Dir
	if dir == "" {
		dir = "./logs"
	}

	// 创建日志目录
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("❌ 创建日志目录失败: %v", err)
	}

	maxSize := int64(cfg.MaxSizeMB) * 1024 * 1024
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 默认 100MB
	}

	maxKeepDays := cfg.MaxKeepDays
	if maxKeepDays <= 0 {
		maxKeepDays = 7
	}

	globalBuffer = &LogBuffer{
		buffer:       make([]model.JobLog, 0, BatchSize),
		flushQueue:   make(chan []model.JobLog, FlushQueueSize),
		stopCh:       make(chan struct{}),
		dir:          dir,
		maxSizeBytes: maxSize,
		maxKeepDays:  maxKeepDays,
	}

	// 打开当前日志文件
	if err := globalBuffer.openCurrentFile(); err != nil {
		log.Fatalf("❌ 打开日志文件失败: %v", err)
	}

	// 启动 worker
	for i := 0; i < FlushWorkers; i++ {
		globalBuffer.wg.Add(1)
		go globalBuffer.flushWorker(i)
	}

	// 启动定时刷盘
	globalBuffer.wg.Add(1)
	go globalBuffer.tickerFlush()

	// 启动过期清理
	globalBuffer.wg.Add(1)
	go globalBuffer.startAutoCleaner()

	fmt.Printf("✅ 文件日志存储已启动 (dir=%s, maxSize=%dMB, keepDays=%d)\n",
		dir, cfg.MaxSizeMB, maxKeepDays)
}

// openCurrentFile 打开当前日志文件
func (b *LogBuffer) openCurrentFile() error {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()

	path := filepath.Join(b.dir, "job.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// 获取当前文件大小
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	b.currentFile = f
	b.currentSize = info.Size()
	return nil
}

// rotateFile 轮转日志文件
func (b *LogBuffer) rotateFile() error {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()

	if b.currentFile != nil {
		b.currentFile.Close()
	}

	// 重命名：job.log -> job-2026-04-27-1.log
	now := time.Now()
	dateStr := now.Format("2006-01-02")
	seq := 1
	for {
		newName := filepath.Join(b.dir, fmt.Sprintf("job-%s-%d.log", dateStr, seq))
		if _, err := os.Stat(newName); os.IsNotExist(err) {
			oldPath := filepath.Join(b.dir, "job.log")
			if err := os.Rename(oldPath, newName); err != nil {
				// 重命名失败，重新打开原文件
				return b.openCurrentFileLocked()
			}
			break
		}
		seq++
	}

	return b.openCurrentFileLocked()
}

// openCurrentFileLocked 已加锁状态下打开文件
func (b *LogBuffer) openCurrentFileLocked() error {
	path := filepath.Join(b.dir, "job.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	b.currentFile = f
	b.currentSize = info.Size()
	return nil
}

// writeBatch 写入一批日志到文件
func (b *LogBuffer) writeBatch(data []model.JobLog) error {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()

	for _, entry := range data {
		bytes, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		bytes = append(bytes, '\n')

		// 检查是否需要轮转
		if b.currentSize+int64(len(bytes)) > b.maxSizeBytes {
			if err := b.rotateFileLocked(); err != nil {
				return err
			}
		}

		n, err := b.currentFile.Write(bytes)
		if err != nil {
			return err
		}
		b.currentSize += int64(n)
	}
	return nil
}

// rotateFileLocked 已加锁状态下轮转文件
func (b *LogBuffer) rotateFileLocked() error {
	if b.currentFile != nil {
		b.currentFile.Close()
	}

	now := time.Now()
	dateStr := now.Format("2006-01-02")
	seq := 1
	for {
		newName := filepath.Join(b.dir, fmt.Sprintf("job-%s-%d.log", dateStr, seq))
		if _, err := os.Stat(newName); os.IsNotExist(err) {
			oldPath := filepath.Join(b.dir, "job.log")
			if err := os.Rename(oldPath, newName); err != nil {
				// 重命名失败，重新打开原文件继续写
				return b.openCurrentFileLocked()
			}
			break
		}
		seq++
	}

	return b.openCurrentFileLocked()
}

// StopLogStorage 优雅停机
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
		_ = globalBuffer.writeBatch(batch)
	}

	globalBuffer.fileMu.Lock()
	if globalBuffer.currentFile != nil {
		globalBuffer.currentFile.Close()
	}
	globalBuffer.fileMu.Unlock()
}

// AddLog 添加日志到缓冲区（非阻塞）
func AddLog(logEntry model.JobLog) {
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

// flushLocked 入队待写入的批次
func (b *LogBuffer) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	batch := make([]model.JobLog, len(b.buffer))
	copy(batch, b.buffer)
	b.buffer = b.buffer[:0]

	select {
	case b.flushQueue <- batch:
	default:
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
			if err := b.writeBatch(batch); err != nil {
				log.Printf("❌ 写入日志文件失败: %v", err)
			}
		}
	}
}

// tickerFlush 兜底机制：定时刷盘
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

// startAutoCleaner 定期清理过期日志
func (b *LogBuffer) startAutoCleaner() {
	defer b.wg.Done()

	// 启动时先清理一次
	b.cleanExpiredLogs()

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.cleanExpiredLogs()
		}
	}
}

// cleanExpiredLogs 清理过期日志文件
func (b *LogBuffer) cleanExpiredLogs() {
	deadline := time.Now().AddDate(0, 0, -b.maxKeepDays)

	files, err := os.ReadDir(b.dir)
	if err != nil {
		log.Printf("⚠️ 读取日志目录失败: %v", err)
		return
	}

	// 收集所有日志文件信息
	type fileInfo struct {
		name    string
		modTime time.Time
	}
	var fileInfos []fileInfo

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasPrefix(name, "job") || !strings.HasSuffix(name, ".log") {
			continue
		}
		// 跳过当前正在写的文件
		if name == "job.log" {
			continue
		}

		info, err := f.Info()
		if err != nil {
			continue
		}
		fileInfos = append(fileInfos, fileInfo{name: name, modTime: info.ModTime()})
	}

	// 按修改时间排序（旧的在前）
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].modTime.Before(fileInfos[j].modTime)
	})

	deletedCount := 0
	for _, fi := range fileInfos {
		if fi.modTime.Before(deadline) {
			path := filepath.Join(b.dir, fi.name)
			if err := os.Remove(path); err != nil {
				log.Printf("⚠️ 删除过期日志 %s 失败: %v", fi.name, err)
				continue
			}
			deletedCount++
		}
	}

	if deletedCount > 0 {
		fmt.Printf("🧹 清理 %d 个过期日志文件（截止 %s）\n", deletedCount, deadline.Format("2006-01-02"))
	}
}
