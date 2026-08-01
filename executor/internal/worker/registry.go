package worker

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// TaskFunc 任务执行函数类型
type TaskFunc func(payload []byte) error

// registry 函数注册表
var registry = make(map[string]TaskFunc)

// Register 注册任务执行函数
func Register(name string, fn TaskFunc) {
	registry[name] = fn
}

// Get 根据函数名获取执行函数
func Get(name string) (TaskFunc, error) {
	fn, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("function %s not registered", name)
	}
	return fn, nil
}

// RegisterDefaults 注册默认的模拟函数（用于压测和演示）
func RegisterDefaults() {
	Register("mock_work", mockWork)
	Register("send_email", mockSendEmail)
	Register("generate_report", mockGenerateReport)
	Register("cleanup_db", mockCleanupDB)
}

// mockWork 模拟通用任务执行
// - 随机执行时间 (50-300ms)
// - 随机失败概率 20%（用于压测重试逻辑）
func mockWork(payload []byte) error {
	// 随机执行时间：50-300ms
	duration := time.Duration(50+rand.Intn(250)) * time.Millisecond
	time.Sleep(duration)

	// 20% 概率失败
	if rand.Float32() < 0.2 {
		return errors.New("mock_work random failure")
	}
	return nil
}

// mockSendEmail 模拟发送邮件
// - 随机执行时间：100-500ms
// - 10% 概率失败
func mockSendEmail(payload []byte) error {
	duration := time.Duration(100+rand.Intn(400)) * time.Millisecond
	time.Sleep(duration)

	if rand.Float32() < 0.1 {
		return errors.New("smtp server timeout")
	}
	return nil
}

// mockGenerateReport 模拟生成报告
// - 较长执行时间：200-1000ms
// - 5% 概率失败
func mockGenerateReport(payload []byte) error {
	duration := time.Duration(200+rand.Intn(800)) * time.Millisecond
	time.Sleep(duration)

	if rand.Float32() < 0.05 {
		return errors.New("database query failed")
	}
	return nil
}

// mockCleanupDB 模拟数据库清理
// - 50-200ms
// - 15% 概率失败
func mockCleanupDB(payload []byte) error {
	duration := time.Duration(50+rand.Intn(150)) * time.Millisecond
	time.Sleep(duration)

	if rand.Float32() < 0.15 {
		return errors.New("lock contention")
	}
	return nil
}
