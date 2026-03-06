package worker

import (
	"fmt"
	"sync"
)

// Pool 是一个自定义的协程池
type Pool struct {
	maxWorkers int
	semaphore  chan struct{}
	wg         sync.WaitGroup
}

// NewPool 创建一个指定最大并发数的协程池
func NewPool(maxWorkers int) *Pool {
	return &Pool{
		maxWorkers: maxWorkers,
		// 使用带缓冲的 channel 作为信号量，容量即为最大并发数
		semaphore: make(chan struct{}, maxWorkers),
	}
}

// Submit 提交一个任务到协程池执行
func (p *Pool) Submit(task func()) {
	// 1. 获取令牌 (Token)
	// 如果 channel 已满，这里会阻塞等待，从而实现对上游的"背压" (Backpressure)
	p.semaphore <- struct{}{}
	p.wg.Add(1)

	// 2. 启动 Goroutine 执行任务
	go func() {
		defer p.wg.Done()
		// 3. 任务执行完毕后，归还令牌
		defer func() { <-p.semaphore }()

		// 执行真正的业务逻辑
		task()
	}()
}

// Wait 等待池中所有任务执行完毕 (用于优雅停机)
func (p *Pool) Wait() {
	p.wg.Wait()
	fmt.Println("🛑 协程池内所有任务已执行完毕")
}