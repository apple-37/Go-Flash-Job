package main

import (
	"fmt"
	"sync"
)

// workerPool 是一个自定义的协程池
type workerPool struct {
	semaphore chan struct{}
	wg        sync.WaitGroup
}

// newPool 创建一个指定最大并发数的协程池
func newPool(maxWorkers int) *workerPool {
	return &workerPool{
		// 使用带缓冲的 channel 作为信号量，容量即为最大并发数
		semaphore: make(chan struct{}, maxWorkers),
	}
}

// submit 提交一个任务到协程池执行
func (p *workerPool) submit(task func()) {
	// 1. 获取令牌 (Token)
	// 如果 channel 已满，这里会阻塞等待
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

// wait 等待池中所有任务执行完毕 (用于优雅停机)
func (p *workerPool) wait() {
	p.wg.Wait()
	fmt.Println("协程池内所有任务已执行完毕")
}
