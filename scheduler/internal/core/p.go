package core

import (
	"container/heap"
	"context"
	"log"
	"sync"
	"time"

	"go-flash-job/pkg/model"
)

// P 仿照 Go GMP 中的 P (Processor)，每个 P 拥有独立的本地队列
type P struct {
	ID          int
	localQueue  TaskHeap
	mu          sync.Mutex
	wakeCh      chan struct{} // 事件驱动：新任务到达时唤醒
	scheduler   *Scheduler    // 反向引用调度器
	stopCh      chan struct{}
}

// NewP 创建一个 P 实例
func NewP(id int, sched *Scheduler) *P {
	return &P{
		ID:         id,
		localQueue: make(TaskHeap, 0),
		wakeCh:     make(chan struct{}, 1),
		scheduler:  sched,
		stopCh:     make(chan struct{}),
	}
}

// Push 添加任务到本地队列
func (p *P) Push(task *model.Task) {
	p.mu.Lock()
	heap.Push(&p.localQueue, task)
	p.mu.Unlock()
	p.wake()
}

// PushBatch 批量添加任务到本地队列
func (p *P) PushBatch(tasks []*model.Task) {
	p.mu.Lock()
	for _, t := range tasks {
		heap.Push(&p.localQueue, t)
	}
	p.mu.Unlock()
	p.wake()
}

// wake 唤醒 P 的执行循环
func (p *P) wake() {
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

// schedule 调度循环：选择下一个要执行的任务
// 仿 GMP 调度策略：
// 1. 优先从本地队列获取
// 2. 本地空，从其他 P 偷一半（Work Stealing）
// 注意：P 不再直接访问全局队列，统一由 fetcherLoop 分发
func (p *P) schedule() *model.Task {
	// 1. 优先本地队列
	p.mu.Lock()
	if p.localQueue.Len() > 0 {
		task := heap.Pop(&p.localQueue).(*model.Task)
		p.mu.Unlock()
		return task
	}
	p.mu.Unlock()

	// 2. 本地空，Work Stealing
	if stolen := p.scheduler.workSteal(p); len(stolen) > 0 {
		p.PushBatch(stolen[1:])
		return stolen[0]
	}

	return nil
}

// peekTop 查看本地堆顶任务（不弹出）
func (p *P) peekTop() *model.Task {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.localQueue.Peek()
}

// StealHalf 被其他 P 偷走一半任务
func (p *P) StealHalf() []*model.Task {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := p.localQueue.Len() / 2
	if n == 0 {
		return nil
	}

	stolen := make([]*model.Task, 0, n)
	for i := 0; i < n; i++ {
		task := heap.Pop(&p.localQueue).(*model.Task)
		stolen = append(stolen, task)
	}
	log.Printf("🦝 [P%d] 被偷走 %d 个任务", p.ID, n)
	return stolen
}

// LocalLen 返回本地队列长度
func (p *P) LocalLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.localQueue.Len()
}

// Run P 的执行循环（事件驱动 + 堆顶精确等待）
func (p *P) Run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		default:
		}

		// 查看堆顶任务
		topTask := p.peekTop()
		if topTask == nil {
			// 本地空，尝试调度获取任务
			task := p.schedule()
			if task == nil {
				// 真的没活了，等待唤醒
				select {
				case <-ctx.Done():
					return
				case <-p.stopCh:
					return
				case <-p.wakeCh:
				}
				continue
			}
			topTask = task
			// 直接处理这个任务
			p.executeTask(ctx, topTask)
			continue
		}

		// 检查堆顶任务是否到期
		now := time.Now().Unix()
		var waitDuration time.Duration
		if topTask.TriggerTime <= now {
			waitDuration = 0
		} else {
			waitDuration = time.Duration(topTask.TriggerTime-now) * time.Second
		}

		if waitDuration == 0 {
			// 到期，弹出并执行
			p.mu.Lock()
			taskToRun := heap.Pop(&p.localQueue).(*model.Task)
			p.mu.Unlock()
			p.executeTask(ctx, taskToRun)
			continue
		}

		// 精确等待：等到堆顶任务触发时间
		timer.Reset(waitDuration)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-p.stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
			// 时间到，执行堆顶任务
		case <-p.wakeCh:
			// 有新任务到达，重新计算等待时间
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// executeTask 执行任务：推送 MQ + 状态机切换
func (p *P) executeTask(ctx context.Context, task *model.Task) {
	// 1. 状态机：READY -> DISPATCHED
	// DISPATCH 失败说明状态机错乱（任务可能已被其他协程处理或 recovery 重置），
	// 跳过 MQ 推送避免重复执行
	if err := p.scheduler.fsm.Fire(ctx, EventDispatch, task); err != nil {
		log.Printf("⚠️ [P%d] FSM DISPATCH 失败 task=%s err=%v，跳过 MQ 推送", p.ID, task.ID, err)
		return
	}

	// 2. 推送到 RabbitMQ
	if err := p.scheduler.publishToMQ(ctx, task); err != nil {
		// 投递失败，重新放回本地队列，等待重试
		log.Printf("❌ [P%d] 推送 MQ 失败 task=%s err=%v", p.ID, task.ID, err)
		task.RetryCount++
		task.TriggerTime = time.Now().Add(p.scheduler.backoff(task.RetryCount)).Unix()
		p.Push(task)
		return
	}

	// 3. 从 Pending 队列移除（任务已成功投递）
	if err := p.scheduler.removeFromPending(ctx, task.ID); err != nil {
		log.Printf("⚠️ [P%d] Pending 移除失败 task=%s err=%v", p.ID, task.ID, err)
	}
}

// Stop 停止 P
func (p *P) Stop() {
	close(p.stopCh)
}
