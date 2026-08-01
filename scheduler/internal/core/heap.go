package core

import (
	"container/heap"

	"go-flash-job/pkg/model"
)

// TaskHeap 实现 container/heap 接口。
//
// 排序规则（定时任务核心语义：到期就执行）：
//  1. 触发时间早的在前（时间优先，保证定时精度）
//  2. 同触发时间，按优先级 High > Medium > Low（仅影响同批次顺序）
//  3. 同优先级，按入队时间早的在前（FIFO 兜底，防止同任务反复重入队饿死其他任务）
type TaskHeap []*model.Task

var priorityMap = map[string]int{
	"High":   3,
	"Medium": 2,
	"Low":    1,
}

func (h TaskHeap) Len() int { return len(h) }
func (h TaskHeap) Less(i, j int) bool {
	// 第 1 层：触发时间优先
	if h[i].TriggerTime != h[j].TriggerTime {
		return h[i].TriggerTime < h[j].TriggerTime
	}
	// 第 2 层：同触发时间，按优先级
	priI := priorityMap[h[i].Priority]
	priJ := priorityMap[h[j].Priority]
	if priI != priJ {
		return priI > priJ
	}
	// 第 3 层：同优先级，按入队时间 FIFO
	return h[i].EnqueueAt < h[j].EnqueueAt
}
func (h TaskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *TaskHeap) Push(x any) {
	*h = append(*h, x.(*model.Task))
}
func (h *TaskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Peek 查看堆顶元素但不弹出
func (h *TaskHeap) Peek() *model.Task {
	if h.Len() == 0 {
		return nil
	}
	return (*h)[0]
}

// Ensure init interface
var _ heap.Interface = (*TaskHeap)(nil)
