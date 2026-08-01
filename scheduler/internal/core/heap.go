package core

import (
	"container/heap"

	"go-flash-job/pkg/model"
)

// TaskHeap 实现 container/heap 接口。
// 排序规则：优先级高的在前，优先级相同时按触发时间早的在前
type TaskHeap []*model.Task

func (h TaskHeap) Len() int { return len(h) }
func (h TaskHeap) Less(i, j int) bool {
	// 首先按优先级排序
	priorityMap := map[string]int{
		"High":   3,
		"Medium": 2,
		"Low":    1,
	}
	priI := priorityMap[h[i].Priority]
	priJ := priorityMap[h[j].Priority]
	if priI != priJ {
		return priI > priJ
	}
	// 优先级相同时按触发时间排序
	return h[i].TriggerTime < h[j].TriggerTime
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
