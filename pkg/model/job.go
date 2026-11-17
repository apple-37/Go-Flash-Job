package model

import "encoding/json"

// Task 代表一个待执行的任务（调度器端）
type Task struct {
	Generation  string          `json:"generation,omitempty"`
	LatestAt    int64           `json:"latest_at,omitempty"` // Unix seconds; zero disables freshness cutoff
	ID          string          `json:"id"`                  // 任务唯一ID
	Name        string          `json:"name"`                // 任务名称
	CallbackURL string          `json:"callback_url"`        // 业务方服务地址，executor 会 HTTP POST 调用
	TriggerTime int64           `json:"trigger_time"`        // 触发时间戳（秒）
	Priority    string          `json:"priority"`            // 兼容提交字段；就绪队列使用 FIFO
	RetryCount  int             `json:"retry_count"`         // 已重试次数
	MaxRetry    int             `json:"max_retry"`           // 最大重试次数
	Timeout     int             `json:"timeout"`             // 执行超时（秒）
	Payload     json.RawMessage `json:"payload"`             // 业务参数（任意 JSON），会作为 HTTP body 转发给 CallbackURL
	EnqueueAt   int64           `json:"enqueue_at"`          // 入队时间戳（秒），提交时间
}
