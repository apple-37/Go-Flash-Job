package model

import "encoding/json"

// TaskStatus 任务状态机状态
type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"     // 已创建，等待触发时间到达
	StatusReady      TaskStatus = "READY"       // 已到期，等待调度
	StatusDispatched TaskStatus = "DISPATCHED"  // 已推送 MQ，等待执行
	StatusRunning    TaskStatus = "RUNNING"     // 执行器正在执行
	StatusSuccess    TaskStatus = "SUCCESS"     // 执行成功（终态）
	StatusRetry      TaskStatus = "RETRY"       // 失败后等待重试
	StatusDead       TaskStatus = "DEAD"        // 死信（终态）
)

// Task 代表一个待执行的任务（调度器端）
type Task struct {
	ID           string `json:"id"`            // 任务唯一ID
	Name         string `json:"name"`          // 任务名称
	CallbackURL  string `json:"callback_url"`  // 业务方服务地址，executor 会 HTTP POST 调用
	TriggerTime  int64  `json:"trigger_time"`  // 触发时间戳（秒）
	Priority     string `json:"priority"`      // 优先级: High, Medium, Low
	RetryCount   int    `json:"retry_count"`   // 已重试次数
	MaxRetry     int    `json:"max_retry"`     // 最大重试次数
	Timeout      int             `json:"timeout"`       // 执行超时（秒）
	Payload      json.RawMessage `json:"payload"`       // 业务参数（任意 JSON），会作为 HTTP body 转发给 CallbackURL
	EnqueueAt    int64           `json:"enqueue_at"`    // 入队时间戳（秒），用于同优先级 FIFO 兜底
}

// TaskCommand 是推送到 MQ 的任务消息体。
type TaskCommand struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	CallbackURL string `json:"callback_url"`
	TriggerTime int64  `json:"trigger_time"`
	Priority    string `json:"priority"`
	RetryCount  int    `json:"retry_count"`
	MaxRetry    int    `json:"max_retry"`
	Timeout     int             `json:"timeout"`
	Payload     json.RawMessage `json:"payload"`
}

// TaskState 用于持久化任务状态到 Redis Hash
type TaskState struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	CallbackURL string     `json:"callback_url"`
	Status      TaskStatus `json:"status"`
	RetryCount  int        `json:"retry_count"`
	MaxRetry    int        `json:"max_retry"`
	TriggerTime int64      `json:"trigger_time"`
	UpdatedAt   int64      `json:"updated_at"`
	ErrorMsg    string     `json:"error_msg"`
}
