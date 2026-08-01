package model

import "time"

// SysJob 对应 MySQL 中的 sys_job 表
type SysJob struct {
	ID              int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Title           string    `gorm:"size:128;not null" json:"title"`
	CronExpr        string    `gorm:"size:64;not null" json:"cron_expr"`
	ServiceUrl      string    `gorm:"size:255;not null" json:"service_url"`
	Method          string    `gorm:"size:10;default:'GET'" json:"method"`
	Status          int       `gorm:"type:tinyint;default:1" json:"status"`
	NextTriggerTime int64     `gorm:"index" json:"next_trigger_time"` // 秒级时间戳
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// TableName 指定 GORM 表名
func (SysJob) TableName() string {
	return "sys_job"
}

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
	ID          string `json:"id"`           // 任务唯一ID
	Name        string `json:"name"`         // 任务名称
	FuncName    string `json:"func_name"`    // 执行函数名（executor 端注册）
	TriggerTime int64  `json:"trigger_time"` // 触发时间戳（秒）
	Priority    string `json:"priority"`     // 优先级: High, Medium, Low
	RetryCount  int    `json:"retry_count"`  // 已重试次数
	MaxRetry    int    `json:"max_retry"`    // 最大重试次数
	Timeout     int    `json:"timeout"`      // 执行超时（秒）
	Payload     []byte `json:"payload"`      // 业务参数（JSON）
}

// TaskCommand 是推送到 MQ 的任务消息体。
type TaskCommand struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	FuncName    string `json:"func_name"`
	TriggerTime int64  `json:"trigger_time"`
	Priority    string `json:"priority"`
	RetryCount  int    `json:"retry_count"`
	MaxRetry    int    `json:"max_retry"`
	Timeout     int    `json:"timeout"`
	Payload     []byte `json:"payload"`
}

// TaskState 用于持久化任务状态到 Redis Hash
type TaskState struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	FuncName    string      `json:"func_name"`
	Status      TaskStatus  `json:"status"`
	RetryCount  int         `json:"retry_count"`
	MaxRetry    int         `json:"max_retry"`
	TriggerTime int64       `json:"trigger_time"`
	UpdatedAt   int64       `json:"updated_at"`
	ErrorMsg    string      `json:"error_msg"`
}
