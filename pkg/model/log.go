package model

import "time"

// JobLog 任务执行日志（用于 Kafka 传输和文件落盘）
type JobLog struct {
	ID          int64     `json:"id"`
	JobID       string    `json:"job_id"`
	Name        string    `json:"name"`
	CallbackURL string    `json:"callback_url"` // 业务方回调地址
	Status      int       `json:"status"`       // 0:成功, 1:失败, 2:死信
	CostMs      int64     `json:"cost_ms"`
	Retry       int       `json:"retry"`
	ErrorMsg    string    `json:"error_msg"`
	CreatedAt   time.Time `json:"created_at"`
}
