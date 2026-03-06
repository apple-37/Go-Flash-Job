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