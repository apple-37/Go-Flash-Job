package model

import "time"

type SysJobLog struct {
	ID        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	JobID     string    `gorm:"size:64;not null;index" json:"job_id"` // 注意：为了配合我们模拟的 string id，这里改为 string
	Status    int       `gorm:"type:tinyint;not null" json:"status"`
	CostMs    int64     `gorm:"not null" json:"cost_ms"`
	CreatedAt time.Time `gorm:"index" json:"created_at"` // 加索引方便按时间清理
}

func (SysJobLog) TableName() string {
	return "sys_job_log"
}