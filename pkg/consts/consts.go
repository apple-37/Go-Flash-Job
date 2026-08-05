package consts

const (
	// Redis Keys
	JobZSetKey          = "flash_job:global_queue"   // 全局任务队列 (ZSet)
	JobPendingZSetKey   = "flash_job:pending_queue"  // 待确认队列 (ZSet)
	JobDeadZSetKey      = "flash_job:dead_queue"     // 死信队列 (ZSet)
	ExecDedupeKeyPrefix = "flash_job:exec_dedupe"    // 执行去重键前缀
	TaskStateKeyPrefix  = "flash_job:task_state"     // 任务状态 Hash 前缀
	TaskDetailKeyPrefix = "flash_job:task_detail"    // 任务详情 Hash 前缀（存完整 Task JSON）
	ActiveTasksKey      = "flash_job:active_tasks"   // 活跃任务索引 ZSet（score=updated_at，非终态任务）
	TerminalTasksKey    = "flash_job:terminal_tasks" // 终态任务索引 ZSet（score=终态时间，供定时清理）
	SchedulerLockKey    = "flash_job:scheduler_lock" // scheduler 选主锁

	// RabbitMQ Queues
	TaskQueue = "flash_job:task_commands" // 任务指令队列

	// Kafka Topics
	JobLogTopic = "flash_job_logs" // 任务执行日志主题

	// 任务优先级
	PriorityHigh   = "High"
	PriorityMedium = "Medium"
	PriorityLow    = "Low"

	// 默认最大重试次数
	DefaultMaxRetry = 3
)
