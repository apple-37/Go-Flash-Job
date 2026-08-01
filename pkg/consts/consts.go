package consts

const (
	// Redis Keys
	JobZSetKey          = "flash_job:global_queue"   // 全局任务队列 (ZSet)
	JobPendingZSetKey   = "flash_job:pending_queue"  // 待确认队列 (ZSet)
	JobDeadZSetKey      = "flash_job:dead_queue"     // 死信队列 (ZSet)
	ExecDedupeKeyPrefix = "flash_job:exec_dedupe"    // 执行去重键前缀
	TaskStateKeyPrefix  = "flash_job:task_state"     // 任务状态 Hash 前缀

	// ZSet 分片配置
	// 将 global_queue 拆成 N 个分片，避免单 ZSet 热点
	// 分片 key 格式: flash_job:global_queue:0, flash_job:global_queue:1, ...
	JobShardCount = 16
	JobShardPrefix = "flash_job:global_queue:" // 注意末尾冒号

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
