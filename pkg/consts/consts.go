package consts

const (
	// Redis Keys
	JobZSetKey        = "flash_job:global_queue"  // 全局任务队列 (ZSet)
	JobPendingZSetKey = "flash_job:pending_queue" // 待确认队列 (ZSet)
	ExecDedupeKeyPrefix = "flash_job:exec_dedupe" // 执行去重键前缀
	
	// RabbitMQ Queues
	TaskQueue = "flash_job:task_commands" // 任务指令队列
	
	// Kafka Topics
	JobLogTopic = "flash_job_logs"        // 任务执行日志主题
)