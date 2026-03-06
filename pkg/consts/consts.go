package consts

const (
	// Redis Keys
	JobZSetKey = "flash_job:global_queue" // 全局任务队列 (ZSet)
	
	// RabbitMQ Queues
	TaskQueue = "flash_job:task_commands" // 任务指令队列
	
	// Kafka Topics
	JobLogTopic = "flash_job_logs"        // 任务执行日志主题
)