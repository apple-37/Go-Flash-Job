package database

const (
	JobZSetKey          = "flash_job:global_queue"
	JobPendingZSetKey   = "flash_job:pending_queue"
	JobDeadZSetKey      = "flash_job:dead_queue"
	ActiveTasksKey      = "flash_job:active_tasks"
	TerminalTasksKey    = "flash_job:terminal_tasks"
	SchedulerLockKey    = "flash_job:scheduler_lock"
	SendingKey          = "flash_job:sending"
	TaskStateKeyPrefix  = "flash_job:task_state"
	TaskDetailKeyPrefix = "flash_job:task_detail"
	DeadDetailKeyPrefix = "flash_job:dead_detail"
)

func StateKey(id string) string      { return TaskStateKeyPrefix + ":" + id }
func DetailKey(id string) string     { return TaskDetailKeyPrefix + ":" + id }
func DeadDetailKey(id string) string { return DeadDetailKeyPrefix + ":" + id }
