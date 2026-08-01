package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 全局指标定义。所有指标以 flash_ 为前缀，便于 Grafana 查询
var (
	// JobsSubmitted 提交的任务总数（Counter，按优先级区分）
	JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flash_jobs_submitted_total",
		Help: "Total number of jobs submitted via API",
	}, []string{"priority"})

	// JobsExecuted 执行结果总数（Counter，按状态区分：success/failed/dead）
	JobsExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flash_jobs_executed_total",
		Help: "Total number of jobs executed, by status",
	}, []string{"status"})

	// QueueSize 队列任务数（Gauge，按队列名区分）
	// queue label: global / pending / local_p0 / local_p1 / dead
	QueueSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flash_queue_size",
		Help: "Number of tasks in queues",
	}, []string{"queue"})

	// TaskDuration 任务执行耗时分布（Histogram，秒）
	// status label: success / failed / dead
	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flash_task_duration_seconds",
		Help:    "HTTP callback execution duration in seconds",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
	}, []string{"status"})

	// ScheduleLatency 调度延迟（Histogram，秒）
	// 从任务触发时间到实际开始执行的时间差
	ScheduleLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flash_schedule_latency_seconds",
		Help:    "Scheduling latency from trigger time to execution start",
		Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1, 5},
	}, []string{})

	// RetryCount 重试次数分布（Histogram）
	RetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flash_retry_count",
		Help:    "Retry count distribution for executed tasks",
		Buckets: []float64{0, 1, 2, 3, 4, 5},
	}, []string{})

	// MQPublishDuration MQ 投递耗时（Histogram）
	MQPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flash_mq_publish_duration_seconds",
		Help:    "RabbitMQ publish duration",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
	}, []string{})

	// RedisOpDuration Redis 操作耗时（Histogram，按操作类型区分）
	RedisOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flash_redis_op_duration_seconds",
		Help:    "Redis operation duration",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
	}, []string{"op"})

	// HTTPCallbackStatus HTTP 回调状态码分布（Counter）
	HTTPCallbackStatus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flash_http_callback_status_total",
		Help: "HTTP callback response status codes",
	}, []string{"status_class"}) // 2xx / 4xx / 5xx / timeout / error

	// PLocalQueueSize 各 P 的本地队列大小（Gauge）
	// 单独定义方便监控 P 的负载均衡情况
	PLocalQueueSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flash_p_local_queue_size",
		Help: "Local queue size of each P (processor)",
	}, []string{"p_id"})

	// WorkStealCount Work Stealing 次数（Counter）
	WorkStealCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flash_work_steal_total",
		Help: "Total work stealing count",
	}, []string{"thief_p_id"})
)
