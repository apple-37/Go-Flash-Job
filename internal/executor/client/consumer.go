package client

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
	"go-flash-job/internal/executor/worker"
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/mq"
)

// ExecutionLog 定义要发送到 Kafka 的日志结构
type ExecutionLog struct {
	JobID     string `json:"job_id"`
	Status    int    `json:"status"` // 0:成功, 1:失败
	CostMs    int64  `json:"cost_ms"`
	Timestamp int64  `json:"timestamp"`
}

func StartConsumer() {
	ch := mq.RabbitChannel

	// [核心面试点] QoS (Quality of Service) 设置
	// PrefetchCount = 50 意味着 RabbitMQ 最多只给这个消费者推送 50 条未经 Ack 的消息。
	// 如果消费者处理得慢，RabbitMQ 就会停止推送，将消息积压在 MQ 中，保护了 Executor 的内存。
	err := ch.Qos(
		50,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("❌ 设置 QoS 失败: %v", err)
	}

	msgs, err := ch.Consume(
		consts.TaskQueue, // queue
		"",               // consumer
		false,            // auto-ack (⚠️ 必须设为 false，我们要手动 Ack 保证至少消费一次)
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Fatalf("❌ 注册消费者失败: %v", err)
	}

	// 初始化一个容量为 50 的协程池 (与 QoS 匹配)
	pool := worker.NewPool(50)

	fmt.Println("🎧 Executor 已启动，正在监听任务队列...")

	// 持续监听消息通道
	for msg := range msgs {
		jobID := string(msg.Body)
		currentMsg := msg // 闭包变量捕获

		// 将任务提交给协程池
		// 如果此时有 50 个任务正在执行，Submit 会阻塞，这里的 for 循环也会暂停取消息
		pool.Submit(func() {
			startTime := time.Now()

			// --- 模拟真实业务逻辑 (如发起 HTTP 请求) ---
			// 这里我们用随机休眠 50~200ms 来模拟业务耗时
			simulateWorkDuration := time.Duration(rand.Intn(150)+50) * time.Millisecond
			time.Sleep(simulateWorkDuration)
			// ----------------------------------------

			cost := time.Since(startTime).Milliseconds()
			fmt.Printf("✅ 任务 [%s] 执行完毕，耗时: %d ms\n", jobID, cost)

			// 1. 业务执行成功后，手动向 RabbitMQ 发送 Ack (确认消费)
			// 如果中途宕机没有 Ack，MQ 会自动将消息重新入队，保证【At-least-once投递】
			currentMsg.Ack(false)

			// 2. 异步将执行日志发送到 Kafka
			sendLogToKafka(jobID, cost)
		})
	}
}

// sendLogToKafka 异步发送日志
func sendLogToKafka(jobID string, cost int64) {
	logData := ExecutionLog{
		JobID:     jobID,
		Status:    0, // 假设都成功
		CostMs:    cost,
		Timestamp: time.Now().Unix(),
	}
	
	bytes, _ := json.Marshal(logData)

	msg := &sarama.ProducerMessage{
		Topic: consts.JobLogTopic,
		Value: sarama.ByteEncoder(bytes),
	}

	// 异步发送，不阻塞 Executor 的当前协程
	mq.KafkaProducer.Input() <- msg
}