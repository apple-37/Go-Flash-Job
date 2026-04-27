package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go-flash-job/executor/internal/worker"
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/mq"

	"github.com/IBM/sarama"
)

// ExecutionLog 定义要发送到 Kafka 的日志结构
type ExecutionLog struct {
	JobID     string `json:"job_id"`
	Status    int    `json:"status"` // 0:成功, 1:失败
	CostMs    int64  `json:"cost_ms"`
	Timestamp int64  `json:"timestamp"`
}

// TaskCommand 对应 scheduler 推送到 RabbitMQ 的消息体。
type TaskCommand struct {
	JobID       string `json:"job_id"`
	TriggerTime int64  `json:"trigger_time"`
}

const dedupeTTL = 24 * time.Hour

var logPool = sync.Pool{
	New: func() interface{} {
		return &ExecutionLog{}
	},
}

func StartConsumer(ctx context.Context) {
	ch := mq.RabbitChannel
	consumerTag := fmt.Sprintf("executor-%d", time.Now().UnixNano())

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
		consumerTag,
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
	for {
		select {
		case <-ctx.Done():
			if err := ch.Cancel(consumerTag, false); err != nil {
				log.Printf("⚠️ 取消 RabbitMQ 消费失败: %v", err)
			}
			pool.Wait()
			return
		case msg, ok := <-msgs:
			if !ok {
				pool.Wait()
				return
			}

			cmd := parseTaskCommand(msg.Body)
			jobID := cmd.JobID
			currentMsg := msg // 闭包变量捕获

			pool.Submit(func() {
				duplicate, err := checkAndMarkIdempotency(ctx, cmd)
				if err != nil {
					log.Printf("⚠️ 任务[%s]幂等检查失败，将重入队: %v", jobID, err)
					if nackErr := currentMsg.Nack(false, true); nackErr != nil {
						log.Printf("❌ Nack 失败: %v", nackErr)
					}
					return
				}

				if duplicate {
					log.Printf("↩️ 任务[%s]命中去重键，跳过重复执行", jobID)
					if err := currentMsg.Ack(false); err != nil {
						log.Printf("❌ Ack 失败: %v", err)
					}
					return
				}

				startTime := time.Now()

				// --- 模拟真实业务逻辑 (如发起 HTTP 请求) ---
				simulateWorkDuration := time.Duration(rand.Intn(150)+50) * time.Millisecond
				time.Sleep(simulateWorkDuration)
				// ----------------------------------------

				cost := time.Since(startTime).Milliseconds()
				fmt.Printf("✅ 任务 [%s] 执行完毕，耗时: %d ms\n", jobID, cost)

				// 先写日志，再 Ack：避免 Ack 成功后日志丢失。
				if err := sendLogToKafka(ctx, jobID, cost); err != nil {
					log.Printf("⚠️ 任务[%s]日志发送失败，消息将重新入队: %v", jobID, err)
					if nackErr := currentMsg.Nack(false, true); nackErr != nil {
						log.Printf("❌ Nack 失败: %v", nackErr)
					}
					return
				}

				if err := currentMsg.Ack(false); err != nil {
					log.Printf("❌ Ack 失败: %v", err)
				}
			})
		}
	}
}

func parseTaskCommand(body []byte) TaskCommand {
	var cmd TaskCommand
	if err := json.Unmarshal(body, &cmd); err == nil && cmd.JobID != "" {
		return cmd
	}

	// 兼容旧版本纯文本消息。
	return TaskCommand{JobID: string(body), TriggerTime: 0}
}

func checkAndMarkIdempotency(ctx context.Context, cmd TaskCommand) (bool, error) {
	if database.RDB == nil {
		return false, errors.New("redis client is nil")
	}

	checkCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	created, err := database.RDB.SetNX(checkCtx, dedupeKey(cmd), 1, dedupeTTL).Result()
	if err != nil {
		return false, err
	}
	return !created, nil
}

func dedupeKey(cmd TaskCommand) string {
	return fmt.Sprintf("%s:%s:%d", consts.ExecDedupeKeyPrefix, cmd.JobID, cmd.TriggerTime)
}

// sendLogToKafka 将日志放入 Kafka 输入通道，超时则快速失败，避免 worker 卡死。
func sendLogToKafka(ctx context.Context, jobID string, cost int64) error {
	if mq.KafkaProducer == nil {
		return errors.New("kafka producer is nil")
	}

	logData := logPool.Get().(*ExecutionLog)
	*logData = ExecutionLog{
		JobID:     jobID,
		Status:    0,
		CostMs:    cost,
		Timestamp: time.Now().Unix(),
	}

	bytes, err := json.Marshal(logData)
	logPool.Put(logData)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: consts.JobLogTopic,
		Value: sarama.ByteEncoder(bytes),
	}

	sendCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	select {
	case <-sendCtx.Done():
		return fmt.Errorf("kafka enqueue timeout: %w", sendCtx.Err())
	case mq.KafkaProducer.Input() <- msg:
		return nil
	}
}