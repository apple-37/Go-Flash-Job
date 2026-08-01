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
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/mq"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// ExecutionLog 定义要发送到 Kafka 的日志结构
type ExecutionLog struct {
	JobID     string `json:"job_id"`
	Name      string `json:"name"`
	FuncName  string `json:"func_name"`
	Status    int    `json:"status"` // 0:成功, 1:失败, 2:死信
	CostMs    int64  `json:"cost_ms"`
	Retry     int    `json:"retry"`
	Timestamp int64  `json:"timestamp"`
	ErrorMsg  string `json:"error_msg"`
}

const dedupeTTL = 24 * time.Hour

var logPool = sync.Pool{
	New: func() interface{} {
		return &ExecutionLog{}
	},
}

func StartConsumer(ctx context.Context) {
	// 1. 注册默认任务执行函数
	worker.RegisterDefaults()

	ch := mq.RabbitChannel
	consumerTag := fmt.Sprintf("executor-%d", time.Now().UnixNano())

	// QoS 限流：prefetch=50，配合协程池背压
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
		false,            // auto-ack (⚠️ 手动 ack 保证至少消费一次)
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Fatalf("❌ 注册消费者失败: %v", err)
	}

	// 协程池：50 个 worker，与 QoS 匹配
	pool := worker.NewPool(50)

	fmt.Println("🎧 Executor 已启动，正在监听任务队列...")

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
			currentMsg := msg

			pool.Submit(func() {
				handleTask(ctx, cmd, currentMsg)
			})
		}
	}
}

// handleTask 处理单个任务（含幂等性、FSM 状态切换、重试、死信）
func handleTask(ctx context.Context, cmd model.TaskCommand, currentMsg amqpDelivery) {
	jobID := cmd.ID

	// 1. 幂等性检查：防止重复执行
	duplicate, err := checkAndMarkIdempotency(ctx, cmd)
	if err != nil {
		log.Printf("⚠️ 任务[%s]幂等检查失败，将重入队: %v", jobID, err)
		nackWithRequeue(currentMsg)
		return
	}
	if duplicate {
		log.Printf("↩️ 任务[%s]命中去重键，跳过重复执行", jobID)
		ackMsg(currentMsg)
		return
	}

	// 2. 状态机：DISPATCHED -> RUNNING
	if err := updateTaskState(ctx, jobID, model.StatusRunning); err != nil {
		log.Printf("⚠️ 任务[%s] FSM 状态更新失败: %v", jobID, err)
	}

	// 3. 获取执行函数
	fn, err := worker.Get(cmd.FuncName)
	if err != nil {
		// 函数不存在，直接进入死信
		log.Printf("💀 任务[%s]函数未注册: %s", jobID, cmd.FuncName)
		handleTaskDead(ctx, cmd, currentMsg, fmt.Sprintf("function not found: %s", cmd.FuncName))
		return
	}

	// 4. 执行任务（带超时控制）
	startTime := time.Now()
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(cmd.Timeout)*time.Second)
	defer cancel()

	execErr := executeWithTimeout(execCtx, fn, cmd.Payload)
	cost := time.Since(startTime).Milliseconds()

	// 5. 处理执行结果
	if execErr == nil {
		// 成功
		fmt.Printf("✅ 任务 [%s] 执行完毕，耗时: %d ms\n", jobID, cost)
		_ = updateTaskState(ctx, jobID, model.StatusSuccess)
		sendLogToKafka(ctx, cmd, 0, cost, "")
		ackMsg(currentMsg)
		return
	}

	// 失败：检查是否还能重试
	cmd.RetryCount++
	if cmd.RetryCount > cmd.MaxRetry {
		// 超过最大重试次数，进入死信
		log.Printf("💀 任务[%s] 已达最大重试次数 %d，进入死信: %v", jobID, cmd.MaxRetry, execErr)
		handleTaskDead(ctx, cmd, currentMsg, execErr.Error())
		return
	}

	// 重试：Nack 重新入队，由 MQ 重投
	log.Printf("⚠️ 任务[%s] 第 %d 次失败，将重试: %v", jobID, cmd.RetryCount, execErr)
	_ = updateTaskState(ctx, jobID, model.StatusRetry)
	sendLogToKafka(ctx, cmd, 1, cost, execErr.Error())
	nackWithRequeue(currentMsg)
}

// executeWithTimeout 带超时的任务执行
func executeWithTimeout(ctx context.Context, fn worker.TaskFunc, payload []byte) error {
	done := make(chan error, 1)
	go func() {
		done <- fn(payload)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("task timeout: %w", ctx.Err())
	case err := <-done:
		return err
	}
}

// handleTaskDead 处理死信任务
func handleTaskDead(ctx context.Context, cmd model.TaskCommand, currentMsg amqpDelivery, errMsg string) {
	// 1. 保存到死信队列
	task := &model.Task{
		ID:         cmd.ID,
		Name:       cmd.Name,
		FuncName:   cmd.FuncName,
		RetryCount: cmd.RetryCount,
		MaxRetry:   cmd.MaxRetry,
	}
	if err := saveDeadTask(ctx, task, errMsg); err != nil {
		log.Printf("⚠️ 任务[%s] 死信保存失败: %v", cmd.ID, err)
	}

	// 2. 状态机：标记为 DEAD
	_ = updateTaskState(ctx, cmd.ID, model.StatusDead)

	// 3. 发送死信日志
	sendLogToKafka(ctx, cmd, 2, 0, errMsg)

	// 4. Ack 消息（已进入死信，不再重投）
	ackMsg(currentMsg)
}

// saveDeadTask 保存死信任务到 Redis
func saveDeadTask(ctx context.Context, task *model.Task, errMsg string) error {
	detailKey := fmt.Sprintf("flash_job:dead_detail:%s", task.ID)
	if err := database.RDB.HSet(ctx, detailKey,
		"id", task.ID,
		"name", task.Name,
		"func_name", task.FuncName,
		"retry_count", task.RetryCount,
		"error", errMsg,
		"dead_at", time.Now().Unix(),
	).Err(); err != nil {
		return err
	}

	_, err := database.RDB.ZAdd(ctx, consts.JobDeadZSetKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.ID,
	}).Result()
	return err
}

// updateTaskState 更新任务状态到 Redis Hash
func updateTaskState(ctx context.Context, taskID string, status model.TaskStatus) error {
	if taskID == "" {
		return errors.New("empty task id")
	}
	key := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, taskID)
	_, err := database.RDB.HSet(ctx, key,
		"status", string(status),
		"updated_at", time.Now().Unix(),
	).Result()
	return err
}

// parseTaskCommand 解析 MQ 消息为 TaskCommand
func parseTaskCommand(body []byte) model.TaskCommand {
	var cmd model.TaskCommand
	if err := json.Unmarshal(body, &cmd); err == nil && cmd.ID != "" {
		return cmd
	}

	// 兼容旧版格式
	var legacy struct {
		JobID       string `json:"job_id"`
		TriggerTime int64  `json:"trigger_time"`
	}
	if err := json.Unmarshal(body, &legacy); err == nil && legacy.JobID != "" {
		return model.TaskCommand{
			ID:          legacy.JobID,
			Name:        "legacy",
			FuncName:    "mock_work",
			TriggerTime: legacy.TriggerTime,
			Priority:    consts.PriorityLow,
			MaxRetry:    consts.DefaultMaxRetry,
			Timeout:     30,
		}
	}

	// 纯文本兼容
	return model.TaskCommand{
		ID:       string(body),
		Name:     "legacy",
		FuncName: "mock_work",
		Priority: consts.PriorityLow,
		MaxRetry: consts.DefaultMaxRetry,
		Timeout:  30,
	}
}

// checkAndMarkIdempotency 幂等性检查
func checkAndMarkIdempotency(ctx context.Context, cmd model.TaskCommand) (bool, error) {
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

func dedupeKey(cmd model.TaskCommand) string {
	return fmt.Sprintf("%s:%s:%d", consts.ExecDedupeKeyPrefix, cmd.ID, cmd.TriggerTime)
}

// sendLogToKafka 发送日志到 Kafka
func sendLogToKafka(ctx context.Context, cmd model.TaskCommand, status int, costMs int64, errMsg string) error {
	if mq.KafkaProducer == nil {
		return errors.New("kafka producer is nil")
	}

	logData := logPool.Get().(*ExecutionLog)
	*logData = ExecutionLog{
		JobID:     cmd.ID,
		Name:      cmd.Name,
		FuncName:  cmd.FuncName,
		Status:    status,
		CostMs:    costMs,
		Retry:     cmd.RetryCount,
		Timestamp: time.Now().Unix(),
		ErrorMsg:  errMsg,
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

// amqpDelivery 抽象 amqp.Delivery，方便测试
type amqpDelivery interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
}

func ackMsg(msg amqpDelivery) {
	if err := msg.Ack(false); err != nil {
		log.Printf("❌ Ack 失败: %v", err)
	}
}

func nackWithRequeue(msg amqpDelivery) {
	if err := msg.Nack(false, true); err != nil {
		log.Printf("❌ Nack 失败: %v", err)
	}
}

// 随机抖动工具函数（重试时使用）
func jitterDuration(base time.Duration) time.Duration {
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	return base + jitter
}
