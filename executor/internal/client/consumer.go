package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"go-flash-job/executor/internal/worker"
	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/metrics"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/mq"
	"go-flash-job/pkg/shard"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// ExecutionLog 定义要发送到 Kafka 的日志结构
type ExecutionLog struct {
	JobID     string `json:"job_id"`
	Name      string `json:"name"`
	CallbackURL string `json:"callback_url"`
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

// httpClient 复用连接池，避免每次任务都建立新连接
var httpClient = &http.Client{
	Timeout: 60 * time.Second, // 全局兜底超时，单任务超时由 context 控制
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

func StartConsumer(ctx context.Context) {
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
		false,            // auto-ack (手动 ack 保证至少消费一次)
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

// handleTask 处理单个任务（含幂等性、FSM 状态切换、HTTP 回调、重试、死信）
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

	// 3. HTTP 回调业务方服务
	startTime := time.Now()
	execErr := executeHTTPCallback(ctx, cmd)
	cost := time.Since(startTime).Milliseconds()

	// 4. 处理执行结果
	if execErr == nil {
		// 成功
		fmt.Printf("✅ 任务 [%s] 执行完毕，耗时: %d ms\n", jobID, cost)
		metrics.JobsExecuted.WithLabelValues("success").Inc()
		metrics.TaskDuration.WithLabelValues("success").Observe(time.Since(startTime).Seconds())
		metrics.RetryCount.WithLabelValues().Observe(float64(cmd.RetryCount))
		if err := updateTaskState(ctx, jobID, model.StatusSuccess); err != nil {
			log.Printf("⚠️ 任务[%s] 状态更新为 SUCCESS 失败: %v", jobID, err)
		}
		if err := sendLogToKafka(ctx, cmd, 0, cost, ""); err != nil {
			log.Printf("⚠️ 任务[%s] 日志发送失败: %v", jobID, err)
		}
		ackMsg(currentMsg)
		return
	}

	// 失败：检查是否还能重试
	cmd.RetryCount++
	if cmd.RetryCount > cmd.MaxRetry {
		// 超过最大重试次数，进入死信
		log.Printf("💀 任务[%s] 已达最大重试次数 %d，进入死信: %v", jobID, cmd.MaxRetry, execErr)
		metrics.JobsExecuted.WithLabelValues("dead").Inc()
		metrics.TaskDuration.WithLabelValues("dead").Observe(time.Since(startTime).Seconds())
		handleTaskDead(ctx, cmd, currentMsg, execErr.Error())
		return
	}

	// 重试：写回 Redis ZSet，带指数退避延迟，由 scheduler 重新调度
	// 不再使用 nack(requeue=true)，避免立即重投导致雪崩
	// member = jobID（保证重试时去重，不会产生多份），详情更新到 Hash
	backoff := backoffDuration(cmd.RetryCount)
	retryAt := time.Now().Add(backoff).Unix()

	task := &model.Task{
		ID:          cmd.ID,
		Name:        cmd.Name,
		CallbackURL: cmd.CallbackURL,
		TriggerTime: retryAt,
		Priority:    cmd.Priority,
		RetryCount:  cmd.RetryCount,
		MaxRetry:    cmd.MaxRetry,
		Timeout:     cmd.Timeout,
		Payload:     cmd.Payload,
	}

	// 原子写入：ZAdd(jobID) + 更新详情 Hash + 更新活跃索引
	if err := model.SaveTaskDetail(ctx, database.RDB, task); err != nil {
		log.Printf("⚠️ 任务[%s] 详情更新失败: %v", jobID, err)
	}
	if err := database.RDB.ZAdd(ctx, shard.ShardKey(task.ID), redis.Z{
		Score:  float64(retryAt),
		Member: task.ID, // jobID 作 member，RetryCount 变化不影响去重
	}).Err(); err != nil {
		// 写回 Redis 失败，降级为 nack requeue（保证至少不丢任务）
		log.Printf("⚠️ 任务[%s] 重试入队失败，降级为 nack requeue: %v", jobID, err)
		nackWithRequeue(currentMsg)
		return
	}

	log.Printf("⏰ 任务[%s] 第 %d 次失败，%v 后重试: %v", jobID, cmd.RetryCount, backoff, execErr)
	metrics.JobsExecuted.WithLabelValues("failed").Inc()
	metrics.TaskDuration.WithLabelValues("failed").Observe(time.Since(startTime).Seconds())
	if err := updateTaskState(ctx, jobID, model.StatusRetry); err != nil {
		log.Printf("⚠️ 任务[%s] 状态更新为 RETRY 失败: %v", jobID, err)
	}
	if err := sendLogToKafka(ctx, cmd, 1, cost, execErr.Error()); err != nil {
		log.Printf("⚠️ 任务[%s] 日志发送失败: %v", jobID, err)
	}
	ackMsg(currentMsg) // 任务已写回 Redis，ack 掉 MQ 消息避免无限堆积
}

// executeHTTPCallback 通过 HTTP POST 调用业务方服务
// - 将 Payload 作为 request body
// - 超时由 cmd.Timeout 控制
// - 2xx 视为成功，其他视为失败
func executeHTTPCallback(ctx context.Context, cmd model.TaskCommand) error {
	if cmd.CallbackURL == "" {
		return errors.New("callback_url is empty")
	}

	// 单任务超时控制
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(cmd.Timeout)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(execCtx, http.MethodPost, cmd.CallbackURL, bytes.NewReader(cmd.Payload))
	if err != nil {
		return fmt.Errorf("build request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Job-ID", cmd.ID)
	req.Header.Set("X-Job-Name", cmd.Name)
	req.Header.Set("X-Retry-Count", fmt.Sprintf("%d", cmd.RetryCount))

	resp, err := httpClient.Do(req)
	if err != nil {
		metrics.HTTPCallbackStatus.WithLabelValues("error").Inc()
		return fmt.Errorf("http call failed: %w", err)
	}
	defer resp.Body.Close()

	// 埋点：HTTP 状态码分类
	statusClass := "2xx"
	switch {
	case resp.StatusCode >= 500:
		statusClass = "5xx"
	case resp.StatusCode >= 400:
		statusClass = "4xx"
	case resp.StatusCode >= 300:
		statusClass = "3xx"
	}
	metrics.HTTPCallbackStatus.WithLabelValues(statusClass).Inc()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("callback returned status %d", resp.StatusCode)
}

// handleTaskDead 处理死信任务
func handleTaskDead(ctx context.Context, cmd model.TaskCommand, currentMsg amqpDelivery, errMsg string) {
	// 1. 保存到死信队列
	task := &model.Task{
		ID:          cmd.ID,
		Name:        cmd.Name,
		CallbackURL: cmd.CallbackURL,
		RetryCount:  cmd.RetryCount,
		MaxRetry:    cmd.MaxRetry,
	}
	if err := saveDeadTask(ctx, task, errMsg); err != nil {
		log.Printf("⚠️ 任务[%s] 死信保存失败: %v", cmd.ID, err)
	}

	// 2. 状态机：标记为 DEAD
	if err := updateTaskState(ctx, cmd.ID, model.StatusDead); err != nil {
		log.Printf("⚠️ 任务[%s] 状态更新为 DEAD 失败: %v", cmd.ID, err)
	}

	// 3. 发送死信日志
	if err := sendLogToKafka(ctx, cmd, 2, 0, errMsg); err != nil {
		log.Printf("⚠️ 任务[%s] 死信日志发送失败: %v", cmd.ID, err)
	}

	// 4. Ack 消息（已进入死信，不再重投）
	ackMsg(currentMsg)
}

// saveDeadTask 保存死信任务到 Redis
func saveDeadTask(ctx context.Context, task *model.Task, errMsg string) error {
	detailKey := fmt.Sprintf("flash_job:dead_detail:%s", task.ID)
	if err := database.RDB.HSet(ctx, detailKey,
		"id", task.ID,
		"name", task.Name,
		"callback_url", task.CallbackURL,
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

// updateTaskState 更新任务状态到 Redis Hash，并维护活跃/终态索引
func updateTaskState(ctx context.Context, taskID string, status model.TaskStatus) error {
	if taskID == "" {
		return errors.New("empty task id")
	}
	key := fmt.Sprintf("%s:%s", consts.TaskStateKeyPrefix, taskID)
	now := time.Now().Unix()
	pipe := database.RDB.Pipeline()
	pipe.HSet(ctx, key, "status", string(status), "updated_at", now)
	// 终态：从 active 移到 terminal；非终态：更新 active 时间戳
	if status == model.StatusSuccess || status == model.StatusDead {
		pipe.ZRem(ctx, consts.ActiveTasksKey, taskID)
		pipe.ZAdd(ctx, consts.TerminalTasksKey, redis.Z{
			Score:  float64(now),
			Member: taskID,
		})
	} else {
		pipe.ZAdd(ctx, consts.ActiveTasksKey, redis.Z{
			Score:  float64(now),
			Member: taskID,
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// parseTaskCommand 解析 MQ 消息为 TaskCommand
func parseTaskCommand(body []byte) model.TaskCommand {
	var cmd model.TaskCommand
	if err := json.Unmarshal(body, &cmd); err == nil && cmd.ID != "" {
		return cmd
	}

	// 兜底：纯文本视为任务 ID（调试用）
	return model.TaskCommand{
		ID:       string(body),
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
		JobID:       cmd.ID,
		Name:        cmd.Name,
		CallbackURL: cmd.CallbackURL,
		Status:      status,
		CostMs:      costMs,
		Retry:       cmd.RetryCount,
		Timestamp:   time.Now().Unix(),
		ErrorMsg:    errMsg,
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

// backoffDuration 计算重试退避时间（指数退避 + 随机抖动）
// retry 1: 1s, 2: 2s, 3: 4s, 4: 8s, 5: 16s, max: 30s
// 加入 0-500ms 随机抖动，避免多任务同时重试导致雪崩
func backoffDuration(retry int) time.Duration {
	if retry <= 0 {
		return 0
	}
	backoff := time.Second << (retry - 1)
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	return backoff + jitter
}
