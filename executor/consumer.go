package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/mq"
)

func startConsumer(ctx context.Context) {
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
		mq.TaskQueue, // queue
		consumerTag,
		false, // auto-ack (手动 ack 保证至少消费一次)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("❌ 注册消费者失败: %v", err)
	}

	// 协程池：50 个 worker，与 QoS 匹配
	pool := newPool(50)

	fmt.Println("🎧 Executor 已启动，正在监听任务队列...")

	for {
		select {
		case <-ctx.Done():
			if err := ch.Cancel(consumerTag, false); err != nil {
				log.Printf("⚠️ 取消 RabbitMQ 消费失败: %v", err)
			}
			pool.wait()
			return
		case msg, ok := <-msgs:
			if !ok {
				pool.wait()
				return
			}

			var cmd model.Task
			if err := json.Unmarshal(msg.Body, &cmd); err != nil || cmd.ID == "" || cmd.CallbackURL == "" {
				log.Printf("invalid task message: %v", err)
				_ = msg.Reject(false)
				continue
			}
			currentMsg := msg

			pool.submit(func() {
				handleTask(ctx, cmd, currentMsg)
			})
		}
	}
}

var begin = redis.NewScript(`
 local status=redis.call('HGET',KEYS[1],'status')
 if status~='READY' and status~='DISPATCHED' then return 0 end
 if (redis.call('HGET',KEYS[1],'generation') or '')~=ARGV[2] then return 0 end
 if tonumber(redis.call('HGET',KEYS[1],'retry_count') or '0')~=tonumber(ARGV[3]) then return 0 end
 redis.call('HDEL',KEYS[1],'claim')
 redis.call('ZREM',KEYS[2],ARGV[1])
 redis.call('HSET',KEYS[1],'status','RUNNING','updated_at',ARGV[4])
 redis.call('ZADD',KEYS[3],ARGV[4]+90,ARGV[1])
 return 1
`)

var finish = redis.NewScript(`
 if redis.call('HGET',KEYS[1],'status')~='RUNNING' or
    (redis.call('HGET',KEYS[1],'generation') or '')~=ARGV[2] or
    tonumber(redis.call('HGET',KEYS[1],'retry_count') or '0')~=tonumber(ARGV[3]) then return 0 end
 redis.call('ZREM',KEYS[2],ARGV[1])
 redis.call('HSET',KEYS[1],'status',ARGV[4],'updated_at',ARGV[5],'error',ARGV[6])
 if ARGV[4]=='PENDING' then
  redis.call('HSET',KEYS[1],'retry_count',tonumber(ARGV[3])+1)
  redis.call('HSET',KEYS[3],'data',ARGV[7])
  redis.call('ZADD',KEYS[4],ARGV[8],ARGV[1])
  redis.call('ZADD',KEYS[5],ARGV[5],ARGV[1])
 else
  redis.call('ZREM',KEYS[5],ARGV[1])
  redis.call('ZADD',KEYS[6],ARGV[5],ARGV[1])
  if ARGV[4]=='DEAD' then
   redis.call('HSET',KEYS[8],'id',ARGV[1],'retry_count',ARGV[3],'error',ARGV[6],'dead_at',ARGV[5],'name',ARGV[9],'callback_url',ARGV[10])
   redis.call('ZADD',KEYS[7],ARGV[5],ARGV[1])
  end
 end
 return 1
`)

func handleTask(ctx context.Context, t model.Task, msg amqp.Delivery) {
	state := database.StateKey(t.ID)
	keys := []string{state, database.JobPendingZSetKey, database.SendingKey}
	claimed, err := begin.Run(ctx, database.RDB, keys, t.ID, t.Generation, t.RetryCount, time.Now().Unix()).Int()
	if err != nil {
		log.Print(err)
		_ = msg.Nack(false, true)
		return
	}
	if claimed == 0 {
		_ = msg.Ack(false)
		return
	}

	result := trigger(ctx, t)
	observed := time.Now()
	attempt := t.RetryCount
	retryAt := int64(0)
	if result.status == "RETRY" {
		retryAt = observed.Unix() + int64(1<<min(attempt, 5))
		switch {
		case attempt >= t.MaxRetry:
			result.status = "DEAD"
		case t.LatestAt > 0 && retryAt > t.LatestAt:
			result.status = "EXPIRED"
		default:
			result.status = "PENDING"
			t.RetryCount++
		}
	}
	data, err := json.Marshal(t)
	if err == nil {
		keys = []string{state, database.SendingKey, database.DetailKey(t.ID), database.JobZSetKey, database.ActiveTasksKey, database.TerminalTasksKey, database.JobDeadZSetKey, database.DeadDetailKey(t.ID)}
		// Allow a bounded result write even if shutdown canceled the HTTP request.
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		err = finish.Run(saveCtx, database.RDB, keys, t.ID, t.Generation, attempt, result.status, observed.Unix(), result.reason, data, retryAt, t.Name, t.CallbackURL).Err()
	}
	if err != nil {
		log.Print(err)
		_ = msg.Nack(false, true)
		return
	}
	if result.status == "ACCEPTED" {
		slog.Info("crawler_accept", "job_id", t.ID, "attempt", attempt, "accept_lag_ms", observed.UnixMilli()-t.TriggerTime*1000)
	} else {
		slog.Info("trigger_result", "job_id", t.ID, "status", result.status, "reason", result.reason, "attempt", attempt)
	}
	if err := msg.Ack(false); err != nil {
		log.Print(err)
	}
}
