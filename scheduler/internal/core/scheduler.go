package core

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go-flash-job/pkg/config"
	"go-flash-job/pkg/database"
	"go-flash-job/pkg/model"
	"go-flash-job/pkg/mq"
)

const preloadWindow = 5 * time.Minute

// Pending expires after the scheduled time, including the prefetch residence.
// ARGV[1] is now; ARGV[8] is the upper bound of the five-minute window.
var claimDue = redis.NewScript(`
 if redis.call('GET',KEYS[3]) ~= ARGV[6] then return {} end
 local entries=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[8],'WITHSCORES','LIMIT',0,ARGV[2])
 local result={}
 for i=1,#entries,2 do
  local id=entries[i]
  local due=tonumber(entries[i+1])
  local state=ARGV[4]..':'..id
  local data=redis.call('HGET',ARGV[5]..':'..id,'data')
  local valid,task=false,nil
  if data then valid,task=pcall(cjson.decode,data) end
  if valid and type(task)=='table' and task.id==id then
   redis.call('HSET',state,'status','READY','claim',ARGV[3],'retry_count',task.retry_count or 0)
   redis.call('ZADD',KEYS[2],math.max(tonumber(ARGV[1]),due)+60,id)
   table.insert(result,id)
   table.insert(result,data)
   table.insert(result,entries[i+1])
  else
   redis.call('HSET',state,'status','DEAD','error','invalid or missing task detail')
   redis.call('ZREM',KEYS[4],id)
   redis.call('ZADD',KEYS[5],ARGV[1],id)
   redis.call('ZADD',KEYS[6],ARGV[1],id)
   redis.call('HSET',ARGV[7]..':'..id,'id',id,'error','invalid or missing task detail')
  end
  redis.call('ZREM',KEYS[1],id)
 end
 return result
`)

var confirmed = redis.NewScript(`
 if redis.call('HGET',KEYS[1],'claim') ~= ARGV[2] then return 0 end
 redis.call('ZREM',KEYS[2],ARGV[1])
 redis.call('HDEL',KEYS[1],'claim')
 if redis.call('HGET',KEYS[1],'status')=='READY' then
  redis.call('HSET',KEYS[1],'status','DISPATCHED')
 end
 return 1
`)

// Sending leases outlive the HTTP timeout. A crashed sender leaves an uncertain
// external outcome, so expiration records UNKNOWN without retrying the POST.
var maintain = redis.NewScript(`
 local now=tonumber(ARGV[1])
 local statePrefix=ARGV[2]..':'
 for _,id in ipairs(redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now,'LIMIT',0,256)) do
  local state=statePrefix..id
  local status=redis.call('HGET',state,'status')
  if status=='READY' or status=='PENDING' or status=='RETRY' or status=='DISPATCHED' then
   redis.call('HSET',state,'status','PENDING')
   redis.call('HDEL',state,'claim')
   redis.call('ZADD',KEYS[2],now,id)
  end
  redis.call('ZREM',KEYS[1],id)
 end
 for _,id in ipairs(redis.call('ZRANGEBYSCORE',KEYS[3],'-inf',now,'LIMIT',0,256)) do
  local state=statePrefix..id
  if redis.call('HGET',state,'status')=='RUNNING' then
   redis.call('HSET',state,'status','UNKNOWN','updated_at',now,'error','executor lease expired')
   redis.call('ZREM',KEYS[4],id)
   redis.call('ZADD',KEYS[5],now,id)
  end
  redis.call('ZREM',KEYS[3],id)
 end
 for _,id in ipairs(redis.call('ZRANGEBYSCORE',KEYS[5],'-inf',now-86400,'LIMIT',0,256)) do
  redis.call('DEL',statePrefix..id,ARGV[3]..':'..id,ARGV[4]..':'..id)
  redis.call('ZREM',KEYS[6],id)
  redis.call('ZREM',KEYS[5],id)
 end
 return 1
`)

type Scheduler struct {
	leader *LeaderElection
	queues atomic.Pointer[readyQueues]
}

func NewScheduler(port string) *Scheduler     { return &Scheduler{leader: NewLeaderElection(port)} }
func (s *Scheduler) IsLeader() bool           { return s.leader.IsLeader() }
func (s *Scheduler) LeaderInstanceID() string { return s.leader.InstanceID() }

func (s *Scheduler) Start(ctx context.Context) {
	for ctx.Err() == nil {
		lost, err := s.leader.WaitForLeadership(ctx)
		if err != nil {
			return
		}
		leaderCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-lost:
				cancel()
			case <-leaderCtx.Done():
			}
		}()
		s.run(leaderCtx)
		cancel()
		<-lost
	}
}

func (s *Scheduler) run(ctx context.Context) {
	q := newQueues(config.AppConfig.Scheduler)
	s.queues.Store(q)
	defer s.queues.Store(nil)
	var workers sync.WaitGroup
	for id := range q.local {
		workers.Add(1)
		go func() { defer workers.Done(); publishLoop(ctx, q, id) }()
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	nextMaintenance := time.Time{}
	for ctx.Err() == nil {
		now := time.Now()
		if !now.Before(nextMaintenance) {
			keys := []string{database.JobPendingZSetKey, database.JobZSetKey, database.SendingKey, database.ActiveTasksKey, database.TerminalTasksKey, database.JobDeadZSetKey}
			if err := maintain.Run(ctx, database.RDB, keys, now.Unix(), database.TaskStateKeyPrefix, database.TaskDetailKeyPrefix, database.DeadDetailKeyPrefix).Err(); err != nil {
				log.Print(err)
			}
			nextMaintenance = now.Add(time.Second)
		}
		reserved := q.reserve(config.AppConfig.Scheduler.FetchBatch)
		limit := len(reserved.owners)
		if limit > 0 {
			token := rand.Text()
			keys := []string{database.JobZSetKey, database.JobPendingZSetKey, database.SchedulerLockKey, database.ActiveTasksKey, database.TerminalTasksKey, database.JobDeadZSetKey}
			rows, err := claimDue.Run(ctx, database.RDB, keys, now.Unix(), limit, token, database.TaskStateKeyPrefix, database.TaskDetailKeyPrefix, s.leader.InstanceID(), database.DeadDetailKeyPrefix, now.Add(preloadWindow).Unix()).StringSlice()
			if err != nil {
				log.Print(err)
			}
			var batch []delivery
			if err == nil && ctx.Err() == nil {
				for i := 0; i+2 < len(rows); i += 3 {
					var t model.Task
					if err := json.Unmarshal([]byte(rows[i+1]), &t); err != nil || t.ID != rows[i] {
						log.Printf("invalid task detail id=%q", rows[i])
						continue
					}
					// The sorted-set score includes retry rescheduling and recovery.
					due, err := strconv.ParseInt(rows[i+2], 10, 64)
					if err != nil {
						log.Printf("invalid schedule id=%q: %v", t.ID, err)
						continue
					}
					t.TriggerTime = due
					batch = append(batch, delivery{task: t, claim: token})
				}
			}
			reserved.complete(batch)
			if err == nil && len(rows) == 3*limit {
				continue
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	q.close()
	workers.Wait()
}

// Each worker owns one channel and at most one outstanding broker confirmation.
func publishLoop(ctx context.Context, q *readyQueues, id int) {
	var ch *amqp.Channel
	var acks <-chan amqp.Confirmation
	var returns <-chan amqp.Return
	defer func() {
		if ch != nil {
			_ = ch.Close()
		}
	}()
	for ctx.Err() == nil {
		if ch == nil {
			var err error
			ch, err = mq.RabbitConn.Channel()
			if err == nil {
				err = ch.Confirm(false)
			}
			if err != nil {
				if ch != nil {
					_ = ch.Close()
					ch = nil
				}
				log.Printf("publisher %d: %v", id, err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			returns = ch.NotifyReturn(make(chan amqp.Return, 1))
			acks = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		}
		d, ok := q.take(id)
		if !ok || ctx.Err() != nil {
			return
		}
		err := publish(ctx, ch, acks, returns, d.task)
		if err != nil {
			log.Printf("publish id=%s: %v", d.task.ID, err)
			_ = ch.Close()
			ch = nil
			continue // the Redis lease makes this claim eligible for redelivery
		}
		lag := time.Now().UnixMilli() - d.task.TriggerTime*1000
		slog.Info("broker_confirm", "job_id", d.task.ID, "attempt", d.task.RetryCount, "dispatch_lag_ms", lag)
		keys := []string{database.StateKey(d.task.ID), database.JobPendingZSetKey}
		if err := confirmed.Run(ctx, database.RDB, keys, d.task.ID, d.claim).Err(); err != nil {
			log.Print(err)
		}
	}
}

func publish(ctx context.Context, ch *amqp.Channel, acks <-chan amqp.Confirmation, returns <-chan amqp.Return, t model.Task) error {
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx, "", mq.TaskQueue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: t.ID, Body: body,
	})
	if err != nil {
		return err
	}
	select {
	case ack, ok := <-acks:
		if !ok || !ack.Ack {
			return fmt.Errorf("broker confirmation failed")
		}
		select {
		case <-returns:
			return fmt.Errorf("unroutable task")
		default:
			return nil
		}
	case <-returns:
		return fmt.Errorf("unroutable task")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) QueueStats() map[string]any {
	if q := s.queues.Load(); q != nil {
		return q.stats()
	}
	return nil
}
