package core

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go-flash-job/pkg/consts"
	"go-flash-job/pkg/database"

	"github.com/redis/go-redis/v9"
)

// 选主参数
const (
	lockTTL          = 10 * time.Second // 锁 TTL：主挂了 10s 后自动释放
	renewInterval    = 3 * time.Second  // 续期间隔：留 7s 容错，避免网络抖动误判
	acquireRetryWait = 1 * time.Second  // 没拿到锁时的重试间隔
)

// releaseScript CAS 释放锁：只有 value == 自己的 instanceID 时才 DEL
// 避免误删别人的锁（比如自己续期失败后，backup 已经拿到新锁，这时不能误删）
var releaseScript = redis.NewScript(`
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	else
		return 0
	end
`)

// renewScript CAS 续期：只有 value == 自己的 instanceID 时才 EXPIRE
// 防止续期时锁已经被别人拿走却续了别人的锁
var renewScript = redis.NewScript(`
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('EXPIRE', KEYS[1], ARGV[2])
	else
		return 0
	end
`)

// LeaderElection 基于 Redis 的分布式锁选主
// 多个 scheduler 实例只有一个能拿到锁成为主，其余 standby 等待接管
type LeaderElection struct {
	instanceID string // 唯一标识（hostname:port），用于 CAS 校验和 leader 发现
	isLeader   bool
}

// NewLeaderElection 创建选主实例
// instanceID 用 hostname:port 而非 hostname:pid，因为监控器需要从 lock value 解析出可访问的地址
func NewLeaderElection(port string) *LeaderElection {
	host, _ := os.Hostname()
	// port 形如 ":8080"，去掉冒号前缀拼成 hostname:port
	return &LeaderElection{
		instanceID: fmt.Sprintf("%s%s", host, port),
	}
}

// InstanceID 返回当前实例 ID（供 API 层查询）
func (le *LeaderElection) InstanceID() string {
	return le.instanceID
}

// WaitForLeadership 阻塞直到拿到锁成为主
// 拿到锁后启动后台续期 goroutine，返回 lostCh 通知主循环
// 主循环监听 lostCh，一旦续期失败则停止调度等待重新选主
func (le *LeaderElection) WaitForLeadership(ctx context.Context) (<-chan struct{}, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		acquired, err := le.tryAcquire(ctx)
		if err != nil {
			log.Printf("⚠️ 选主获取锁失败: %v，%v 后重试", err, acquireRetryWait)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(acquireRetryWait):
			}
			continue
		}

		if acquired {
			le.isLeader = true
			log.Printf("👑 [%s] 已成为 Leader，开始调度", le.instanceID)
			lostCh := make(chan struct{})
			go le.renewLoop(ctx, lostCh)
			return lostCh, nil
		}

		// 没拿到锁，standby 等待
		log.Printf("⏳ [%s] 等待成为 Leader（锁被占用）...", le.instanceID)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(acquireRetryWait):
		}
	}
}

// tryAcquire 尝试获取锁（SET NX EX）
func (le *LeaderElection) tryAcquire(ctx context.Context) (bool, error) {
	ok, err := database.RDB.SetNX(ctx, consts.SchedulerLockKey, le.instanceID, lockTTL).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// renewLoop 后台续期，TTL 10s / 续期 3s，留 7s 容错
// 续期失败（网络断开、Redis 重启、锁被抢占）→ 关闭 lostCh 通知主循环
func (le *LeaderElection) renewLoop(ctx context.Context, lostCh chan struct{}) {
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			le.isLeader = false
			close(lostCh)
			return
		case <-ticker.C:
		}

		ok, err := renewScript.Run(ctx, database.RDB,
			[]string{consts.SchedulerLockKey},
			le.instanceID, int(lockTTL/time.Second)).Int64()
		if err != nil || ok == 0 {
			le.isLeader = false
			log.Printf("⚠️ [%s] 续期失败（err=%v, ok=%d），让出 Leader 身份", le.instanceID, err, ok)
			close(lostCh)
			return
		}
	}
}

// Release 释放锁（优雅退出时调用）
// 用 CAS 脚本，避免误删别人的锁
func (le *LeaderElection) Release(ctx context.Context) {
	if !le.isLeader {
		return
	}
	_, err := releaseScript.Run(ctx, database.RDB,
		[]string{consts.SchedulerLockKey}, le.instanceID).Int64()
	if err != nil {
		log.Printf("⚠️ 释放锁失败: %v", err)
		return
	}
	le.isLeader = false
	log.Printf("🔓 [%s] 已释放 Leader 锁", le.instanceID)
}

// IsLeader 返回当前是否为主
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader
}
