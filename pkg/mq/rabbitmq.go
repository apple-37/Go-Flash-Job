// 文件: pkg/mq/rabbitmq.go
package mq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-flash-job/pkg/consts"

	amqp "github.com/rabbitmq/amqp091-go"
)

var RabbitChannel *amqp.Channel // 消费者专用（单协程消费，安全）
var RabbitConn *amqp.Connection

// publisherPool 发布者 Channel 池
// AMQP 协议规定一个 Channel 上的操作必须串行，多协程并发 Publish 会触发 channel exception
// 池化后每个 Publish 调用独占一个 Channel，用完归还
var publisherPool *ChannelPool

// ChannelPool AMQP Channel 池
type ChannelPool struct {
	conn   *amqp.Connection
	mu     sync.Mutex
	chans  []*amqp.Channel
	idx    int
	size   int
}

// NewChannelPool 创建 Channel 池
func NewChannelPool(conn *amqp.Connection, size int) (*ChannelPool, error) {
	pool := &ChannelPool{
		conn: conn,
		chans: make([]*amqp.Channel, 0, size),
		size:  size,
	}
	for i := 0; i < size; i++ {
		ch, err := conn.Channel()
		if err != nil {
			return nil, fmt.Errorf("create channel %d failed: %w", i, err)
		}
		pool.chans = append(pool.chans, ch)
	}
	return pool, nil
}

// Get 轮询获取一个 Channel（每个调用方短暂独占，用完不归还）
// 简单轮询 + mutex 保证同一 Channel 不会同时被两个 goroutine 拿到
func (p *ChannelPool) Get() *amqp.Channel {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := p.chans[p.idx%len(p.chans)]
	p.idx++
	return ch
}

// Close 关闭池中所有 Channel
func (p *ChannelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ch := range p.chans {
		_ = ch.Close()
	}
}

// InitRabbitMQ 接收 URL 字符串作为参数
func InitRabbitMQ(url string) {
	var (
		conn *amqp.Connection
		err  error
	)

	for attempt := 1; attempt <= 8; attempt++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}

		delay := time.Second * time.Duration(1<<(attempt-1))
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		log.Printf("⚠️ RabbitMQ 连接失败(第 %d/8 次): %v，%v 后重试", attempt, err, delay)
		time.Sleep(delay)
	}
	if err != nil {
		log.Fatalf("❌ RabbitMQ 连接失败，重试后仍失败: %v", err)
	}

	// 消费者专用 Channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ RabbitMQ Channel 打开失败: %v", err)
	}
	_, err = ch.QueueDeclare(consts.TaskQueue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ RabbitMQ 队列声明失败: %v", err)
	}
	RabbitConn = conn
	RabbitChannel = ch

	// M1: 发布者 Channel 池（4 个，匹配 NumP=2 的并发量 + 余量）
	publisherPool, err = NewChannelPool(conn, 4)
	if err != nil {
		log.Fatalf("❌ RabbitMQ 发布者 Channel 池创建失败: %v", err)
	}

	fmt.Println("✅ RabbitMQ 连接并声明队列成功（含 4 Channel 发布池）")
}

// GetPublishChannel 从池中获取一个 Channel 用于发布
func GetPublishChannel() *amqp.Channel {
	if publisherPool == nil {
		return RabbitChannel // 降级：池未初始化时用消费者 Channel（不推荐，仅兜底）
	}
	return publisherPool.Get()
}

func CloseRabbitMQ() {
	if publisherPool != nil {
		publisherPool.Close()
	}
	if RabbitChannel != nil {
		if err := RabbitChannel.Close(); err != nil {
			log.Printf("⚠️ RabbitMQ Channel 关闭失败: %v", err)
		}
	}
	if RabbitConn != nil {
		if err := RabbitConn.Close(); err != nil {
			log.Printf("⚠️ RabbitMQ Connection 关闭失败: %v", err)
		}
	}
}
