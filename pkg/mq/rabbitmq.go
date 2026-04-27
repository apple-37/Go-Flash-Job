// 文件: pkg/mq/rabbitmq.go
package mq

import (
	"fmt"
	"log"
	"time"

	"go-flash-job/pkg/consts"

	amqp "github.com/rabbitmq/amqp091-go"
)

var RabbitChannel *amqp.Channel
var RabbitConn *amqp.Connection

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
	fmt.Println("✅ RabbitMQ 连接并声明队列成功")
}

func CloseRabbitMQ() {
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