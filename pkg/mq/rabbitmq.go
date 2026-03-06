// 文件: pkg/mq/rabbitmq.go
package mq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"go-flash-job/pkg/consts"
)

var RabbitChannel *amqp.Channel

// InitRabbitMQ 接收 URL 字符串作为参数
func InitRabbitMQ(url string) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("❌ RabbitMQ 连接失败: %v", err)
	}

	// ... (后续代码不变)
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ RabbitMQ Channel 打开失败: %v", err)
	}
	_, err = ch.QueueDeclare(consts.TaskQueue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ RabbitMQ 队列声明失败: %v", err)
	}
	RabbitChannel = ch
	fmt.Println("✅ RabbitMQ 连接并声明队列成功")
}