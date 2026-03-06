// 文件: pkg/mq/kafka.go
package mq

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

var KafkaProducer sarama.AsyncProducer

// InitKafka 接收 brokers 列表作为参数
func InitKafka(brokers []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Kafka 生产者创建失败: %v", err)
	}

	KafkaProducer = producer
	fmt.Println("✅ Kafka 异步生产者已就绪")
}