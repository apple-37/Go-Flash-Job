// 文件: pkg/mq/kafka.go
package mq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var KafkaProducer sarama.AsyncProducer
var kafkaCloseOnce sync.Once

// InitKafka 接收 brokers 列表作为参数
func InitKafka(brokers []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 200 * time.Millisecond

	var (
		producer sarama.AsyncProducer
		err      error
	)

	for attempt := 1; attempt <= 8; attempt++ {
		producer, err = sarama.NewAsyncProducer(brokers, config)
		if err == nil {
			KafkaProducer = producer
			startKafkaErrorDrainer(producer)
			fmt.Println("✅ Kafka 异步生产者已就绪")
			return
		}

		delay := time.Second * time.Duration(1<<(attempt-1))
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		log.Printf("⚠️ Kafka 生产者创建失败(第 %d/8 次): %v，%v 后重试", attempt, err, delay)
		time.Sleep(delay)
	}

	log.Fatalf("❌ Kafka 生产者创建失败，重试后仍失败: %v", err)
}

func startKafkaErrorDrainer(producer sarama.AsyncProducer) {
	go func() {
		for err := range producer.Errors() {
			if err == nil {
				continue
			}
			if err.Msg != nil {
				log.Printf("⚠️ Kafka 异步发送失败 topic=%s err=%v", err.Msg.Topic, err.Err)
				continue
			}
			log.Printf("⚠️ Kafka 异步发送失败 err=%v", err.Err)
		}
	}()
}

func CloseKafka() {
	if KafkaProducer == nil {
		return
	}
	kafkaCloseOnce.Do(func() {
		KafkaProducer.AsyncClose()
	})
}