package config

import (
	"log"

	"github.com/spf13/viper"
)

// Config 对应 config.yaml 的结构
type Config struct {
	Server   ServerConfig
	MySQL    MySQLConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	RabbitMQ RabbitMQConfig
}

type ServerConfig struct {
	Port string
}
type MySQLConfig struct {
	DSN string
}
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}
type KafkaConfig struct {
	Brokers []string
}
type RabbitMQConfig struct {
	URL string
}

var AppConfig Config

func InitConfig() {
	viper.SetConfigName("config") // 配置文件名 (不带扩展名)
	viper.SetConfigType("yaml") // 配置文件类型
	viper.AddConfigPath(".")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath("..")
	viper.AddConfigPath("../..")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("❌ 读取配置文件失败: %v", err)
	}

	if err := viper.Unmarshal(&AppConfig); err != nil {
		log.Fatalf("❌ 解析配置到结构体失败: %v", err)
	}
	
	log.Println("✅ 配置文件加载成功")
}