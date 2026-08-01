package config

import (
	"log"

	"github.com/spf13/viper"
)

// Config 对应 config.yaml 的结构
type Config struct {
	Server   ServerConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	RabbitMQ RabbitMQConfig
	Logger   LoggerConfig
}

type ServerConfig struct {
	Port string
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

// LoggerConfig 日志文件配置
type LoggerConfig struct {
	Dir        string // 日志目录
	MaxSizeMB  int    // 单文件最大大小（MB）
	MaxKeepDays int   // 日志保留天数
}

var AppConfig Config

func InitConfig() {
	viper.SetConfigName("config") // 配置文件名 (不带扩展名)
	viper.SetConfigType("yaml")   // 配置文件类型
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

	// S1: 默认值规范化
	if AppConfig.Server.Port == "" {
		AppConfig.Server.Port = "8080"
	}
	if AppConfig.Redis.Addr == "" {
		AppConfig.Redis.Addr = "localhost:6379"
	}
	if AppConfig.Logger.Dir == "" {
		AppConfig.Logger.Dir = "./logs"
	}
	if AppConfig.Logger.MaxSizeMB == 0 {
		AppConfig.Logger.MaxSizeMB = 100
	}
	if AppConfig.Logger.MaxKeepDays == 0 {
		AppConfig.Logger.MaxKeepDays = 7
	}

	// S1: 关键配置校验（缺失则启动失败，避免运行时才报错）
	if AppConfig.Redis.Addr == "" {
		log.Fatalf("❌ 配置校验失败: redis.addr 不能为空")
	}
	if len(AppConfig.Kafka.Brokers) == 0 {
		log.Printf("⚠️ 配置警告: kafka.brokers 为空，日志功能不可用")
	}
	if AppConfig.RabbitMQ.URL == "" {
		log.Fatalf("❌ 配置校验失败: rabbitmq.url 不能为空")
	}

	log.Printf("✅ 配置文件加载成功 (server=%s redis=%s)", AppConfig.Server.Port, AppConfig.Redis.Addr)
}
