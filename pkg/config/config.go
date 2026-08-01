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

	// 设置默认值
	if AppConfig.Logger.Dir == "" {
		AppConfig.Logger.Dir = "./logs"
	}
	if AppConfig.Logger.MaxSizeMB == 0 {
		AppConfig.Logger.MaxSizeMB = 100
	}
	if AppConfig.Logger.MaxKeepDays == 0 {
		AppConfig.Logger.MaxKeepDays = 7
	}

	log.Println("✅ 配置文件加载成功")
}
