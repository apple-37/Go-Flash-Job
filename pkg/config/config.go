package config

import (
	"log"

	"github.com/spf13/viper"
)

// Config 对应 config.yaml 的结构
type Config struct {
	Server    ServerConfig
	Redis     RedisConfig
	Scheduler SchedulerConfig
	RabbitMQ  RabbitMQConfig
}

type ServerConfig struct {
	Port string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type SchedulerConfig struct {
	Workers  int
	Batch    int
	Capacity int
	Strategy string
}

type RabbitMQConfig struct {
	URL string
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
		AppConfig.Server.Port = ":8080"
	}
	if AppConfig.Redis.Addr == "" {
		AppConfig.Redis.Addr = "localhost:6379"
	}
	// S1: 关键配置校验（缺失则启动失败，避免运行时才报错）
	if AppConfig.Redis.Addr == "" {
		log.Fatalf("❌ 配置校验失败: redis.addr 不能为空")
	}
	c := &AppConfig.Scheduler
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.Batch == 0 {
		c.Batch = 16
	}
	if c.Capacity == 0 {
		c.Capacity = 512
	}
	if c.Strategy == "" {
		c.Strategy = "steal"
	}
	if c.Workers < 1 || c.Workers > 256 || c.Batch < 1 || c.Batch > 256 || c.Capacity < 1 || c.Capacity > 65536 {
		log.Fatal("invalid scheduler workers/batch/capacity")
	}
	switch c.Strategy {
	case "shared", "local", "steal":
	default:
		log.Fatal("invalid scheduler.strategy")
	}
	if AppConfig.RabbitMQ.URL == "" {
		log.Fatalf("❌ 配置校验失败: rabbitmq.url 不能为空")
	}

	log.Printf("✅ 配置文件加载成功 (server=%s redis=%s)", AppConfig.Server.Port, AppConfig.Redis.Addr)
}
