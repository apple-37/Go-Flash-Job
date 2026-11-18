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
	Workers       int
	FetchBatch    int `mapstructure:"fetch_batch"`
	LocalCapacity int `mapstructure:"local_capacity"`
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
	if c.FetchBatch == 0 {
		c.FetchBatch = 128
	}
	if !viper.IsSet("scheduler.local_capacity") && viper.IsSet("scheduler.capacity") {
		// Earlier versions used one total global capacity. Preserve approximately
		// that queued-task budget when loading an existing local configuration.
		oldCapacity := viper.GetInt("scheduler.capacity")
		if oldCapacity < 1 || oldCapacity > 65536 || c.Workers < 1 {
			log.Fatal("invalid legacy scheduler.capacity or workers")
		}
		c.LocalCapacity = (oldCapacity + c.Workers - 1) / c.Workers
		log.Print("scheduler.capacity migrated to per-P local_capacity; update config.yaml using config.example.yaml")
	}
	if c.LocalCapacity == 0 {
		c.LocalCapacity = 128
	}
	if c.Workers < 1 || c.Workers > 256 || c.FetchBatch < 1 || c.FetchBatch > 256 || c.LocalCapacity < 1 || c.LocalCapacity > 65536 {
		log.Fatal("invalid scheduler workers/fetch_batch/local_capacity")
	}
	if viper.IsSet("scheduler.batch") || viper.IsSet("scheduler.strategy") {
		log.Print("scheduler.batch and scheduler.strategy are retired; using round-robin dispatch with work stealing")
	}
	if AppConfig.RabbitMQ.URL == "" {
		log.Fatalf("❌ 配置校验失败: rabbitmq.url 不能为空")
	}

	log.Printf("✅ 配置文件加载成功 (server=%s redis=%s)", AppConfig.Server.Port, AppConfig.Redis.Addr)
}
