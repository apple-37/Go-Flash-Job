# Go-Flash-Job

高性能分布式任务调度引擎（微服务版）。

项目将任务调度、任务执行、日志清洗拆分为三个服务，通过 Redis、RabbitMQ、Kafka、MySQL 串联出完整任务链路，适合学习调度系统与高并发背压设计。

## 一、架构总览

- scheduler：调度服务，负责从 Redis 全局队列拉取任务并按时推送到 RabbitMQ。
- executor：执行服务，负责消费 RabbitMQ 任务并执行，同时把执行日志写入 Kafka。
- logger：日志服务，负责消费 Kafka 日志并批量落库 MySQL。

任务链路：

1. 任务写入 Redis ZSet（全局调度队列）
2. scheduler 预拉取任务到本地最小堆并按触发时间推送到 RabbitMQ
3. executor 消费 RabbitMQ 执行任务，发送日志到 Kafka
4. logger 消费 Kafka，批量写入 MySQL，并定时清理过期日志

## 二、技术栈

- Go
- Gin
- GORM
- Redis（ZSet）
- RabbitMQ
- Kafka
- MySQL

## 三、目录结构

- scheduler：调度服务
- executor：执行服务
- logger：日志服务
- pkg：公共包（config、database、mq、model、consts）
- config.yaml：运行配置
- config.example.yaml：配置示例
- Makefile：常用命令入口

## 四、运行前准备

请先确保以下依赖可用，且与 config.yaml 中地址一致：

1. MySQL（默认 127.0.0.1:3306）
2. Redis（默认 127.0.0.1:6379）
3. Kafka（默认 127.0.0.1:9092）
4. RabbitMQ（默认 localhost:5672）

## 五、快速开始

在项目根目录执行：

1. 同步依赖

   make tidy

2. 编译与测试

   make build
   make test

3. 分别启动三个服务（建议三个终端）

   make run-scheduler
   make run-executor
   make run-logger

## 六、Makefile 命令

- make help：查看全部命令
- make tidy：整理依赖
- make fmt：格式化代码
- make vet：静态检查
- make test：运行测试
- make build：编译所有包
- make run-scheduler：启动调度服务
- make run-executor：启动执行服务
- make run-logger：启动日志服务

## 七、功能验证

scheduler 启动后，可调用压测接口写入测试任务：

- 方法：POST
- 路径：/api/v1/jobs/seed
- 示例：

  http://127.0.0.1:8080/api/v1/jobs/seed?count=100

预期结果：

1. scheduler 日志显示任务被推送到 RabbitMQ
2. executor 日志显示任务被消费并执行
3. logger 日志显示日志被批量落库

## 八、设计亮点

1. 调度核心无忙等待
   通过 Redis ZSet + 本地最小堆 + Timer 精准挂起，避免轮询空转。

2. 全链路背压
   RabbitMQ QoS + executor 协程池共同限制并发，防止瞬时流量压垮服务。

3. 批量 I/O 优化
   Redis Pipeline 批量写入 + logger 批量刷盘，显著降低网络与数据库开销。

4. 可靠性保障
   RabbitMQ 手动 Ack 保证至少一次投递；Kafka 作为日志缓冲总线避免日志丢失。

5. 自动清理机制
   logger 定时清理过期日志，防止磁盘与表数据无限增长。
