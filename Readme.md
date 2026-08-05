# Go-Flash-Job

高性能分布式任务调度引擎，仿 GMP 调度模型，支持 HTTP 回调、重试退避、FSM 状态机恢复、Redis 分布式锁选主。

## 架构总览

```
┌─────────────┐     ┌──────────────────────────────────────────┐     ┌──────────────┐
│  Business   │ HTTP│                Scheduler                  │     │   Executor   │
│  Service    ├────►│  ┌────────┐  ┌─────┐  ┌───────────────┐  │     │              │
│ (Python/Go) │     │  │  API   │  │ FSM │  │ GMP Scheduler │  │MQ   │  HTTP Callback│
└─────────────┘     │  │ Limiter│  │     │  │  fetcher + P  ├─┼────►│  ┌──────────┐│
                    │  └────┬───┘  └─────┘  └───────┬───────┘  │     │  │ Worker   ││
                    │       │                       │          │     │  │ Pool(50) ││
                    │       ▼                       ▼          │     │  └────┬─────┘│
                    │  ┌─────────────────────────────────────┐ │     │       │      │
                    │  │ Redis: ZSet 队列 + Hash 详情         │◄┼─────┼───────┘      │
                    │  │ active/terminal_tasks 索引 + 选主锁 │ │     │              │
                    │  └─────────────────────────────────────┘ │     └──────┬───────┘
                    └──────────────────────────────────────────┘            │
                                                                     Kafka    │
                                                                         ▼    │
                                                                 ┌──────────────┐
                                                                 │   Logger     │
                                                                 │  (file)     │
                                                                 └──────────────┘
```

### 三大服务

| 服务          | 职责                                                   | 端口 |
| ------------- | ------------------------------------------------------ | ---- |
| **scheduler** | 从 Redis ZSet 拉取到期任务，经 GMP 调度推送到 RabbitMQ | 8080 |
| **executor**  | 消费 RabbitMQ，HTTP 回调业务方服务，失败重试退避       | -    |
| **logger**    | 消费 Kafka 日志，批量刷盘（100条/批，文件轮转）        | -    |

### 任务链路

1. 业务方 POST `/api/v1/jobs/submit` 提交任务到 scheduler
2. scheduler 写入 Redis ZSet（member=jobID）+ detail Hash
3. fetcherLoop（200ms tick）拉取到期任务，老化提升后分配到 P 本地堆
4. P 从本地堆取任务，推送到 RabbitMQ，executor 消费
5. executor HTTP POST 回调业务方 CallbackURL，结果写 Kafka
6. logger 消费 Kafka 日志，批量写入文件

## 技术栈

- **语言**: Go 1.21+
- **存储**: Redis（ZSet + Hash + Lua 脚本 + 分布式锁）
- **消息队列**: RabbitMQ（任务分发）+ Kafka（日志管道）
- **压测**: JMeter + Mock Python 服务

## 目录结构

```
Go-Flash-Job/
├── scheduler/              # 调度服务
│   ├── cmd/main.go
│   └── internal/
│       ├── api/            # HTTP API + 限流
│       ├── core/           # GMP 调度核心（scheduler/p/fsm/heap）
│       └── service/        # 任务提交服务
├── executor/               # 执行服务
│   ├── cmd/main.go
│   └── internal/
│       ├── client/         # MQ 消费 + HTTP 回调 + 重试退避
│       └── worker/         # 协程池
├── logger/                 # 日志服务
│   ├── cmd/main.go
│   └── internal/store/     # 批量刷盘 + 文件轮转
├── pkg/                    # 公共包
│   ├── config/             # 配置加载 + 校验
│   ├── consts/             # 常量定义
│   ├── database/           # Redis 客户端
│   ├── model/              # 数据模型 + Redis 辅助函数
│   └── mq/                 # RabbitMQ + Kafka 客户端
├── benchmark/              # 压测工具
│   ├── jmeter/             # JMeter 测试计划
│   ├── mock_service/       # Mock Python 服务
│   └── submit_tasks.py     # 批量任务生成器
├── config.example.yaml     # 配置模板
└── go.mod
```

## 快速开始

### 1. 环境准备

```bash
# Redis
docker run -d --name redis -p 6379:6379 redis

# RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management

# Kafka
docker run -d --name kafka -p 9092:9092 bitnami/kafka:latest
```

### 2. 配置

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml 填入你的 Redis/RabbitMQ/Kafka 地址
```

### 3. 启动服务

```bash
# 终端 1: 启动 scheduler
go run scheduler/cmd/main.go

# 终端 2: 启动 executor
go run executor/cmd/main.go

# 终端 3: 启动 mock 业务服务（压测用）
cd benchmark/mock_service
pip install fastapi uvicorn
uvicorn mock_service:app --port 8000

# 终端 4: 启动 logger（可选）
go run logger/cmd/main.go
```

### 4. 提交任务

```bash
# 单个任务
curl -X POST http://localhost:8080/api/v1/jobs/submit \
  -H "Content-Type: application/json" \
  -d '{
    "id": "job_001",
    "name": "crawl_task",
    "callback_url": "http://localhost:8000/execute",
    "trigger_time": 5,
    "priority": "High",
    "timeout": 30,
    "payload": {"url": "https://example.com"}
  }'

# 批量提交
curl -X POST http://localhost:8080/api/v1/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"tasks": [...]}'

# 或用 Python 脚本
python benchmark/submit_tasks.py --count 1000
```

### 5. 查看统计

```bash
# Mock 服务统计
curl http://localhost:8000/stats

# 死信队列
curl http://localhost:8080/api/v1/jobs/dead
```

## 核心设计

### GMP 调度模型

- **G (Goroutine)**: 任务协程
- **M (Machine)**: HTTP 回调执行单元
- **P (Processor)**: 本地任务堆（2个），work stealing 负载均衡

### 调度策略

三层排序，保证定时精度 + 公平性：

1. **触发时间优先**（定时任务核心语义）
2. **同时间按优先级**（High > Medium > Low）
3. **同优先级按入队时间**（FIFO 兜底）

**老化机制**：Low 任务等待超过 5 分钟自动提升为 High，防止饿死。

### 重试退避

失败任务写回 Redis ZSet（不 nack requeue），指数退避 + 随机抖动：

```
1s → 2s → 4s → 8s → 16s → 30s（封顶）+ 0~500ms jitter
```

### 分布式选主

scheduler 通过 Redis 分布式锁实现选主，支持多实例部署：

- **获取锁**：`SET lockKey instanceID NX EX 10`，拿到锁的实例成为 Leader
- **续期**：后台 goroutine 每 3s 用 Lua CAS 续期（TTL 10s，留 7s 容错）
- **释放**：优雅退出时用 Lua CAS 释放锁（避免误删别人的锁）
- **故障接管**：主挂了 10s 后锁过期，backup 自动接管

### FSM 状态机

Lua CAS 原子状态转换，7 个状态 + 6 个事件：

```
PENDING → READY → DISPATCHED → RUNNING → SUCCESS
                  ↓                ↓
                RETRY ────────────┘
                  ↓
                 DEAD
```

### 可靠性保障

- **至少一次投递**：RabbitMQ 手动 Ack
- **幂等去重**：Redis SetNX，key = `jobID:triggerTime`，TTL 24h
- **任务去重**：ZSet member = jobID，重试时 score 更新而非新增
- **FSM 恢复**：active_tasks 索引 + 卡死任务监控（2分钟超时）
- **pending 缓冲**：global → pending → P，崩溃后可恢复

## API 接口

| 方法 | 路径                  | 说明                 |
| ---- | --------------------- | -------------------- |
| POST | `/api/v1/jobs/submit` | 提交单个任务         |
| POST | `/api/v1/jobs/batch`  | 批量提交（≤10000）   |
| GET  | `/api/v1/jobs/dead`   | 查看死信队列（分页） |

### 限流

令牌桶算法，每 IP 100 QPS，突发 200。

## 压测

```bash
# JMeter 测试计划
jmeter -n -t benchmark/jmeter/go-flash-job-benchmark.jmx -l result.jtl

# Mock 服务支持失败率和延迟控制
curl http://localhost:8000/execute?fail_rate=0.1&delay_ms=100
```
