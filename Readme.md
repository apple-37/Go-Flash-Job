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

令牌桶算法，按 IP 独立计数，按接口语义分级：

| 接口           | QPS  | 突发 | 设计意图                 |
| -------------- | ---- | ---- | ------------------------ |
| `/jobs/submit` | 100  | 200  | 防止外部滥用打爆 Redis   |
| `/jobs/batch`  | 2000 | 5000 | 支持业务方大批量注入任务 |

## 压测与验证

### 0. 环境准备（5 个终端）

```bash
# 终端 1: Redis
redis-server --daemonize yes --port 6379
# 终端 2: Kafka（KRaft 模式，首次需 format）
cd ~/kafka/kafka_2.13-4.1.0
bin/kafka-server-start.sh -daemon config/server.properties
# 终端 3: RabbitMQ
sudo service rabbitmq-server start
# 终端 4: Mock 业务服务
cd benchmark/mock_service && pip install fastapi uvicorn requests
uvicorn mock_service:app --host 0.0.0.0 --port 8000
# 终端 5: Executor
go run executor/cmd/main.go
# 终端 6: Scheduler
go run scheduler/cmd/main.go
```

启动后看到 `👑 已成为 Leader` + `🌟 Scheduler HTTP 服务启动于 :8080` 即就绪。

### 1. 单实例高并发压测

```bash
# 1. 清空历史数据
redis-cli FLUSHDB && redis-cli DEL flash_job:scheduler_lock

# 场景 A: 纯吞吐（默认批量模式，推荐）
python3 benchmark/submit_tasks.py --count 10000 --workers 20

# 场景 B: 10% 失败率（验证重试逻辑）
python3 benchmark/submit_tasks.py --count 10000 --workers 20 --fail-rate 0.1

# 场景 C: 慢回调（验证超时 + 调度堆积）
python3 benchmark/submit_tasks.py --count 5000 --workers 20 --delay-ms 200

# 场景 D: 限流验证（单条接口模式，预期被限流到 ~100 QPS）
python3 benchmark/submit_tasks.py --count 1000 --workers 50 --single

# 场景 E: 大并发冲击
python3 benchmark/submit_tasks.py --count 50000 --workers 50
```

压测时新开终端观察指标：

```bash
redis-cli ZCARD flash_job:active_tasks        # 活跃任务数
redis-cli ZCARD flash_job:terminal_tasks      # 终态任务数
redis-cli GET flash_job:scheduler_lock        # 当前 Leader
curl http://localhost:8000/stats              # Mock 收到的回调数
```

### 2. 分布式选主验证（多实例）

```bash
# 准备第二个实例配置
mkdir -p instance2 && cp config.yaml instance2/
# 编辑 instance2/config.yaml，端口改为 ":8081"

# 同时启动两个 scheduler
go run scheduler/cmd/main.go             # 实例 1 端口 8080
cd instance2 && go run ../scheduler/cmd/main.go   # 实例 2 端口 8081

# 验证只有一个 Leader
redis-cli GET flash_job:scheduler_lock    # hostname:pid 对应其中一个实例
```

两个终端只会打印一个 `👑 已成为 Leader`，另一个打印 `⏳ 等待成为 Leader`。

### 3. 故障切换验证

```bash
# 1. 提交任务到主节点
python3 benchmark/submit_tasks.py --count 100 --workers 10

# 2. 强杀 Leader（在打印 👑 的终端按 Ctrl+C）
#    注意：锁会在 10s 后自动过期，backup 自动接管

# 3. 等待 ~10s（锁 TTL 过期），观察 backup 自动接管
#    backup 终端打印：👑 [...] 已成为 Leader，开始调度

# 4. 再提交任务（用 backup 的 8081 端口）
python3 benchmark/submit_tasks.py --count 100 --workers 10 --url http://localhost:8081

# 5. 验证两批任务都被执行
curl http://localhost:8000/stats    # total 应该是 200
```

### 4. 脑裂测试（可选，验证 CAS 续期）

```bash
# 手动篡改锁（模拟主节点网络分区）
redis-cli SET flash_job:scheduler_lock "fake-id" XX

# 主节点下次续期（3s 内）会 CAS 失败，让出 Leader 身份：
# ⚠️ 续期失败（err=<nil>, ok=0），让出 Leader 身份
# backup 随后接管
```

### 5. 正确性验证 checklist

| 场景       | 验证方法                       | 期望结果                         |
| ---------- | ------------------------------ | -------------------------------- |
| 单机吞吐   | `--count 10000 --workers 20`   | QPS ≥ 1000，mock 收到 10000 次   |
| 重试正确性 | `--fail-rate 0.1 --count 1000` | 最终成功率 ≈ 100%（max_retry=3） |
| 超时处理   | `--delay-ms 5000`              | 任务超时后进入 RETRY             |
| 选主       | 起两个实例                     | 只有一个 Leader                  |
| 故障切换   | 杀 Leader                      | 10s 内 backup 接管               |
| 队列零丢失 | 压测后查 ZSet                  | active + terminal = 提交总数     |
| 幂等       | 同 jobID 提交两次              | 第二次返回已存在                 |
| 死信       | `--fail-rate 1.0`              | 重试 3 次后进 dead_tasks         |

### 6. 关键指标检查

```bash
redis-cli
> ZCARD flash_job:active_tasks     # 活跃任务（压测后应趋近 0）
> ZCARD flash_job:terminal_tasks   # 终态任务（应等于成功提交数）
> ZCARD flash_job:dead_queue       # 死信队列
> ZREVRANGE flash_job:active_tasks 0 9 WITHSCORES   # 看前 10 个活跃任务

# 死信查询 API
curl "http://localhost:8080/api/v1/jobs/dead?start=0&stop=9"

# JMeter 测试计划（可选）
jmeter -n -t benchmark/jmeter/go-flash-job-benchmark.jmx -l result.jtl
```
