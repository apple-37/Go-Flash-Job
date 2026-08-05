# 测试指南

本文档介绍如何对 Go-Flash-Job 进行功能验证、压测和分布式测试。详细的 Bug 排查实战见 [wiki.md](file:///d:/0找实习/项目/Go-Flash-Job/wiki.md) 第九章。

---

## 一、环境准备

### 1.1 启动中间件

```bash
# Redis
redis-server --daemonize yes --port 6379
# 或在 WSL: sudo service redis-server start

# Kafka（KRaft 模式，首次需 format）
cd ~/kafka/kafka_2.13-4.1.0
# 首次启动需格式化
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t $CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh -daemon config/server.properties

# RabbitMQ
sudo service rabbitmq-server start
```

### 1.2 启动项目服务（4 个终端）

```bash
# 终端 1: Mock 业务服务（接收 executor 的 HTTP 回调，测试用）
cd benchmark/mock_service
pip install fastapi uvicorn requests
python3 mock_service.py
# 或: uvicorn mock_service:app --host 0.0.0.0 --port 8000

# 终端 2: Executor（消费 RabbitMQ，执行 HTTP 回调）
go run executor/cmd/main.go

# 终端 3: Scheduler（选主 + GMP 调度）
go run scheduler/cmd/main.go
# 看到 👑 已成为 Leader + 🌟 Scheduler HTTP 服务启动于 :8080 即就绪

# 终端 4（可选）: Logger（消费 Kafka 日志，批量刷盘）
go run logger/cmd/main.go
```

### 1.3 Mock 业务服务接口说明

Mock 服务（[benchmark/mock_service/mock_service.py](file:///d:/0找实习/项目/Go-Flash-Job/benchmark/mock_service/mock_service.py)）提供以下接口：

| 方法 | 路径           | 说明                                                  |
| ---- | -------------- | ----------------------------------------------------- |
| POST | `/run`         | 接收 executor 回调，支持 `fail_rate`、`delay_ms` 参数 |
| GET  | `/stats`       | 查看回调统计（total/success/failed）                  |
| POST | `/stats/reset` | 重置统计                                              |
| GET  | `/health`      | 健康检查                                              |

---

## 二、Reset 流程（重新测试前必做）

重新测试前必须彻底清空历史状态，否则会残留旧任务、旧锁、旧幂等键，导致测试结果不准确。

### 2.1 停掉项目服务

```bash
# 在 scheduler / executor / logger 终端按 Ctrl+C 停止
# 或用 pkill 批量停止
pkill -f "go run" 2>/dev/null
pkill -f "go-flash-job" 2>/dev/null
sleep 2

# 确认都停了
ps aux | grep -E "go run|go-flash-job" | grep -v grep
# 应该无输出
```

### 2.2 清空 Redis

```bash
# 必须在服务停止后清空，否则 scheduler 会立即重新写入 leader 锁
redis-cli FLUSHDB

# 验证清空
redis-cli DBSIZE
# 应该返回 (integer) 0
```

### 2.3 清空 RabbitMQ 队列

```bash
# 删除任务队列（executor 启动时会自动重新声明）
rabbitmqctl delete_queue flash_job:task_queue 2>/dev/null

# 或在 web 界面 http://localhost:15672 → Queues → Purge
```

### 2.4 重置 Mock 统计

```bash
# Mock 服务在运行则用 API 重置
curl -X POST http://localhost:8000/stats/reset
# 验证
curl http://localhost:8000/stats
# 应该返回 {"total":0,"success":0,"failed":0}
```

### 2.5 一键 Reset 脚本

```bash
#!/bin/bash
# reset.sh - 一键清空所有测试状态
echo "🧹 开始清空..."

# 1. 停服务
pkill -f "go run" 2>/dev/null
sleep 2

# 2. 清 Redis
redis-cli FLUSHDB > /dev/null

# 3. 清 RabbitMQ
rabbitmqctl delete_queue flash_job:task_queue 2>/dev/null

# 4. 重置 Mock
curl -s -X POST http://localhost:8000/stats/reset > /dev/null

echo "✅ 清空完成"
echo "Redis DBSIZE: $(redis-cli DBSIZE)"
echo "Mock Stats:   $(curl -s http://localhost:8000/stats)"
```

---

## 三、再次测试流程

### 3.1 启动服务

按 1.2 节顺序启动 Mock → Executor → Scheduler。

### 3.2 提交任务

```bash
# 批量提交（默认走 /jobs/batch，推荐）
python3 benchmark/submit_tasks.py --count 10000 --workers 20

# 单条接口提交
python3 benchmark/submit_tasks.py --count 1000 --workers 20 --single

# 指定 callback（默认 http://localhost:8000）
python3 benchmark/submit_tasks.py --count 1000 --callback http://localhost:8000

# 指定 scheduler 地址（故障切换后用）
python3 benchmark/submit_tasks.py --count 100 --url http://localhost:8081
```

### 3.3 观察指标

压测时新开终端观察：

```bash
# Redis 队列状态
echo "global_queue:   $(redis-cli ZCARD flash_job:global_queue)"     # 全局待调度
echo "pending_queue:  $(redis-cli ZCARD flash_job:pending_queue)"    # 已分发待确认
echo "active_tasks:   $(redis-cli ZCARD flash_job:active_tasks)"     # 活跃任务
echo "terminal_tasks: $(redis-cli ZCARD flash_job:terminal_tasks)"   # 终态任务
echo "dead_queue:     $(redis-cli ZCARD flash_job:dead_queue)"       # 死信
echo "dedupe_keys:    $(redis-cli --scan --pattern 'flash_job:exec:dedupe:*' | wc -l)"

# Leader 状态
echo "leader_lock:    $(redis-cli GET flash_job:scheduler_lock)"
curl -s http://localhost:8080/api/v1/status

# Mock 收到的回调
curl http://localhost:8000/stats
```

### 3.4 验证最终一致性

等所有任务执行完后（`active_tasks` 趋近 0）：

```bash
# 1. Mock 统计应该等于提交数
curl http://localhost:8000/stats

# 2. 队列零丢失：active + terminal = 提交总数
echo "active: $(redis-cli ZCARD flash_job:active_tasks)"
echo "terminal: $(redis-cli ZCARD flash_job:terminal_tasks)"
echo "dead: $(redis-cli ZCARD flash_job:dead_queue)"

# 3. 死信查询（如有失败任务）
curl "http://localhost:8080/api/v1/jobs/dead?start=0&stop=9"
```

---

## 四、测试场景

### 4.1 单实例高并发压测

```bash
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

### 4.2 分布式选主验证（多实例）

```bash
# 1. 准备第二个实例配置
mkdir -p instance2 && cp config.yaml instance2/
# 编辑 instance2/config.yaml，端口改为 ":8081"

# 2. 同时启动两个 scheduler（两个终端）
go run scheduler/cmd/main.go                          # 实例 1 端口 8080
cd instance2 && go run ../scheduler/cmd/main.go       # 实例 2 端口 8081

# 3. 验证只有一个 Leader
redis-cli GET flash_job:scheduler_lock
# 返回 hostname:8080 或 hostname:8081，对应当前 Leader

# 4. 通过 /status 端点发现 Leader（监控器用）
curl http://localhost:8080/api/v1/status
curl http://localhost:8081/api/v1/status
# 只有 Leader 返回 is_leader:true
```

### 4.3 故障切换验证

```bash
# 1. 提交任务到主节点
python3 benchmark/submit_tasks.py --count 100 --workers 10

# 2. 强杀 Leader（在打印 👑 的终端按 Ctrl+C）
#    锁会在 10s 后自动过期，backup 自动接管

# 3. 等待 ~10s，观察 backup 自动接管
#    backup 终端打印：👑 [...] 已成为 Leader，开始调度

# 4. 再提交任务（用 backup 的 8081 端口）
python3 benchmark/submit_tasks.py --count 100 --workers 10 --url http://localhost:8081

# 5. 验证两批任务都被执行
curl http://localhost:8000/stats    # total 应该是 200
```

### 4.4 脑裂测试（验证 CAS 续期）

```bash
# 1. 手动篡改锁（模拟主节点网络分区）
redis-cli SET flash_job:scheduler_lock "fake-id" XX

# 2. 主节点下次续期（3s 内）会 CAS 失败，让出 Leader 身份：
#    ⚠️ 续期失败（err=<nil>, ok=0），让出 Leader 身份
# 3. backup 随后接管
```

### 4.5 JMeter 压测

```bash
# 非 GUI 模式运行 + 生成 HTML 报告
jmeter -n -t benchmark/jmeter/go-flash-job-benchmark.jmx \
  -l result.jtl -e -o report/

# 查看报告
# 浏览器打开 report/index.html
```

---

## 五、正确性验证 checklist

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

---

## 六、压测结果

### 6.1 单接口吞吐（/api/v1/jobs/submit）

| 并发线程 | 任务总数 | 耗时 | QPS | 平均延迟 | 错误率 |
| -------- | -------- | ---- | --- | -------- | ------ |
| 50       | 5000     |      |     |          |        |
| 50       | 10000    |      |     |          |        |
| 50       | 50000    |      |     |          |        |

### 6.2 批量接口吞吐（/api/v1/jobs/batch）

| 并发线程 | 任务总数 | batch_size | 耗时 | QPS | 平均延迟 | 错误率 |
| -------- | -------- | ---------- | ---- | --- | -------- | ------ |
| 50       | 50000    | 100        |      |     |          |        |

### 6.3 端到端执行吞吐

| 提交任务数 | mock 收到回调 | 成功 | 失败 | 执行耗时 | 端到端 QPS |
| ---------- | ------------- | ---- | ---- | -------- | ---------- |
| 5000       |               |      |      |          |            |
| 10000      |               |      |      |          |            |
| 50000      |               |      |      |          |            |

### 6.4 重试与死信验证

| 场景        | fail_rate | 提交数 | 最终成功 | 死信数 | 重试次数范围   |
| ----------- | --------- | ------ | -------- | ------ | -------------- |
| 10% 失败率  | 0.1       | 1000   |          |        |                |
| 100% 失败率 | 1.0       | 100    |          |        | 3（max_retry） |

### 6.5 分布式选主验证

| 场景         | 验证结果 |
| ------------ | -------- |
| 双实例启动   |          |
| Leader 挂掉  |          |
| 故障接管耗时 |          |

### 6.6 性能瓶颈分析

- **提交层瓶颈**：\_\_\_\_（API 限流 / Redis 写入 / JSON 序列化）
- **调度层瓶颈**：\_\_\_\_（fetcher tick / Lua 脚本 / P 数量）
- **执行层瓶颈**：\_\_\_\_（HTTP 回调延迟 / executor worker 数 / mock 服务处理能力）

### 6.7 关键设计指标达成

| 指标         | 目标      | 实测 |
| ------------ | --------- | ---- |
| 提交 QPS     | ≥ 2000    |      |
| 调度延迟 P99 | < 200ms   |      |
| 端到端吞吐   | ≥ 500 QPS |      |
| 故障接管时间 | < 10s     |      |
| 任务零丢失   | 是        |      |
| 重试幂等     | 是        |      |
