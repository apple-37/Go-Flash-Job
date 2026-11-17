# Go-Flash-Job

Go 实现的爬虫任务调度器，按指定时间触发 HTTP 回调。

## Quickstart

准备 Go 1.25.1 或更高版本，启动 Redis 和 RabbitMQ。在项目根目录用 PowerShell 执行：

```powershell
if (!(Test-Path config.yaml)) { Copy-Item config.example.yaml config.yaml }
```

在 `config.yaml` 中填入 Redis 地址和 RabbitMQ 连接信息，然后打开两个终端，分别运行：

```powershell
go run ./scheduler
```

```powershell
go run ./executor
```

使用默认端口时，在另一个终端检查调度器状态：

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/status
```

单实例启动后，`is_leader` 应为 `True`。
