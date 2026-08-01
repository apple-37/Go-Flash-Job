"""
Mock 业务方服务 - 用于 JMeter 压测

启动：
    pip install fastapi uvicorn
    uvicorn mock_service:app --host 0.0.0.0 --port 8000

压测时 executor 会回调这个服务的 /run 接口
通过 ?fail_rate=0.1 可控制 10% 失败率，测试重试逻辑
通过 ?delay_ms=100 可控制响应延迟，测试超时
"""
import os
import random
import time
import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Mock Business Service")

# 全局统计
stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
}


@app.post("/run")
async def run_task(request: Request):
    """接收 executor 的 HTTP 回调"""
    stats["total"] += 1

    payload = await request.json()
    job_id = request.headers.get("X-Job-ID", "unknown")
    retry = request.headers.get("X-Retry-Count", "0")

    # 可选延迟（测试超时）
    delay_ms = int(request.query_params.get("delay_ms", 0))
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)

    # 可选失败率（测试重试）
    fail_rate = float(request.query_params.get("fail_rate", 0.0))
    if random.random() < fail_rate:
        stats["failed"] += 1
        return JSONResponse(
            status_code=500,
            content={"error": "mock failure", "job_id": job_id},
        )

    stats["success"] += 1
    return {
        "status": "ok",
        "job_id": job_id,
        "retry": retry,
        "payload_size": len(str(payload)),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/stats")
async def get_stats():
    """查看统计信息"""
    return stats


@app.post("/stats/reset")
async def reset_stats():
    """重置统计"""
    global stats
    stats = {"total": 0, "success": 0, "failed": 0}
    return {"msg": "stats reset"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
