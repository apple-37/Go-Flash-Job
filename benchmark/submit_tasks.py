"""
压测任务生成器 - 批量提交任务到 scheduler

用法：
    # 默认批量模式（推荐，绕开单接口限流）
    python submit_tasks.py --count 10000

    # 单条模式（测限流/单接口 QPS 时用）
    python submit_tasks.py --count 1000 --single

会批量提交 N 个任务到 scheduler，每个任务的 callback_url 指向 mock_service
"""
import argparse
import json
import time
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor


def build_task(callback_url, idx, fail_rate=0.0, delay_ms=0):
    """构造一个任务"""
    task_id = f"bench_{int(time.time())}_{idx:06d}"
    return {
        "id": task_id,
        "name": f"压测任务_{idx}",
        "callback_url": f"{callback_url}/run?fail_rate={fail_rate}&delay_ms={delay_ms}",
        "trigger_time": 1,  # 1 秒后执行
        "priority": "Medium",
        "max_retry": 3,
        "timeout": 30,
        "payload": {
            "task_idx": idx,
            "data": f"payload_{idx}",
        },
    }


def submit_one(scheduler_url, callback_url, idx, fail_rate=0.0, delay_ms=0):
    """提交单个任务"""
    task = build_task(callback_url, idx, fail_rate, delay_ms)
    try:
        resp = requests.post(scheduler_url, json=task, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ 提交失败 idx={idx}: {e}")
        return False


def submit_batch(batch_url, callback_url, start_idx, batch_size, fail_rate=0.0, delay_ms=0):
    """批量提交任务（一次 HTTP 请求提交 batch_size 个任务）"""
    tasks = [
        build_task(callback_url, start_idx + i, fail_rate, delay_ms)
        for i in range(batch_size)
    ]
    try:
        resp = requests.post(batch_url, json={"tasks": tasks}, timeout=10)
        if resp.status_code == 200:
            return batch_size
        else:
            print(f"❌ 批量提交失败 status={resp.status_code} resp={resp.text[:200]}")
            return 0
    except Exception as e:
        print(f"❌ 批量提交异常: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(description="批量提交压测任务")
    parser.add_argument("--count", type=int, default=1000, help="任务数量")
    parser.add_argument("--url", type=str, default="",
                        help="scheduler API 地址（默认自动选择）")
    parser.add_argument("--callback", type=str, default="http://localhost:8000",
                        help="mock_service 地址")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="失败率 0-1")
    parser.add_argument("--delay-ms", type=int, default=0, help="回调延迟 ms")
    parser.add_argument("--workers", type=int, default=20, help="并发提交线程数")
    parser.add_argument("--batch-size", type=int, default=100, help="批量模式每批任务数")
    parser.add_argument("--single", action="store_true", help="使用单条接口（默认批量接口）")
    args = parser.parse_args()

    # 自动选择 API 路径
    base = "http://localhost:8080"
    if args.url:
        base = args.url.rstrip("/submit").rstrip("/batch").rstrip("/")
    submit_url = f"{base}/api/v1/jobs/submit"
    batch_url = f"{base}/api/v1/jobs/batch"

    if args.single:
        print(f"🚀 [单条模式] 开始提交 {args.count} 个任务到 {submit_url}")
        print(f"   callback: {args.callback}, workers: {args.workers}")
        start = time.time()
        success = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [
                pool.submit(submit_one, submit_url, args.callback, i,
                            args.fail_rate, args.delay_ms)
                for i in range(args.count)
            ]
            for i, f in enumerate(futures):
                if f.result():
                    success += 1
                if (i + 1) % 1000 == 0:
                    print(f"   已提交 {i+1}/{args.count}")
        cost = time.time() - start
        print(f"\n✅ 提交完成: {success}/{args.count} 成功, 耗时 {cost:.2f}s, QPS {success/cost:.1f}")
    else:
        print(f"🚀 [批量模式] 开始提交 {args.count} 个任务到 {batch_url}")
        print(f"   callback: {args.callback}, batch_size: {args.batch_size}, workers: {args.workers}")
        # 构造批次列表
        batches = []
        idx = 0
        while idx < args.count:
            sz = min(args.batch_size, args.count - idx)
            batches.append((idx, sz))
            idx += sz

        start = time.time()
        success = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [
                pool.submit(submit_batch, batch_url, args.callback, s, sz,
                            args.fail_rate, args.delay_ms)
                for s, sz in batches
            ]
            done = 0
            for f in futures:
                success += f.result()
                done += 1
                if done % 10 == 0:
                    print(f"   已完成 {done}/{len(batches)} 批 ({success}/{args.count} 任务)")
        cost = time.time() - start
        print(f"\n✅ 提交完成: {success}/{args.count} 成功, 耗时 {cost:.2f}s, QPS {success/cost:.1f}")


if __name__ == "__main__":
    main()
