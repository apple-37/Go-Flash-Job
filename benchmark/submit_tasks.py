"""
压测任务生成器 - 批量提交任务到 scheduler

用法：
    python submit_tasks.py --count 10000 --url http://localhost:8080/api/v1/jobs/submit

会批量提交 N 个任务到 scheduler，每个任务的 callback_url 指向 mock_service
"""
import argparse
import json
import time
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor


def submit_one(scheduler_url, callback_url, idx, fail_rate=0.0, delay_ms=0):
    """提交单个任务"""
    task_id = f"bench_{int(time.time())}_{idx:06d}"
    task = {
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
    try:
        resp = requests.post(scheduler_url, json=task, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ 提交失败 idx={idx}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="批量提交压测任务")
    parser.add_argument("--count", type=int, default=1000, help="任务数量")
    parser.add_argument("--url", type=str, default="http://localhost:8080/api/v1/jobs/submit",
                        help="scheduler API 地址")
    parser.add_argument("--callback", type=str, default="http://localhost:8000",
                        help="mock_service 地址")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="失败率 0-1")
    parser.add_argument("--delay-ms", type=int, default=0, help="回调延迟 ms")
    parser.add_argument("--workers", type=int, default=20, help="并发提交线程数")
    args = parser.parse_args()

    print(f"🚀 开始提交 {args.count} 个任务到 {args.url}")
    print(f"   callback: {args.callback}")
    print(f"   fail_rate: {args.fail_rate}, delay_ms: {args.delay_ms}")

    start = time.time()
    success = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        for i in range(args.count):
            futures.append(pool.submit(submit_one, args.url, args.callback, i,
                                       args.fail_rate, args.delay_ms))

        for i, f in enumerate(futures):
            if f.result():
                success += 1
            if (i + 1) % 1000 == 0:
                print(f"   已提交 {i+1}/{args.count}")

    cost = time.time() - start
    print(f"\n✅ 提交完成: {success}/{args.count} 成功, 耗时 {cost:.2f}s, QPS {success/cost:.1f}")


if __name__ == "__main__":
    main()
