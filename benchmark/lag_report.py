"""Summarize first-attempt timing for one submit_tasks.py run (Go slog text logs)."""
import argparse
import math
import re

parser = argparse.ArgumentParser()
parser.add_argument("run_id")
parser.add_argument("scheduler_log")
parser.add_argument("executor_log")
args = parser.parse_args()


def report(path, field):
    samples = {}
    with open(path, encoding="utf-8-sig") as log:
        for line in log:
            fields = dict(re.findall(r'([a-z_]+)=([^\s"]+)', line))
            job = fields.get("job_id", "")
            if (job.startswith(f"bench_{args.run_id}_") and field in fields
                    and fields.get("attempt") == "0"):
                # Recovery can confirm the same job twice. Count its first observation.
                value = int(fields[field])
                samples[job] = min(value, samples.get(job, value))
    values = sorted(samples.values())
    if not values:
        print(f"{field}: no matching samples")
        return
    p99 = values[math.ceil(len(values) * 0.99) - 1]
    print(f"{field}: unique_jobs={len(values)} P99={p99}ms max={values[-1]}ms")


report(args.scheduler_log, "dispatch_lag_ms")
report(args.executor_log, "accept_lag_ms")
