#!/usr/bin/env python3
"""
AI Signals — Load Testing Suite
================================
Simulates three traffic profiles against the FastAPI trading engine:

  1. Sustained Load    — 15-20 req/s for configurable duration
  2. Burst Stress      — 100 concurrent users hitting a single endpoint
  3. Soak Test         — Long-running scheduler & memory monitoring

Usage:
  python tests/load/load_test_suite.py sustained  [--duration 300] [--rps 20]
  python tests/load/load_test_suite.py burst      [--users 100] [--rounds 5]
  python tests/load/load_test_suite.py soak       [--duration 3600]
  python tests/load/load_test_suite.py all        (runs sustained + burst)
  python tests/load/load_test_suite.py report     (generates markdown report from last run)
"""

import asyncio
import aiohttp
import argparse
import json
import math
import os
import statistics
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

BASE_URL = os.getenv("LOAD_TEST_BASE_URL", "http://localhost:5001")

ENDPOINTS = [
    {"method": "GET", "path": "/api/v1/signals/latest", "weight": 30},
    {"method": "GET", "path": "/api/v1/signals/active", "weight": 20},
    {"method": "GET", "path": "/api/v1/health/public", "weight": 15},
    {"method": "GET", "path": "/api/v1/strategies", "weight": 10},
    {"method": "GET", "path": "/api/v1/market/pairs", "weight": 10},
    {"method": "GET", "path": "/api/v1/metrics/summary", "weight": 10},
    {"method": "GET", "path": "/api/v1/scheduler/status", "weight": 5},
]


@dataclass
class RequestResult:
    endpoint: str
    status: int
    latency_ms: float
    error: Optional[str] = None
    timestamp: float = 0.0


@dataclass
class TestMetrics:
    test_name: str
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    rate_limited: int = 0
    errors_by_code: dict = field(default_factory=dict)
    latencies_ms: list = field(default_factory=list)
    requests_per_second: list = field(default_factory=list)
    endpoint_stats: dict = field(default_factory=dict)
    memory_samples: list = field(default_factory=list)

    @property
    def success_rate(self):
        return (self.successful / self.total_requests * 100) if self.total_requests else 0

    @property
    def p50(self):
        return self._percentile(50)

    @property
    def p95(self):
        return self._percentile(95)

    @property
    def p99(self):
        return self._percentile(99)

    @property
    def avg_latency(self):
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0

    @property
    def max_latency(self):
        return max(self.latencies_ms) if self.latencies_ms else 0

    @property
    def min_latency(self):
        return min(self.latencies_ms) if self.latencies_ms else 0

    @property
    def avg_rps(self):
        return statistics.mean(self.requests_per_second) if self.requests_per_second else 0

    def _percentile(self, p):
        if not self.latencies_ms:
            return 0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(math.ceil(p / 100 * len(sorted_lat))) - 1
        return sorted_lat[max(0, idx)]


def pick_endpoint():
    import random
    total_weight = sum(e["weight"] for e in ENDPOINTS)
    r = random.randint(1, total_weight)
    cumulative = 0
    for e in ENDPOINTS:
        cumulative += e["weight"]
        if r <= cumulative:
            return e
    return ENDPOINTS[0]


async def make_request(session: aiohttp.ClientSession, endpoint: dict) -> RequestResult:
    url = f"{BASE_URL}{endpoint['path']}"
    start = time.monotonic()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            await resp.read()
            latency = (time.monotonic() - start) * 1000
            return RequestResult(
                endpoint=endpoint["path"],
                status=resp.status,
                latency_ms=round(latency, 2),
                timestamp=time.time()
            )
    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        return RequestResult(
            endpoint=endpoint["path"],
            status=0,
            latency_ms=round(latency, 2),
            error=str(e)[:100],
            timestamp=time.time()
        )


async def sample_memory(session: aiohttp.ClientSession) -> Optional[dict]:
    try:
        async with session.get(f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json()
            return {
                "timestamp": time.time(),
                "database": data.get("database", {}),
                "scheduler": data.get("scheduler", {}),
                "status": data.get("status", "unknown"),
            }
    except Exception:
        return None


def print_progress(metrics: TestMetrics, interval_count: int, interval_seconds: float):
    rps = interval_count / interval_seconds if interval_seconds > 0 else 0
    metrics.requests_per_second.append(round(rps, 1))
    recent = metrics.latencies_ms[-interval_count:] if interval_count > 0 else [0]
    avg = statistics.mean(recent) if recent else 0
    p95_val = sorted(recent)[int(len(recent) * 0.95) - 1] if len(recent) > 1 else avg

    rate_limited_pct = (metrics.rate_limited / metrics.total_requests * 100) if metrics.total_requests else 0
    sys.stdout.write(
        f"\r  [{metrics.total_requests:>6} reqs] "
        f"RPS: {rps:>6.1f} | "
        f"Avg: {avg:>7.1f}ms | "
        f"P95: {p95_val:>7.1f}ms | "
        f"OK: {metrics.successful} | "
        f"429: {metrics.rate_limited} ({rate_limited_pct:.1f}%) | "
        f"Err: {metrics.failed}"
    )
    sys.stdout.flush()


def record_result(metrics: TestMetrics, result: RequestResult):
    metrics.total_requests += 1
    metrics.latencies_ms.append(result.latency_ms)

    if result.status == 200:
        metrics.successful += 1
    elif result.status == 429:
        metrics.rate_limited += 1
    elif result.status == 403:
        metrics.rate_limited += 1
    else:
        metrics.failed += 1

    code_key = str(result.status) if result.status else "conn_error"
    metrics.errors_by_code[code_key] = metrics.errors_by_code.get(code_key, 0) + 1

    ep = result.endpoint
    if ep not in metrics.endpoint_stats:
        metrics.endpoint_stats[ep] = {"count": 0, "success": 0, "latencies": []}
    metrics.endpoint_stats[ep]["count"] += 1
    if result.status == 200:
        metrics.endpoint_stats[ep]["success"] += 1
    metrics.endpoint_stats[ep]["latencies"].append(result.latency_ms)


# ─────────────────────────────────────────────────────────────
# TEST 1: SUSTAINED LOAD
# ─────────────────────────────────────────────────────────────
async def test_sustained_load(duration_seconds: int = 300, target_rps: int = 20) -> TestMetrics:
    metrics = TestMetrics(test_name="Sustained Load")
    metrics.started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*70}")
    print(f"  SUSTAINED LOAD TEST")
    print(f"  Target: {target_rps} req/s for {duration_seconds}s ({duration_seconds // 60}m {duration_seconds % 60}s)")
    print(f"  Endpoints: {len(ENDPOINTS)} weighted endpoints")
    print(f"{'='*70}")

    delay = 1.0 / target_rps
    connector = aiohttp.TCPConnector(limit=50, force_close=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        mem = await sample_memory(session)
        if mem:
            metrics.memory_samples.append(mem)

        start_time = time.monotonic()
        interval_start = start_time
        interval_count = 0
        tasks = []

        while (time.monotonic() - start_time) < duration_seconds:
            endpoint = pick_endpoint()
            task = asyncio.create_task(make_request(session, endpoint))
            tasks.append(task)

            interval_count += 1
            elapsed_interval = time.monotonic() - interval_start

            if elapsed_interval >= 5.0:
                done = [t for t in tasks if t.done()]
                for t in done:
                    record_result(metrics, t.result())
                    tasks.remove(t)
                print_progress(metrics, interval_count, elapsed_interval)
                interval_start = time.monotonic()
                interval_count = 0

                mem = await sample_memory(session)
                if mem:
                    metrics.memory_samples.append(mem)

            await asyncio.sleep(delay)

        if tasks:
            remaining = await asyncio.gather(*tasks, return_exceptions=True)
            for r in remaining:
                if isinstance(r, RequestResult):
                    record_result(metrics, r)

    metrics.finished_at = datetime.now(timezone.utc).isoformat()
    metrics.duration_seconds = round(time.monotonic() - start_time, 2)
    print(f"\n  Completed in {metrics.duration_seconds:.1f}s")
    return metrics


# ─────────────────────────────────────────────────────────────
# TEST 2: BURST STRESS
# ─────────────────────────────────────────────────────────────
async def test_burst_stress(concurrent_users: int = 100, rounds: int = 5) -> TestMetrics:
    metrics = TestMetrics(test_name="Burst Stress")
    metrics.started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*70}")
    print(f"  BURST STRESS TEST")
    print(f"  {concurrent_users} concurrent users x {rounds} rounds")
    print(f"  Target endpoint: /api/v1/signals/latest")
    print(f"{'='*70}")

    target = {"method": "GET", "path": "/api/v1/signals/latest", "weight": 100}
    connector = aiohttp.TCPConnector(limit=concurrent_users + 10, force_close=False)
    start_time = time.monotonic()

    async with aiohttp.ClientSession(connector=connector) as session:
        for round_num in range(1, rounds + 1):
            print(f"\n  Round {round_num}/{rounds}: Firing {concurrent_users} simultaneous requests...")
            round_start = time.monotonic()

            tasks = [make_request(session, target) for _ in range(concurrent_users)]
            results = await asyncio.gather(*tasks)

            round_latencies = []
            round_success = 0
            round_limited = 0
            round_errors = 0

            for r in results:
                record_result(metrics, r)
                round_latencies.append(r.latency_ms)
                if r.status == 200:
                    round_success += 1
                elif r.status in (429, 403):
                    round_limited += 1
                else:
                    round_errors += 1

            round_duration = time.monotonic() - round_start
            metrics.requests_per_second.append(round(concurrent_users / round_duration, 1))

            avg_lat = statistics.mean(round_latencies)
            p95_lat = sorted(round_latencies)[int(len(round_latencies) * 0.95) - 1]
            max_lat = max(round_latencies)

            print(f"    Duration: {round_duration*1000:.0f}ms | "
                  f"Avg: {avg_lat:.1f}ms | P95: {p95_lat:.1f}ms | Max: {max_lat:.1f}ms")
            print(f"    OK: {round_success} | Rate-Limited: {round_limited} | Errors: {round_errors}")

            if round_num < rounds:
                print(f"    Cooling down 3s...")
                await asyncio.sleep(3)

    metrics.finished_at = datetime.now(timezone.utc).isoformat()
    metrics.duration_seconds = round(time.monotonic() - start_time, 2)
    print(f"\n  Completed in {metrics.duration_seconds:.1f}s")
    return metrics


# ─────────────────────────────────────────────────────────────
# TEST 3: SOAK TEST (Scheduler & Memory Monitor)
# ─────────────────────────────────────────────────────────────
async def test_soak(duration_seconds: int = 3600) -> TestMetrics:
    metrics = TestMetrics(test_name="Soak Test")
    metrics.started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*70}")
    print(f"  SOAK TEST (Scheduler & Memory Monitor)")
    print(f"  Duration: {duration_seconds}s ({duration_seconds // 3600}h {(duration_seconds % 3600) // 60}m)")
    print(f"  Polling: /health every 30s + light traffic (2 req/s)")
    print(f"{'='*70}")

    connector = aiohttp.TCPConnector(limit=20, force_close=False)
    start_time = time.monotonic()
    sample_interval = 30
    last_sample = 0
    check_count = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        while (time.monotonic() - start_time) < duration_seconds:
            elapsed = time.monotonic() - start_time

            endpoint = pick_endpoint()
            result = await make_request(session, endpoint)
            record_result(metrics, result)

            if elapsed - last_sample >= sample_interval:
                last_sample = elapsed
                check_count += 1

                try:
                    async with session.get(f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        health = await resp.json()

                    async with session.get(
                        f"{BASE_URL}/admin/api/scheduler/health",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        sched = await resp.json()

                    sample = {
                        "timestamp": time.time(),
                        "elapsed_minutes": round(elapsed / 60, 1),
                        "check_number": check_count,
                        "system_status": health.get("status", "unknown"),
                        "db_connected": health.get("database", {}).get("connected", False),
                        "scheduler_running": health.get("scheduler", {}).get("running", False),
                        "scheduler_jobs": health.get("scheduler", {}).get("jobs_registered", 0),
                        "watchdog_heartbeat": health.get("watchdog", {}).get("last_heartbeat"),
                        "sched_24h_success": sched.get("last_24h_success", 0),
                        "sched_24h_failures": sched.get("last_24h_failures", 0),
                        "ws_clients": health.get("websocket", {}).get("clients", 0),
                    }
                    metrics.memory_samples.append(sample)

                    status_icon = "✅" if sample["system_status"] == "healthy" else "⚠️"
                    sched_icon = "✅" if sample["scheduler_running"] else "❌"

                    sys.stdout.write(
                        f"\r  [{elapsed/60:>5.1f}m] Check #{check_count} | "
                        f"System: {status_icon} | Sched: {sched_icon} ({sample['scheduler_jobs']} jobs) | "
                        f"24h: {sample['sched_24h_success']}ok/{sample['sched_24h_failures']}fail | "
                        f"Reqs: {metrics.total_requests} | "
                        f"Avg: {metrics.avg_latency:.1f}ms"
                    )
                    sys.stdout.flush()

                except Exception as e:
                    print(f"\n  ⚠️  Health check #{check_count} failed: {e}")

            await asyncio.sleep(0.5)

    metrics.finished_at = datetime.now(timezone.utc).isoformat()
    metrics.duration_seconds = round(time.monotonic() - start_time, 2)
    print(f"\n  Completed in {metrics.duration_seconds:.1f}s with {check_count} health checks")
    return metrics


# ─────────────────────────────────────────────────────────────
# REPORT GENERATION
# ─────────────────────────────────────────────────────────────
def generate_report(all_metrics: list[TestMetrics]) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    report = f"""# AI Signals — Load Test Report

**Date:** {timestamp}
**Environment:** Development (Replit)
**Base URL:** {BASE_URL}
**Engine:** Python FastAPI + SQLite

---

## Executive Summary

"""

    overall_pass = True
    for m in all_metrics:
        status = "PASS" if m.success_rate > 90 else "DEGRADED" if m.success_rate > 70 else "FAIL"
        if status != "PASS":
            overall_pass = False
        report += f"- **{m.test_name}:** {status} — {m.total_requests} requests, {m.success_rate:.1f}% success, P95={m.p95:.1f}ms\n"

    report += f"\n**Overall Result:** {'ALL TESTS PASSED' if overall_pass else 'ISSUES DETECTED — SEE DETAILS'}\n"

    for m in all_metrics:
        report += f"""
---

## {m.test_name}

| Metric | Value |
|--------|-------|
| Duration | {m.duration_seconds:.1f}s |
| Total Requests | {m.total_requests:,} |
| Successful (200) | {m.successful:,} ({m.success_rate:.1f}%) |
| Rate Limited (429/403) | {m.rate_limited:,} |
| Errors | {m.failed:,} |
| Avg RPS | {m.avg_rps:.1f} |
| Min Latency | {m.min_latency:.1f}ms |
| Avg Latency | {m.avg_latency:.1f}ms |
| P50 Latency | {m.p50:.1f}ms |
| P95 Latency | {m.p95:.1f}ms |
| P99 Latency | {m.p99:.1f}ms |
| Max Latency | {m.max_latency:.1f}ms |

"""
        if m.errors_by_code:
            report += "### Response Code Distribution\n\n"
            report += "| Code | Count | Percentage |\n|------|-------|------------|\n"
            for code, count in sorted(m.errors_by_code.items()):
                pct = count / m.total_requests * 100
                report += f"| {code} | {count:,} | {pct:.1f}% |\n"
            report += "\n"

        if m.endpoint_stats:
            report += "### Per-Endpoint Breakdown\n\n"
            report += "| Endpoint | Requests | Success Rate | Avg Latency | P95 Latency |\n"
            report += "|----------|----------|--------------|-------------|-------------|\n"
            for ep, stats in sorted(m.endpoint_stats.items()):
                ep_success = stats["success"] / stats["count"] * 100 if stats["count"] else 0
                ep_avg = statistics.mean(stats["latencies"]) if stats["latencies"] else 0
                ep_sorted = sorted(stats["latencies"])
                ep_p95 = ep_sorted[int(len(ep_sorted) * 0.95) - 1] if len(ep_sorted) > 1 else ep_avg
                report += f"| `{ep}` | {stats['count']:,} | {ep_success:.1f}% | {ep_avg:.1f}ms | {ep_p95:.1f}ms |\n"
            report += "\n"

        if m.requests_per_second:
            report += "### Throughput Over Time\n\n"
            report += "| Interval | RPS |\n|----------|-----|\n"
            for i, rps in enumerate(m.requests_per_second):
                report += f"| {(i+1)*5}s | {rps:.1f} |\n"
            report += "\n"

        if m.memory_samples and m.test_name == "Soak Test":
            report += "### Health Check Timeline\n\n"
            report += "| Elapsed | System | Scheduler | Jobs | 24h OK/Fail |\n"
            report += "|---------|--------|-----------|------|-------------|\n"
            for s in m.memory_samples:
                sys_status = "Healthy" if s.get("system_status") == "healthy" else "Degraded"
                sched_status = "Running" if s.get("scheduler_running") else "Stopped"
                report += (
                    f"| {s.get('elapsed_minutes', 0):.1f}m | {sys_status} | "
                    f"{sched_status} | {s.get('scheduler_jobs', '--')} | "
                    f"{s.get('sched_24h_success', 0)}/{s.get('sched_24h_failures', 0)} |\n"
                )
            report += "\n"

    report += """---

## Methodology

### Test Profiles

1. **Sustained Load** — Constant request rate using weighted endpoint distribution.
   Tests in-memory cache stability, connection pool health, and steady-state latency.

2. **Burst Stress** — Simultaneous concurrent connections to a single endpoint.
   Tests rate limiting, burst protection, connection handling under spike load.

3. **Soak Test** — Extended monitoring with light traffic.
   Tracks scheduler health, database connectivity, memory stability over time.

### Infrastructure Notes

- All tests run from within the same Replit container (loopback network).
- Latency measurements include full HTTP round-trip (connect + transfer + parse).
- Rate limiting thresholds: 20 req/2s burst, 60/min, 1000/hr per IP.
- In-memory cache (TTLCache) with 60s default TTL reduces database load.

---

*Report generated by AI Signals Load Test Suite v1.0*
"""
    return report


def print_summary(metrics: TestMetrics):
    print(f"\n{'─'*70}")
    print(f"  {metrics.test_name} — SUMMARY")
    print(f"{'─'*70}")
    print(f"  Total Requests:  {metrics.total_requests:,}")
    print(f"  Successful:      {metrics.successful:,} ({metrics.success_rate:.1f}%)")
    print(f"  Rate Limited:    {metrics.rate_limited:,}")
    print(f"  Errors:          {metrics.failed:,}")
    print(f"  Avg RPS:         {metrics.avg_rps:.1f}")
    print(f"  Latency (avg):   {metrics.avg_latency:.1f}ms")
    print(f"  Latency (P50):   {metrics.p50:.1f}ms")
    print(f"  Latency (P95):   {metrics.p95:.1f}ms")
    print(f"  Latency (P99):   {metrics.p99:.1f}ms")
    print(f"  Latency (max):   {metrics.max_latency:.1f}ms")
    if metrics.errors_by_code:
        print(f"  Status codes:    {dict(sorted(metrics.errors_by_code.items()))}")
    print(f"{'─'*70}")


async def main():
    parser = argparse.ArgumentParser(description="AI Signals Load Test Suite")
    parser.add_argument("test", choices=["sustained", "burst", "soak", "all", "report"],
                        help="Test profile to run")
    parser.add_argument("--duration", type=int, default=300,
                        help="Duration in seconds (sustained/soak)")
    parser.add_argument("--rps", type=int, default=20,
                        help="Target requests/second (sustained)")
    parser.add_argument("--users", type=int, default=100,
                        help="Concurrent users (burst)")
    parser.add_argument("--rounds", type=int, default=5,
                        help="Number of burst rounds")
    parser.add_argument("--output", type=str, default="docs/LOAD_TEST_REPORT.md",
                        help="Output report path")

    args = parser.parse_args()
    all_metrics = []

    print(f"\n  AI Signals Load Test Suite v1.0")
    print(f"  Target: {BASE_URL}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                health = await resp.json()
                print(f"  Engine Status: {health.get('status', 'unknown')}")
    except Exception as e:
        print(f"\n  ERROR: Cannot reach {BASE_URL}/health — {e}")
        print(f"  Make sure the trading engine is running.")
        sys.exit(1)

    if args.test in ("sustained", "all"):
        metrics = await test_sustained_load(args.duration, args.rps)
        print_summary(metrics)
        all_metrics.append(metrics)

    if args.test in ("burst", "all"):
        if all_metrics:
            print("\n  Cooling down 5s before burst test...")
            await asyncio.sleep(5)
        metrics = await test_burst_stress(args.users, args.rounds)
        print_summary(metrics)
        all_metrics.append(metrics)

    if args.test == "soak":
        metrics = await test_soak(args.duration)
        print_summary(metrics)
        all_metrics.append(metrics)

    if all_metrics:
        report = generate_report(all_metrics)
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            f.write(report)
        print(f"\n  Report saved to: {args.output}")

        raw_data = []
        for m in all_metrics:
            raw_data.append({
                "test_name": m.test_name,
                "started_at": m.started_at,
                "finished_at": m.finished_at,
                "duration_seconds": m.duration_seconds,
                "total_requests": m.total_requests,
                "successful": m.successful,
                "failed": m.failed,
                "rate_limited": m.rate_limited,
                "errors_by_code": m.errors_by_code,
                "avg_latency_ms": round(m.avg_latency, 2),
                "p50_ms": round(m.p50, 2),
                "p95_ms": round(m.p95, 2),
                "p99_ms": round(m.p99, 2),
                "max_latency_ms": round(m.max_latency, 2),
                "avg_rps": round(m.avg_rps, 2),
                "endpoint_stats": {
                    ep: {
                        "count": s["count"],
                        "success": s["success"],
                        "avg_latency_ms": round(statistics.mean(s["latencies"]), 2) if s["latencies"] else 0,
                    }
                    for ep, s in m.endpoint_stats.items()
                },
                "memory_samples_count": len(m.memory_samples),
            })
        json_path = args.output.replace(".md", ".json")
        with open(json_path, "w") as f:
            json.dump({"tests": raw_data, "generated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        print(f"  Raw data saved to: {json_path}")


if __name__ == "__main__":
    asyncio.run(main())
