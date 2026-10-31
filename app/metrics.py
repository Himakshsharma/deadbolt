"""In-process metrics: counters, rates and latency percentiles per strategy.

Everything here runs on a single asyncio event loop, so plain Python
objects are safe without locks (no await happens mid-update).
"""
from __future__ import annotations

import asyncio
import time
from collections import Counter, deque
from dataclasses import dataclass, field

from .models import DROPPED_OUTCOMES, OrderResult, Outcome, Strategy

HISTORY_POINTS = 120          # seconds of rate history kept for charts
LATENCY_SAMPLES = 50_000      # rolling latency window per strategy


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


@dataclass
class StrategyStats:
    strategy: Strategy
    requests: int = 0
    outcomes: Counter = field(default_factory=Counter)
    db_commits: int = 0
    deadlocks_hit: int = 0
    conflicts_hit: int = 0
    retries: int = 0
    latencies: deque = field(default_factory=lambda: deque(maxlen=LATENCY_SAMPLES))
    # rate tracking
    rps: float = 0.0
    cps: float = 0.0
    peak_rps: float = 0.0
    _last_requests: int = 0
    _last_commits: int = 0
    _last_deadlocks: int = 0
    history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_POINTS))

    def record(self, result: OrderResult) -> None:
        self.requests += 1
        self.outcomes[result.outcome.value] += 1
        self.deadlocks_hit += result.deadlocks_hit
        self.conflicts_hit += result.conflicts_hit
        self.retries += max(result.attempts - 1, 0)
        self.latencies.append(result.latency_ms)

    def tick(self, now: float, elapsed: float) -> None:
        elapsed = max(elapsed, 1e-6)
        self.rps = (self.requests - self._last_requests) / elapsed
        self.cps = (self.db_commits - self._last_commits) / elapsed
        self.peak_rps = max(self.peak_rps, self.rps)
        dps = (self.deadlocks_hit - self._last_deadlocks) / elapsed
        self._last_requests = self.requests
        self._last_commits = self.db_commits
        self._last_deadlocks = self.deadlocks_hit
        # (timestamp, requests/s, commits/s, deadlocks/s)
        self.history.append((round(now, 3), round(self.rps, 1), round(self.cps, 1), round(dps, 1)))

    def snapshot(self) -> dict:
        lat = sorted(self.latencies)
        dropped = sum(self.outcomes.get(o.value, 0) for o in DROPPED_OUTCOMES)
        return {
            "strategy": self.strategy.value,
            "requests": self.requests,
            "confirmed": self.outcomes.get(Outcome.CONFIRMED.value, 0),
            "sold_out": self.outcomes.get(Outcome.SOLD_OUT.value, 0),
            "dropped": dropped,
            "outcomes": dict(self.outcomes),
            "db_commits": self.db_commits,
            "deadlocks_hit": self.deadlocks_hit,
            "deadlock_drops": self.outcomes.get(Outcome.DEADLOCK.value, 0),
            "conflicts_hit": self.conflicts_hit,
            "retries": self.retries,
            "deadlock_rate_pct": round(100.0 * self.deadlocks_hit / self.requests, 3) if self.requests else 0.0,
            "drop_rate_pct": round(100.0 * dropped / self.requests, 3) if self.requests else 0.0,
            "rps": round(self.rps, 1),
            "cps": round(self.cps, 1),
            "peak_rps": round(self.peak_rps, 1),
            "latency_ms": {
                "p50": round(_percentile(lat, 0.50), 2),
                "p95": round(_percentile(lat, 0.95), 2),
                "p99": round(_percentile(lat, 0.99), 2),
                "max": round(lat[-1], 2) if lat else 0.0,
            },
            "history": list(self.history),
        }


@dataclass
class BatchStats:
    queue_depth: int = 0
    queue_capacity: int = 0
    window_ms: float = 0.0
    batches: int = 0
    flush_by_size: int = 0
    flush_by_time: int = 0
    flush_by_idle: int = 0
    last_batch_size: int = 0
    avg_batch_size: float = 0.0
    last_flush_ms: float = 0.0
    avg_flush_ms: float = 0.0
    batch_retries: int = 0
    rejected: int = 0
    depth_history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_POINTS))

    def record_flush(self, size: int, reason: str, flush_ms: float) -> None:
        self.batches += 1
        if reason == "size":
            self.flush_by_size += 1
        elif reason == "time":
            self.flush_by_time += 1
        else:
            self.flush_by_idle += 1
        self.last_batch_size = size
        self.last_flush_ms = flush_ms
        alpha = 0.1
        if self.batches == 1:
            self.avg_batch_size, self.avg_flush_ms = float(size), flush_ms
        else:
            self.avg_batch_size += alpha * (size - self.avg_batch_size)
            self.avg_flush_ms += alpha * (flush_ms - self.avg_flush_ms)

    def snapshot(self) -> dict:
        return {
            "queue_depth": self.queue_depth,
            "queue_capacity": self.queue_capacity,
            "window_ms": round(self.window_ms, 2),
            "batches": self.batches,
            "flush_by_size": self.flush_by_size,
            "flush_by_time": self.flush_by_time,
            "flush_by_idle": self.flush_by_idle,
            "last_batch_size": self.last_batch_size,
            "avg_batch_size": round(self.avg_batch_size, 1),
            "last_flush_ms": round(self.last_flush_ms, 2),
            "avg_flush_ms": round(self.avg_flush_ms, 2),
            "batch_retries": self.batch_retries,
            "rejected": self.rejected,
            "depth_history": list(self.depth_history),
        }


class MetricsHub:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.strategies: dict[Strategy, StrategyStats] = {}
        self.batch = BatchStats()
        self._last_tick = time.monotonic()
        self._sampler: asyncio.Task | None = None
        self.reset()

    def reset(self) -> None:
        queue_capacity = self.batch.queue_capacity
        self.strategies = {s: StrategyStats(strategy=s) for s in Strategy}
        self.batch = BatchStats(queue_capacity=queue_capacity)
        self.started_at = time.time()
        self._last_tick = time.monotonic()

    def record(self, result: OrderResult) -> None:
        self.strategies[result.strategy].record(result)

    def record_commits(self, strategy: Strategy, n: int = 1) -> None:
        self.strategies[strategy].db_commits += n

    def tick(self) -> None:
        now_mono = time.monotonic()
        elapsed = now_mono - self._last_tick
        self._last_tick = now_mono
        now = time.time()
        for stats in self.strategies.values():
            stats.tick(now, elapsed)
        self.batch.depth_history.append((round(now, 3), self.batch.queue_depth))

    async def _sample_forever(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            self.tick()

    def start(self) -> None:
        if self._sampler is None or self._sampler.done():
            self._sampler = asyncio.create_task(self._sample_forever(), name="metrics-sampler")

    async def stop(self) -> None:
        if self._sampler:
            self._sampler.cancel()
            try:
                await self._sampler
            except asyncio.CancelledError:
                pass
            self._sampler = None

    def snapshot(self) -> dict:
        return {
            "uptime_s": round(time.time() - self.started_at, 1),
            "strategies": {s.value: st.snapshot() for s, st in self.strategies.items()},
            "batch": self.batch.snapshot(),
        }
