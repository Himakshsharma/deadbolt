"""Unit tests for the micro-batcher. No database needed: the executor is faked."""
from __future__ import annotations

import asyncio
import time

from app.batch_worker import MicroBatcher
from app.db_engine import BatchOutcome
from app.metrics import MetricsHub
from app.models import OrderRequest, Outcome, Strategy


class FakeExecutor:
    """Confirms everything except product 99, records each batch it receives."""

    def __init__(self, delay_s: float = 0.0) -> None:
        self.batches: list[list[OrderRequest]] = []
        self.delay_s = delay_s

    async def __call__(self, requests: list[OrderRequest]) -> BatchOutcome:
        self.batches.append(list(requests))
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        outcomes = [Outcome.SOLD_OUT if any(pid == 99 for pid, _ in r.items) else Outcome.CONFIRMED
                    for r in requests]
        return BatchOutcome(outcomes, attempts=1, deadlocks_hit=0, conflicts_hit=0)


def _req(pid: int = 1, qty: int = 1) -> OrderRequest:
    return OrderRequest(user_id=1, items=[(pid, qty)])


def test_flushes_on_max_size():
    async def scenario():
        executor = FakeExecutor(delay_s=0.01)
        batcher = MicroBatcher(executor, MetricsHub(), max_size=10, max_wait_ms=5_000, workers=1, adaptive=False)
        batcher.start()
        results = await asyncio.wait_for(asyncio.gather(*(batcher.submit(_req()) for _ in range(30))), timeout=3)
        await batcher.stop()
        return executor, results, batcher

    executor, results, batcher = asyncio.run(scenario())
    assert len(results) == 30
    assert all(r.outcome is Outcome.CONFIRMED and r.strategy is Strategy.BATCHED for r in results)
    assert [len(b) for b in executor.batches] == [10, 10, 10]


def test_flushes_on_time_window():
    async def scenario():
        metrics = MetricsHub()
        executor = FakeExecutor()
        batcher = MicroBatcher(executor, metrics, max_size=1000, max_wait_ms=80, workers=1, adaptive=False)
        batcher.start()
        t0 = time.perf_counter()
        results = await asyncio.gather(*(batcher.submit(_req()) for _ in range(5)))
        elapsed = time.perf_counter() - t0
        await batcher.stop()
        return executor, results, elapsed, metrics

    executor, results, elapsed, metrics = asyncio.run(scenario())
    assert len(executor.batches) == 1 and len(executor.batches[0]) == 5
    assert 0.07 <= elapsed < 1.0, elapsed
    assert metrics.batch.flush_by_time == 1


def test_adaptive_idle_flush_under_light_load():
    async def scenario():
        metrics = MetricsHub()
        executor = FakeExecutor()
        batcher = MicroBatcher(executor, metrics, max_size=50, max_wait_ms=500, workers=1, adaptive=True)
        batcher.start()
        t0 = time.perf_counter()
        result = await batcher.submit(_req())  # a single, isolated request
        elapsed = time.perf_counter() - t0
        await batcher.stop()
        return result, elapsed, metrics

    result, elapsed, metrics = asyncio.run(scenario())
    assert result.outcome is Outcome.CONFIRMED
    assert elapsed < 0.2, "a lone request must not wait for the whole 500ms window"
    assert metrics.batch.flush_by_idle == 1


def test_results_are_routed_to_the_right_caller_in_fifo_order():
    async def scenario():
        executor = FakeExecutor()
        batcher = MicroBatcher(executor, MetricsHub(), max_size=8, max_wait_ms=20, workers=1, adaptive=False)
        batcher.start()
        reqs = [_req(pid=99 if i % 3 == 0 else 1) for i in range(16)]
        results = await asyncio.gather(*(batcher.submit(r) for r in reqs))
        await batcher.stop()
        return executor, reqs, results

    executor, reqs, results = asyncio.run(scenario())
    for req, res in zip(reqs, results):
        assert res.request_id == req.request_id
        expected = Outcome.SOLD_OUT if req.items[0][0] == 99 else Outcome.CONFIRMED
        assert res.outcome is expected
    flattened = [r.request_id for batch in executor.batches for r in batch]
    assert flattened == [r.request_id for r in reqs], "batches must preserve arrival order"


def test_back_pressure_rejects_when_queue_full():
    async def scenario():
        metrics = MetricsHub()
        executor = FakeExecutor(delay_s=0.2)
        batcher = MicroBatcher(executor, metrics, max_size=1, max_wait_ms=1, workers=1, capacity=2, adaptive=False)
        batcher.start()
        results = await asyncio.gather(*(batcher.submit(_req()) for _ in range(10)))
        await batcher.stop()
        return results, metrics

    results, metrics = asyncio.run(scenario())
    rejected = [r for r in results if r.outcome is Outcome.REJECTED]
    assert rejected, "queue capacity 2 must reject part of a burst of 10"
    assert metrics.batch.rejected == len(rejected)


def test_executor_crash_fails_batch_but_worker_survives():
    calls = {"n": 0}

    async def flaky(requests):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return BatchOutcome([Outcome.CONFIRMED] * len(requests), 1, 0, 0)

    async def scenario():
        batcher = MicroBatcher(flaky, MetricsHub(), max_size=1, max_wait_ms=1, workers=1, adaptive=False)
        batcher.start()
        first = await batcher.submit(_req())
        second = await batcher.submit(_req())
        await batcher.stop()
        return first, second

    first, second = asyncio.run(scenario())
    assert first.outcome is Outcome.ERROR and "boom" in first.error
    assert second.outcome is Outcome.CONFIRMED


def test_normalized_items_sorts_and_merges():
    req = OrderRequest(user_id=7, items=[(3, 1), (1, 2), (3, 2)])
    assert req.normalized_items() == [(1, 2), (3, 3)]
