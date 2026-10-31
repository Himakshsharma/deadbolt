"""Adaptive asynchronous micro-batcher.

Individual order requests are pushed onto a bounded asyncio.Queue. A small
pool of worker coroutines pulls them off and flushes a batch when the FIRST
of these happens:

  1. size  - the batch reached BATCH_MAX_SIZE requests
  2. time  - BATCH_MAX_WAIT_MS elapsed since the first request of the batch arrived
  3. idle  - adaptive early flush: the observed arrival rate says fewer than one
             more request is expected inside the window, so waiting would only
             add latency without improving the batch

Each batch is committed by DBEngine.execute_batch in ONE transaction with a
fixed number of round trips, whatever its size. Every caller awaits its own
Future and receives an individual OrderResult (CONFIRMED / SOLD_OUT / ...).

asyncio.Queue is coroutine-safe within the event loop; the API, the load
runner and the workers all live on the same loop.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from .db_engine import BatchOutcome
from .metrics import MetricsHub
from .models import OrderRequest, OrderResult, Outcome, Strategy

log = logging.getLogger("deadbolt.batcher")

BatchExecutor = Callable[[list[OrderRequest]], Awaitable[BatchOutcome]]


@dataclass(slots=True)
class _Pending:
    request: OrderRequest
    future: asyncio.Future
    enqueued_at: float = field(default_factory=time.perf_counter)


class MicroBatcher:
    def __init__(
        self,
        executor: BatchExecutor,
        metrics: MetricsHub,
        max_size: int = 50,
        max_wait_ms: float = 50.0,
        workers: int = 4,
        capacity: int = 20_000,
        adaptive: bool = True,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._executor = executor
        self._metrics = metrics
        self.max_size = max_size
        self.max_wait_s = max_wait_ms / 1000.0
        self.worker_count = workers
        self.adaptive = adaptive
        self.queue: asyncio.Queue[_Pending] = asyncio.Queue(maxsize=capacity)
        self._workers: list[asyncio.Task] = []
        # EWMA of request inter-arrival time (seconds); drives the adaptive flush
        self._ewma_gap_s: float | None = None
        self._last_arrival: float | None = None
        metrics.batch.queue_capacity = capacity
        metrics.batch.window_ms = max_wait_ms

    # ------------------------------------------------------------------ public API
    @property
    def depth(self) -> int:
        return self.queue.qsize()

    def _observe_arrival(self) -> None:
        now = time.perf_counter()
        if self._last_arrival is not None:
            gap = now - self._last_arrival
            self._ewma_gap_s = gap if self._ewma_gap_s is None else 0.8 * self._ewma_gap_s + 0.2 * gap
        self._last_arrival = now

    def expected_arrivals(self, window_s: float) -> float:
        """How many more requests we expect to see within `window_s`."""
        if self._ewma_gap_s is None or self._ewma_gap_s <= 0:
            return float("inf") if self._ewma_gap_s == 0 else 0.0
        # if nothing arrived for a while, the stale EWMA must not keep us waiting
        since_last = time.perf_counter() - (self._last_arrival or 0.0)
        effective_gap = max(self._ewma_gap_s, since_last)
        return window_s / effective_gap

    async def submit(self, request: OrderRequest) -> OrderResult:
        loop = asyncio.get_running_loop()
        pending = _Pending(request=request, future=loop.create_future())
        self._observe_arrival()
        try:
            self.queue.put_nowait(pending)
        except asyncio.QueueFull:
            self._metrics.batch.rejected += 1
            return OrderResult(request.request_id, Strategy.BATCHED, Outcome.REJECTED, 0.0,
                               error="micro-batch queue full (back-pressure)")
        self._metrics.batch.queue_depth = self.queue.qsize()
        return await pending.future

    def start(self) -> None:
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"batch-worker-{i}")
            for i in range(self.worker_count)
        ]
        log.info("micro-batcher started: %d workers, max_size=%d, window=%.1fms",
                 self.worker_count, self.max_size, self.max_wait_s * 1000)

    async def stop(self) -> None:
        for task in self._workers:
            task.cancel()
        for task in self._workers:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._workers = []
        # fail anything still queued so no caller hangs forever
        while not self.queue.empty():
            pending = self.queue.get_nowait()
            if not pending.future.done():
                pending.future.set_result(OrderResult(
                    pending.request.request_id, Strategy.BATCHED, Outcome.ERROR, 0.0,
                    error="batcher stopped"))

    # ------------------------------------------------------------------ internals
    async def collect(self) -> tuple[list[_Pending], str]:
        """Block for the first item, then gather more until size / time / idle."""
        loop = asyncio.get_running_loop()
        batch = [await self.queue.get()]
        deadline = loop.time() + self.max_wait_s

        # 1. take everything that is already waiting (no syscall, no sleep)
        while len(batch) < self.max_size:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if len(batch) >= self.max_size:
            return batch, "size"

        # 2. adaptive: under light load don't hold requests hostage for the full window
        if self.adaptive and self.expected_arrivals(self.max_wait_s) < 1.0:
            return batch, "idle"

        # 3. wait for more until the window closes or the batch fills up
        while len(batch) < self.max_size:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return batch, "time"
            try:
                batch.append(await asyncio.wait_for(self.queue.get(), timeout=remaining))
            except asyncio.TimeoutError:
                return batch, "time"
        return batch, "size"

    async def _worker(self, worker_id: int) -> None:
        while True:
            batch, reason = await self.collect()
            self._metrics.batch.queue_depth = self.queue.qsize()
            started = time.perf_counter()
            try:
                result = await self._executor([p.request for p in batch])
            except asyncio.CancelledError:
                for p in batch:
                    if not p.future.done():
                        p.future.set_result(OrderResult(p.request.request_id, Strategy.BATCHED,
                                                        Outcome.ERROR, 0.0, error="batcher stopped"))
                raise
            except Exception as exc:  # executor must not kill the worker
                log.exception("batch executor crashed")
                result = BatchOutcome([Outcome.ERROR] * len(batch), 1, 0, 0, f"{type(exc).__name__}: {exc}")

            finished = time.perf_counter()
            self._metrics.batch.record_flush(len(batch), reason, (finished - started) * 1000.0)

            for index, (pending, outcome) in enumerate(zip(batch, result.outcomes)):
                if pending.future.done():
                    continue
                # deadlocks / conflicts are per transaction: attribute them once per batch
                pending.future.set_result(OrderResult(
                    request_id=pending.request.request_id,
                    strategy=Strategy.BATCHED,
                    outcome=outcome,
                    latency_ms=(finished - pending.enqueued_at) * 1000.0,
                    attempts=result.attempts if index == 0 else 1,
                    deadlocks_hit=result.deadlocks_hit if index == 0 else 0,
                    conflicts_hit=result.conflicts_hit if index == 0 else 0,
                    error=result.error if outcome not in (Outcome.CONFIRMED, Outcome.SOLD_OUT) else None,
                ))
