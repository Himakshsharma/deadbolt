"""In-process flash-sale load generator driven from the dashboard.

Virtual users are asyncio tasks that call OrderService directly (no HTTP hop),
so a laptop can push 10,000+ requests through the database in seconds and the
measurement reflects the database strategy rather than HTTP parsing overhead.
The standalone `load_simulator.py` does the same thing over real HTTP.

The strategy is read per request, so it can be switched live mid-run.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from dataclasses import asdict, dataclass, field

from .metrics import _percentile
from .models import DROPPED_OUTCOMES, OrderRequest, Outcome, Strategy
from .service import OrderService

log = logging.getLogger("deadbolt.runner")


@dataclass
class LoadConfig:
    users: int = 1000                  # concurrent virtual users
    total_requests: int = 10_000       # requests to send in this run
    contention: float = 0.9            # probability a cart contains the hot product
    hot_product_id: int = 1
    max_items: int = 3                 # max distinct products per cart (multi-row txns => deadlock risk)
    max_quantity: int = 2              # max units per line
    strategy: Strategy = Strategy.BATCHED
    seed: int | None = None

    def validate(self) -> None:
        if not 1 <= self.users <= 20_000:
            raise ValueError("users must be between 1 and 20000")
        if not 1 <= self.total_requests <= 1_000_000:
            raise ValueError("total_requests must be between 1 and 1000000")
        if not 0.0 <= self.contention <= 1.0:
            raise ValueError("contention must be between 0 and 1")
        if not 1 <= self.max_items <= 5:
            raise ValueError("max_items must be between 1 and 5")
        if not 1 <= self.max_quantity <= 10:
            raise ValueError("max_quantity must be between 1 and 10")


class CartFactory:
    """Generates flash-sale carts with a configurable hot-row bias."""

    def __init__(self, product_ids: list[int], config: LoadConfig) -> None:
        if not product_ids:
            raise ValueError("no products in database")
        self.product_ids = product_ids
        self.cfg = config
        self.hot = config.hot_product_id if config.hot_product_id in product_ids else product_ids[0]
        self.cold = [p for p in product_ids if p != self.hot] or [self.hot]
        self.rng = random.Random(config.seed)
        self._user_seq = 0

    def make(self) -> OrderRequest:
        rng = self.rng
        n_items = min(self.cfg.max_items, len(self.product_ids))
        # 55% single item, the rest multi-item carts (multi-row transactions)
        k = 1 if n_items == 1 or rng.random() < 0.55 else rng.randint(2, n_items)
        first = self.hot if rng.random() < self.cfg.contention else rng.choice(self.cold)
        chosen = [first]
        pool = [p for p in self.product_ids if p != first]
        rng.shuffle(pool)
        chosen.extend(pool[: k - 1])
        rng.shuffle(chosen)  # client-side ordering is arbitrary -> lock-order inversion for naive code
        items = [(pid, rng.randint(1, self.cfg.max_quantity)) for pid in chosen]
        self._user_seq += 1
        return OrderRequest(user_id=100_000 + rng.randint(0, 900_000), items=items)


@dataclass
class RunSummary:
    label: str
    strategy: str
    config: dict
    elapsed_s: float = 0.0
    requests: int = 0
    throughput_rps: float = 0.0
    outcomes: dict = field(default_factory=dict)
    confirmed: int = 0
    sold_out: int = 0
    dropped: int = 0
    deadlocks_hit: int = 0
    db_commits: int = 0
    latency_ms: dict = field(default_factory=dict)
    oversold_units: int = 0
    negative_rows: int = 0
    ledger_mismatch: int = 0
    integrity_ok: bool = True
    strategy_switches: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


class LoadRunner:
    def __init__(self, service: OrderService) -> None:
        self.service = service
        self.state = "idle"            # idle | running | comparing | finished | stopped | error
        self.config: LoadConfig | None = None
        self.strategy: Strategy = Strategy.BATCHED
        self.sent = 0
        self.completed = 0
        self.started_at: float | None = None
        self.last_summary: RunSummary | None = None
        self.comparison: list[RunSummary] = []
        self.error: str | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._switches: list[dict] = []

    # ------------------------------------------------------------------ control
    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    def set_strategy(self, strategy: Strategy) -> None:
        if strategy != self.strategy:
            self._switches.append({"at_request": self.sent, "from": self.strategy.value, "to": strategy.value})
        self.strategy = strategy

    def start(self, config: LoadConfig) -> None:
        config.validate()
        if self.busy:
            raise RuntimeError("a load test is already running")
        self.config = config
        self.strategy = config.strategy
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._guarded(self._run_once(config, label="live")), name="load-run")

    def start_comparison(self, config: LoadConfig, strategies: list[Strategy]) -> None:
        config.validate()
        if self.busy:
            raise RuntimeError("a load test is already running")
        self.config = config
        self._stop = asyncio.Event()
        self.comparison = []
        self._task = asyncio.create_task(self._guarded(self._compare(config, strategies)), name="compare-run")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=30)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def wait(self) -> None:
        if self._task:
            await self._task

    async def _guarded(self, coro) -> None:
        try:
            await coro
        except Exception as exc:
            log.exception("load run failed")
            self.state, self.error = "error", f"{type(exc).__name__}: {exc}"

    # ------------------------------------------------------------------ execution
    async def _compare(self, config: LoadConfig, strategies: list[Strategy]) -> None:
        self.state = "comparing"
        for strategy in strategies:
            if self._stop.is_set():
                break
            await self.service.engine.reset()
            self.service.audit.discard()
            run_cfg = LoadConfig(**{**asdict(config), "strategy": strategy})
            summary = await self._run_once(run_cfg, label=f"compare:{strategy.value}", final_state=None)
            self.comparison.append(summary)
        self.state = "stopped" if self._stop.is_set() else "finished"

    async def _run_once(self, config: LoadConfig, label: str, final_state: str | None = "finished") -> RunSummary:
        engine = self.service.engine
        metrics = self.service.metrics
        async with engine.admin_pool.acquire() as conn:
            product_ids = [r["id"] for r in await conn.fetch("SELECT id FROM products ORDER BY id")]
        factory = CartFactory(product_ids, config)

        self.strategy = config.strategy
        self.state = "running" if final_state else self.state
        self.error = None
        self.sent = self.completed = 0
        self._switches = []
        outcomes: Counter = Counter()
        latencies: list[float] = []
        deadlocks = 0
        commits_before = {s: metrics.strategies[s].db_commits for s in Strategy}
        self.started_at = time.perf_counter()

        async def virtual_user() -> None:
            nonlocal deadlocks
            while not self._stop.is_set() and self.sent < config.total_requests:
                self.sent += 1
                request = factory.make()
                result = await self.service.place(request, self.strategy)
                self.completed += 1
                outcomes[result.outcome.value] += 1
                latencies.append(result.latency_ms)
                deadlocks += result.deadlocks_hit

        users = min(config.users, config.total_requests)
        await asyncio.gather(*(virtual_user() for _ in range(users)))
        elapsed = time.perf_counter() - self.started_at

        await self.service.audit.flush()
        integrity = await engine.integrity()
        latencies.sort()
        dropped = sum(outcomes.get(o.value, 0) for o in DROPPED_OUTCOMES)

        summary = RunSummary(
            label=label,
            strategy=config.strategy.value if not self._switches else "mixed",
            config={**asdict(config), "strategy": config.strategy.value},
            elapsed_s=round(elapsed, 3),
            requests=self.completed,
            throughput_rps=round(self.completed / elapsed, 1) if elapsed > 0 else 0.0,
            outcomes=dict(outcomes),
            confirmed=outcomes.get(Outcome.CONFIRMED.value, 0),
            sold_out=outcomes.get(Outcome.SOLD_OUT.value, 0),
            dropped=dropped,
            deadlocks_hit=deadlocks,
            db_commits=sum(metrics.strategies[s].db_commits - commits_before[s] for s in Strategy),
            latency_ms={
                "p50": round(_percentile(latencies, 0.50), 2),
                "p95": round(_percentile(latencies, 0.95), 2),
                "p99": round(_percentile(latencies, 0.99), 2),
                "max": round(latencies[-1], 2) if latencies else 0.0,
            },
            oversold_units=integrity["oversold_units"],
            negative_rows=integrity["negative_rows"],
            ledger_mismatch=integrity["ledger_mismatch"],
            integrity_ok=integrity["ok"],
            strategy_switches=list(self._switches),
        )
        self.last_summary = summary
        if final_state:
            self.state = "stopped" if self._stop.is_set() else final_state
        log.info("run %s done: %s", label, summary.to_dict())
        return summary

    # ------------------------------------------------------------------ status
    def status(self) -> dict:
        elapsed = (time.perf_counter() - self.started_at) if self.started_at and self.busy else None
        return {
            "state": self.state,
            "strategy": self.strategy.value,
            "sent": self.sent,
            "completed": self.completed,
            "target": self.config.total_requests if self.config else 0,
            "elapsed_s": round(elapsed, 2) if elapsed is not None else None,
            "error": self.error,
            "last_summary": self.last_summary.to_dict() if self.last_summary else None,
            "comparison": [s.to_dict() for s in self.comparison],
        }
