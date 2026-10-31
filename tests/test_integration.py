"""End-to-end correctness tests against a real PostgreSQL ($DATABASE_URL).

These are the claims the project makes, written as assertions:
  * every resolver strategy keeps stock >= 0 and the orders ledger exact
  * pessimistic and micro-batched never drop a request to a deadlock
  * the naive strategy really does break under the same load (oversell / deadlocks)

Skipped automatically if the database is unreachable.
WARNING: these tests TRUNCATE orders/audit_logs and reset product stock.
"""
from __future__ import annotations

import asyncio
import dataclasses

import pytest

from app.audit import AuditWriter
from app.batch_worker import MicroBatcher
from app.config import settings
from app.db_engine import DBEngine
from app.load_runner import LoadConfig, LoadRunner
from app.metrics import MetricsHub
from app.models import OrderRequest, Outcome, Strategy
from app.service import OrderService

pytestmark = pytest.mark.integration

# Small stock so every run sells out and the boundary is fought over hard.
TEST_STOCK = {1: 150, 2: 120, 3: 100, 4: 80, 5: 60}


async def _build(**overrides):
    cfg = dataclasses.replace(settings, pool_max_size=40, pool_min_size=2, **overrides)
    metrics = MetricsHub()
    engine = DBEngine(cfg, metrics)
    await engine.connect(retries=1)
    await engine.apply_schema()
    audit = AuditWriter(engine.admin_pool, cfg.audit_flush_interval_ms, cfg.audit_flush_max_rows)
    batcher = MicroBatcher(engine.execute_batch, metrics, cfg.batch_max_size, cfg.batch_max_wait_ms,
                           cfg.batch_workers, cfg.batch_queue_capacity)
    batcher.start()
    service = OrderService(engine, batcher, metrics, audit)
    return engine, batcher, audit, service


async def _teardown(engine, batcher, audit):
    await batcher.stop()
    await audit.stop()
    await engine.reset()  # back to seed stock
    await engine.close()


async def _run(strategy: Strategy, requests: int = 1500, users: int = 200, **overrides) -> dict:
    engine, batcher, audit, service = await _build(**overrides)
    try:
        await engine.reset(TEST_STOCK)
        runner = LoadRunner(service)
        runner.start(LoadConfig(users=users, total_requests=requests, contention=0.9,
                                max_items=3, max_quantity=2, strategy=strategy, seed=7))
        await runner.wait()
        summary = runner.last_summary.to_dict()
        summary["integrity"] = await engine.integrity()
        return summary
    finally:
        await _teardown(engine, batcher, audit)


def _db_available() -> bool:
    async def probe():
        engine = DBEngine(dataclasses.replace(settings, pool_min_size=1, pool_max_size=1), MetricsHub())
        try:
            await engine.connect(retries=1)
            return True
        except Exception:
            return False
        finally:
            await engine.close()
    return asyncio.run(probe())


if not _db_available():
    pytest.skip("PostgreSQL not reachable at $DATABASE_URL", allow_module_level=True)


@pytest.mark.parametrize("strategy", [Strategy.PESSIMISTIC, Strategy.OPTIMISTIC, Strategy.BATCHED])
def test_resolvers_never_oversell(strategy):
    s = asyncio.run(_run(strategy))
    integ = s["integrity"]
    assert integ["oversold_units"] == 0, integ
    assert integ["negative_rows"] == 0, integ
    assert integ["ledger_mismatch"] == 0, integ
    assert all(p["available"] >= 0 for p in integ["products"])
    assert s["confirmed"] > 0
    assert s["confirmed"] + s["sold_out"] + s["dropped"] == s["requests"] == 1500


@pytest.mark.parametrize("strategy", [Strategy.PESSIMISTIC, Strategy.BATCHED])
def test_ordered_locking_has_zero_deadlocks(strategy):
    s = asyncio.run(_run(strategy))
    assert s["deadlocks_hit"] == 0
    assert s["outcomes"].get(Outcome.DEADLOCK.value, 0) == 0


def test_hot_product_sells_out_exactly_with_batching():
    s = asyncio.run(_run(Strategy.BATCHED, requests=3000))
    hot = next(p for p in s["integrity"]["products"] if p["id"] == 1)
    # Demand is ~10x supply: the hot row must be drained to 0 (or 1 when only qty-2
    # carts remain) - batching must not leave sellable stock behind, nor go below zero.
    assert 0 <= hot["available"] <= 1, hot
    assert hot["sold_units"] == hot["reserved_stock"] == hot["total_stock"] - hot["available"]


def test_micro_batching_commits_far_fewer_transactions():
    s = asyncio.run(_run(Strategy.BATCHED, requests=2000, users=400))
    assert s["db_commits"] > 0
    assert s["confirmed"] / s["db_commits"] >= 3, s


def test_naive_strategy_breaks_under_contention():
    # A slightly longer check->write gap makes the race deterministic enough for CI.
    s = asyncio.run(_run(Strategy.NAIVE, requests=1500, users=200, naive_think_ms=3.0))
    integ = s["integrity"]
    broke = integ["oversold_units"] > 0 or integ["negative_rows"] > 0 or s["deadlocks_hit"] > 0
    assert broke, f"expected naive strategy to oversell or deadlock: {s}"


def test_audit_log_records_every_request():
    async def scenario():
        engine, batcher, audit, service = await _build()
        try:
            await engine.reset(TEST_STOCK)
            reqs = [OrderRequest(user_id=i + 1, items=[(1, 1)]) for i in range(40)]
            await asyncio.gather(*(service.place(r, Strategy.BATCHED) for r in reqs))
            await audit.flush()
            rows = await engine.audit_summary()
            return rows
        finally:
            await _teardown(engine, batcher, audit)

    rows = asyncio.run(scenario())
    assert sum(r["count"] for r in rows if r["strategy"] == "batched") == 40
