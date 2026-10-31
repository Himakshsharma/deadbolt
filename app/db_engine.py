"""Core SQL engine: raw asyncpg, explicit transactions, no ORM.

Four ways to sell inventory under contention:

  naive        BEGIN; per item (in client order): SELECT stock, check, UPDATE; COMMIT
               - check-then-act race  -> overselling (stock goes negative)
               - unordered row locks  -> deadlocks (SQLSTATE 40P01), request dropped

  pessimistic  BEGIN ISOLATION LEVEL READ COMMITTED;
               SELECT ... WHERE id = ANY($ids) ORDER BY id FOR UPDATE;   -- global lock order
               check, single UPDATE+INSERT statement; COMMIT
               + exponential backoff retry on 40P01 / 40001

  optimistic   read (no lock) -> BEGIN; per item in id order:
               UPDATE ... SET version = version + 1 WHERE id = $1 AND version = $read_version
               0 rows -> someone else won -> ROLLBACK and retry with backoff

  batched      (see batch_worker.py) one transaction per micro-batch of up to N requests:
               lock all touched rows once in id order, allocate in FIFO order in
               memory, then ONE UNNEST() statement updates stock and inserts all orders.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass

import asyncpg

from .config import SCHEMA_PATH, Settings
from .metrics import MetricsHub
from .models import OrderRequest, OrderResult, Outcome, Strategy

log = logging.getLogger("deadbolt.engine")

SQLSTATE_DEADLOCK = "40P01"
SQLSTATE_SERIALIZATION = "40001"
RETRYABLE_SQLSTATES = {SQLSTATE_DEADLOCK, SQLSTATE_SERIALIZATION}

SCHEMA_LOCK_KEY = 0x0DEAD_B017

# Must match the seed in db/schema.sql. Demand in the default load profile is
# several times this supply, like a real flash sale, so every run fights over
# the sell-out boundary - exactly where check-then-act code oversells.
DEFAULT_STOCK = {1: 600, 2: 400, 3: 300, 4: 200, 5: 150}

# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------
SQL_NAIVE_READ = "SELECT total_stock, reserved_stock FROM products WHERE id = $1"

SQL_NAIVE_WRITE = """
UPDATE products
   SET reserved_stock = reserved_stock + $2,
       version        = version + 1,
       updated_at     = now()
 WHERE id = $1
"""

SQL_LOCK_ROWS = """
SELECT id, total_stock, reserved_stock
  FROM products
 WHERE id = ANY($1::int[])
 ORDER BY id
   FOR UPDATE
"""

SQL_READ_VERSIONS = """
SELECT id, total_stock, reserved_stock, version
  FROM products
 WHERE id = ANY($1::int[])
 ORDER BY id
"""

SQL_OPTIMISTIC_WRITE = """
UPDATE products
   SET reserved_stock = reserved_stock + $2,
       version        = version + 1,
       updated_at     = now()
 WHERE id = $1
   AND version = $3
   AND total_stock - reserved_stock >= $2
RETURNING version
"""

# One statement, one round trip: apply all stock deltas and insert all order
# lines using data-modifying CTEs fed by UNNEST()'d arrays. Used by the
# pessimistic strategy (one request) and the micro-batcher (a whole batch).
SQL_APPLY = """
WITH deltas AS (
    SELECT * FROM unnest($1::int[], $2::int[]) AS d(product_id, qty)
),
stock AS (
    UPDATE products AS p
       SET reserved_stock = p.reserved_stock + deltas.qty,
           version        = p.version + 1,
           updated_at     = now()
      FROM deltas
     WHERE p.id = deltas.product_id
    RETURNING p.id
),
lines AS (
    INSERT INTO orders (request_id, user_id, product_id, quantity)
    SELECT * FROM unnest($3::uuid[], $4::int[], $5::int[], $6::int[])
    RETURNING id
)
SELECT (SELECT count(*) FROM stock) AS products_updated,
       (SELECT count(*) FROM lines) AS order_lines
"""

SQL_INSERT_ORDER_LINES = """
INSERT INTO orders (request_id, user_id, product_id, quantity)
SELECT * FROM unnest($1::uuid[], $2::int[], $3::int[], $4::int[])
"""

SQL_INTEGRITY = """
SELECT p.id,
       p.name,
       p.total_stock,
       p.reserved_stock,
       p.total_stock - p.reserved_stock AS available,
       p.version,
       p.price,
       COALESCE(o.sold_units, 0)  AS sold_units,
       COALESCE(o.order_lines, 0) AS order_lines
  FROM products p
  LEFT JOIN (
        SELECT product_id, SUM(quantity) AS sold_units, COUNT(*) AS order_lines
          FROM orders
         WHERE status = 'CONFIRMED'
         GROUP BY product_id
       ) o ON o.product_id = p.id
 ORDER BY p.id
"""

SQL_AUDIT_SUMMARY = """
SELECT strategy_used, status, COUNT(*) AS n,
       ROUND(AVG(execution_time_ms)::numeric, 3) AS avg_ms
  FROM audit_logs
 GROUP BY strategy_used, status
 ORDER BY strategy_used, status
"""


class _SoldOut(Exception):
    """Business rejection: not enough stock. Rolls the transaction back."""


class _VersionConflict(Exception):
    """Optimistic lock lost the race. Rolls the transaction back."""


def sqlstate_of(exc: BaseException) -> str | None:
    return getattr(exc, "sqlstate", None)


def short_error(exc: BaseException) -> str:
    state = sqlstate_of(exc)
    text = f"{type(exc).__name__}: {exc}".strip()
    return f"[{state}] {text}" if state else text


@dataclass(slots=True)
class BatchOutcome:
    outcomes: list[Outcome]
    attempts: int
    deadlocks_hit: int
    conflicts_hit: int
    error: str | None = None


class DBEngine:
    def __init__(self, settings: Settings, metrics: MetricsHub) -> None:
        self.settings = settings
        self.metrics = metrics
        self.pool: asyncpg.Pool | None = None          # request traffic
        self.admin_pool: asyncpg.Pool | None = None    # integrity polling, audit, admin

    # ------------------------------------------------------------------ lifecycle
    async def connect(self, retries: int = 30, delay_s: float = 1.0) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                server_settings = {
                    "application_name": "deadbolt",
                    "statement_timeout": str(self.settings.statement_timeout_ms),
                }
                self.pool = await asyncpg.create_pool(
                    dsn=self.settings.database_url,
                    min_size=self.settings.pool_min_size,
                    max_size=self.settings.pool_max_size,
                    command_timeout=self.settings.statement_timeout_ms / 1000.0 + 5,
                    server_settings=server_settings,
                )
                # A separate tiny pool so monitoring keeps working even while the
                # naive strategy has exhausted the request pool with lock waits.
                self.admin_pool = await asyncpg.create_pool(
                    dsn=self.settings.database_url,
                    min_size=1,
                    max_size=4,
                    command_timeout=30,
                    server_settings={**server_settings, "application_name": "deadbolt-admin"},
                )
                log.info("connected to PostgreSQL (pool %d..%d)",
                         self.settings.pool_min_size, self.settings.pool_max_size)
                return
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                last_exc = exc
                log.warning("database not ready (attempt %d/%d): %s", attempt, retries, exc)
                await asyncio.sleep(delay_s)
        raise RuntimeError(f"could not connect to PostgreSQL: {last_exc}")

    async def close(self) -> None:
        for name in ("pool", "admin_pool"):
            pool = getattr(self, name)
            if pool is not None:
                await pool.close()
                setattr(self, name, None)

    async def schema_ready(self, conn) -> bool:
        """True when every table and index exists and the seed products are present."""
        names = ["products", "orders", "audit_logs", "idx_orders_product_id", "idx_orders_request_id",
                 "idx_audit_logs_created_at", "idx_audit_logs_strategy"]
        present = await conn.fetchval(
            "SELECT count(*) FROM unnest($1::text[]) AS n WHERE to_regclass('public.' || n) IS NOT NULL", names)
        if present != len(names):
            return False
        seeded = await conn.fetchval("SELECT count(*) FROM products WHERE id = ANY($1::int[])", list(DEFAULT_STOCK))
        return seeded == len(DEFAULT_STOCK)

    async def apply_schema(self, attempts: int = 8) -> None:
        """Create tables, indexes and seed data, once.

        During a rolling deploy the previous instance is still serving traffic
        and holding row locks. Re-running DDL and seed inserts against a live
        database can then deadlock with it, so an already-initialised database
        is left untouched, and a first-time setup retries on lock conflicts.
        """
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        for attempt in range(1, attempts + 1):
            async with self.admin_pool.acquire() as conn:
                if await self.schema_ready(conn):
                    log.info("schema already in place; skipping setup")
                    return
                await conn.execute("SELECT pg_advisory_lock($1)", SCHEMA_LOCK_KEY)
                try:
                    await conn.execute("SET lock_timeout = '5s'")
                    await conn.execute(sql)
                    log.info("schema created")
                    return
                except asyncpg.PostgresError as exc:
                    if sqlstate_of(exc) not in RETRYABLE_SQLSTATES | {"55P03"} or attempt == attempts:
                        raise
                    log.warning("schema setup hit a lock conflict (attempt %d/%d): %s", attempt, attempts, exc)
                finally:
                    await conn.execute("RESET lock_timeout")
                    await conn.execute("SELECT pg_advisory_unlock($1)", SCHEMA_LOCK_KEY)
            await asyncio.sleep(min(0.5 * 2 ** attempt, 8))

    def _acquire(self):
        return self.pool.acquire(timeout=self.settings.pool_acquire_timeout_s)

    async def _backoff(self, attempt: int) -> None:
        base = self.settings.retry_base_delay_ms * (2 ** (attempt - 1))
        delay_ms = min(self.settings.retry_max_delay_ms, base) * random.uniform(0.5, 1.5)
        await asyncio.sleep(delay_ms / 1000.0)

    @staticmethod
    def _order_line_arrays(request: OrderRequest, items: list[tuple[int, int]]):
        n = len(items)
        return ([request.request_id] * n, [request.user_id] * n,
                [pid for pid, _ in items], [qty for _, qty in items])

    # ------------------------------------------------------------------ naive
    async def place_naive(self, request: OrderRequest) -> OrderResult:
        """Check-then-act with no row locks, items processed in client order, no retry."""
        t0 = time.perf_counter()
        think_s = self.settings.naive_think_ms / 1000.0
        outcome, error, deadlocks = Outcome.CONFIRMED, None, 0
        try:
            async with self._acquire() as conn:
                async with conn.transaction():                       # BEGIN (READ COMMITTED)
                    for product_id, qty in request.items:            # unsorted -> lock-order inversion
                        row = await conn.fetchrow(SQL_NAIVE_READ, product_id)
                        if row is None or row["total_stock"] - row["reserved_stock"] < qty:
                            raise _SoldOut()
                        if think_s > 0:
                            await asyncio.sleep(think_s)             # stale read window
                        await conn.execute(SQL_NAIVE_WRITE, product_id, qty)
                    await conn.execute(SQL_INSERT_ORDER_LINES,
                                       *self._order_line_arrays(request, request.items))
                                                                     # COMMIT on exit
            self.metrics.record_commits(Strategy.NAIVE)
        except _SoldOut:
            outcome = Outcome.SOLD_OUT                               # ROLLBACK on exit
        except Exception as exc:
            if sqlstate_of(exc) == SQLSTATE_DEADLOCK:
                outcome, deadlocks = Outcome.DEADLOCK, 1
            else:
                outcome = Outcome.ERROR
            error = short_error(exc)
        return OrderResult(request.request_id, Strategy.NAIVE, outcome,
                           (time.perf_counter() - t0) * 1000.0, 1, deadlocks, 0, error)

    # ------------------------------------------------------------------ pessimistic
    async def place_pessimistic(self, request: OrderRequest) -> OrderResult:
        """SELECT ... FOR UPDATE in global id order + retry with exponential backoff."""
        t0 = time.perf_counter()
        items = request.normalized_items()
        product_ids = [pid for pid, _ in items]
        deadlocks = conflicts = 0
        error: str | None = None
        outcome = Outcome.DEADLOCK
        attempt = 0
        try:
            async with self._acquire() as conn:
                while attempt < self.settings.retry_max_attempts:
                    attempt += 1
                    try:
                        async with conn.transaction(isolation="read_committed"):
                            rows = await conn.fetch(SQL_LOCK_ROWS, product_ids)
                            available = {r["id"]: r["total_stock"] - r["reserved_stock"] for r in rows}
                            if any(available.get(pid, -1) < qty for pid, qty in items):
                                raise _SoldOut()
                            await conn.fetchrow(SQL_APPLY, product_ids, [q for _, q in items],
                                                *self._order_line_arrays(request, items))
                        self.metrics.record_commits(Strategy.PESSIMISTIC)
                        outcome, error = Outcome.CONFIRMED, None
                        break
                    except _SoldOut:
                        outcome, error = Outcome.SOLD_OUT, None
                        break
                    except asyncpg.PostgresError as exc:
                        state = sqlstate_of(exc)
                        if state not in RETRYABLE_SQLSTATES:
                            raise
                        if state == SQLSTATE_DEADLOCK:
                            deadlocks += 1
                            outcome = Outcome.DEADLOCK
                        else:
                            conflicts += 1
                            outcome = Outcome.CONFLICT
                        error = short_error(exc)
                        if attempt < self.settings.retry_max_attempts:
                            await self._backoff(attempt)
        except Exception as exc:
            outcome, error = Outcome.ERROR, short_error(exc)
        return OrderResult(request.request_id, Strategy.PESSIMISTIC, outcome,
                           (time.perf_counter() - t0) * 1000.0, max(attempt, 1), deadlocks, conflicts, error)

    # ------------------------------------------------------------------ optimistic
    async def place_optimistic(self, request: OrderRequest) -> OrderResult:
        """Version-checked compare-and-swap UPDATEs; retry on lost race."""
        t0 = time.perf_counter()
        items = request.normalized_items()
        product_ids = [pid for pid, _ in items]
        deadlocks = conflicts = 0
        error: str | None = None
        outcome = Outcome.CONFLICT
        attempt = 0
        try:
            async with self._acquire() as conn:
                while attempt < self.settings.retry_max_attempts:
                    attempt += 1
                    snapshot = {r["id"]: r for r in await conn.fetch(SQL_READ_VERSIONS, product_ids)}
                    if any(pid not in snapshot or
                           snapshot[pid]["total_stock"] - snapshot[pid]["reserved_stock"] < qty
                           for pid, qty in items):
                        outcome, error = Outcome.SOLD_OUT, None
                        break
                    try:
                        async with conn.transaction(isolation="read_committed"):
                            for pid, qty in items:                       # id order: no deadlock cycles
                                new_version = await conn.fetchval(
                                    SQL_OPTIMISTIC_WRITE, pid, qty, snapshot[pid]["version"])
                                if new_version is None:
                                    raise _VersionConflict()
                            await conn.execute(SQL_INSERT_ORDER_LINES,
                                               *self._order_line_arrays(request, items))
                        self.metrics.record_commits(Strategy.OPTIMISTIC)
                        outcome, error = Outcome.CONFIRMED, None
                        break
                    except _VersionConflict:
                        conflicts += 1
                        outcome, error = Outcome.CONFLICT, "version conflict: retries exhausted"
                    except asyncpg.PostgresError as exc:
                        state = sqlstate_of(exc)
                        if state not in RETRYABLE_SQLSTATES:
                            raise
                        if state == SQLSTATE_DEADLOCK:
                            deadlocks += 1
                            outcome = Outcome.DEADLOCK
                        else:
                            conflicts += 1
                            outcome = Outcome.CONFLICT
                        error = short_error(exc)
                    if attempt < self.settings.retry_max_attempts:
                        await self._backoff(attempt)
        except Exception as exc:
            outcome, error = Outcome.ERROR, short_error(exc)
        return OrderResult(request.request_id, Strategy.OPTIMISTIC, outcome,
                           (time.perf_counter() - t0) * 1000.0, max(attempt, 1), deadlocks, conflicts, error)

    # ------------------------------------------------------------------ batched
    async def execute_batch(self, requests: list[OrderRequest]) -> BatchOutcome:
        """Commit a whole micro-batch in ONE transaction.

        Round trips per batch, independent of batch size:
          BEGIN, SELECT ... FOR UPDATE (all rows, id order), one UNNEST apply statement, COMMIT.
        Requests are allocated strictly in arrival (FIFO) order; each request
        is all-or-nothing across its line items.
        """
        normalized = [r.normalized_items() for r in requests]
        product_ids = sorted({pid for items in normalized for pid, _ in items})
        deadlocks = conflicts = 0
        last_error: str | None = None

        for attempt in range(1, self.settings.retry_max_attempts + 1):
            try:
                async with self._acquire() as conn:
                    async with conn.transaction(isolation="read_committed"):
                        rows = await conn.fetch(SQL_LOCK_ROWS, product_ids)
                        available = {r["id"]: r["total_stock"] - r["reserved_stock"] for r in rows}

                        outcomes: list[Outcome] = []
                        deltas: dict[int, int] = defaultdict(int)
                        req_ids: list[uuid.UUID] = []
                        user_ids: list[int] = []
                        line_pids: list[int] = []
                        line_qtys: list[int] = []

                        for request, items in zip(requests, normalized):
                            if all(available.get(pid, -1) >= qty for pid, qty in items):
                                for pid, qty in items:
                                    available[pid] -= qty
                                    deltas[pid] += qty
                                    req_ids.append(request.request_id)
                                    user_ids.append(request.user_id)
                                    line_pids.append(pid)
                                    line_qtys.append(qty)
                                outcomes.append(Outcome.CONFIRMED)
                            else:
                                outcomes.append(Outcome.SOLD_OUT)

                        if deltas:
                            delta_ids = sorted(deltas)
                            await conn.fetchrow(SQL_APPLY, delta_ids, [deltas[p] for p in delta_ids],
                                                req_ids, user_ids, line_pids, line_qtys)
                self.metrics.record_commits(Strategy.BATCHED)
                return BatchOutcome(outcomes, attempt, deadlocks, conflicts)
            except asyncpg.PostgresError as exc:
                state = sqlstate_of(exc)
                last_error = short_error(exc)
                if state == SQLSTATE_DEADLOCK:
                    deadlocks += 1
                elif state == SQLSTATE_SERIALIZATION:
                    conflicts += 1
                else:
                    return BatchOutcome([Outcome.ERROR] * len(requests), attempt, deadlocks, conflicts, last_error)
                self.metrics.batch.batch_retries += 1
                if attempt < self.settings.retry_max_attempts:
                    await self._backoff(attempt)
            except Exception as exc:
                return BatchOutcome([Outcome.ERROR] * len(requests), attempt, deadlocks, conflicts, short_error(exc))

        final = Outcome.DEADLOCK if deadlocks else Outcome.CONFLICT
        return BatchOutcome([final] * len(requests), self.settings.retry_max_attempts,
                            deadlocks, conflicts, last_error)

    # ------------------------------------------------------------------ admin
    async def reset(self, stock: dict[int, int] | None = None) -> None:
        """Empty orders/audit and restore stock (seed levels unless `stock` overrides some)."""
        levels = {**DEFAULT_STOCK, **(stock or {})}
        async with self.admin_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE orders, audit_logs RESTART IDENTITY")
                await conn.execute(
                    "UPDATE products SET reserved_stock = 0, version = 0, updated_at = now()")
                await conn.executemany(
                    "UPDATE products SET total_stock = $2 WHERE id = $1",
                    [(int(pid), int(qty)) for pid, qty in levels.items()])

    async def integrity(self) -> dict:
        async with self.admin_pool.acquire() as conn:
            rows = await conn.fetch(SQL_INTEGRITY)
        products = []
        oversold_units = 0
        negative_rows = 0
        ledger_mismatch = 0
        for r in rows:
            oversold = max(int(r["sold_units"]) - r["total_stock"], 0)
            mismatch = abs(int(r["sold_units"]) - r["reserved_stock"])
            oversold_units += oversold
            negative_rows += 1 if r["available"] < 0 else 0
            ledger_mismatch += mismatch
            products.append({
                "id": r["id"],
                "name": r["name"],
                "total_stock": r["total_stock"],
                "reserved_stock": r["reserved_stock"],
                "available": r["available"],
                "version": r["version"],
                "price": float(r["price"]),
                "sold_units": int(r["sold_units"]),
                "order_lines": int(r["order_lines"]),
                "oversold_units": oversold,
                "ledger_mismatch": mismatch,
            })
        return {
            "ok": oversold_units == 0 and negative_rows == 0 and ledger_mismatch == 0,
            "oversold_units": oversold_units,
            "negative_rows": negative_rows,
            "ledger_mismatch": ledger_mismatch,
            "products": products,
        }

    async def audit_summary(self) -> list[dict]:
        async with self.admin_pool.acquire() as conn:
            rows = await conn.fetch(SQL_AUDIT_SUMMARY)
        return [{"strategy": r["strategy_used"], "status": r["status"],
                 "count": r["n"], "avg_ms": float(r["avg_ms"])} for r in rows]

    async def db_stats(self) -> dict:
        """Server-side deadlock counter straight from pg_stat_database (ground truth)."""
        async with self.admin_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT deadlocks, xact_commit, xact_rollback FROM pg_stat_database "
                "WHERE datname = current_database()")
        return {"pg_deadlocks": row["deadlocks"], "pg_commits": row["xact_commit"],
                "pg_rollbacks": row["xact_rollback"]}
