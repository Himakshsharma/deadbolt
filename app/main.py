"""FastAPI service: order ingestion, control plane, and live WebSocket telemetry."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .audit import AuditWriter
from .batch_worker import MicroBatcher
from .config import FRONTEND_DIR, settings
from .db_engine import DEFAULT_STOCK, DBEngine
from .load_runner import LoadConfig, LoadRunner
from .metrics import MetricsHub
from .models import OrderRequest, Strategy
from .service import OrderService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("deadbolt.api")


# ---------------------------------------------------------------------------- schemas
class OrderItemIn(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0, le=100)


class OrderIn(BaseModel):
    user_id: int = Field(gt=0)
    items: list[OrderItemIn] = Field(min_length=1, max_length=10)
    strategy: Strategy | None = None


class StrategyIn(BaseModel):
    strategy: Strategy


class LoadConfigIn(BaseModel):
    users: int = Field(1000, ge=1, le=20_000)
    total_requests: int = Field(10_000, ge=1, le=1_000_000)
    contention: float = Field(0.9, ge=0.0, le=1.0)
    hot_product_id: int = Field(1, gt=0)
    max_items: int = Field(3, ge=1, le=5)
    max_quantity: int = Field(2, ge=1, le=10)
    strategy: Strategy = Strategy.BATCHED
    seed: int | None = None

    def to_config(self) -> LoadConfig:
        return LoadConfig(**self.model_dump())


class CompareIn(LoadConfigIn):
    strategies: list[Strategy] = Field(
        default_factory=lambda: [Strategy.NAIVE, Strategy.PESSIMISTIC, Strategy.OPTIMISTIC, Strategy.BATCHED])

    @field_validator("strategies")
    @classmethod
    def _non_empty(cls, value: list[Strategy]) -> list[Strategy]:
        if not value:
            raise ValueError("at least one strategy is required")
        return value

    def to_config(self) -> LoadConfig:
        return LoadConfig(**self.model_dump(exclude={"strategies"}))


class ResetIn(BaseModel):
    stock: dict[int, int] | None = None

    @field_validator("stock")
    @classmethod
    def _non_negative(cls, value: dict[int, int] | None) -> dict[int, int] | None:
        if value and any(v < 0 for v in value.values()):
            raise ValueError("stock values must be >= 0")
        return value


# ---------------------------------------------------------------------------- app state
class AppState:
    def __init__(self) -> None:
        self.metrics = MetricsHub()
        self.engine = DBEngine(settings, self.metrics)
        self.audit: AuditWriter | None = None
        self.batcher: MicroBatcher | None = None
        self.service: OrderService | None = None
        self.runner: LoadRunner | None = None
        self.http_strategy: Strategy = Strategy.BATCHED
        self.integrity: dict = {}
        self.pg_stats: dict = {}
        self._poller: asyncio.Task | None = None

    async def start(self) -> None:
        await self.engine.connect()
        await self.engine.apply_schema()
        self.audit = AuditWriter(self.engine.admin_pool, settings.audit_flush_interval_ms, settings.audit_flush_max_rows)
        self.batcher = MicroBatcher(
            executor=self.engine.execute_batch,
            metrics=self.metrics,
            max_size=settings.batch_max_size,
            max_wait_ms=settings.batch_max_wait_ms,
            workers=settings.batch_workers,
            capacity=settings.batch_queue_capacity,
        )
        self.service = OrderService(self.engine, self.batcher, self.metrics, self.audit)
        self.runner = LoadRunner(self.service)
        self.metrics.start()
        self.audit.start()
        self.batcher.start()
        self._poller = asyncio.create_task(self._poll_db(), name="integrity-poller")

    async def stop(self) -> None:
        if self.runner and self.runner.busy:
            await self.runner.stop()
        if self._poller:
            self._poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poller
        if self.batcher:
            await self.batcher.stop()
        if self.audit:
            await self.audit.stop()
        await self.metrics.stop()
        await self.engine.close()

    async def _poll_db(self) -> None:
        """Refresh the integrity view once a second (shared by all WebSocket clients)."""
        while True:
            try:
                self.integrity = await self.engine.integrity()
                self.pg_stats = await self.engine.db_stats()
            except Exception as exc:
                log.warning("integrity poll failed: %s", exc)
            await asyncio.sleep(1.0)

    def active_strategy(self) -> str:
        if self.runner and self.runner.busy:
            return self.runner.strategy.value
        return self.http_strategy.value

    def live_frame(self) -> dict:
        """Tiny 100 ms frame: cumulative counters only.

        No percentile sorting, no history, no comparison tables, so pushing it
        ten times a second costs the event loop almost nothing while a load
        test is running on the same loop. Rates are derived client-side from
        the deltas between frames.
        """
        runner = self.runner
        strategies = {
            s.value: [st.requests, st.outcomes.get("CONFIRMED", 0), st.db_commits, st.deadlocks_hit,
                      sum(st.outcomes.get(o, 0) for o in ("DEADLOCK", "CONFLICT", "ERROR", "REJECTED"))]
            for s, st in self.metrics.strategies.items()
        }
        integ = self.integrity or {}
        return {
            "type": "live",
            "ts": time.time(),
            "active_strategy": self.active_strategy(),
            # [requests, confirmed, db_commits, deadlocks, dropped] per strategy
            "s": strategies,
            "run": {
                "state": runner.state if runner else "idle",
                "completed": runner.completed if runner else 0,
                "target": runner.config.total_requests if runner and runner.config else 0,
            },
            "q": self.batcher.depth if self.batcher else 0,
            "integrity": {
                "ok": integ.get("ok", True),
                "oversold_units": integ.get("oversold_units", 0),
                "negative_rows": integ.get("negative_rows", 0),
            },
        }

    def snapshot(self) -> dict:
        self.metrics.batch.queue_depth = self.batcher.depth if self.batcher else 0
        return {
            "type": "full",
            "ts": time.time(),
            "active_strategy": self.active_strategy(),
            "metrics": self.metrics.snapshot(),
            "integrity": self.integrity,
            "pg": self.pg_stats,
            "runner": self.runner.status() if self.runner else None,
            "audit": {"rows_written": self.audit.rows_written if self.audit else 0},
            "settings": {
                "batch_max_size": settings.batch_max_size,
                "batch_max_wait_ms": settings.batch_max_wait_ms,
                "batch_workers": settings.batch_workers,
                "pool_max_size": settings.pool_max_size,
                "naive_think_ms": settings.naive_think_ms,
                "default_stock": {str(k): v for k, v in DEFAULT_STOCK.items()},
                "load_max_users": settings.load_max_users,
                "load_max_requests": settings.load_max_requests,
            },
        }


state = AppState()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await state.start()
    log.info("Deadbolt ready")
    try:
        yield
    finally:
        await state.stop()


app = FastAPI(
    title="Deadbolt",
    description="High-Concurrency Micro-Batching Engine & Database Deadlock Resolver",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# ---------------------------------------------------------------------------- routes
@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict:
    async with state.engine.admin_pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "batch_queue_depth": state.batcher.depth}


@app.post("/api/orders")
async def place_order(body: OrderIn) -> JSONResponse:
    request = OrderRequest(user_id=body.user_id, items=[(i.product_id, i.quantity) for i in body.items])
    strategy = body.strategy or state.http_strategy
    result = await state.service.place(request, strategy)
    status_code = {"CONFIRMED": 201, "SOLD_OUT": 409, "REJECTED": 503}.get(result.outcome.value, 500)
    return JSONResponse(result.to_dict(), status_code=status_code)


@app.get("/api/products")
async def products() -> dict:
    return await state.engine.integrity()


@app.get("/api/integrity")
async def integrity() -> dict:
    return await state.engine.integrity()


@app.get("/api/metrics")
async def metrics() -> dict:
    return state.snapshot()


@app.get("/api/audit/summary")
async def audit_summary() -> list[dict]:
    await state.audit.flush()
    return await state.engine.audit_summary()


@app.post("/api/strategy")
async def set_strategy(body: StrategyIn) -> dict:
    state.http_strategy = body.strategy
    state.runner.set_strategy(body.strategy)
    return {"active_strategy": body.strategy.value}


def _check_limits(body: LoadConfigIn) -> None:
    if body.users > settings.load_max_users or body.total_requests > settings.load_max_requests:
        raise HTTPException(
            status_code=422,
            detail=f"This deployment allows up to {settings.load_max_users:,} users and "
                   f"{settings.load_max_requests:,} requests per test.")


@app.post("/api/load/start")
async def load_start(body: LoadConfigIn) -> dict:
    _check_limits(body)
    try:
        state.runner.start(body.to_config())
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    state.http_strategy = body.strategy
    return {"started": True, "config": body.model_dump()}


@app.post("/api/load/compare")
async def load_compare(body: CompareIn) -> dict:
    _check_limits(body)
    try:
        state.runner.start_comparison(body.to_config(), body.strategies)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"started": True, "strategies": [s.value for s in body.strategies]}


@app.post("/api/load/stop")
async def load_stop() -> dict:
    await state.runner.stop()
    return state.runner.status()


@app.get("/api/load/status")
async def load_status() -> dict:
    return state.runner.status()


@app.post("/api/reset")
async def reset(body: ResetIn | None = None) -> dict:
    if state.runner.busy:
        raise HTTPException(status_code=409, detail="stop the running load test first")
    await state.engine.reset(body.stock if body else None)
    state.audit.discard()
    state.metrics.reset()
    state.runner.last_summary = None
    state.runner.comparison = []
    state.runner.state = "idle"
    state.integrity = await state.engine.integrity()
    return {"reset": True, "integrity": state.integrity}


@app.websocket("/ws/stream")
async def stream(ws: WebSocket) -> None:
    """Live frame every WS_PUSH_INTERVAL_MS (100 ms), full snapshot every WS_FULL_INTERVAL_MS (1 s)."""
    await ws.accept()
    interval = settings.ws_push_interval_ms / 1000.0
    full_every = max(1, round(settings.ws_full_interval_ms / settings.ws_push_interval_ms))
    tick = 0
    try:
        while True:
            await ws.send_json(state.snapshot() if tick % full_every == 0 else state.live_frame())
            tick += 1
            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        return
    except Exception as exc:  # client vanished mid-send (ConnectionClosed, RuntimeError, ...)
        log.debug("websocket closed: %s", exc)
        return
