"""Single entry point used by the HTTP API and the in-process load runner."""
from __future__ import annotations

from .audit import AuditWriter
from .batch_worker import MicroBatcher
from .db_engine import DBEngine
from .metrics import MetricsHub
from .models import OrderRequest, OrderResult, Strategy


class OrderService:
    def __init__(self, engine: DBEngine, batcher: MicroBatcher, metrics: MetricsHub, audit: AuditWriter) -> None:
        self.engine = engine
        self.batcher = batcher
        self.metrics = metrics
        self.audit = audit

    async def place(self, request: OrderRequest, strategy: Strategy) -> OrderResult:
        if strategy is Strategy.NAIVE:
            result = await self.engine.place_naive(request)
        elif strategy is Strategy.PESSIMISTIC:
            result = await self.engine.place_pessimistic(request)
        elif strategy is Strategy.OPTIMISTIC:
            result = await self.engine.place_optimistic(request)
        else:
            result = await self.batcher.submit(request)

        self.metrics.record(result)
        self.audit.add(result.request_id, result.strategy.value, result.latency_ms,
                       result.outcome.value, result.error)
        return result
