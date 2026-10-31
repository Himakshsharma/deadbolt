"""Plain data types shared by the engine, the batcher, the API and the load runner."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum


class Strategy(str, Enum):
    NAIVE = "naive"
    PESSIMISTIC = "pessimistic"
    OPTIMISTIC = "optimistic"
    BATCHED = "batched"


class Outcome(str, Enum):
    CONFIRMED = "CONFIRMED"   # order written, stock reserved
    SOLD_OUT = "SOLD_OUT"     # correct business rejection: not enough stock
    DEADLOCK = "DEADLOCK"     # PostgreSQL aborted the transaction (SQLSTATE 40P01), request dropped
    CONFLICT = "CONFLICT"     # optimistic version conflicts / serialization failures exhausted retries
    ERROR = "ERROR"           # any other failure (timeouts, pool exhaustion, ...)
    REJECTED = "REJECTED"     # back-pressure: micro-batch queue full


# Outcomes that represent a request the system *failed* to handle
# (as opposed to a correct business answer such as SOLD_OUT).
DROPPED_OUTCOMES = frozenset({Outcome.DEADLOCK, Outcome.CONFLICT, Outcome.ERROR, Outcome.REJECTED})


@dataclass(slots=True)
class OrderRequest:
    user_id: int
    items: list[tuple[int, int]]  # [(product_id, quantity), ...] in the order the client sent them
    request_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def normalized_items(self) -> list[tuple[int, int]]:
        """Merge duplicate product lines and sort by product id.

        Sorting is the heart of deadlock prevention: if every transaction
        acquires row locks in the same global order (ascending id), a cycle
        in the wait-for graph is impossible.
        """
        merged: dict[int, int] = {}
        for product_id, quantity in self.items:
            merged[product_id] = merged.get(product_id, 0) + quantity
        return sorted(merged.items())


@dataclass(slots=True)
class OrderResult:
    request_id: uuid.UUID
    strategy: Strategy
    outcome: Outcome
    latency_ms: float
    attempts: int = 1
    deadlocks_hit: int = 0      # deadlocks observed while handling (may be retried successfully)
    conflicts_hit: int = 0      # version conflicts / serialization failures observed
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "request_id": str(self.request_id),
            "strategy": self.strategy.value,
            "outcome": self.outcome.value,
            "latency_ms": round(self.latency_ms, 3),
            "attempts": self.attempts,
            "deadlocks_hit": self.deadlocks_hit,
            "conflicts_hit": self.conflicts_hit,
            "error": self.error,
        }
