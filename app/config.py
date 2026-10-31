"""Runtime configuration, read once from environment variables.

Every knob has a sane default so the project runs with zero configuration
against the docker-compose PostgreSQL instance.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT_DIR / "db" / "schema.sql"
FRONTEND_DIR = ROOT_DIR / "frontend"


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "DATABASE_URL", "postgresql://deadbolt:deadbolt@localhost:5432/deadbolt"
        )
    )

    # Connection pool. PostgreSQL is configured with max_connections=200,
    # so the app keeps comfortable headroom below that.
    pool_min_size: int = field(default_factory=lambda: _int("POOL_MIN_SIZE", 10))
    pool_max_size: int = field(default_factory=lambda: _int("POOL_MAX_SIZE", 80))
    pool_acquire_timeout_s: float = field(default_factory=lambda: _float("POOL_ACQUIRE_TIMEOUT_S", 10.0))
    statement_timeout_ms: int = field(default_factory=lambda: _int("STATEMENT_TIMEOUT_MS", 15000))

    # Micro-batching
    batch_max_size: int = field(default_factory=lambda: _int("BATCH_MAX_SIZE", 50))
    batch_max_wait_ms: float = field(default_factory=lambda: _float("BATCH_MAX_WAIT_MS", 50.0))
    batch_workers: int = field(default_factory=lambda: _int("BATCH_WORKERS", 4))
    batch_queue_capacity: int = field(default_factory=lambda: _int("BATCH_QUEUE_CAPACITY", 20000))

    # Retry policy for the locking strategies (deadlock / serialization / version conflict)
    retry_max_attempts: int = field(default_factory=lambda: _int("RETRY_MAX_ATTEMPTS", 6))
    retry_base_delay_ms: float = field(default_factory=lambda: _float("RETRY_BASE_DELAY_MS", 4.0))
    retry_max_delay_ms: float = field(default_factory=lambda: _float("RETRY_MAX_DELAY_MS", 250.0))

    # Simulated application latency between "check stock" and "write stock"
    # in the naive strategy (e.g. a pricing or fraud call). Real services
    # always have some gap here; it widens the race window realistically.
    naive_think_ms: float = field(default_factory=lambda: _float("NAIVE_THINK_MS", 1.0))

    # Audit log writer
    audit_flush_interval_ms: float = field(default_factory=lambda: _float("AUDIT_FLUSH_INTERVAL_MS", 250.0))
    audit_flush_max_rows: int = field(default_factory=lambda: _int("AUDIT_FLUSH_MAX_ROWS", 5000))

    # Dashboard
    ws_push_interval_ms: float = field(default_factory=lambda: _float("WS_PUSH_INTERVAL_MS", 100.0))
    ws_full_interval_ms: float = field(default_factory=lambda: _float("WS_FULL_INTERVAL_MS", 1000.0))

    # Upper bounds for dashboard-triggered load tests. Lower them on small
    # public hosts (e.g. a free cloud instance) so one visitor can't swamp it.
    load_max_users: int = field(default_factory=lambda: _int("LOAD_MAX_USERS", 20000))
    load_max_requests: int = field(default_factory=lambda: _int("LOAD_MAX_REQUESTS", 1000000))
    # Starting values for the dashboard sliders.
    load_default_users: int = field(default_factory=lambda: _int("LOAD_DEFAULT_USERS", 1000))
    load_default_requests: int = field(default_factory=lambda: _int("LOAD_DEFAULT_REQUESTS", 10000))


settings = Settings()
