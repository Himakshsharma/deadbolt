"""Buffered audit-log writer.

Writing one audit row per request with its own INSERT would double the
database load and distort the very benchmark we are trying to measure.
Instead rows are buffered in memory and flushed in bulk with a single
UNNEST() INSERT every AUDIT_FLUSH_INTERVAL_MS (or when the buffer fills).

Audit rows are written OUTSIDE the business transaction on purpose: a
request whose transaction rolled back (deadlock, sold out) must still
leave an audit trail.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

import asyncpg

log = logging.getLogger("deadbolt.audit")

INSERT_AUDIT_SQL = """
INSERT INTO audit_logs (request_id, strategy_used, execution_time_ms, status, error_message)
SELECT *
FROM unnest($1::uuid[], $2::text[], $3::float8[], $4::text[], $5::text[])
"""


class AuditWriter:
    def __init__(self, pool: asyncpg.Pool, flush_interval_ms: float, flush_max_rows: int) -> None:
        self._pool = pool
        self._interval = flush_interval_ms / 1000.0
        self._max_rows = flush_max_rows
        self._buffer: list[tuple[uuid.UUID, str, float, str, str | None]] = []
        self._task: asyncio.Task | None = None
        self._wakeup = asyncio.Event()
        self.rows_written = 0
        self.flush_errors = 0

    def add(self, request_id: uuid.UUID, strategy: str, execution_ms: float, status: str, error: str | None) -> None:
        self._buffer.append((request_id, strategy, round(execution_ms, 3), status, error[:500] if error else None))
        if len(self._buffer) >= self._max_rows:
            self._wakeup.set()

    async def flush(self) -> int:
        if not self._buffer:
            return 0
        rows, self._buffer = self._buffer, []
        columns = list(zip(*rows))
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(INSERT_AUDIT_SQL, list(columns[0]), list(columns[1]),
                                   list(columns[2]), list(columns[3]), list(columns[4]))
            self.rows_written += len(rows)
            return len(rows)
        except Exception as exc:  # audit must never take the service down
            self.flush_errors += 1
            log.warning("audit flush of %d rows failed: %s", len(rows), exc)
            return 0

    def discard(self) -> None:
        self._buffer.clear()

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass
            self._wakeup.clear()
            await self.flush()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="audit-writer")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.flush()
