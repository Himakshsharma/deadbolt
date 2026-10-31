# Deadbolt
### High-Concurrency Micro-Batching Engine & Database Deadlock Resolver

Deadbolt is a flash-sale order engine built on **Python 3.11, asyncio, asyncpg, FastAPI and PostgreSQL**. It takes thousands of concurrent "buy" requests aimed at the same few inventory rows and shows, live and side by side, what happens with four different write strategies:

| Strategy | What it does | What happens under a flash sale |
|---|---|---|
| **Naïve** | `BEGIN` → per item: `SELECT` stock, check, `UPDATE` → `COMMIT`, items in client order, no retry | Oversells (stock goes negative), deadlocks (`40P01`), drops requests |
| **Pessimistic lock** | `SELECT … FOR UPDATE` on all rows **in ascending id order**, one `UNNEST` write, retry with exponential backoff | Correct, 0 deadlocks, but the hot row serialises every request |
| **Optimistic lock** | Read version → `UPDATE … WHERE id = $1 AND version = $v` → 0 rows = lost the race → retry | Correct, but burns retries and drops requests when contention is extreme |
| **Micro-batched** | Async queue groups up to 50 requests / 50 ms; **one transaction per batch**: lock rows once, allocate in FIFO order in memory, one `UNNEST` statement writes everything | Correct, 0 deadlocks, ~10× throughput, flat latency |

A real-time dashboard (dark, minimal, Chart.js over WebSockets) shows throughput, commits, deadlocks and a continuously verified stock ledger, and lets you switch strategy **while a load test is running**.

---

## 1. Quick start

```bash
cd deadbolt
chmod +x run.sh
./run.sh            # builds and starts PostgreSQL + API, opens http://localhost:8000
```

Then press **Race all four**, or pick **Naïve**, start a test, and switch to **Micro-Batched** halfway through.

| Command | What it does |
|---|---|
| `./run.sh` | Build and start everything (Docker Compose) |
| `./run.sh bench` | 4-strategy benchmark, 10,000 requests, 1,000 concurrent users, prints a Markdown table |
| `./run.sh test` | Run the test suite inside the container |
| `./run.sh logs` | Follow API logs |
| `./run.sh down` / `./run.sh clean` | Stop / stop and delete the database volume |
| `./run.sh local` | Run the API without Docker (needs Python 3.11+ and a PostgreSQL at `$DATABASE_URL`) |

URLs: dashboard `http://localhost:8000`, OpenAPI docs `http://localhost:8000/docs`, database `postgresql://deadbolt:deadbolt@localhost:5432/deadbolt`.

---

## 2. Architecture

```mermaid
flowchart LR
    subgraph Clients
        LS[load_simulator.py<br/>aiohttp, N users]
        UI[Dashboard<br/>HTML + Chart.js]
    end

    subgraph API["FastAPI process (single event loop)"]
        ING[POST /api/orders]
        LR[In-process LoadRunner<br/>virtual users]
        SVC[OrderService<br/>strategy router]
        Q[[asyncio.Queue<br/>bounded, back-pressure]]
        W1[batch worker 1]
        W2[batch worker …N]
        ENG[DBEngine<br/>raw SQL / asyncpg]
        MET[MetricsHub<br/>rates, p50/p95/p99]
        AUD[AuditWriter<br/>bulk UNNEST every 250 ms]
        WS[/ws/stream<br/>100 ms live · 1 s full/]
    end

    PG[(PostgreSQL 16<br/>products · orders · audit_logs)]

    LS -->|HTTP| ING --> SVC
    UI -->|start / toggle / compare| LR --> SVC
    SVC -->|naive / pessimistic / optimistic| ENG
    SVC -->|batched| Q --> W1 & W2 --> ENG
    ENG -->|request pool, 80 conns| PG
    AUD -->|admin pool, 4 conns| PG
    SVC --> MET & AUD
    MET --> WS --> UI
```

ASCII version of the batched hot path:

```
 request ─┐
 request ─┤   asyncio.Queue    ┌──────────── one transaction ────────────┐
 request ─┼──► [r1 r2 … r50] ──► BEGIN                                    │
   …      │   flush on:         │ SELECT … WHERE id = ANY($ids)            │
 request ─┘   • 50 items        │        ORDER BY id FOR UPDATE   (1 RTT)  │
              • 50 ms window    │ allocate FIFO in memory (0 RTT)          │
              • idle (adaptive) │ WITH stock AS (UPDATE … FROM unnest()),  │
                                │      lines AS (INSERT … unnest())        │
                                │ SELECT counts                   (1 RTT)  │
                                │ COMMIT                                   │
                                └───── each caller's Future resolved ──────┘
```

### Project layout

```
deadbolt/
├── app/
│   ├── config.py         env-driven settings
│   ├── models.py         OrderRequest / OrderResult / Strategy / Outcome
│   ├── db_engine.py      the four strategies in raw SQL, integrity checks, reset
│   ├── batch_worker.py   adaptive micro-batcher (size / window / idle flush)
│   ├── service.py        strategy router + metrics + audit
│   ├── audit.py          buffered bulk audit-log writer
│   ├── metrics.py        per-strategy counters, rates, latency percentiles
│   ├── load_runner.py    in-process virtual users, live toggle, compare mode
│   └── main.py           FastAPI routes + WebSocket stream
├── db/schema.sql         tables, indexes, seed data (idempotent)
├── frontend/             index.html, app.js, styles.css (no build step, no CSS framework)
├── tests/                unit tests (no DB) + integration tests (real PostgreSQL)
├── load_simulator.py     CLI load generator: --mode http | direct, --compare
├── docker-compose.yml    PostgreSQL 16 (tuned) + API
├── render.yaml           one-click cloud deploy (Render Blueprint)
├── .github/workflows/    CI: tests against PostgreSQL + Docker build
├── Dockerfile
└── run.sh
```

---

## 3. Database design (`db/schema.sql`)

- `products(id, name, total_stock, reserved_stock, price, version, updated_at)` — `available = total_stock - reserved_stock`. `version` is bumped on every write (used by the optimistic strategy).
- `orders(id, request_id, user_id, product_id, quantity, status, created_at)` — one row per confirmed line item; `request_id` joins back to the audit log.
- `audit_logs(id, request_id, strategy_used, execution_time_ms, status, error_message, created_at)` — one row per request outcome for every strategy.
- Indexes: `orders(product_id)`, `orders(request_id)`, `audit_logs(created_at)`, `audit_logs(strategy_used, status)`.
- Seed: 5 hot flash-sale products (600 / 400 / 300 / 200 / 150 units). Default demand is several times supply, so every run fights over the sell-out boundary.

**Deliberate design choice:** there is no `CHECK (reserved_stock <= total_stock)`. In production you would add it as a last line of defence, but here it would hide the exact bug the naïve strategy exists to demonstrate. Integrity is instead verified continuously:

```
oversold_units  = Σ max(sold_units − total_stock, 0)
negative_rows   = rows where available < 0
ledger_mismatch = Σ |Σ orders.quantity − reserved_stock|     (detects lost updates)
```

---

## 4. How each strategy works, and why

### Naïve — the bug, on purpose
```sql
BEGIN;                                            -- READ COMMITTED
SELECT total_stock, reserved_stock FROM products WHERE id = 3;   -- no lock
-- app checks available >= qty  (stale by the time we write)
UPDATE products SET reserved_stock = reserved_stock + 2 WHERE id = 3;  -- row lock taken here
SELECT … WHERE id = 1;  UPDATE … WHERE id = 1;    -- items in whatever order the client sent
COMMIT;
```
Two failures fall out of this:
1. **Check-then-act race → overselling.** Many transactions read "1 left", all pass the check, all add to `reserved_stock`.
2. **Lock-order inversion → deadlocks.** Txn A locks row 3 then wants row 1; txn B holds row 1 and wants row 3. PostgreSQL detects the cycle after `deadlock_timeout` and aborts one with `SQLSTATE 40P01`. Naïve code does not retry, so the request is dropped. Meanwhile lock waits pin connections, the pool empties, and more requests time out — a cascading failure.

`NAIVE_THINK_MS` (default 1 ms) simulates the small app-side gap between check and write that every real service has (pricing call, validation).

### Pessimistic — ordered row locks
```sql
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT id, total_stock, reserved_stock FROM products
 WHERE id = ANY($1::int[]) ORDER BY id FOR UPDATE;   -- every txn locks in ascending id order
-- check stock under the lock
WITH deltas AS (SELECT * FROM unnest($1::int[], $2::int[]) d(product_id, qty)),
     stock  AS (UPDATE products p SET reserved_stock = p.reserved_stock + d.qty, version = p.version + 1
                FROM deltas d WHERE p.id = d.product_id RETURNING p.id),
     lines  AS (INSERT INTO orders (…) SELECT * FROM unnest($3::uuid[], $4::int[], $5::int[], $6::int[]) RETURNING id)
SELECT …;
COMMIT;
```
- **Why no deadlocks:** a deadlock needs a cycle in the wait-for graph. If every transaction acquires locks in the same global order (ascending primary key), a cycle is impossible.
- **Why still retry:** defence in depth. On `40P01` or `40001` the transaction is retried with exponential backoff and jitter (`4 ms × 2ⁿ`, capped at 250 ms, ±50%), up to `RETRY_MAX_ATTEMPTS`.
- **The limit:** the hot row's lock is held for ~3–4 network round trips per request, so throughput on that row is capped near `1 / lock_hold_time` no matter how many connections you add.

### Optimistic — version compare-and-swap
```sql
SELECT id, total_stock, reserved_stock, version FROM products WHERE id = ANY($1) ORDER BY id;  -- no lock
BEGIN;
UPDATE products SET reserved_stock = reserved_stock + $2, version = version + 1
 WHERE id = $1 AND version = $3 AND total_stock - reserved_stock >= $2
RETURNING version;          -- NULL => someone else won => ROLLBACK, back off, re-read
COMMIT;
```
Great when conflicts are rare. On a single scorching row almost everyone loses the race, so it spends its time retrying and eventually gives up (`CONFLICT`). It is in the benchmark precisely to show *when not to use it*.

### Micro-batched — the resolver
`batch_worker.py` + `DBEngine.execute_batch`:

1. Requests go onto a bounded `asyncio.Queue`; each caller awaits its own `Future`.
2. A worker takes the first request, drains whatever is already queued, then waits for more until the **batch hits 50** or the **50 ms window closes**.
3. **Adaptive idle flush:** the batcher keeps an EWMA of inter-arrival time. If fewer than one more request is expected inside the window, it flushes immediately, so a lone request at 3 a.m. is not held for 50 ms.
4. One transaction per batch: lock all touched rows once (sorted), allocate stock to requests strictly in arrival order (each cart all-or-nothing), then a single `UNNEST` statement applies all stock deltas and inserts all order lines.
5. Back-pressure: when the queue is full, new requests are rejected immediately (`REJECTED`, HTTP 503) instead of piling up.

Why it is so much faster: the hot-row lock is still held for a few round trips, but now those round trips pay for **up to 50 orders instead of 1**. Commits drop by ~50×, WAL flushes and lock hand-offs drop with them, and nobody waits in the connection-pool queue.

---

## 5. Benchmark methodology

`load_simulator.py --mode direct --compare` (or **Race all four** in the dashboard) runs the identical seeded workload against each strategy, resetting the database between runs.

- **Workload:** 10,000 requests, 1,000 concurrent virtual users (closed loop: each user sends its next request as soon as the previous one returns).
- **Contention:** 90 % of carts contain product 1. 55 % of carts have one item, the rest 2–3 distinct products in random order (multi-row transactions are what make deadlocks possible). 1–2 units per line.
- **Seed:** fixed (`--seed 42`), so every strategy sees the same request stream.
- **Metrics:** throughput = completed requests / wall time; `deadlocks` = `40P01` errors seen by the application (including ones later retried successfully); `dropped` = DEADLOCK + CONFLICT + ERROR + REJECTED; `commits` = committed business transactions; latency is per request, measured client-side; integrity is read from the database after the run.
- `direct` mode calls the engine in-process so you measure the database strategy, not HTTP parsing. `--mode http` measures the full stack through FastAPI.

### Reference results

Measured with the exact workload above against PostgreSQL 16 (`max_connections=200`, `deadlock_timeout=200ms`) on a single sandbox VM:

| Strategy | Throughput (req/s) | Confirmed | Sold out | Dropped | Deadlocks | DB commits | p95 latency (ms) | Oversold units | Integrity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Naïve | 134.5 | 593 | 2,053 | 7,354 | 840 | 593 | 10,001.8 | **62** | **BROKEN** |
| Pessimistic | 1,850.7 | 714 | 9,286 | 0 | 0 | 714 | 5,205.1 | 0 | OK |
| Optimistic | 1,747.8 | 663 | 8,659 | 678 | 0 | 663 | 5,452.5 | 0 | OK |
| **Micro-batched** | **16,511.5** | 713 | 9,287 | **0** | **0** | **200** | **78.5** | **0** | **OK** |

How to read it:
- **Naïve** oversold 62 units, hit 840 deadlocks, and dropped 73 % of requests. Most drops are pool-acquire timeouts: transactions stuck in lock waits held connections until the pool ran dry. That cascade is the real-world failure mode.
- **Pessimistic** is correct but its p95 is 5 s. With 1,000 users sharing 80 connections behind one hot-row lock, requests queue for a connection; some wait much longer than others.
- **Micro-batched** confirmed the same orders as pessimistic with **~9× the throughput, 3.5× fewer commits (200 transactions for 10,000 requests) and a 66× lower p95**.

These numbers were produced in a sandbox where the database driver ran through a pure-Python protocol layer, which makes every round trip more expensive than real asyncpg. That inflates absolute latencies and somewhat exaggerates batching's advantage. Your machine will give different absolute numbers; run `./run.sh bench` and paste your own table here. The ordering of the strategies and the correctness results do not change, because they follow from lock-hold time and lock ordering, not from the driver.

---

## 6. Dashboard

Plain HTML, hand-written CSS and Chart.js, no build step. Dark only, Inter, three zones:

1. **Header**: status pill (`System ready` / `Rush active` / `Race in progress` / `Reconnecting`) and a segmented control for **Naïve · Lock First · Optimistic · Micro-Batched**. Switching applies immediately, including mid-test, and a one-paragraph note under the header explains the selected strategy.
2. **Four stat cards**
   - *Throughput*: requests per second, with the session peak.
   - *DB commits*: commits per second, with orders settled per commit (and average batch fill in Micro-Batched mode).
   - *Deadlocks detected*: for the strategy in use. Red when it is deadlocking, green at zero for a fixed strategy, with naïve vs fixed totals underneath.
   - *Inventory integrity*: oversold units. Green at zero; turns red and names the product with the lowest stock as soon as anything goes negative.
3. **Actions and analytics**: *Start high-concurrency test* (becomes *Stop test* while running), *Reset stock*, and *Race all four*. Users, requests, hot-row share, stock level and cart size sit in a collapsed **Parameters** row that shows a one-line summary when closed. Below it, two line charts: live throughput (requests vs commits) and deadlocks over time (naïve vs fixed). Race results and a one-sentence verdict appear only after a race.

### Live updates without stutter

The WebSocket sends two kinds of message:

| Message | Every | Contents | Size |
|---|---|---|---|
| `live` | 100 ms (`WS_PUSH_INTERVAL_MS`) | Cumulative counters per strategy, run progress, queue depth, cached integrity totals | ~0.4 KB |
| `full` | 1 s (`WS_FULL_INTERVAL_MS`) | Rate history for the charts, latency percentiles, product rows, race results | ~16 KB |

The live frame does no sorting or aggregation on the server, so pushing it ten times a second barely touches the event loop that is also running the load test. The browser derives req/s and commits/s from the counter deltas over the last second, so the big numbers move every 100 ms instead of once a second.

On the client, socket messages only update an in-memory model. A single `requestAnimationFrame` loop writes to the DOM at most every 100 ms, eases the large numbers toward their new values between writes, and redraws the charts only when new history arrives (once a second, with Chart.js animations off). In a headless-Chrome test the page held 58 fps while the naïve strategy was deadlocking under 500 concurrent users.

Monitoring uses a separate 4-connection pool, so the integrity card keeps updating even while the naïve strategy has exhausted the request pool.

---

## 7. API

| Method & path | Body | Purpose |
|---|---|---|
| `POST /api/orders` | `{"user_id": 1, "items": [{"product_id": 1, "quantity": 2}], "strategy": "batched"}` | Place an order. `strategy` optional (defaults to the live toggle). 201 confirmed, 409 sold out, 503 back-pressure, 500 dropped |
| `POST /api/strategy` | `{"strategy": "naive"}` | Switch strategy live |
| `POST /api/load/start` | `{"users": 1000, "total_requests": 10000, "contention": 0.9, "max_items": 3, "strategy": "batched"}` | Start an in-process load test |
| `POST /api/load/compare` | same + `"strategies": [...]` | Run strategies back to back with resets in between |
| `POST /api/load/stop` | – | Stop the running test |
| `GET /api/load/status` | – | Progress, last summary, comparison results |
| `POST /api/reset` | `{"stock": {"1": 1000}}` (optional) | Truncate orders/audit, restore seed stock (with optional overrides), clear metrics |
| `GET /api/integrity` | – | Stock vs ledger verification |
| `GET /api/metrics` | – | Same snapshot the WebSocket pushes |
| `GET /api/audit/summary` | – | Audit log grouped by strategy and outcome |
| `WS /ws/stream` | – | `live` frame every 100 ms, `full` snapshot every 1 s |

Load over real HTTP:
```bash
docker compose exec api python load_simulator.py --mode http --url http://localhost:8000 --compare --users 500 --requests 10000
```

---

## Deploy (live demo)

The repo ships a [Render](https://render.com) Blueprint (`render.yaml`) that creates a free PostgreSQL database and the web service from the `Dockerfile`, wired together:

1. Push this repo to GitHub.
2. In Render: **New → Blueprint**, pick the repo, and apply. The database and the service are created and linked automatically.
3. Open `https://<service-name>.onrender.com`.

On the free plan the service sleeps after 15 minutes without traffic, so the first visit after a pause takes about a minute to wake. The free instance has a tenth of a CPU, so the blueprint starts visitors on a light workload (150 users, 1,000 requests) and caps tests at 500 users and 5,000 requests. The dashboard sliders follow those values automatically. Expect throughput numbers far below a laptop's; the relative ordering of the strategies is what the demo shows. For the benchmark numbers in this README, run it locally with `./run.sh`.

Any host that runs a Docker image and provides PostgreSQL works the same way: set `DATABASE_URL`, and the container listens on `$PORT` (default 8000). The schema is created on first start.

`.github/workflows/ci.yml` runs the full test suite against a real PostgreSQL service and builds the Docker image on every push.

---

## 8. Tests

```bash
./run.sh test                      # inside Docker
# or locally, with PostgreSQL running:
DATABASE_URL=postgresql://deadbolt:deadbolt@localhost:5432/deadbolt python -m pytest -q
```

- `tests/test_batch_worker.py` (no database): flush on size, flush on window, adaptive idle flush, FIFO routing of results to the right caller, back-pressure rejection, worker survives an executor crash, cart normalisation.
- `tests/test_integration.py` (real PostgreSQL, skipped if unreachable): pessimistic / optimistic / batched never oversell and keep the ledger exact; pessimistic and batched hit zero deadlocks; the hot product sells out exactly; batching commits ≥3× fewer transactions than orders; the naïve strategy *does* break under the same load; every request lands in the audit log.

The integration tests truncate `orders` and `audit_logs`. Don't point them at data you care about.

---

## 9. Configuration

All via environment variables (see `docker-compose.yml`).

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://deadbolt:deadbolt@localhost:5432/deadbolt` | Connection string |
| `POOL_MIN_SIZE` / `POOL_MAX_SIZE` | 10 / 80 | Request connection pool |
| `POOL_ACQUIRE_TIMEOUT_S` | 10 | Request is dropped if no connection within this time |
| `BATCH_MAX_SIZE` | 50 | Flush when a batch reaches this many requests |
| `BATCH_MAX_WAIT_MS` | 50 | Flush when the oldest request in a batch has waited this long |
| `BATCH_WORKERS` | 4 | Concurrent batch workers |
| `BATCH_QUEUE_CAPACITY` | 20000 | Back-pressure threshold |
| `RETRY_MAX_ATTEMPTS` | 6 | Retries for deadlock / serialization / version conflict |
| `RETRY_BASE_DELAY_MS` / `RETRY_MAX_DELAY_MS` | 4 / 250 | Exponential backoff bounds |
| `NAIVE_THINK_MS` | 1 | Simulated app latency between check and write (naïve only) |
| `STATEMENT_TIMEOUT_MS` | 15000 | Server-side statement timeout |
| `WS_PUSH_INTERVAL_MS` / `WS_FULL_INTERVAL_MS` | 100 / 1000 | Dashboard live-frame and full-snapshot intervals |
| `LOAD_MAX_USERS` / `LOAD_MAX_REQUESTS` | 20000 / 1000000 | Caps on dashboard-triggered load tests (lower on small public hosts) |
| `LOAD_DEFAULT_USERS` / `LOAD_DEFAULT_REQUESTS` | 1000 / 10000 | Starting values of the dashboard sliders |
| `PORT` | 8000 | Port the container listens on |

PostgreSQL tuning in `docker-compose.yml`: `max_connections=200`, `shared_buffers=256MB`, `deadlock_timeout=200ms`, `max_wal_size=2GB`, `log_lock_waits=on`, and `synchronous_commit=off` (a benchmark convenience that applies equally to all strategies; remove it for production durability).

---

## 10. Design notes and limits

- **Lock ordering only works if every writer follows it.** If you switch from naïve to batched mid-run, naïve transactions still in flight lock rows in random order and can occasionally deadlock with a batch. The batch retries and succeeds, but the counter will show it. That is the honest lesson: the protocol has to be universal.
- **Why one API process.** The queue and metrics live in process memory. To scale out, shard hot products across instances (route by `product_id`) so each row still has one batcher.
- **Latency trade-off.** Batching adds up to one window (50 ms) of wait under moderate load. Under heavy load batches fill instantly; under light load the adaptive idle flush removes the wait.
- **Fairness.** Requests are allocated strictly in arrival order inside each batch, and batches are formed in queue order, so the micro-batcher is FIFO-fair by construction.
- **Audit writes** happen outside the business transaction and in bulk, so a rolled-back request still leaves a record and auditing doesn't double the database load.
- **Not a substitute for constraints in production:** pair this with `CHECK (reserved_stock <= total_stock)` as a final safety net once you no longer need to demonstrate the bug.
