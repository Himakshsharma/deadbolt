#!/usr/bin/env python3
"""Deadbolt load simulator.

Two modes:

  http    Real HTTP load against a running Deadbolt API (aiohttp, N concurrent users).
          Measures the whole stack: HTTP + FastAPI + strategy + PostgreSQL.

  direct  Builds the engine in-process and calls the strategies directly.
          Measures the database strategy itself; this is what the README
          benchmark table is produced with.

Examples:
  python load_simulator.py --mode http   --strategy naive   --users 500 --requests 10000
  python load_simulator.py --mode direct --compare          --users 1000 --requests 10000
  python load_simulator.py --mode direct --strategy batched --contention 0.95 --out results.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from dataclasses import asdict

from app.load_runner import CartFactory, LoadConfig
from app.metrics import _percentile
from app.models import DROPPED_OUTCOMES, Strategy

ALL_STRATEGIES = [Strategy.NAIVE, Strategy.PESSIMISTIC, Strategy.OPTIMISTIC, Strategy.BATCHED]


# ----------------------------------------------------------------------------- output
def print_table(rows: list[dict]) -> None:
    headers = ["strategy", "requests", "rps", "confirmed", "sold_out", "dropped", "deadlocks",
               "commits", "p50_ms", "p95_ms", "p99_ms", "oversold", "integrity"]
    table = []
    for r in rows:
        lat = r.get("latency_ms", {})
        table.append([
            r["strategy"], r["requests"], r["throughput_rps"], r["confirmed"], r["sold_out"],
            r["dropped"], r["deadlocks_hit"], r.get("db_commits", "-"),
            lat.get("p50", 0), lat.get("p95", 0), lat.get("p99", 0), r["oversold_units"],
            "OK" if r["integrity_ok"] else "BROKEN",
        ])
    widths = [max(len(str(h)), *(len(str(row[i])) for row in table)) for i, h in enumerate(headers)]
    line = "+".join("-" * (w + 2) for w in widths)
    print("+" + line + "+")
    print("| " + " | ".join(str(h).ljust(w) for h, w in zip(headers, widths)) + " |")
    print("+" + line + "+")
    for row in table:
        print("| " + " | ".join(str(c).ljust(w) for c, w in zip(row, widths)) + " |")
    print("+" + line + "+")


def print_markdown(rows: list[dict]) -> None:
    print("\n| Strategy | Throughput (req/s) | Confirmed | Sold out | Dropped | Deadlocks | DB commits "
          "| p95 latency (ms) | Oversold units | Integrity |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for r in rows:
        print(f"| {r['strategy']} | {r['throughput_rps']} | {r['confirmed']} | {r['sold_out']} | "
              f"{r['dropped']} | {r['deadlocks_hit']} | {r.get('db_commits', '-')} | "
              f"{r['latency_ms']['p95']} | {r['oversold_units']} | "
              f"{'OK' if r['integrity_ok'] else 'BROKEN'} |")


# ----------------------------------------------------------------------------- direct mode
async def run_direct(args: argparse.Namespace, strategies: list[Strategy]) -> list[dict]:
    from app.audit import AuditWriter
    from app.batch_worker import MicroBatcher
    from app.config import settings
    from app.db_engine import DBEngine
    from app.load_runner import LoadRunner
    from app.metrics import MetricsHub
    from app.service import OrderService

    metrics = MetricsHub()
    engine = DBEngine(settings, metrics)
    await engine.connect()
    await engine.apply_schema()
    audit = AuditWriter(engine.admin_pool, settings.audit_flush_interval_ms, settings.audit_flush_max_rows)
    batcher = MicroBatcher(engine.execute_batch, metrics, settings.batch_max_size,
                           settings.batch_max_wait_ms, settings.batch_workers, settings.batch_queue_capacity)
    service = OrderService(engine, batcher, metrics, audit)
    runner = LoadRunner(service)
    metrics.start()
    audit.start()
    batcher.start()

    results = []
    try:
        for strategy in strategies:
            await engine.reset()
            audit.discard()
            cfg = build_config(args, strategy)
            print(f"\n>>> {strategy.value}: {cfg.total_requests} requests, {cfg.users} users, "
                  f"contention={cfg.contention}", flush=True)
            runner.start(cfg)
            await runner.wait()
            summary = runner.last_summary.to_dict()
            results.append(summary)
            print(f"    done in {summary['elapsed_s']}s  ->  {summary['throughput_rps']} req/s, "
                  f"deadlocks={summary['deadlocks_hit']}, oversold={summary['oversold_units']}", flush=True)
    finally:
        await batcher.stop()
        await audit.stop()
        await metrics.stop()
        await engine.close()
    return results


# ----------------------------------------------------------------------------- http mode
async def run_http(args: argparse.Namespace, strategies: list[Strategy]) -> list[dict]:
    import aiohttp

    base = args.url.rstrip("/")
    timeout = aiohttp.ClientTimeout(total=120)
    connector = aiohttp.TCPConnector(limit=args.users, limit_per_host=args.users)
    results = []
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.get(f"{base}/api/integrity") as resp:
            resp.raise_for_status()
            product_ids = [p["id"] for p in (await resp.json())["products"]]

        for strategy in strategies:
            if not args.no_reset:
                async with session.post(f"{base}/api/reset", json={}) as resp:
                    resp.raise_for_status()
            cfg = build_config(args, strategy)
            factory = CartFactory(product_ids, cfg)
            outcomes: Counter = Counter()
            latencies: list[float] = []
            deadlocks = 0
            sent = 0

            async def virtual_user() -> None:
                nonlocal sent, deadlocks
                while sent < cfg.total_requests:
                    sent += 1
                    req = factory.make()
                    payload = {"user_id": req.user_id, "strategy": strategy.value,
                               "items": [{"product_id": p, "quantity": q} for p, q in req.items]}
                    t0 = time.perf_counter()
                    try:
                        async with session.post(f"{base}/api/orders", json=payload) as resp:
                            body = await resp.json()
                            outcomes[body.get("outcome", "ERROR")] += 1
                            deadlocks += body.get("deadlocks_hit", 0)
                    except Exception:
                        outcomes["ERROR"] += 1
                    latencies.append((time.perf_counter() - t0) * 1000.0)

            print(f"\n>>> {strategy.value} over HTTP: {cfg.total_requests} requests, {cfg.users} users", flush=True)
            started = time.perf_counter()
            await asyncio.gather(*(virtual_user() for _ in range(min(cfg.users, cfg.total_requests))))
            elapsed = time.perf_counter() - started

            async with session.get(f"{base}/api/integrity") as resp:
                integrity = await resp.json()
            latencies.sort()
            total = sum(outcomes.values())
            results.append({
                "label": f"http:{strategy.value}",
                "strategy": strategy.value,
                "config": {**asdict(cfg), "strategy": strategy.value},
                "elapsed_s": round(elapsed, 3),
                "requests": total,
                "throughput_rps": round(total / elapsed, 1) if elapsed else 0.0,
                "outcomes": dict(outcomes),
                "confirmed": outcomes.get("CONFIRMED", 0),
                "sold_out": outcomes.get("SOLD_OUT", 0),
                "dropped": sum(outcomes.get(o.value, 0) for o in DROPPED_OUTCOMES),
                "deadlocks_hit": deadlocks,
                "db_commits": "-",
                "latency_ms": {"p50": round(_percentile(latencies, 0.5), 2),
                               "p95": round(_percentile(latencies, 0.95), 2),
                               "p99": round(_percentile(latencies, 0.99), 2),
                               "max": round(latencies[-1], 2) if latencies else 0.0},
                "oversold_units": integrity["oversold_units"],
                "negative_rows": integrity["negative_rows"],
                "ledger_mismatch": integrity["ledger_mismatch"],
                "integrity_ok": integrity["ok"],
            })
            r = results[-1]
            print(f"    done in {r['elapsed_s']}s  ->  {r['throughput_rps']} req/s, "
                  f"deadlocks={r['deadlocks_hit']}, oversold={r['oversold_units']}", flush=True)
    return results


# ----------------------------------------------------------------------------- cli
def build_config(args: argparse.Namespace, strategy: Strategy) -> LoadConfig:
    cfg = LoadConfig(users=args.users, total_requests=args.requests, contention=args.contention,
                     hot_product_id=args.hot_product, max_items=args.max_items,
                     max_quantity=args.max_quantity, strategy=strategy, seed=args.seed)
    cfg.validate()
    return cfg


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deadbolt flash-sale load simulator")
    p.add_argument("--mode", choices=["http", "direct"], default="direct")
    p.add_argument("--url", default="http://localhost:8000", help="API base URL for --mode http")
    p.add_argument("--strategy", choices=[s.value for s in Strategy], default="batched")
    p.add_argument("--compare", action="store_true", help="run every strategy back to back")
    p.add_argument("--users", type=int, default=1000, help="concurrent virtual users")
    p.add_argument("--requests", type=int, default=10_000, help="total requests per strategy")
    p.add_argument("--contention", type=float, default=0.9, help="share of carts containing the hot product")
    p.add_argument("--hot-product", type=int, default=1)
    p.add_argument("--max-items", type=int, default=3, help="max distinct products per cart")
    p.add_argument("--max-quantity", type=int, default=2)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--no-reset", action="store_true", help="http mode: don't reset the DB between runs")
    p.add_argument("--out", help="write JSON results to this file")
    p.add_argument("--markdown", action="store_true", help="also print a Markdown table")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    strategies = ALL_STRATEGIES if args.compare else [Strategy(args.strategy)]
    runner = run_http if args.mode == "http" else run_direct
    results = asyncio.run(runner(args, strategies))
    print()
    print_table(results)
    if args.markdown:
        print_markdown(results)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2, default=str)
        print(f"\nresults written to {args.out}")
    resolver_broken = [r for r in results if r["strategy"] != "naive" and not r["integrity_ok"]]
    return 1 if resolver_broken else 0


if __name__ == "__main__":
    sys.exit(main())
