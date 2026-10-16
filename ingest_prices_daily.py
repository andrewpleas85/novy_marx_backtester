"""Incremental daily price refresh for the universe.

ingest.py is a DESTRUCTIVE full rebuild (it DELETEs every table), so it can't be used as a daily
job. This script appends only the MISSING dates per ticker — cheap, idempotent, and safe to run
from cron. Without it, historical_prices silently freezes and the model scores stale momentum /
price-screen features (this is exactly what happened 2026-06-18 → 2026-07-22).

Run:  PYTHONPATH=. venv/bin/python3 ingest_prices_daily.py [--limit N]
"""
import asyncio
import logging
import argparse
from datetime import date, timedelta

import httpx

from config import settings
from dbutil import connect_with_retry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

EOD = "https://financialmodelingprep.com/stable/historical-price-eod/full"
CONCURRENCY = 8


async def _fetch(client, sem, ticker, start, end):
    """Fetch [start, end] EOD rows for one ticker. Returns list of tuples or []."""
    async with sem:
        for attempt in range(3):
            try:
                r = await client.get(EOD, params={"symbol": ticker, "from": start.isoformat(),
                                                  "to": end.isoformat(),
                                                  "apikey": settings.FMP_API_KEY})
                if r.status_code in (403, 404):
                    return []
                r.raise_for_status()
                data = r.json()
                hist = (data[0]["historical"]
                        if (isinstance(data, list) and data and isinstance(data[0], dict)
                            and "historical" in data[0]) else data)
                if not isinstance(hist, list):
                    return []
                return [(ticker, row["date"][:10], row.get("open"), row.get("close"),
                         row.get("adjClose", row.get("close")), row.get("volume"))
                        for row in hist if isinstance(row, dict) and row.get("date")]
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
        return []


async def _refresh(conn, limit=None):
    rows = conn.execute("""SELECT ticker, MAX(date) FROM historical_prices
                           GROUP BY ticker ORDER BY ticker""").fetchall()
    if limit:
        rows = rows[:limit]
    today = date.today()
    todo = [(t, d + timedelta(days=1)) for t, d in rows if d and d < today]
    logger.info(f"{len(todo)} ticker(s) behind (of {len(rows)}); fetching to {today}...")
    if not todo:
        return 0

    sem = asyncio.Semaphore(CONCURRENCY)
    inserted = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        # process in chunks so we commit progressively rather than buffering everything
        for i in range(0, len(todo), 250):
            chunk = todo[i:i + 250]
            batches = await asyncio.gather(
                *[_fetch(client, sem, t, start, today) for t, start in chunk])
            for (ticker, start), batch in zip(chunk, batches):
                if not batch:
                    continue
                existing = {r[0] for r in conn.execute(
                    "SELECT CAST(date AS VARCHAR) FROM historical_prices "
                    "WHERE ticker = ? AND date >= ?", [ticker, start]).fetchall()}
                new = [b for b in batch if b[1] not in existing and b[3] is not None]
                if new:
                    conn.executemany(
                        "INSERT INTO historical_prices VALUES (?,?,?,?,?,?)", new)
                    inserted += len(new)
            logger.info(f"  ...{min(i+250, len(todo))}/{len(todo)} tickers, {inserted:,} rows so far")
    return inserted


def run(limit=None):
    conn = connect_with_retry(settings.DUCKDB_PATH)          # history.duckdb
    before = conn.execute("SELECT CAST(MAX(date) AS VARCHAR) FROM historical_prices").fetchone()[0]
    n = asyncio.run(_refresh(conn, limit=limit))
    after = conn.execute("SELECT CAST(MAX(date) AS VARCHAR) FROM historical_prices").fetchone()[0]
    conn.close()
    logger.info(f"price refresh: +{n:,} rows | latest {before} -> {after}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="cap tickers (dev/testing)")
    args = ap.parse_args()
    run(limit=args.limit)
