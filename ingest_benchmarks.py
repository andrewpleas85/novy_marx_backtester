"""Refresh SPY/IWM benchmark_prices — the strategy's market-regime gates depend on these being
CURRENT. analysis.py::fetch_and_store_benchmarks only
seeds once (count>0 → skip), so this runs daily and inserts only the missing dates (idempotent).

Run:  PYTHONPATH=. venv/bin/python3 ingest_benchmarks.py
"""
import asyncio
import logging

import httpx

from config import settings
from dbutil import connect_with_retry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

BENCHMARKS = ("SPY", "IWM")


async def _fetch(client, ticker):
    for url in (
        f"https://financialmodelingprep.com/stable/historical-price-eod/full"
        f"?symbol={ticker}&apikey={settings.FMP_API_KEY}",
        f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
        f"?apikey={settings.FMP_API_KEY}",
    ):
        for attempt in range(3):                       # retry transient failures
            try:
                r = await client.get(url)
                if r.status_code in (403, 404):
                    break                              # try the fallback URL
                r.raise_for_status()
                data = r.json()
                hist = (data[0]["historical"]
                        if (isinstance(data, list) and data and "historical" in data[0]) else data)
                return [(ticker, row["date"][:10], row.get("close", 0.0),
                         row.get("adjClose", row.get("close", 0.0)))
                        for row in hist if "date" in row]
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                else:
                    logger.warning(f"{ticker}: fetch failed ({str(e)[:60]})")
    return []


def run():
    conn = connect_with_retry(settings.DUCKDB_PATH)     # history.duckdb
    conn.execute("""CREATE TABLE IF NOT EXISTS benchmark_prices (
                        ticker VARCHAR, date DATE, close DOUBLE, adj_close DOUBLE)""")

    async def _all():
        async with httpx.AsyncClient(timeout=30.0) as client:
            return await asyncio.gather(*[_fetch(client, t) for t in BENCHMARKS])

    total_new = 0
    for rows in asyncio.run(_all()):
        if not rows:
            continue
        ticker = rows[0][0]
        existing = {r[0] for r in conn.execute(
            "SELECT CAST(date AS VARCHAR) FROM benchmark_prices WHERE ticker = ?", [ticker]).fetchall()}
        new = [r for r in rows if r[1] not in existing]     # only dates we don't already have
        if new:
            conn.executemany("INSERT INTO benchmark_prices VALUES (?,?,?,?)", new)
            total_new += len(new)
        latest = conn.execute("SELECT MAX(date) FROM benchmark_prices WHERE ticker = ?",
                              [ticker]).fetchone()[0]
        logger.info(f"{ticker}: +{len(new)} new rows, latest now {latest}")
    conn.close()
    logger.info(f"benchmark refresh complete: {total_new} new rows total.")


if __name__ == "__main__":
    run()
