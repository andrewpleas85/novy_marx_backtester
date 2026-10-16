"""Incremental daily refresh of company FINANCIAL STATEMENTS (income/balance/cash-flow/filings).

ingest.py is a DESTRUCTIVE full rebuild, so it can't run daily. This appends only NEW quarters per
ticker — idempotent, safe from cron. It REUSES FMPIngester.get_all_fundamentals() so the exact
column mapping (24/28/20/4 cols) matches the full rebuild; we never re-implement the parsing.

Without this, income_statements silently freezes and no NEW filings enter the universe — the model
just re-scores a stale snapshot and keeps re-picking the same names (this is what happened
2026-06-17 → 2026-07-23).

Run:  PYTHONPATH=. venv/bin/python3 ingest_fundamentals_daily.py [--scope all|universe] [--limit N]
"""
import asyncio
import logging
import argparse

from config import settings
from dbutil import connect_live
from ingest import FMPIngester

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

TABLES = [                       # (table, index of the dedup key in the row tuple, n columns)
    ('income_statements',    1, 24),
    ('balance_sheets',       1, 28),
    ('cash_flow_statements', 1, 20),
]


def _universe_tickers(conn, scope):
    if scope == 'universe':
        rows = conn.execute("""SELECT DISTINCT ticker FROM universe_scores
                               WHERE run_date=(SELECT MAX(run_date) FROM universe_scores)""").fetchall()
        if rows:
            return [r[0] for r in rows]
    # 'all' (or universe empty) → every ticker that already has fundamentals
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM income_statements ORDER BY ticker").fetchall()]


def _insert_new(conn, ticker, income, balance, cashflow, filings):
    """Insert only rows whose key isn't already present for this ticker. Returns counts dict."""
    added = {}
    for (table, keyidx, ncols), rows in zip(TABLES, (income, balance, cashflow)):
        if not rows:
            continue
        existing = {x[0] for x in conn.execute(
            f"SELECT CAST(date AS VARCHAR) FROM {table} WHERE ticker = ?", [ticker]).fetchall()}
        new = [r for r in rows if r[keyidx] not in existing]
        if new:
            conn.executemany(f"INSERT INTO {table} VALUES ({','.join(['?']*ncols)})", new)
            added[table] = added.get(table, 0) + len(new)
    if filings:
        existing_f = {x[0] for x in conn.execute(
            "SELECT CAST(accepted_date AS VARCHAR) FROM financial_filings WHERE ticker = ?",
            [ticker]).fetchall()}
        newf = [r for r in filings if r[1] not in existing_f]
        if newf:
            conn.executemany("INSERT INTO financial_filings VALUES (?,?,?,?)", newf)
            added['financial_filings'] = len(newf)
    return added


async def _refresh(conn, tickers):
    ing = FMPIngester()
    sem = asyncio.Semaphore(6)                    # bound concurrent fetches (3 API calls each)

    async def _one(t):
        async with sem:
            try:
                return t, await ing.get_all_fundamentals(t)
            except Exception:
                return t, None

    totals, done = {}, 0
    try:
        for i in range(0, len(tickers), 150):     # chunk so we commit progressively
            chunk = tickers[i:i + 150]
            for t, res in await asyncio.gather(*[_one(t) for t in chunk]):
                if not res:
                    continue
                for k, v in _insert_new(conn, t, *res).items():
                    totals[k] = totals.get(k, 0) + v
            done = min(i + 150, len(tickers))
            logger.info(f"  ...{done}/{len(tickers)} tickers | new rows so far: {totals}")
    finally:
        await ing.client.aclose()
    return totals


def run(scope='all', limit=None):
    conn = connect_live(settings.DUCKDB_PATH, settings.LIVE_DB_PATH)   # writes land in history
    before = conn.execute(
        "SELECT CAST(MAX(accepted_date) AS VARCHAR) FROM income_statements").fetchone()[0]
    tickers = _universe_tickers(conn, scope)
    if limit:
        tickers = tickers[:limit]
    logger.info(f"Refreshing fundamentals for {len(tickers)} tickers (scope={scope})...")
    totals = asyncio.run(_refresh(conn, tickers))
    after = conn.execute(
        "SELECT CAST(MAX(accepted_date) AS VARCHAR) FROM income_statements").fetchone()[0]
    conn.close()
    logger.info(f"fundamentals refresh: {totals or 'no new filings'} | "
                f"latest income filing {before} -> {after}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["all", "universe"], default="all")
    ap.add_argument("--limit", type=int, default=None, help="cap tickers (dev/testing)")
    args = ap.parse_args()
    run(scope=args.scope, limit=args.limit)
