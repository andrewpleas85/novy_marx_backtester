"""
FINRA RegSHO Short Sale Volume Downloader
==========================================
Streams FINRA's public OTC Market short sale API into DuckDB.
No API key required.  Aggregates across all market venues per (ticker, date).

Usage:
    python download_finra.py              # default: 1 year back
    python download_finra.py --days 500   # 2 years
    python download_finra.py --days 90    # quick top-up

After this script completes, run:
    python model.py                       # re-train with DTC activated
    python analysis.py --dtc              # permutation importance + scatter
"""
import argparse
import asyncio
import logging
import duckdb
import httpx
import pandas as pd
from datetime import date, timedelta
from config import settings

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://api.finra.org/data/group/OTCMarket/name/regShoDaily"
PAGE_SIZE = 5000     # FINRA API max
CONCURRENT = 8       # parallel in-flight requests
FLUSH_ROWS = 80_000  # rows accumulated before a DuckDB write


def _setup(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS finra_short_interest (
            ticker       VARCHAR NOT NULL,
            date         DATE    NOT NULL,
            short_volume DOUBLE  NOT NULL,
            total_volume DOUBLE  NOT NULL
        )
    """)


def _loaded_dates(conn: duckdb.DuckDBPyConnection) -> set[str]:
    try:
        return {r[0] for r in conn.execute(
            "SELECT DISTINCT STRFTIME(date, '%Y-%m-%d') FROM finra_short_interest"
        ).fetchall()}
    except Exception:
        return set()


def _flush(conn: duckdb.DuckDBPyConnection, buffer: list) -> int:
    """Aggregate buffer rows and upsert into DuckDB. Returns rows written."""
    if not buffer:
        return 0

    df = pd.DataFrame(buffer, columns=['ticker', 'date', 'short_volume', 'total_volume'])
    # Aggregate across venues (NQTRF, NYTRF, NCTRF, …) per (ticker, date)
    agg = (
        df.groupby(['ticker', 'date'], sort=False)
        [['short_volume', 'total_volume']]
        .sum()
        .reset_index()
    )
    agg = agg[agg['total_volume'] > 0]

    conn.register('_finra_buf', agg)
    conn.execute("""
        DELETE FROM finra_short_interest
        WHERE date IN (SELECT DISTINCT CAST(date AS DATE) FROM _finra_buf)
    """)
    n = conn.execute("""
        INSERT INTO finra_short_interest
        SELECT ticker, CAST(date AS DATE), short_volume, total_volume
        FROM _finra_buf
    """).rowcount
    conn.unregister('_finra_buf')
    return n or 0


async def _fetch_page(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    offset: int,
) -> list:
    """Fetch one page from the FINRA API; return [] on error or empty."""
    url = f"{BASE_URL}?limit={PAGE_SIZE}&offset={offset}"
    async with sem:
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json() or []
            return []
        except Exception as exc:
            logger.warning(f"  Page offset={offset} failed: {exc}")
            return []


async def download(days_back: int = 252, skip_existing: bool = True) -> None:
    conn = duckdb.connect(settings.DUCKDB_PATH)
    _setup(conn)

    loaded = _loaded_dates(conn) if skip_existing else set()
    cutoff_iso = str(date.today() - timedelta(days=int(days_back * 1.5)))
    target_iso = str(date.today() - timedelta(days=days_back))

    logger.info(
        f"Downloading FINRA RegSHO: {days_back} trading-day window "
        f"(target ≥ {target_iso}, hard stop ≤ {cutoff_iso})"
    )
    if loaded:
        logger.info(f"Skipping {len(loaded)} already-loaded dates.")

    sem  = asyncio.Semaphore(CONCURRENT)
    buffer: list  = []
    total_written = 0
    offset        = 0
    done          = False
    dates_seen: set[str] = set()

    async with httpx.AsyncClient(
        headers={'Accept': 'application/json'},
        timeout=30.0,
    ) as client:

        while not done:
            # Fire CONCURRENT pages in parallel
            tasks = [
                _fetch_page(client, sem, offset + i * PAGE_SIZE)
                for i in range(CONCURRENT)
            ]
            pages = await asyncio.gather(*tasks)

            for page in pages:
                if not page:
                    done = True
                    break

                for row in page:
                    trade_date = row.get('tradeReportDate', '')
                    if not trade_date:
                        continue

                    # Stop if we've gone past our lookback window
                    if trade_date < cutoff_iso:
                        done = True
                        break

                    # Skip already-loaded dates
                    if skip_existing and trade_date in loaded:
                        continue

                    ticker = row.get('securitiesInformationProcessorSymbolIdentifier', '')
                    # Keep real equity tickers: uppercase letters + optional dot
                    if not ticker or not all(c.isalpha() or c == '.' for c in ticker):
                        continue
                    if len(ticker) > 10:
                        continue

                    buffer.append((
                        ticker,
                        trade_date,
                        float(row.get('shortParQuantity', 0)),
                        float(row.get('totalParQuantity', 0)),
                    ))
                    dates_seen.add(trade_date)

                if done:
                    break

            offset += CONCURRENT * PAGE_SIZE

            # Periodic flush to keep memory bounded
            if len(buffer) >= FLUSH_ROWS:
                n = _flush(conn, buffer)
                total_written += n
                buffer = []
                date_range = f"{min(dates_seen)} → {max(dates_seen)}" if dates_seen else "—"
                logger.info(
                    f"  Written {total_written:,} rows so far | "
                    f"dates: {date_range} | offset={offset:,}"
                )

    # Final flush
    if buffer:
        n = _flush(conn, buffer)
        total_written += n

    logger.info(f"\nDownload complete. Total rows written: {total_written:,}")

    # Summary
    try:
        row = conn.execute("""
            SELECT
                MIN(date) AS earliest, MAX(date) AS latest,
                COUNT(DISTINCT date) AS trading_days,
                COUNT(DISTINCT ticker) AS tickers,
                COUNT(*) AS total_rows
            FROM finra_short_interest
        """).fetchone()
        logger.info(
            f"Table stats: {row[4]:,} rows | "
            f"{row[2]} trading days ({row[0]} → {row[1]}) | "
            f"{row[3]} tickers"
        )
    except Exception:
        pass

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download FINRA RegSHO short sale data into DuckDB.'
    )
    parser.add_argument(
        '--days', type=int, default=252,
        help='Number of trading days to fetch (default: 252 = ~1 year)'
    )
    parser.add_argument(
        '--replace', action='store_true',
        help='Re-download dates already present (default: skip)'
    )
    args = parser.parse_args()

    asyncio.run(download(days_back=args.days, skip_existing=not args.replace))
