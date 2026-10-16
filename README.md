# novy_marx_backtester — pipeline tooling

A small set of infrastructure pieces from a personal quantitative equity
pipeline: data ingestion against the [Financial Modeling Prep](https://site.financialmodelingprep.com/)
API, a DuckDB storage layer, and a Discord-webhook notification system for
daily portfolio digests and buy/sell alerts.

**This is a deliberately partial excerpt.** The actual trading model,
feature engineering, backtests, and research notes are not included here —
this repo only publishes the generic plumbing: how data gets ingested, how
the two DuckDB files are organized, and how results get formatted and sent
to Discord. References in `run_daily.sh` to scripts like `picks.py`,
`tracker.py`, `real_tracker.py`, and `penny_watch.py` point at that private
strategy code, which isn't in this repo.

## What's here

- **`ingest.py`** — full/destructive rebuild of the historical price,
  fundamentals, and benchmark tables from FMP.
- **`ingest_prices_daily.py`**, **`ingest_fundamentals_daily.py`**,
  **`ingest_benchmarks.py`** — incremental daily refreshes (idempotent,
  cron-safe; only append missing dates/quarters instead of rebuilding).
- **`download_finra.py`** — streams FINRA's public RegSHO short-sale-volume
  data into DuckDB (no API key required).
- **`dbutil.py`** — shared DuckDB helpers: lock-retry on connect, a daily
  backup/rotation snapshot, and `connect_live()`, which attaches a second
  "live state" database (mutable, for paper/live positions) alongside the
  immutable historical one via DuckDB's `search_path`.
- **`notify.py`** — posts plain-text alerts to a Discord channel via an
  incoming webhook (no bot/OAuth needed). Formats "bought"/"sold" alerts and
  a daily holdings digest table.
- **`morning_report.py`** — the pre-market/end-of-day cron entry point that
  pulls open positions out of the live DB, fetches current quotes, and
  posts the digest + any new buy alerts through `notify.py`.
- **`run_daily.sh`** — the shape of the daily cron orchestration (ingest →
  picks → tracker → alerts), shown for reference even though some of the
  scripts it calls are private.

## Setup

```bash
pip install -r requirements.txt
cp config.py.example config.py   # fill in your own FMP key, thresholds, etc.
```

Create a `.env` file for secrets (never committed):

```
FMP_API_KEY=your_key_here
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

The `WIN_THRESHOLD` / `FORWARD_RETURN_DAYS` / `STOP_LOSS_PCT` /
`BUY_GATE_SCORE` settings in `config.py` drive the numbers that show up in
Discord messages (win %, horizon, stop-loss %, buy-rated gate) — set them to
whatever your own strategy actually uses.

## Why DuckDB, why two files

`history.duckdb` is treated as append-only/immutable market and fundamental
data that every script reads. `live_state.duckdb` holds the mutable
paper/live-trading tables (open positions, stops, alert flags). They're kept
as separate files so the daily cron can safely snapshot and roll back the
mutable state without touching the much larger historical data — see the
docstring on `dbutil.connect_live()` for the exact DuckDB `ATTACH` /
`search_path` mechanics (and a gotcha: `CREATE TABLE` ignores `search_path`,
so any new live table must be created with an explicit `live_state.` prefix).
