#!/usr/bin/env bash
# Novy-Marx daily pipeline (invoked by cron, weekdays 15:33 CT).
# Best-effort ingestors run first and never block the picks/tracker chain; tracker runs only
# if picks succeeds. See crontab: `33 15 * * 1-5 .../run_daily.sh`.
cd /home/andrewpleas/Projects/novy_marx_backtester || exit 1
export PYTHONPATH=.
PY=/home/andrewpleas/Projects/novy_marx_backtester/venv/bin/python3
LOG=/home/andrewpleas/Projects/novy_marx_backtester/cron.log

# --- best-effort data ingestion (a failure here must NOT stop picks/tracker) ---
"$PY" events/sec_ingestor.py --scope universe >> "$LOG" 2>&1   # 8-K catalysts
"$PY" events/biotech_tracker.py               >> "$LOG" 2>&1   # biotech Phase 2/3 dates
"$PY" ingest_benchmarks.py                    >> "$LOG" 2>&1   # SPY/IWM (regime gates depend on these)
"$PY" ingest_prices_daily.py                  >> "$LOG" 2>&1   # universe EOD prices (momentum/screen features)
"$PY" ingest_fundamentals_daily.py --scope universe >> "$LOG" 2>&1  # new quarterly filings (fundamentals)

# --- core: tracker only if picks succeeds ---
# tracker runs REGARDLESS of picks' exit (';' not '&&') — it manages stops/exits on the open book
# and must never be skipped just because picks had a quiet/failed day.
"$PY" picks.py   >> "$LOG" 2>&1
"$PY" tracker.py >> "$LOG" 2>&1
# REAL-money book: update peaks/stops and post SELL alerts for the user's actual positions. Best-effort.
"$PY" real_tracker.py check >> "$LOG" 2>&1
# EXPERIMENTAL research sleeve — separate penny_paper table, never touches the live book. Best-effort.
"$PY" penny_watch.py >> "$LOG" 2>&1
