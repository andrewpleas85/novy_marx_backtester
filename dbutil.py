"""Shared DB helpers for the picks/tracker pipeline."""
import os
import glob
import time
import shutil
import logging
from datetime import date, timedelta

import duckdb

logger = logging.getLogger(__name__)


def connect_with_retry(path, *, attempts=5, base_delay=2.0, read_only=False):
    """duckdb.connect that retries on a transient lock conflict.

    A manual run and the weekday cron can fire close together; whichever grabs the
    database file second hits "Conflicting lock is held ...". Rather than aborting the
    run (and silently skipping a trading day, as happened on 2026-06-17/18), wait and
    retry a few times with linear backoff. Re-raises the last error if the lock never
    clears, so the caller can fail loudly and gracefully.
    """
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:
            if 'lock' not in str(e).lower():
                raise  # not a lock problem — surface it immediately
            last_err = e
            if attempt < attempts:
                wait = base_delay * attempt
                logger.warning(
                    f"Database is locked by another process "
                    f"(attempt {attempt}/{attempts}); retrying in {wait:.0f}s..."
                )
                time.sleep(wait)
    raise last_err


def snapshot_live_state(live_path, backup_dir="backups", keep_days=14):
    """Copy live_state.duckdb into ./backups/ with a daily timestamp BEFORE any write run,
    then prune backups older than keep_days. Cheap (the live DB is a few MB) and the only
    disaster-recovery net for the mutable paper-trading state. Idempotent per day: re-running
    on the same date overwrites that day's snapshot rather than piling up copies.

    Returns the snapshot path, or None if the live DB doesn't exist yet.
    """
    if not os.path.exists(live_path):
        logger.warning(f"snapshot_live_state: {live_path} not found — skipping backup.")
        return None
    os.makedirs(backup_dir, exist_ok=True)
    stamp = date.today().isoformat()
    dest = os.path.join(backup_dir, f"live_state_{stamp}.duckdb")
    shutil.copy2(live_path, dest)
    logger.info(f"Snapshotted live state → {dest}")

    # Rotation: keep only the newest `keep_days` daily snapshots.
    cutoff = date.today() - timedelta(days=keep_days)
    for old in glob.glob(os.path.join(backup_dir, "live_state_*.duckdb")):
        base = os.path.basename(old)[len("live_state_"):-len(".duckdb")]
        try:
            if date.fromisoformat(base) < cutoff:
                os.remove(old)
                logger.info(f"Pruned old snapshot {base} (> {keep_days}d).")
        except ValueError:
            continue  # unexpected filename — leave it alone
    return dest


def connect_live(history_path, live_path, *, alias="live_state", **retry_kwargs):
    """Open the HISTORY db (default catalog) and ATTACH live_state, with a search_path so
    unqualified reads/writes to live tables resolve to live_state automatically.

    IMPORTANT (verified on DuckDB 1.5.3): SELECT/INSERT/UPDATE/DELETE on unqualified live-table
    names resolve to live_state via search_path, but CREATE TABLE ignores search_path and lands
    in the DEFAULT catalog (history). So any CREATE/ALTER of a live table MUST be qualified with
    the `live_state.` prefix. History tables (read by model.build_features) resolve unqualified
    against the default catalog with no changes needed.
    """
    conn = connect_with_retry(history_path, **retry_kwargs)
    conn.execute(f"ATTACH '{live_path}' AS {alias}")
    conn.execute(f"SET search_path='{_catalog_name(history_path)},{alias}'")
    return conn


def _catalog_name(db_path):
    """DuckDB derives an attached/opened catalog's name from the file stem (e.g.
    history.duckdb -> 'history')."""
    return os.path.splitext(os.path.basename(db_path))[0]
