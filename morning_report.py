"""Morning report → Discord, pre-market. Two parts:
  1) BUY TODAY — buy alerts (limit at the current price + stop-loss) for positions the strategy
     enrolled at the last 3:33pm run, so you can place the order while the market is open. Each
     position alerts exactly once (morning_alerted flag).
  2) Portfolio digest — a table of holdings: bought price, now, return, model score, sell-stop.

No-op posting if no Discord webhook is configured. Run:  PYTHONPATH=. venv/bin/python3 morning_report.py
"""
import concurrent.futures
from datetime import date, datetime, timedelta

import httpx
import pandas as pd

from config import settings
from dbutil import connect_live
import notify

RECENT_DAYS = 4          # only "buy today" a pick enrolled within this many days (covers a weekend)


def _quote(t):
    try:
        q = httpx.get("https://financialmodelingprep.com/stable/quote",
                      params={"symbol": t, "apikey": settings.FMP_API_KEY}, timeout=15).json()
        return t, (float(q[0]["price"]) if q and q[0].get("price") else None)
    except Exception:
        return t, None


def run(mode="morning"):
    title = "MORNING UPDATE" if mode == "morning" else "END-OF-DAY UPDATE"
    print(f"[{mode}_report] START {datetime.now():%Y-%m-%d %H:%M:%S}", flush=True)
    conn = connect_live(settings.DUCKDB_PATH, settings.LIVE_DB_PATH)
    conn.execute("ALTER TABLE live_state.pick_entries "
                 "ADD COLUMN IF NOT EXISTS morning_alerted BOOLEAN DEFAULT FALSE")
    pos = conn.execute("""SELECT ticker, entry_price, trailing_stop, pick_date, win_probability,
                                 reasons, sector, COALESCE(morning_alerted, FALSE) AS alerted
                          FROM pick_entries WHERE status='open'""").fetchdf()
    if pos.empty:
        ok = notify.post_discord(notify.format_digest(date.today(), [], title=title))
        conn.close()
        print(f"[{mode}_report] DONE — no open positions, digest sent={ok}", flush=True)
        return
    scores = {r[0]: r[1] for r in conn.execute("""
        SELECT ticker, win_probability FROM (
            SELECT ticker, win_probability,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY run_date DESC) rn
            FROM universe_scores) WHERE rn = 1""").fetchall()}

    px = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for t, p in ex.map(_quote, pos["ticker"].tolist()):
            px[t] = p

    # --- 1) BUY TODAY (morning only): not-yet-alerted recent picks → place the order this morning ---
    pos["pick_date"] = pd.to_datetime(pos["pick_date"]).dt.date
    if mode == "morning":
        to_buy = pos[(~pos["alerted"]) & (pos["pick_date"] >= date.today() - timedelta(days=RECENT_DAYS))]
        for _, r in to_buy.iterrows():
            cur = px.get(r.ticker)
            if not cur:
                continue
            notify.post_discord(notify.format_buy(
                r.ticker, round(cur, 2), float(r.win_probability), r.reasons,
                round(cur * (1 + settings.WIN_THRESHOLD), 2), round(cur * (1 - settings.STOP_LOSS_PCT), 2),
                r.get("sector", ""), morning=True))
            conn.execute("UPDATE pick_entries SET morning_alerted=TRUE WHERE ticker=? AND pick_date=?",
                         [r.ticker, r.pick_date])
    conn.close()

    # --- 2) Holdings digest table ---
    rows = []
    for _, r in pos.iterrows():
        cur = px.get(r.ticker)
        if not cur:
            continue
        rows.append(dict(ticker=r.ticker, entry=r.entry_price, price=cur,
                         ret=cur / r.entry_price - 1, score=scores.get(r.ticker),
                         stop=r.trailing_stop,
                         days=(date.today() - r.pick_date).days))
    ok = notify.post_discord(notify.format_digest(date.today(), rows, title=title))
    print(f"[{mode}_report] DONE — digest for {len(rows)} holdings sent={ok}", flush=True)

    # --- 3) The user's REAL-money book (their actual positions + stops) ---
    try:
        import real_tracker
        conn2 = connect_live(settings.DUCKDB_PATH, settings.LIVE_DB_PATH)
        real_msg = real_tracker.digest_string(conn2)
        conn2.close()
        if real_msg:
            rok = notify.post_discord(real_msg)
            print(f"[morning_report] real-book section sent={rok}", flush=True)
    except Exception as e:
        print(f"[morning_report] real-book section skipped: {e}", flush=True)


if __name__ == "__main__":
    import sys
    run(mode="eod" if len(sys.argv) > 1 and sys.argv[1] == "eod" else "morning")
