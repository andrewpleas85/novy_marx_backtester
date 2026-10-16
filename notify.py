"""Post notifications to a Discord channel via an incoming webhook.

No bot / no OAuth — a webhook is just a URL you POST JSON to. Create one in Discord:
  Server Settings -> Integrations -> Webhooks -> New Webhook -> pick the channel -> Copy URL,
then put it in .env as:  DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/....
If the URL is not set, posting is a silent no-op (feature off).
"""
import logging
import time

import httpx

from config import settings

logger = logging.getLogger(__name__)


def post_discord(content: str, retries: int = 3) -> bool:
    """Post a plain-text message (<=2000 chars) to the configured Discord webhook.
    Retries on transient failures / rate limits. Returns True on success, False if
    unconfigured or every attempt failed (never raises). Failures print to stdout so a
    cron run leaves a visible trace instead of silently dropping the message."""
    url = getattr(settings, "DISCORD_WEBHOOK_URL", "") or ""
    if not url:
        print("[notify] DISCORD_WEBHOOK_URL is empty — message NOT sent", flush=True)
        return False
    # if a user id is configured, @-mention so the message actually notifies (webhooks are silent otherwise)
    uid = getattr(settings, "DISCORD_USER_ID", "") or ""
    if uid:
        content = f"<@{uid}>\n{content}"
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = httpx.post(url, json={"content": content[:1990]}, timeout=15)
            if r.status_code == 429:  # rate limited — honor Discord's retry_after
                wait = float(r.json().get("retry_after", 1.0))
                last = f"429 rate-limited (retry_after={wait}s)"
                time.sleep(min(wait, 5.0))
                continue
            r.raise_for_status()
            return True
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            if attempt < retries:
                time.sleep(min(2 ** attempt, 8))
    msg = f"[notify] Discord post FAILED after {retries} attempts: {last}"
    print(msg, flush=True)
    logger.warning(msg)
    return False


def format_sell(ticker, exit_price, ret_pct, reason, days_held):
    """Format a 'sold' alert for Discord."""
    emoji = "🟢" if ret_pct >= 0 else "🔴"
    reason_txt = {
        "stopped_out": "trailing stop hit (dipped below stop)",
        "winner": f"reached horizon as a WINNER (≥+{settings.WIN_THRESHOLD*100:.0f}%)",
        "expired": f"reached {settings.FORWARD_RETURN_DAYS}-day horizon",
        "exited_pre_catalyst": "pre-catalyst exit",
        "closed_low_conviction": "manually closed (low conviction)",
    }.get(reason, reason)
    return (f"{emoji} **SOLD — {ticker}**\n"
            f"Exit: ${exit_price:.2f}   (**{ret_pct*100:+.1f}%**)\n"
            f"Reason: {reason_txt}   ·   held {days_held}d")


def format_digest(today, rows, title="MORNING UPDATE"):
    """Portfolio digest. rows: list of dicts with
    ticker, price, ret, score, stop, days (score/stop may be None)."""
    if not rows:
        return f"📊 **{title} — {today}**\nNo open positions right now."
    rows = sorted(rows, key=lambda x: x["ret"], reverse=True)
    up = sum(1 for r in rows if r["ret"] >= 0)
    # monospace table inside a code block (Discord doesn't render markdown tables)
    hdr = f"{'Ticker':<6} {'Entry':>6} {'Now':>7} {'Ret':>6} {'Model':>5} {'Stop':>7} {'Gap':>5}"
    tbl = [hdr, "-" * len(hdr)]
    for r in rows:
        sc = f"{r['score']*100:.0f}%" if r.get("score") is not None else "n/a"
        buy = f"${r['entry']:.2f}" if r.get("entry") else "-"
        if r.get("stop"):
            stop = f"${r['stop']:.2f}"
            gap = f"{(r['price']/r['stop']-1)*100:+.0f}%"
        else:
            stop, gap = "n/a", "-"
        tbl.append(f"{r['ticker']:<6} {buy:>6} ${r['price']:>6.2f} {r['ret']*100:>+5.0f}% "
                   f"{sc:>5} {stop:>7} {gap:>5}")
    # which holdings does the model STILL rate a buy (≥50%, the pick gate)?
    gate = settings.BUY_GATE_SCORE
    stop_pct = settings.STOP_LOSS_PCT
    buy_rated = sorted([r for r in rows if r.get("score") is not None and r["score"] >= gate],
                       key=lambda x: -x["score"])
    if buy_rated:
        lines = [f"✅ **Still rated a BUY (model ≥{gate*100:.0f}%)** — if you want to add:"]
        for r in buy_rated:
            buy_at = r["price"]
            stop_at = round(buy_at * (1 - stop_pct), 2)
            lines.append(f"   • **{r['ticker']}** ({r['score']*100:.0f}%)  ·  "
                         f"buy ≤ **${buy_at:.2f}**  ·  set stop **${stop_at:.2f}** (−{stop_pct*100:.0f}%)")
        rated_line = "\n".join(lines) + "\n"
    else:
        rated_line = ""   # nothing genuinely flagged as a buy -> show nothing
    return (f"📊 **{title} — {today}**   ·   {len(rows)} holdings · 🟢{up} / 🔴{len(rows)-up}\n"
            "```\n" + "\n".join(tbl) + "\n```"
            + rated_line
            + "_Entry=what we paid · Stop=current trailing sell price · Gap=cushion above stop (checked at 3:33pm)._")


def format_buy(ticker, limit_price, win_prob, reasons, target, stop, sector="", morning=False):
    """Format a buy alert for Discord. morning=True frames it as an order to place NOW
    (limit at the current price), vs the after-close 'new pick' alert."""
    head = f"☀️ **BUY TODAY — {ticker}**  _(place your order now)_" if morning \
        else f"🟢 **NEW BUY — {ticker}**"
    stop_pct = (stop / limit_price - 1) * 100 if limit_price else -settings.STOP_LOSS_PCT * 100
    tgt_pct = (target / limit_price - 1) * 100 if limit_price else settings.WIN_THRESHOLD * 100
    return (
        f"{head}\n"
        f"💵 **Buy limit: ${limit_price:.2f}**   _(limit order — don't pay more)_\n"
        f"🛑 **Stop-loss: ${stop:.2f}**  ({stop_pct:+.0f}%)   _(set this — sells if it dips here)_\n"
        f"🎯 Target: ${target:.2f}  ({tgt_pct:+.0f}%)\n"
        f"Conviction: P(Win) **{win_prob*100:.0f}%**"
        + (f"  ·  {sector}" if sector else "") + "\n"
        f"Why: {reasons}"
    )
