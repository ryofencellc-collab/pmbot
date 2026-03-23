"""
paper_trade.py - Paper trading engine.
Places paper trades based on signals.
Records every decision with full reasoning.
Checks outcomes automatically.
"""

import json
from datetime import datetime, timezone, timedelta
from data.database import get_conn

CAPITAL_START = 100.0
BET_SIZE_PCT  = 0.10
MIN_BET       = 1.00
MAX_BET       = 50.00
MAX_BETS_DAY  = 3


def get_current_capital():
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT SUM(pnl) as sum FROM paper_trades WHERE outcome IS NOT NULL")
    row = c.fetchone()
    total_pnl = row["sum"] if row and row["sum"] else 0
    conn.close()
    return round(CAPITAL_START + total_pnl, 2)


def get_bets_today():
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    conn  = get_conn()
    c     = conn.cursor()
    c.execute("SELECT COUNT(*) as count FROM paper_trades WHERE trade_date=%s", (today,))
    count = c.fetchone()["count"]
    conn.close()
    return count


def place_paper_trade(signal, capital):
    size  = min(MAX_BET, max(MIN_BET, capital * BET_SIZE_PCT))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    conn  = get_conn()
    c     = conn.cursor()
    c.execute("""INSERT INTO paper_trades
        (trade_date, market_id, question, city, entry_price,
         noaa_forecast_f, predicted_range, size, capital_at_entry,
         outcome, pnl)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL)""",
        (today, signal["market_id"], signal["question"],
         signal["city"], signal["entry_price"],
         signal["forecast_f"],
         f"{signal['target_low']}-{signal['target_high']}F",
         size, capital))
    conn.commit()
    conn.close()
    return {
        "city":        signal["city"],
        "question":    signal["question"],
        "entry_price": signal["entry_price"],
        "size":        size,
        "reasoning":   signal["reasoning"],
        "forecast_f":  signal["forecast_f"],
    }


def run_morning_session():
    from strategy.signals import scan_signals

    today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    capital = get_current_capital()
    bets    = get_bets_today()

    log = []
    log.append(f"=== MORNING SESSION {today} ===")
    log.append(f"Capital: ${capital:.2f}")
    log.append(f"Bets today so far: {bets}")
    log.append("")

    if bets >= MAX_BETS_DAY:
        log.append("Max bets reached for today. Skipping.")
        return [], "\n".join(log)

    signals, scan_log = scan_signals(today)
    log.append(scan_log)

    placed = []
    for sig in signals[:MAX_BETS_DAY - bets]:
        trade = place_paper_trade(sig, capital)
        capital -= trade["size"]
        placed.append(trade)
        log.append(f"✅ PLACED: {trade['city']} | ${trade['size']:.2f} @ {trade['entry_price']}")
        log.append(f"   Reason: {trade['reasoning']}")

    log.append(f"\nTotal trades placed: {len(placed)}")
    log.append(f"Remaining capital: ${capital:.2f}")

    save_log("morning", "\n".join(log))
    return placed, "\n".join(log)


def run_evening_session():
    import requests

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    conn  = get_conn()
    c     = conn.cursor()

    c.execute("""SELECT id, market_id, entry_price, size, city
                 FROM paper_trades
                 WHERE trade_date=%s AND outcome IS NULL""", (today,))
    pending = c.fetchall()
    conn.close()

    log = []
    log.append(f"=== EVENING SESSION {today} ===")
    log.append(f"Checking {len(pending)} pending trades...")

    resolved = 0
    for row in pending:
        tid, market_id, entry_price, size, city = row["id"], row["market_id"], row["entry_price"], row["size"], row["city"]

        try:
            r = requests.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}",
                timeout=10)
            if r.status_code != 200:
                continue

            m      = r.json()
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)

            outcome = None
            if prices and str(prices[0]) == "1":
                outcome = "Yes"
            elif prices and str(prices[1]) == "1":
                outcome = "No"

            if not outcome:
                log.append(f"  {city}: market not yet resolved")
                continue

            pnl = size * (1.0 / entry_price - 1.0) if outcome == "Yes" else -size

            conn = get_conn()
            c2   = conn.cursor()
            c2.execute("UPDATE paper_trades SET outcome=%s, pnl=%s WHERE id=%s",
                       (outcome, round(pnl, 4), tid))
            conn.commit()
            conn.close()

            icon = "✅" if outcome == "Yes" else "❌"
            log.append(f"  {icon} {city}: {outcome} | pnl=${pnl:.2f}")
            resolved += 1

        except Exception as e:
            log.append(f"  {city}: error - {e}")

    log.append(f"\nResolved: {resolved}/{len(pending)}")
    save_log("evening", "\n".join(log))
    return "\n".join(log)


def save_log(session_type, content):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("INSERT INTO session_logs (session_type, logged_at, content) VALUES (%s,%s,%s)",
                  (session_type, datetime.now(timezone.utc).isoformat(), content))
        conn.commit()
    except Exception as e:
        print(f"[LOG ERR] {e}")
    conn.close()


def get_performance():
    conn = get_conn()
    c    = conn.cursor()

    c.execute("""SELECT COUNT(*) as total,
                        SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
                        SUM(COALESCE(pnl,0)) as total_pnl,
                        MAX(pnl) as best,
                        MIN(pnl) as worst
                 FROM paper_trades WHERE outcome IS NOT NULL""")
    s = dict(c.fetchone())

    c.execute("""SELECT trade_date, city, question, entry_price, size,
                        noaa_forecast_f, predicted_range, outcome, pnl
                 FROM paper_trades ORDER BY trade_date DESC, id DESC""")
    trades = [dict(r) for r in c.fetchall()]
    conn.close()

    total    = s["total"] or 0
    wins     = s["wins"] or 0
    pnl      = s["total_pnl"] or 0
    capital  = round(CAPITAL_START + pnl, 2)
    win_rate = round(wins / total * 100, 1) if total > 0 else 0

    return {
        "total_bets":    total,
        "wins":          wins,
        "win_rate":      win_rate,
        "total_pnl":     round(pnl, 2),
        "final_capital": capital,
        "roi":           round(pnl / CAPITAL_START * 100, 1),
        "best_trade":    round(s["best"] or 0, 2),
        "worst_trade":   round(s["worst"] or 0, 2),
        "trades":        trades,
    }


if __name__ == '__main__':
    print("Running morning session...")
    trades, log = run_morning_session()
    print(log)
