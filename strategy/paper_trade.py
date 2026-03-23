"""
paper_trade.py - Paper trading engine.

Places up to 3 bets per city per day (the 3 most mispriced ranges).
Checks outcomes in real-time every 30 min via scheduler — not just at 8PM.
Records actual WU temp into noaa_forecasts for error model building.
"""

import json
import requests
from datetime import datetime, timezone
from data.database import get_conn

CAPITAL_START = 100.0
BET_SIZE      = 10.0   # Fixed $10 per signal — clean and simple
MAX_BETS_DAY  = 9      # Up to 3 per city, 1 city for now


def get_current_capital():
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT SUM(pnl) as total FROM paper_trades WHERE outcome IS NOT NULL")
    row       = c.fetchone()
    total_pnl = float(row["total"]) if row and row["total"] else 0.0
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
    """Insert one paper trade. Returns trade dict."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    size  = BET_SIZE

    conn = get_conn()
    c    = conn.cursor()
    try:
        c.execute("""
            INSERT INTO paper_trades
                (trade_date, market_id, question, city, entry_price,
                 noaa_forecast_f, predicted_range, size, capital_at_entry,
                 outcome, pnl)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL)
        """, (
            today,
            signal["market_id"],
            signal["question"],
            signal["city"],
            signal["entry_price"],
            signal["forecast_f"],
            f"{signal['target_low']}-{signal['target_high']}F",
            size,
            capital
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[TRADE ERR] {e}")
        conn.close()
        return None
    conn.close()

    return {
        "city":        signal["city"],
        "question":    signal["question"],
        "entry_price": signal["entry_price"],
        "true_prob":   signal["true_prob"],
        "edge":        signal["edge"],
        "ev":          signal["ev"],
        "size":        size,
        "reasoning":   signal["reasoning"],
        "forecast_f":  signal["forecast_f"],
    }


def run_morning_session():
    """
    Morning session:
    1. Scan signals (3 per city using error model)
    2. Place paper trades for each signal
    3. Log everything
    """
    from strategy.signals import scan_signals

    today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    capital = get_current_capital()
    bets    = get_bets_today()

    log = []
    log.append(f"=== MORNING SESSION {today} ===")
    log.append(f"Capital: ${capital:.2f}")
    log.append(f"Bets placed today so far: {bets}")
    log.append("")

    if bets >= MAX_BETS_DAY:
        log.append("Max bets reached. Skipping.")
        save_log("morning", "\n".join(log))
        return [], "\n".join(log)

    signals, scan_log = scan_signals(today)
    log.append(scan_log)
    log.append("")

    placed = []
    slots  = MAX_BETS_DAY - bets

    for sig in signals[:slots]:
        trade = place_paper_trade(sig, capital)
        if trade:
            capital -= trade["size"]
            placed.append(trade)
            log.append(f"✅ BET: {trade['city']} | ${trade['size']} @ {trade['entry_price']:.4f} | "
                       f"edge={trade['edge']:.3f} | ev={trade['ev']:.2f}x")
            log.append(f"   {trade['question']}")
            log.append(f"   {trade['reasoning']}")
        else:
            log.append(f"❌ FAILED to place trade for {sig['city']}")

    log.append(f"\nPlaced: {len(placed)} trades")
    log.append(f"Capital remaining: ${capital:.2f}")

    save_log("morning", "\n".join(log))
    return placed, "\n".join(log)


def check_pending_outcomes():
    """
    Check all pending (unresolved) trades against Polymarket API.
    Called every 30 min by scheduler — not just at 8PM.
    Returns number of trades resolved.
    """
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT id, market_id, entry_price, size, city, trade_date
                 FROM paper_trades WHERE outcome IS NULL""")
    pending = c.fetchall()
    conn.close()

    if not pending:
        return 0

    resolved = 0
    log      = []
    log.append(f"=== OUTCOME CHECK {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} ===")
    log.append(f"Checking {len(pending)} pending trades...")

    for row in pending:
        tid          = row["id"]
        market_id    = row["market_id"]
        entry_price  = row["entry_price"]
        size         = row["size"]
        city         = row["city"]

        try:
            r = requests.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}",
                timeout=10, headers={"User-Agent": "PolyEdge/1.0"})

            if r.status_code != 200:
                continue

            m      = r.json()
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)

            outcome = None
            if prices and str(prices[0]) == "1":
                outcome = "Yes"
            elif len(prices) > 1 and str(prices[1]) == "1":
                outcome = "No"

            if not outcome:
                continue  # Not resolved yet

            pnl = round(size * (1.0 / entry_price - 1.0), 4) if outcome == "Yes" else -size

            conn = get_conn()
            c2   = conn.cursor()
            c2.execute("UPDATE paper_trades SET outcome=%s, pnl=%s WHERE id=%s",
                       (outcome, pnl, tid))
            conn.commit()
            conn.close()

            icon = "✅" if outcome == "Yes" else "❌"
            log.append(f"  {icon} {city}: {outcome} | pnl=${pnl:.2f} | market={market_id}")
            resolved += 1

            # Record actual temp in noaa_forecasts for error model
            if outcome in ("Yes", "No"):
                _record_actual_temp(city, row["trade_date"], market_id, outcome, m)

        except Exception as e:
            log.append(f"  ERR {city} market {market_id}: {e}")

    log.append(f"Resolved: {resolved}/{len(pending)}")

    if resolved > 0:
        save_log("outcome_check", "\n".join(log))

    return resolved


def _record_actual_temp(city, date_str, market_id, outcome, market_data):
    """
    When a market resolves Yes, record the actual temp range midpoint
    into noaa_forecasts so we can calculate NOAA error over time.
    """
    if outcome != "Yes":
        return  # Only the winning range tells us the actual temp

    try:
        # Get the market's range from our DB
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT target_low, target_high, market_type FROM markets WHERE id=%s",
                  (str(market_id),))
        row = c.fetchone()
        conn.close()

        if not row:
            return

        # Calculate midpoint as actual temp
        if row["market_type"] == "range":
            actual_f = (row["target_low"] + row["target_high"]) / 2.0
        elif row["market_type"] == "below":
            actual_f = row["target_high"] - 1.0
        elif row["market_type"] == "above":
            actual_f = row["target_low"] + 1.0
        else:
            return

        # Update noaa_forecasts with actual
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            UPDATE noaa_forecasts
            SET actual_f = %s,
                delta_f  = forecast_f - %s
            WHERE city=%s AND date=%s
        """, (actual_f, actual_f, city, date_str))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"[ACTUAL TEMP ERR] {e}")


def run_evening_session():
    """Legacy endpoint — now just calls check_pending_outcomes."""
    resolved = check_pending_outcomes()
    log = f"Evening session: resolved {resolved} trades."
    save_log("evening", log)
    return log


def save_log(session_type, content):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO session_logs (session_type, logged_at, content)
                     VALUES (%s,%s,%s)""",
                  (session_type, datetime.now(timezone.utc).isoformat(), content))
        conn.commit()
    except Exception as e:
        print(f"[LOG ERR] {e}")
    conn.close()


def get_performance():
    conn = get_conn()
    c    = conn.cursor()

    c.execute("""SELECT
                    COUNT(*) as total,
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
    pnl      = float(s["total_pnl"] or 0)
    capital  = round(CAPITAL_START + pnl, 2)
    win_rate = round(wins / total * 100, 1) if total > 0 else 0

    return {
        "total_bets":    total,
        "wins":          wins,
        "win_rate":      win_rate,
        "total_pnl":     round(pnl, 2),
        "final_capital": capital,
        "roi":           round(pnl / CAPITAL_START * 100, 1),
        "best_trade":    round(float(s["best"] or 0), 2),
        "worst_trade":   round(float(s["worst"] or 0), 2),
        "trades":        trades,
    }


if __name__ == '__main__':
    trades, log = run_morning_session()
    print(log)
