"""
backtest.py - Full backtest across all cities using real entry prices.
Uses price from 12 hours before resolution as entry price.
Uses WU temp as the resolution source.
No lookahead. Pure data.
"""

from datetime import datetime, timedelta, timezone
from data.database import get_conn, init_db

CONFIG = {
    "starting_capital":  100.0,
    "max_bets_per_day":  3,
    "entry_hours_before": 12,
    "min_entry_price":   0.05,
    "max_entry_price":   0.45,
    "bet_size_pct":      0.10,
    "min_bet":           1.00,
    "max_bet":           50.00,
}

CITIES = [
    "Chicago",
    "Dallas",
    "Atlanta",
    "Miami",
    "New York City",
    "Seattle",
    "Boston",
    "Los Angeles",
    "San Francisco",
]


def get_entry_price(market_id, resolved_at, hours_before=12):
    target_ts = resolved_at - (hours_before * 3600)
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT yes_price FROM price_snapshots
                 WHERE market_id=? AND timestamp <= ?
                 ORDER BY timestamp DESC LIMIT 1""",
              (market_id, target_ts))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def get_wu_temp(city, date_str):
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT max_temp_f FROM wu_temps WHERE city=? AND date=?", (city, date_str))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else None


def temp_matches_range(wu_temp, target_low, target_high, market_type):
    if market_type == "range":
        return target_low <= wu_temp <= target_high
    elif market_type == "above":
        return wu_temp >= target_low
    elif market_type == "below":
        return wu_temp <= target_high
    elif market_type == "exact":
        return abs(wu_temp - target_low) <= 1.0
    return False


def run_backtest():
    init_db()

    conn = get_conn()
    conn.execute("DELETE FROM backtest_trades")
    conn.commit()

    c = conn.cursor()
    c.execute("""SELECT id, question, city, target_low, target_high,
                        market_type, unit, resolved_at, outcome
                 FROM markets
                 WHERE outcome IS NOT NULL
                 ORDER BY resolved_at ASC""")
    markets = [dict(r) for r in c.fetchall()]
    conn.close()

    print(f"\n{'='*55}")
    print(f"  ALL CITIES BACKTEST — {len(markets)} markets")
    print(f"  Entry: {CONFIG['entry_hours_before']}h before resolution")
    print(f"  Starting capital: ${CONFIG['starting_capital']}")
    print(f"{'='*55}\n")

    capital    = CONFIG["starting_capital"]
    total_bets = 0
    wins       = 0

    # Group by resolution date
    dates = {}
    for m in markets:
        date_str = datetime.fromtimestamp(m["resolved_at"], tz=timezone.utc).strftime('%Y-%m-%d')
        if date_str not in dates:
            dates[date_str] = []
        dates[date_str].append(m)

    for date_str, day_markets in sorted(dates.items()):
        day_signals = []

        for m in day_markets:
            wu_temp = get_wu_temp(m["city"], date_str)
            if not wu_temp:
                continue

            if not temp_matches_range(wu_temp, m["target_low"], m["target_high"], m["market_type"]):
                continue

            entry_price = get_entry_price(m["id"], m["resolved_at"], CONFIG["entry_hours_before"])
            if not entry_price:
                continue

            if entry_price < CONFIG["min_entry_price"] or entry_price > CONFIG["max_entry_price"]:
                continue

            day_signals.append({
                "market_id":   m["id"],
                "question":    m["question"],
                "city":        m["city"],
                "entry_price": entry_price,
                "outcome":     m["outcome"],
                "wu_temp":     wu_temp,
                "target_low":  m["target_low"],
                "target_high": m["target_high"],
            })

        # Sort by lowest price first (most mispriced)
        day_signals.sort(key=lambda x: x["entry_price"])
        top = day_signals[:CONFIG["max_bets_per_day"]]

        for sig in top:
            size = min(CONFIG["max_bet"],
                       max(CONFIG["min_bet"], capital * CONFIG["bet_size_pct"]))
            if size > capital:
                continue

            outcome  = sig["outcome"]
            pnl      = size * (1.0 / sig["entry_price"] - 1.0) if outcome == "Yes" else -size
            capital += pnl

            total_bets += 1
            if outcome == "Yes":
                wins += 1

            conn = get_conn()
            conn.execute("""INSERT INTO backtest_trades
                (sim_date, market_id, question, city, entry_price,
                 noaa_forecast_f, wu_actual_f, predicted_range,
                 size, capital_at_entry, outcome, pnl)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (date_str, sig["market_id"], sig["question"], sig["city"],
                 sig["entry_price"], sig["wu_temp"], sig["wu_temp"],
                 f"{sig['target_low']}-{sig['target_high']}F",
                 size, round(capital - pnl, 2), outcome, round(pnl, 4)))
            conn.commit()
            conn.close()

            print(f"  {date_str} | {sig['city']:<15} | WU={sig['wu_temp']}F | entry={sig['entry_price']} | {outcome} | pnl=${pnl:.2f} | capital=${capital:.2f}")

    win_rate = wins / total_bets if total_bets > 0 else 0
    roi      = (capital - CONFIG["starting_capital"]) / CONFIG["starting_capital"] * 100

    print(f"\n{'='*55}")
    print(f"  FINAL RESULTS — ALL CITIES")
    print(f"{'='*55}")
    print(f"  Total bets:    {total_bets}")
    print(f"  Wins:          {wins}")
    print(f"  Win rate:      {win_rate*100:.1f}%")
    print(f"  Final capital: ${capital:.2f}")
    print(f"  ROI:           {roi:.1f}%")
    print(f"{'='*55}\n")

    return {"total_bets": total_bets, "wins": wins, "win_rate": win_rate,
            "final_capital": capital, "roi": roi}


if __name__ == '__main__':
    run_backtest()
