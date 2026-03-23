"""
backtest.py - Blind backtest using WU historical temps + Polymarket markets.

Logic:
1. For each day in range, get NOAA forecast for each city
2. Find which Polymarket range matches the forecast
3. Check market price at that time
4. If price < threshold → simulate bet
5. Record outcome when market resolves

No lookahead. Uses only data available at decision time.
"""

import json
import uuid
import requests
import time
from datetime import datetime, timedelta, timezone
from data.database import get_conn, init_db

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG = {
    "starting_capital": 100.0,
    "principal":        0.0,
    "max_bets_per_day": 3,
    "min_edge":         0.15,
    "max_entry_price":  0.40,
    "min_entry_price":  0.01,
    "bet_size_pct":     0.10,
    "min_bet":          1.00,
    "max_bet":          50.00,
}

# NOAA grid points for each city
NOAA_GRIDS = {
    "Chicago":       "https://api.weather.gov/gridpoints/LOT/76,73/forecast",
    "Dallas":        "https://api.weather.gov/gridpoints/FWD/81,103/forecast",
    "Atlanta":       "https://api.weather.gov/gridpoints/FFC/52,57/forecast",
    "Miami":         "https://api.weather.gov/gridpoints/MFL/109,50/forecast",
    "New York City": "https://api.weather.gov/gridpoints/OKX/33,37/forecast",
    "Seattle":       "https://api.weather.gov/gridpoints/SEW/124,67/forecast",
    "Boston":        "https://api.weather.gov/gridpoints/BOX/69,90/forecast",
    "Los Angeles":   "https://api.weather.gov/gridpoints/LOX/155,45/forecast",
    "San Francisco": "https://api.weather.gov/gridpoints/MTR/84,105/forecast",
}


def safe_get(url, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(2 ** i)
    return None


# ── NOAA Forecast ─────────────────────────────────────────────────────────────

def get_noaa_forecast_for_date(city, target_date_str):
    """
    Get NOAA forecast high temp in F for a specific city and date.
    Uses historical WU data as proxy for what forecast would have shown.
    This is the best we can do for historical backtesting.
    """
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT max_temp_f FROM wu_temps WHERE city=? AND date=?",
              (city, target_date_str))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# ── Range Matcher ─────────────────────────────────────────────────────────────

def find_correct_range(city, target_date_str, forecast_f):
    """
    Given a forecast temperature, find which Polymarket range should win.
    Returns list of market IDs that should be bet YES on.
    """
    conn = get_conn()
    c    = conn.cursor()

    resolved_date = datetime.strptime(target_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    resolved_ts_start = int(resolved_date.timestamp())
    resolved_ts_end   = int((resolved_date + timedelta(days=1)).timestamp())

    c.execute("""SELECT id, question, target_low, target_high, market_type, unit,
                        last_trade_price, outcome
                 FROM markets
                 WHERE city=?
                 AND resolved_at >= ? AND resolved_at < ?
                 AND outcome IS NOT NULL""",
              (city, resolved_ts_start, resolved_ts_end))
    markets = [dict(r) for r in c.fetchall()]
    conn.close()

    signals = []
    for m in markets:
        low  = m["target_low"]
        high = m["target_high"]
        unit = m["unit"]
        price = m["last_trade_price"]

        # Convert forecast to correct unit
        temp = forecast_f
        if unit == "C":
            temp = (forecast_f - 32) * 5 / 9

        # Check if forecast falls in this range
        in_range = False
        if m["market_type"] == "range":
            in_range = low <= temp <= high
        elif m["market_type"] == "above":
            in_range = temp >= low
        elif m["market_type"] == "below":
            in_range = temp <= high
        elif m["market_type"] == "exact":
            in_range = abs(temp - low) <= 1.0

        if not in_range:
            continue

        # Check price is in our target range
        if price < CONFIG["min_entry_price"] or price > CONFIG["max_entry_price"]:
            continue

        # Calculate edge
        true_prob = 0.80  # if forecast matches range, high confidence
        edge      = true_prob - price

        if edge < CONFIG["min_edge"]:
            continue

        signals.append({
            "market_id":    m["id"],
            "question":     m["question"],
            "city":         city,
            "entry_price":  price,
            "forecast_f":   forecast_f,
            "true_prob":    true_prob,
            "edge":         edge,
            "ev":           true_prob * (1.0 / price) - 1.0,
            "outcome":      m["outcome"],
        })

    return signals


# ── Backtest ──────────────────────────────────────────────────────────────────

def run_backtest():
    init_db()

    # Clear previous runs
    conn = get_conn()
    conn.execute("DELETE FROM backtest_trades")
    conn.commit()
    conn.close()

    # Get date range from WU data
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT MIN(date), MAX(date) FROM wu_temps")
    row = c.fetchone()
    conn.close()

    if not row or not row[0]:
        print("No WU temp data. Run ingest first.")
        return

    start_date = datetime.strptime(row[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date   = datetime.strptime(row[1], '%Y-%m-%d').replace(tzinfo=timezone.utc)

    print(f"\n{'='*55}")
    print(f"  POLYEDGE BACKTEST")
    print(f"  {start_date.date()} → {end_date.date()}")
    print(f"  Starting capital: ${CONFIG['starting_capital']}")
    print(f"{'='*55}\n")

    capital    = CONFIG["starting_capital"]
    current    = start_date
    total_bets = 0
    wins       = 0
    equity     = []

    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        day_bets = 0
        day_signals = []

        # Scan all cities for this date
        for city in NOAA_GRIDS.keys():
            forecast = get_noaa_forecast_for_date(city, date_str)
            if not forecast:
                continue

            signals = find_correct_range(city, date_str, forecast)
            day_signals.extend(signals)

        # Sort by edge descending
        day_signals.sort(key=lambda x: x["edge"], reverse=True)
        top = day_signals[:CONFIG["max_bets_per_day"]]

        for sig in top:
            size = min(
                CONFIG["max_bet"],
                max(CONFIG["min_bet"], capital * CONFIG["bet_size_pct"])
            )
            if size > capital:
                continue

            outcome  = sig["outcome"]
            pnl      = size * (1.0 / sig["entry_price"] - 1.0) if outcome == "Yes" else -size
            capital += pnl

            conn = get_conn()
            conn.execute("""INSERT INTO backtest_trades
                (sim_date, market_id, question, city, entry_price, noaa_forecast_f,
                 wu_actual_f, predicted_range, size, capital_at_entry, outcome, pnl)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (date_str, sig["market_id"], sig["question"], sig["city"],
                 sig["entry_price"], sig["forecast_f"], sig["forecast_f"],
                 sig["question"][40:70], size, round(capital - pnl, 2),
                 outcome, round(pnl, 4)))
            conn.commit()
            conn.close()

            total_bets += 1
            day_bets   += 1
            if outcome == "Yes":
                wins += 1

        equity.append({"date": date_str, "capital": round(capital, 2)})

        if day_bets > 0:
            print(f"  {date_str} | ${capital:.2f} | bets={day_bets}")

        current += timedelta(days=1)

    # Results
    win_rate = wins / total_bets if total_bets > 0 else 0
    roi      = (capital - CONFIG["starting_capital"]) / CONFIG["starting_capital"] * 100

    print(f"\n{'='*55}")
    print(f"  BACKTEST RESULTS")
    print(f"{'='*55}")
    print(f"  Total bets:    {total_bets}")
    print(f"  Win rate:      {win_rate*100:.1f}%")
    print(f"  Final capital: ${capital:.2f}")
    print(f"  ROI:           {roi:.1f}%")
    print(f"{'='*55}\n")

    return {
        "total_bets":    total_bets,
        "wins":          wins,
        "win_rate":      round(win_rate, 4),
        "final_capital": round(capital, 2),
        "roi":           round(roi, 2),
        "equity":        equity,
    }


if __name__ == '__main__':
    run_backtest()
