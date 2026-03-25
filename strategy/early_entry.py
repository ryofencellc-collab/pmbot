"""
early_entry.py - Honda Civic early entry strategy.

Scans all 21 cities for cheap ranges (0.1-5¢) on markets
that just opened (resolving in 3-7 days).

Strategy: Buy YES on any range priced <= 5¢
- Most lose $10
- Winners pay 200-1000x
- Proven +64% ROI in 30-day real backtest

Runs alongside existing forecast-based signals.
"""

import json
import time
import requests
from datetime import datetime, date, timedelta, timezone
from data.database import get_conn

GAMMA       = "https://gamma-api.polymarket.com"
BET_SIZE    = 10.0
MAX_PRICE   = 0.05   # only buy ranges priced <= 5¢
MIN_PRICE   = 0.001  # ignore completely dead markets
DAYS_AHEAD  = 7      # look at markets resolving up to 7 days out
DAYS_MIN    = 2      # minimum 2 days before resolution (not same day)

ALL_CITIES = {
    "London":       "london",
    "NYC":          "nyc",
    "Toronto":      "toronto",
    "Seoul":        "seoul",
    "Dallas":       "dallas",
    "Atlanta":      "atlanta",
    "Miami":        "miami",
    "Seattle":      "seattle",
    "Paris":        "paris",
    "Tokyo":        "tokyo",
    "Singapore":    "singapore",
    "Madrid":       "madrid",
    "Warsaw":       "warsaw",
    "Beijing":      "beijing",
    "Shanghai":     "shanghai",
    "Taipei":       "taipei",
    "Tel Aviv":     "tel-aviv",
    "Sao Paulo":    "sao-paulo",
    "Milan":        "milan",
    "Munich":       "munich",
    "Buenos Aires": "buenos-aires",
}


def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15,
                             headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(20)
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(1.5 * (i + 1))
    return None


def get_early_signals():
    """
    Find all cheap YES opportunities across all 21 cities.
    Returns list of signals with entry price <= 5¢.
    """
    today    = date.today()
    signals  = []
    log      = [f"=== EARLY ENTRY SCAN {today} ===\n"]

    conn = get_conn()
    c    = conn.cursor()

    for city, slug in ALL_CITIES.items():
        city_signals = []

        # Look at markets resolving 2-7 days from now
        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target_date = today + timedelta(days=days_out)
            date_str    = target_date.strftime("%B-%-d").lower()
            event_slug  = f"highest-temperature-in-{slug}-on-{date_str}-{target_date.year}"

            data = safe_get(f"{GAMMA}/events", params={"slug": event_slug})
            if not data or not isinstance(data, list) or not data:
                continue

            event   = data[0]
            markets = event.get("markets", [])

            for m in markets:
                # Skip closed or non-accepting markets
                if not m.get("acceptingOrders", False):
                    continue
                if m.get("closed") or not m.get("active"):
                    continue

                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        continue

                yes_price = float(prices[0]) if prices else 0.0

                # Only cheap ranges
                if yes_price < MIN_PRICE or yes_price > MAX_PRICE:
                    continue

                # Check not already bet today
                trade_date = today.isoformat()
                c.execute("""
                    SELECT id FROM paper_trades
                    WHERE market_id = %s AND trade_date = %s
                """, (m["id"], trade_date))
                if c.fetchone():
                    continue

                city_signals.append({
                    "city":        city,
                    "market_id":   m["id"],
                    "question":    m.get("question", ""),
                    "days_out":    days_out,
                    "entry_price": yes_price,
                    "side":        "YES",
                    "bet_size":    BET_SIZE,
                    "ev":          round((1.0 / yes_price) * BET_SIZE, 2),
                    "reasoning":   f"Early entry: {yes_price*100:.2f}¢ on {target_date} market ({days_out}d out)",
                })

            time.sleep(0.1)

        if city_signals:
            log.append(f"[{city}] {len(city_signals)} early signals")
            signals.extend(city_signals)
        else:
            log.append(f"[{city}] no signals")

    conn.close()
    log.append(f"\nTotal early signals: {len(signals)}")
    print("\n".join(log))
    return signals, "\n".join(log)


def place_early_trades(capital=10000.0):
    """Place early entry paper trades."""
    signals, log = get_early_signals()

    if not signals:
        return {"trades": 0, "signals": 0, "capital": capital, "log": log}

    conn = get_conn()
    c    = conn.cursor()
    placed = 0
    today  = date.today().isoformat()

    for sig in signals:
        if capital < sig["bet_size"]:
            break

        # Double-check duplicate
        c.execute("""
            SELECT id FROM paper_trades
            WHERE market_id = %s AND trade_date = %s
        """, (sig["market_id"], today))
        if c.fetchone():
            continue

        c.execute("""
            INSERT INTO paper_trades
            (trade_date, market_id, question, city, entry_price,
             noaa_forecast_f, predicted_range, size, capital_at_entry)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            today,
            sig["market_id"],
            sig["question"],
            sig["city"],
            sig["entry_price"],
            0.0,
            f"EARLY:{sig['days_out']}d",
            sig["bet_size"],
            capital,
        ))
        capital -= 0
        placed  += 1

    conn.commit()
    conn.close()

    return {
        "trades":   placed,
        "signals":  len(signals),
        "capital":  capital,
        "log":      log,
    }


if __name__ == '__main__':
    signals, log = get_early_signals()
    print(f"\nFound {len(signals)} early entry signals")
    for s in signals[:10]:
        print(f"  {s['city']} | {s['question'][:50]} | {s['entry_price']*100:.2f}¢ | EV=${s['ev']:.0f}")
