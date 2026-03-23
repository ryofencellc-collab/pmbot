"""
signals.py - Core signal engine.
Fetches NOAA forecast, finds matching Polymarket markets, calculates edge.
Logs every decision with full reasoning.
"""

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from data.database import get_conn

# NOAA forecast URLs for each city
NOAA_FORECASTS = {
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

# WU stations for resolution verification
WU_STATIONS = {
    "Chicago":       "KORD",
    "Dallas":        "KDFW",
    "Atlanta":       "KATL",
    "Miami":         "KMIA",
    "New York City": "KLGA",
    "Seattle":       "KSEA",
    "Boston":        "KBOS",
    "Los Angeles":   "KLAX",
    "San Francisco": "KSFO",
}

WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"


def get_noaa_forecast(city):
    """
    Get today's high temperature forecast from NOAA.
    Returns temp in Fahrenheit or None.
    """
    url = NOAA_FORECASTS.get(city)
    if not url:
        return None, "No NOAA URL for city"

    try:
        r = requests.get(url, timeout=15,
                        headers={"User-Agent": "PolyEdge/1.0"})
        if r.status_code != 200:
            return None, f"NOAA returned {r.status_code}"

        periods = r.json()["properties"]["periods"]

        # Find today's daytime high
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        for p in periods:
            start = p.get("startTime", "")
            if today in start and p.get("isDaytime", False):
                temp = p.get("temperature")
                unit = p.get("temperatureUnit", "F")
                if unit == "C":
                    temp = temp * 9/5 + 32
                return float(temp), f"NOAA forecast: {temp}°F ({p.get('shortForecast', '')})"

        # Fallback: first daytime period
        for p in periods:
            if p.get("isDaytime", False):
                temp = p.get("temperature")
                return float(temp), f"NOAA forecast (next daytime): {temp}°F"

        return None, "No daytime period found in NOAA"

    except Exception as e:
        return None, f"NOAA error: {e}"


def get_open_markets(city, target_date=None):
    """
    Get all open Polymarket markets for a city resolving today.
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    conn = get_conn()
    c    = conn.cursor()

    date_dt    = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    ts_start   = int(date_dt.timestamp())
    ts_end     = int((date_dt + timedelta(days=1)).timestamp())

    c.execute("""SELECT id, question, target_low, target_high, market_type, unit
                 FROM markets
                 WHERE city=%s
                 AND resolved_at >= %s AND resolved_at < %s""",
              (city, ts_start, ts_end))
    markets = [dict(r) for r in c.fetchall()]
    conn.close()
    return markets


def get_current_price(market_id):
    """
    Get the most recent price for a market from Polymarket API.
    """
    try:
        r = requests.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=10)
        if r.status_code == 200:
            price = float(r.json().get("lastTradePrice") or 0)
            return price if price > 0 else None
    except Exception:
        pass
    return None


def temp_matches_range(temp_f, target_low, target_high, market_type):
    """Check if temperature falls in market range."""
    if market_type == "range":
        return target_low <= temp_f <= target_high
    elif market_type == "above":
        return temp_f >= target_low
    elif market_type == "below":
        return temp_f <= target_high
    elif market_type == "exact":
        return abs(temp_f - target_low) <= 1.0
    return False


def scan_signals(target_date=None):
    """
    Main signal scanner. Runs every morning.
    Returns list of signals with full reasoning.
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    signals  = []
    log      = []

    log.append(f"=== SIGNAL SCAN {target_date} ===")
    log.append(f"Scanning {len(NOAA_FORECASTS)} cities...")
    log.append("")

    for city in NOAA_FORECASTS.keys():
        # Step 1: Get NOAA forecast
        forecast_f, forecast_reason = get_noaa_forecast(city)
        log.append(f"[{city}]")
        log.append(f"  Forecast: {forecast_reason}")

        if not forecast_f:
            log.append(f"  SKIP: No forecast available")
            log.append("")
            continue

        # Step 2: Get open markets for today
        markets = get_open_markets(city, target_date)
        log.append(f"  Open markets today: {len(markets)}")

        if not markets:
            log.append(f"  SKIP: No markets open today")
            log.append("")
            continue

        # Step 3: Find matching range
        matched = []
        for m in markets:
            if temp_matches_range(forecast_f, m["target_low"],
                                  m["target_high"], m["market_type"]):
                matched.append(m)

        if not matched:
            log.append(f"  SKIP: No market range matches {forecast_f}°F")
            log.append("")
            continue

        # Step 4: Get current price and calculate edge
        for m in matched:
            price = get_current_price(m["id"])
            if not price:
                log.append(f"  SKIP: Could not get price for market {m['id']}")
                continue

            # True probability: forecast matches range = high confidence
            # Use 80% as conservative estimate
            true_prob = 0.80
            edge      = true_prob - price
            ev        = true_prob * (1.0 / price) - 1.0

            log.append(f"  Market: {m['question'][:60]}")
            log.append(f"  Price: {price} | True prob: {true_prob} | Edge: {edge:.3f}")

            if price < 0.05 or price > 0.45:
                log.append(f"  SKIP: Price {price} outside entry range (0.05-0.45)")
                continue

            if edge < 0.15:
                log.append(f"  SKIP: Edge {edge:.3f} below minimum 0.15")
                continue

            log.append(f"  ✅ SIGNAL: BET YES")
            log.append(f"  Reasoning: NOAA forecast {forecast_f}°F falls in range {m['target_low']}-{m['target_high']}°F")
            log.append(f"  Expected return: {ev:.1f}x")

            signals.append({
                "city":         city,
                "market_id":    m["id"],
                "question":     m["question"],
                "forecast_f":   forecast_f,
                "forecast_note": forecast_reason,
                "target_low":   m["target_low"],
                "target_high":  m["target_high"],
                "market_type":  m["market_type"],
                "entry_price":  price,
                "true_prob":    true_prob,
                "edge":         round(edge, 4),
                "ev":           round(ev, 4),
                "reasoning":    f"NOAA={forecast_f}°F, range={m['target_low']}-{m['target_high']}°F, price={price}, edge={edge:.3f}",
            })

        log.append("")
        time.sleep(0.3)

    log.append(f"Total signals found: {len(signals)}")
    return signals, "\n".join(log)


if __name__ == '__main__':
    signals, log = scan_signals()
    print(log)
    print(f"\n{len(signals)} signals found")
    for s in signals:
        print(f"  {s['city']}: {s['question'][:60]} @ {s['entry_price']}")
