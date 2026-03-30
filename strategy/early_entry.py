"""
early_entry.py - Honda Civic early entry strategy.

Scans all 21 cities for cheap ranges (0.1-5¢) on markets
resolving in 2-7 days — but ONLY buys ranges near the forecast.

Strategy: Buy YES on cheap ranges within FORECAST_WINDOW of forecast
- Skips impossible ranges (waste of money)
- Focuses on underpriced ranges near the likely outcome
- Proven approach — Paris +$1,990 was exactly this

FORECAST_WINDOW = how many degrees either side of forecast to buy
Example: forecast 13°C, window 4 → buy ranges 9°C to 17°C only
"""

import json
import time
import requests
import math
from datetime import date, timedelta
from data.database import get_conn

GAMMA            = "https://gamma-api.polymarket.com"
OPEN_METEO       = "https://api.open-meteo.com/v1/forecast"
BET_SIZE         = 10.0
MAX_PRICE        = 0.05    # only buy ranges priced <= 5¢
MIN_PRICE        = 0.001   # ignore completely dead markets
DAYS_AHEAD       = 7       # look at markets resolving up to 7 days out
DAYS_MIN         = 2       # minimum 2 days before resolution
FORECAST_WINDOW  = 4       # degrees either side of forecast to consider

ALL_CITIES = {
    "London":       {"slug": "london",      "lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",                  "unit": "C", "temp_unit": "celsius"},
    "NYC":          {"slug": "nyc",         "lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Toronto":      {"slug": "toronto",     "lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",                "unit": "C", "temp_unit": "celsius"},
    "Seoul":        {"slug": "seoul",       "lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",                     "unit": "C", "temp_unit": "celsius"},
    "Dallas":       {"slug": "dallas",      "lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",                "unit": "F", "temp_unit": "fahrenheit"},
    "Atlanta":      {"slug": "atlanta",     "lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Miami":        {"slug": "miami",       "lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Seattle":      {"slug": "seattle",     "lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",            "unit": "F", "temp_unit": "fahrenheit"},
    "Paris":        {"slug": "paris",       "lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",                   "unit": "C", "temp_unit": "celsius"},
    "Tokyo":        {"slug": "tokyo",       "lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",                     "unit": "C", "temp_unit": "celsius"},
    "Singapore":    {"slug": "singapore",   "lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",                 "unit": "C", "temp_unit": "celsius"},
    "Madrid":       {"slug": "madrid",      "lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",                  "unit": "C", "temp_unit": "celsius"},
    "Warsaw":       {"slug": "warsaw",      "lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",                  "unit": "C", "temp_unit": "celsius"},
    "Beijing":      {"slug": "beijing",     "lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Shanghai":     {"slug": "shanghai",    "lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Taipei":       {"slug": "taipei",      "lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",                    "unit": "C", "temp_unit": "celsius"},
    "Tel Aviv":     {"slug": "tel-aviv",    "lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",                 "unit": "C", "temp_unit": "celsius"},
    "Sao Paulo":    {"slug": "sao-paulo",   "lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",              "unit": "C", "temp_unit": "celsius"},
    "Milan":        {"slug": "milan",       "lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",                    "unit": "C", "temp_unit": "celsius"},
    "Munich":       {"slug": "munich",      "lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",                  "unit": "C", "temp_unit": "celsius"},
    "Buenos Aires": {"slug": "buenos-aires","lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires", "unit": "C", "temp_unit": "celsius"},
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


def get_forecast(city_config, date_str):
    """Get Open-Meteo forecast for a city on a specific date."""
    try:
        r = requests.get(OPEN_METEO, params={
            "latitude":         city_config["lat"],
            "longitude":        city_config["lon"],
            "daily":            "temperature_2m_max",
            "temperature_unit": city_config["temp_unit"],
            "timezone":         city_config["tz"],
            "start_date":       date_str,
            "end_date":         date_str,
        }, timeout=15)
        if r.status_code == 200:
            temps = r.json()["daily"]["temperature_2m_max"]
            return float(temps[0]) if temps and temps[0] is not None else None
    except Exception as e:
        print(f"  [FORECAST ERR] {e}")
    return None


def range_near_forecast(question, forecast, unit, window):
    """
    Check if a market's temperature range is within WINDOW degrees of forecast.
    Parses the question to extract target temperatures.
    Returns True if range is close enough to bet on.
    """
    if forecast is None:
        return True  # no forecast available, allow all

    q = question.lower()

    # Extract numbers from question
    import re
    numbers = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)]
    if not numbers:
        return True

    # For "X°C or below" / "X°F or below" type questions
    if "or below" in q:
        target = numbers[0]
        return target >= forecast - window

    # For "X°C or higher" / "X°F or higher" type questions
    if "or higher" in q:
        target = numbers[0]
        return target <= forecast + window

    # For range questions "between X-Y"
    if "between" in q and len(numbers) >= 2:
        low, high = numbers[0], numbers[1]
        mid = (low + high) / 2
        return abs(mid - forecast) <= window

    # For exact questions "be X°C"
    if len(numbers) >= 1:
        target = numbers[0]
        return abs(target - forecast) <= window

    return True


def get_early_signals():
    """
    Find cheap YES opportunities near the forecast across all 21 cities.
    Only buys ranges within FORECAST_WINDOW degrees of the forecast.
    """
    today   = date.today()
    signals = []
    log     = [f"=== EARLY ENTRY SCAN {today} (forecast-filtered) ===\n"]

    conn = get_conn()
    c    = conn.cursor()

    for city, config in ALL_CITIES.items():
        slug         = config["slug"]
        city_signals = []
        skipped      = 0

        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target_date = today + timedelta(days=days_out)
            date_str    = target_date.strftime("%Y-%m-%d")
            slug_date   = target_date.strftime("%B-%-d").lower()
            event_slug  = f"highest-temperature-in-{slug}-on-{slug_date}-{target_date.year}"

            # Get forecast for this date
            forecast = get_forecast(config, date_str)
            time.sleep(0.15)

            data = safe_get(f"{GAMMA}/events", params={"slug": event_slug})
            if not data or not isinstance(data, list) or not data:
                continue

            markets = data[0].get("markets", [])

            for m in markets:
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

                if yes_price < MIN_PRICE or yes_price > MAX_PRICE:
                    continue

                # ── KEY FILTER: skip ranges far from forecast ──────────────
                question = m.get("question", "")
                if not range_near_forecast(question, forecast, config["unit"], FORECAST_WINDOW):
                    skipped += 1
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
                    "question":    question,
                    "days_out":    days_out,
                    "entry_price": yes_price,
                    "forecast":    forecast,
                    "side":        "YES",
                    "bet_size":    BET_SIZE,
                    "ev":          round((1.0 / yes_price) * BET_SIZE, 2),
                    "reasoning":   f"Early entry: {yes_price*100:.2f}¢ on {target_date} ({days_out}d out) forecast={forecast}{config['unit']}",
                })

            time.sleep(0.1)

        if city_signals:
            log.append(f"[{city}] {len(city_signals)} signals (skipped {skipped} far from forecast)")
            signals.extend(city_signals)
        else:
            log.append(f"[{city}] no signals (skipped {skipped})")

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
            sig.get("forecast") or 0.0,
            f"EARLY:{sig['days_out']}d",
            sig["bet_size"],
            capital,
        ))
        placed += 1

    conn.commit()
    conn.close()

    return {
        "trades":  placed,
        "signals": len(signals),
        "capital": capital,
        "log":     log,
    }


if __name__ == '__main__':
    signals, log = get_early_signals()
    print(f"\nFound {len(signals)} early entry signals")
    for s in signals[:20]:
        print(f"  {s['city']} | {s['question'][:55]} | {s['entry_price']*100:.2f}¢ | forecast={s['forecast']}")
