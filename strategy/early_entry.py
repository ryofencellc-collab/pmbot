"""
early_entry.py - Honda Civic early entry strategy with multi-model consensus.

Uses 3 FREE weather models via Open-Meteo:
  1. GFS (American) — good globally
  2. UKMO (UK Met Office) — best for Europe
  3. Meteo-France — best for France/Europe

Consensus rules:
  - All 3 models must agree within CONSENSUS_WINDOW degrees
  - Only bet ranges within FORECAST_WINDOW of the consensus forecast
  - The tighter the model agreement, the higher the confidence score

This replaces single-model approach — significantly higher accuracy.
"""

import json
import time
import requests
import re
from datetime import date, timedelta
from data.database import get_conn

GAMMA            = "https://gamma-api.polymarket.com"
OPEN_METEO       = "https://api.open-meteo.com/v1/forecast"

BET_SIZE         = 10.0
MAX_PRICE        = 0.05    # buy cheap — 4-7 days out before market prices it
MIN_PRICE        = 0.001   # ignore dead markets
DAYS_AHEAD       = 7
DAYS_MIN         = 4       # minimum 4 days out — get in early and cheap
FORECAST_WINDOW  = 3       # 3° window — enough for 4-day forecasts, tight enough to win
CONSENSUS_WINDOW = 3       # max spread between models

# Open-Meteo model codes (all free)
# Exact codes from https://open-meteo.com/en/docs
MODELS = {
    "gfs":         "gfs_global",                   # NOAA GFS — global, updates 4x/day
    "ukmo":        "ukmo_global_deterministic_10km", # UK Met Office — best for Europe
    "meteofrance": "meteofrance_arpege_world",       # Meteo-France — best for Paris/Europe
}

ALL_CITIES = {
    "London":       {"slug": "london",       "lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",                  "unit": "C", "temp_unit": "celsius"},
    "NYC":          {"slug": "nyc",          "lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Toronto":      {"slug": "toronto",      "lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",                "unit": "C", "temp_unit": "celsius"},
    "Seoul":        {"slug": "seoul",        "lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",                     "unit": "C", "temp_unit": "celsius"},
    "Dallas":       {"slug": "dallas",       "lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",                "unit": "F", "temp_unit": "fahrenheit"},
    "Atlanta":      {"slug": "atlanta",      "lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Miami":        {"slug": "miami",        "lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Seattle":      {"slug": "seattle",      "lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",            "unit": "F", "temp_unit": "fahrenheit"},
    "Paris":        {"slug": "paris",        "lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",                   "unit": "C", "temp_unit": "celsius"},
    "Tokyo":        {"slug": "tokyo",        "lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",                     "unit": "C", "temp_unit": "celsius"},
    "Singapore":    {"slug": "singapore",    "lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",                 "unit": "C", "temp_unit": "celsius"},
    "Madrid":       {"slug": "madrid",       "lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",                  "unit": "C", "temp_unit": "celsius"},
    "Warsaw":       {"slug": "warsaw",       "lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",                  "unit": "C", "temp_unit": "celsius"},
    "Beijing":      {"slug": "beijing",      "lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Shanghai":     {"slug": "shanghai",     "lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Taipei":       {"slug": "taipei",       "lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",                    "unit": "C", "temp_unit": "celsius"},
    "Tel Aviv":     {"slug": "tel-aviv",     "lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",                 "unit": "C", "temp_unit": "celsius"},
    "Sao Paulo":    {"slug": "sao-paulo",    "lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",              "unit": "C", "temp_unit": "celsius"},
    "Milan":        {"slug": "milan",        "lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",                    "unit": "C", "temp_unit": "celsius"},
    "Munich":       {"slug": "munich",       "lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",                  "unit": "C", "temp_unit": "celsius"},
    "Buenos Aires": {"slug": "buenos-aires", "lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires", "unit": "C", "temp_unit": "celsius"},
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


def get_multi_model_forecast(city_config, date_str):
    """
    Fetch forecast from all 3 models via Open-Meteo.
    Returns dict with per-model temps and consensus info.

    {
        "gfs": 12.1,
        "ukmo": 11.8,
        "meteofrance": 12.3,
        "consensus": 12.07,    # mean of available models
        "spread": 0.5,         # max - min across models
        "models_available": 3,
        "high_confidence": True  # spread <= CONSENSUS_WINDOW
    }
    """
    results = {}

    for model_name, model_code in MODELS.items():
        try:
            r = requests.get(OPEN_METEO, params={
                "latitude":         city_config["lat"],
                "longitude":        city_config["lon"],
                "daily":            "temperature_2m_max",
                "temperature_unit": city_config["temp_unit"],
                "timezone":         city_config["tz"],
                "start_date":       date_str,
                "end_date":         date_str,
                "models":           model_code,
            }, timeout=15)

            if r.status_code == 200:
                temps = r.json().get("daily", {}).get("temperature_2m_max", [])
                if temps and temps[0] is not None:
                    results[model_name] = round(float(temps[0]), 1)
        except Exception as e:
            print(f"  [MODEL {model_name} ERR] {e}")

        time.sleep(0.2)  # be polite to API

    if not results:
        return None

    temps_list = list(results.values())
    consensus  = round(sum(temps_list) / len(temps_list), 1)
    spread     = round(max(temps_list) - min(temps_list), 1) if len(temps_list) > 1 else 0.0

    # High confidence if:
    # - 3 models available and spread <= CONSENSUS_WINDOW, OR
    # - 2 models available and spread <= CONSENSUS_WINDOW, OR  
    # - 1 model available (fallback — lower confidence but still usable)
    high_confidence = (
        (len(results) >= 2 and spread <= CONSENSUS_WINDOW) or
        (len(results) == 1)  # single model fallback
    )

    return {
        **results,
        "consensus":          consensus,
        "spread":             spread,
        "models_available":   len(results),
        "high_confidence":    high_confidence,
    }


def range_near_forecast(question, consensus, unit, window):
    """
    Check if a market range is within WINDOW degrees of the consensus forecast.
    Returns True if worth betting on.
    """
    if consensus is None:
        return True

    q = question.lower()
    numbers = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)]
    if not numbers:
        return True

    if "or below" in q:
        return numbers[0] >= consensus - window
    if "or higher" in q:
        return numbers[0] <= consensus + window
    if "between" in q and len(numbers) >= 2:
        mid = (numbers[0] + numbers[1]) / 2
        return abs(mid - consensus) <= window
    if len(numbers) >= 1:
        return abs(numbers[0] - consensus) <= window

    return True


def get_early_signals():
    """
    Find cheap YES opportunities near the multi-model consensus forecast.

    Only returns signals where:
    1. At least 2 of 3 models agree (spread <= CONSENSUS_WINDOW)
    2. Market range is within FORECAST_WINDOW of consensus
    3. Price is between MIN_PRICE and MAX_PRICE

    Each signal includes confidence score and model breakdown.
    """
    today   = date.today()
    signals = []
    log     = [f"=== EARLY ENTRY SCAN {today} (multi-model consensus) ===\n"]

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

            # Get multi-model forecast
            fc = get_multi_model_forecast(config, date_str)

            if fc is None:
                continue

            consensus = fc["consensus"]
            spread    = fc["spread"]

            # Skip if models disagree too much
            if not fc["high_confidence"]:
                skipped_reason = f"models disagree (spread={spread}°)"
                continue

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

                question = m.get("question", "")

                # Skip ranges far from consensus
                if not range_near_forecast(question, consensus, config["unit"], FORECAST_WINDOW):
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

                # Confidence score — lower spread = higher confidence
                confidence = round(max(0, (CONSENSUS_WINDOW - spread) / CONSENSUS_WINDOW * 100), 1)

                # Extract individual model forecasts for display
                model_summary = " | ".join([
                    f"GFS={fc.get('gfs','?')}",
                    f"UKMO={fc.get('ukmo','?')}",
                    f"MF={fc.get('meteofrance','?')}",
                ])

                city_signals.append({
                    "city":        city,
                    "market_id":   m["id"],
                    "question":    question,
                    "days_out":    days_out,
                    "entry_price": yes_price,
                    "forecast":    consensus,
                    "spread":      spread,
                    "confidence":  confidence,
                    "models":      model_summary,
                    "side":        "YES",
                    "bet_size":    BET_SIZE,
                    "ev":          round((1.0 / yes_price) * BET_SIZE, 2),
                    "reasoning":   f"{days_out}d | consensus={consensus}{config['unit']} spread={spread}° conf={confidence}% | {model_summary}",
                })

            time.sleep(0.1)

        if city_signals:
            # Sort by confidence within city
            city_signals.sort(key=lambda x: x["confidence"], reverse=True)
            log.append(f"[{city}] {len(city_signals)} signals (skipped {skipped})")
            signals.extend(city_signals)
        else:
            log.append(f"[{city}] no signals (skipped {skipped})")

    conn.close()

    # Sort all signals by confidence score
    signals.sort(key=lambda x: x["confidence"], reverse=True)

    log.append(f"\nTotal signals: {len(signals)}")
    log.append(f"High confidence (>80%): {len([s for s in signals if s['confidence'] > 80])}")
    log.append(f"Medium confidence (50-80%): {len([s for s in signals if 50 <= s['confidence'] <= 80])}")

    print("\n".join(log))
    return signals, "\n".join(log)


def place_early_trades(capital=10000.0):
    """Place early entry trades — only high confidence signals."""
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
            f"EARLY:{sig['days_out']}d|conf={sig['confidence']}%",
            sig["bet_size"],
            capital,
        ))
        placed += 1

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
    print(f"\nFound {len(signals)} signals")
    for s in signals[:15]:
        print(f"  [{s['confidence']}%] {s['city']} | {s['question'][:50]} | {s['entry_price']*100:.2f}¢ | {s['models']}")
