"""
city_cards.py

For each city generates a complete betting card showing:
- Next market resolution date (4+ days out)
- Forecast for that date from GFS + UKMO + MF
- Which ranges to buy (forecast ± 2)
- Current Polymarket prices for those ranges
- Market open time in EST

This powers the City Accuracy tab cards.
"""

import requests
import json
import re
import time
from datetime import date, timedelta
from datetime import timezone, timedelta as td

GAMMA      = "https://gamma-api.polymarket.com"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
EST        = timezone(td(hours=-5))

MODELS = {
    "gfs":  "gfs_global",
    "ukmo": "ukmo_global_deterministic_10km",
    "mf":   "meteofrance_arpege_world",
}

ALL_CITIES = {
    "Seattle":      {"slug": "seattle",      "lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles", "unit": "F", "temp_unit": "fahrenheit"},
    "Dallas":       {"slug": "dallas",       "lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",     "unit": "F", "temp_unit": "fahrenheit"},
    "NYC":          {"slug": "nyc",          "lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "Chicago":      {"slug": "chicago",      "lat": 41.8781,  "lon": -87.6298,  "tz": "America/Chicago",     "unit": "F", "temp_unit": "fahrenheit"},
    "Atlanta":      {"slug": "atlanta",      "lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "Miami":        {"slug": "miami",        "lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "London":       {"slug": "london",       "lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",       "unit": "C", "temp_unit": "celsius"},
    "Paris":        {"slug": "paris",        "lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",        "unit": "C", "temp_unit": "celsius"},
    "Toronto":      {"slug": "toronto",      "lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",     "unit": "C", "temp_unit": "celsius"},
    "Tokyo":        {"slug": "tokyo",        "lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",          "unit": "C", "temp_unit": "celsius"},
    "Seoul":        {"slug": "seoul",        "lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",          "unit": "C", "temp_unit": "celsius"},
    "Singapore":    {"slug": "singapore",    "lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",      "unit": "C", "temp_unit": "celsius"},
    "Madrid":       {"slug": "madrid",       "lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",       "unit": "C", "temp_unit": "celsius"},
    "Munich":       {"slug": "munich",       "lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",       "unit": "C", "temp_unit": "celsius"},
    "Milan":        {"slug": "milan",        "lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",         "unit": "C", "temp_unit": "celsius"},
    "Warsaw":       {"slug": "warsaw",       "lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",       "unit": "C", "temp_unit": "celsius"},
    "Beijing":      {"slug": "beijing",      "lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",       "unit": "C", "temp_unit": "celsius"},
    "Shanghai":     {"slug": "shanghai",     "lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",       "unit": "C", "temp_unit": "celsius"},
    "Taipei":       {"slug": "taipei",       "lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",         "unit": "C", "temp_unit": "celsius"},
    "Tel Aviv":     {"slug": "tel-aviv",     "lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",      "unit": "C", "temp_unit": "celsius"},
    "Buenos Aires": {"slug": "buenos-aires", "lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires", "unit": "C", "temp_unit": "celsius"},
    "Sao Paulo":    {"slug": "sao-paulo",    "lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",   "unit": "C", "temp_unit": "celsius"},
}


def get_forecast(config, date_str):
    """Get GFS + UKMO + MF forecast for a city on a specific date."""
    results = {}
    for name, code in MODELS.items():
        try:
            r = requests.get(OPEN_METEO, params={
                "latitude":         config["lat"],
                "longitude":        config["lon"],
                "daily":            "temperature_2m_max",
                "temperature_unit": config["temp_unit"],
                "timezone":         config["tz"],
                "start_date":       date_str,
                "end_date":         date_str,
                "models":           code,
            }, timeout=15)
            if r.status_code == 200:
                temps = r.json().get("daily", {}).get("temperature_2m_max", [])
                if temps and temps[0] is not None:
                    results[name] = round(float(temps[0]), 1)
        except Exception:
            pass
        time.sleep(0.15)

    if not results:
        return None

    vals      = list(results.values())
    consensus = round(sum(vals) / len(vals), 1)
    spread    = round(max(vals) - min(vals), 1) if len(vals) > 1 else 0.0

    return {
        "gfs":       results.get("gfs"),
        "ukmo":      results.get("ukmo"),
        "mf":        results.get("mf"),
        "consensus": consensus,
        "spread":    spread,
        "models":    len(results),
    }


def get_market_ranges(slug, date_str, consensus, unit):
    """
    Get all ranges from Polymarket for a city/date
    and find the ones closest to the forecast.
    Returns ranges with prices sorted by closeness to forecast.
    """
    target_date = date.fromisoformat(date_str)
    slug_date   = target_date.strftime("%B-%-d").lower()
    event_slug  = f"highest-temperature-in-{slug}-on-{slug_date}-{target_date.year}"

    try:
        data = requests.get(f"{GAMMA}/events", params={"slug": event_slug}, timeout=15).json()
        if not data or not isinstance(data, list) or not data:
            return []

        markets = data[0].get("markets", [])
        ranges  = []

        for m in markets:
            if not m.get("acceptingOrders"):
                continue
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    continue
            yes_price = float(prices[0]) if prices else 0
            if yes_price <= 0:
                continue

            question = m.get("question", "")
            q        = question.lower()
            nums     = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)
                        if -60 < float(x) < 200]
            if not nums:
                continue

            if "or below" in q:
                mid = nums[0]
            elif "or higher" in q:
                mid = nums[0]
            elif "between" in q and len(nums) >= 2:
                mid = (nums[0] + nums[1]) / 2
            else:
                mid = nums[0]

            gap = abs(mid - consensus)

            ranges.append({
                "market_id": m["id"],
                "question":  question,
                "mid":       mid,
                "gap":       round(gap, 1),
                "price_c":   round(yes_price * 100, 2),
                "is_forecast": gap <= 1.0,
                "is_safety":   1.0 < gap <= 3.0,
            })

        # Sort by closeness to forecast
        return sorted(ranges, key=lambda x: x["gap"])

    except Exception as e:
        print(f"  [MARKET ERR] {slug}: {e}")
        return []


def get_city_card(city, config, days_out=4):
    """Build a complete betting card for a city."""
    today       = date.today()
    target_date = today + timedelta(days=days_out)
    date_str    = target_date.strftime("%Y-%m-%d")
    date_label  = target_date.strftime("%B %d")

    # Get 3-model forecast for the resolution date
    fc = get_forecast(config, date_str)
    if not fc:
        return {"city": city, "error": "No forecast available"}

    consensus = fc["consensus"]
    unit      = config["unit"]

    # Get Polymarket ranges and prices
    ranges = get_market_ranges(config["slug"], date_str, consensus, unit)

    # Pick best ranges to bet: forecast + 2 each side
    buy_ranges  = [r for r in ranges if r["gap"] <= 3.0][:5]
    forecast_r  = next((r for r in buy_ranges if r["is_forecast"]), None)

    return {
        "city":         city,
        "target_date":  date_str,
        "date_label":   date_label,
        "days_out":     days_out,
        "forecast": {
            "consensus": consensus,
            "gfs":       fc["gfs"],
            "ukmo":      fc["ukmo"],
            "mf":        fc["mf"],
            "spread":    fc["spread"],
            "unit":      unit,
            "models":    fc["models"],
        },
        "buy_ranges":   buy_ranges,
        "forecast_range": forecast_r,
        "total_ranges": len(ranges),
        "has_market":   len(ranges) > 0,
    }


def get_all_city_cards(days_out=4):
    """Get betting cards for all cities."""
    cards = []
    for city, config in ALL_CITIES.items():
        print(f"  Loading {city}...")
        card = get_city_card(city, config, days_out)
        cards.append(card)
        time.sleep(0.5)
    return cards


if __name__ == '__main__':
    card = get_city_card("Seattle", ALL_CITIES["Seattle"], days_out=4)
    print(json.dumps(card, indent=2))
