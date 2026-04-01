"""
forecast_accuracy_test.py

Tests Open-Meteo forecast accuracy using HISTORICAL data.

For each resolved market in our DB:
1. Gets the WU actual temp (what Polymarket resolved on)
2. Gets what Open-Meteo says the temp was for that date
3. Compares: how far off was the forecast?

This tells us RIGHT NOW which cities our models are
most accurate in — no need to wait 2 weeks.

Uses Open-Meteo historical API (free, real data).
"""

import requests
import time
from datetime import date, timedelta
from data.database import get_conn
from forecast_logger import fetch_wu_temp, ALL_CITIES

OPEN_METEO_HISTORICAL = "https://archive-api.open-meteo.com/v1/archive"

hist_cache = {}

def get_historical_forecast(city, date_str):
    """
    Get what Open-Meteo recorded for a city on a past date.
    This is the ERA5 reanalysis data — real historical temps.
    We compare this to WU to see model accuracy.
    """
    key = f"{city}_{date_str}"
    if key in hist_cache:
        return hist_cache[key]

    config = ALL_CITIES.get(city)
    if not config:
        return None

    try:
        r = requests.get(OPEN_METEO_HISTORICAL, params={
            "latitude":         config["lat"],
            "longitude":        config["lon"],
            "daily":            "temperature_2m_max",
            "temperature_unit": config["temp_unit"],
            "timezone":         config["tz"],
            "start_date":       date_str,
            "end_date":         date_str,
        }, timeout=15)

        if r.status_code == 200:
            temps = r.json().get("daily", {}).get("temperature_2m_max", [])
            if temps and temps[0] is not None:
                hist_cache[key] = round(float(temps[0]), 1)
                return hist_cache[key]
    except Exception as e:
        print(f"  [HIST ERR] {city} {date_str}: {e}")
    return None


def run_accuracy_test(days_back=30):
    """
    Test forecast accuracy per city using real historical data.
    Shows which cities to bet big on RIGHT NOW.
    """
    conn = get_conn()
    c    = conn.cursor()

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    # Get resolved Yes markets (1 per city per date)
    c.execute("""
        SELECT DISTINCT ON (city, TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD'))
               city,
               TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date
        FROM markets
        WHERE outcome = 'Yes'
        AND TO_TIMESTAMP(resolved_at)::date >= %s::date
        AND city IS NOT NULL
        ORDER BY city, TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD')
    """, (cutoff,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    print(f"Testing forecast accuracy for {len(rows)} city/date pairs...")

    results    = []
    by_city    = {}

    for row in rows:
        city     = row["city"]
        date_str = row["res_date"]

        # Get WU actual temp
        wu_temp = fetch_wu_temp(city, date_str)
        if wu_temp is None:
            continue

        # Get Open-Meteo historical (ERA5 reanalysis)
        om_temp = get_historical_forecast(city, date_str)
        if om_temp is None:
            time.sleep(0.3)
            continue

        error   = round(abs(om_temp - wu_temp), 1)
        correct = error <= 2  # within 2° = accurate enough to win bet

        entry = {
            "city":     city,
            "date":     date_str,
            "om_temp":  om_temp,
            "wu_temp":  wu_temp,
            "error":    error,
            "correct":  correct,
        }
        results.append(entry)

        if city not in by_city:
            by_city[city] = []
        by_city[city].append(entry)

        time.sleep(0.2)

    # City accuracy summary
    city_stats = {}
    for city, entries in by_city.items():
        correct   = [e for e in entries if e["correct"]]
        accuracy  = round(len(correct)/len(entries)*100, 1)
        avg_error = round(sum(e["error"] for e in entries)/len(entries), 1)
        city_stats[city] = {
            "days_tested":  len(entries),
            "correct":      len(correct),
            "accuracy_pct": accuracy,
            "avg_error":    avg_error,
            "bet_big":      accuracy >= 75,
            "caution":      50 <= accuracy < 75,
            "avoid":        accuracy < 50,
        }

    # Rankings
    ranked = sorted(city_stats.items(), key=lambda x: -x[1]["accuracy_pct"])

    print(f"\n{'='*60}")
    print(f"  FORECAST ACCURACY BY CITY — Last {days_back} days")
    print(f"  (Open-Meteo ERA5 vs WU actual, within 2°)")
    print(f"{'='*60}")

    print(f"\n  {'City':<15} {'Days':>5} {'Correct':>8} {'Accuracy':>9} {'Avg Err':>8} {'Action':>10}")
    print(f"  {'-'*15} {'-'*5} {'-'*8} {'-'*9} {'-'*8} {'-'*10}")

    for city, s in ranked:
        action = "✅ BET BIG" if s["bet_big"] else "⚠️ CAUTION" if s["caution"] else "❌ AVOID"
        print(f"  {city:<15} {s['days_tested']:>5} {s['correct']:>8} {s['accuracy_pct']:>8.1f}% {s['avg_error']:>7.1f}° {action:>10}")

    bet_cities = [c for c, s in ranked if s["bet_big"]]
    print(f"\n  BET BIG ON: {', '.join(bet_cities) if bet_cities else 'None yet'}")
    print(f"  Overall accuracy: {round(sum(1 for r in results if r['correct'])/len(results)*100,1) if results else 0}%")

    return {
        "days_back":      days_back,
        "total_tested":   len(results),
        "city_stats":     city_stats,
        "ranked":         [{"city": c, **s} for c, s in ranked],
        "bet_big_cities": bet_cities,
        "results":        results,
    }


if __name__ == '__main__':
    run_accuracy_test(days_back=30)
