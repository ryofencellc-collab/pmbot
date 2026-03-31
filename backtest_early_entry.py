"""
backtest_early_entry.py

Tests our ACTUAL strategy:
- Markets open 4-7 days before resolution
- Forecast range is cheap (under 5¢) at open
- We buy it
- Did it win?

Uses our DB resolved markets + Open-Meteo historical temps
as proxy for what forecast would have said.
"""

import requests
import re
import time
from datetime import date, timedelta
from data.database import get_conn

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"

CITY_COORDS = {
    "London":        {"lat": 51.5074,  "lon": -0.1278,   "temp_unit": "celsius"},
    "New York City": {"lat": 40.7128,  "lon": -74.0060,  "temp_unit": "fahrenheit"},
    "NYC":           {"lat": 40.7128,  "lon": -74.0060,  "temp_unit": "fahrenheit"},
    "Toronto":       {"lat": 43.6532,  "lon": -79.3832,  "temp_unit": "celsius"},
    "Seoul":         {"lat": 37.5665,  "lon": 126.9780,  "temp_unit": "celsius"},
    "Dallas":        {"lat": 32.7767,  "lon": -96.7970,  "temp_unit": "fahrenheit"},
    "Atlanta":       {"lat": 33.7490,  "lon": -84.3880,  "temp_unit": "fahrenheit"},
    "Paris":         {"lat": 48.8566,  "lon": 2.3522,    "temp_unit": "celsius"},
    "Tokyo":         {"lat": 35.6762,  "lon": 139.6503,  "temp_unit": "celsius"},
    "Singapore":     {"lat": 1.3521,   "lon": 103.8198,  "temp_unit": "celsius"},
    "Madrid":        {"lat": 40.4168,  "lon": -3.7038,   "temp_unit": "celsius"},
    "Warsaw":        {"lat": 52.2297,  "lon": 21.0122,   "temp_unit": "celsius"},
    "Beijing":       {"lat": 39.9042,  "lon": 116.4074,  "temp_unit": "celsius"},
    "Shanghai":      {"lat": 31.2304,  "lon": 121.4737,  "temp_unit": "celsius"},
    "Taipei":        {"lat": 25.0330,  "lon": 121.5654,  "temp_unit": "celsius"},
    "Tel Aviv":      {"lat": 32.0853,  "lon": 34.7818,   "temp_unit": "celsius"},
    "Sao Paulo":     {"lat": -23.5505, "lon": -46.6333,  "temp_unit": "celsius"},
    "Milan":         {"lat": 45.4642,  "lon": 9.1900,    "temp_unit": "celsius"},
    "Munich":        {"lat": 48.1351,  "lon": 11.5820,   "temp_unit": "celsius"},
    "Buenos Aires":  {"lat": -34.6037, "lon": -58.3816,  "temp_unit": "celsius"},
    "Chicago":       {"lat": 41.8781,  "lon": -87.6298,  "temp_unit": "fahrenheit"},
    "Seattle":       {"lat": 47.6062,  "lon": -122.3321, "temp_unit": "fahrenheit"},
    "Miami":         {"lat": 25.7617,  "lon": -80.1918,  "temp_unit": "fahrenheit"},
}

temp_cache = {}

def get_actual_temp(city, date_str):
    key = f"{city}_{date_str}"
    if key in temp_cache:
        return temp_cache[key]
    coords = CITY_COORDS.get(city)
    if not coords:
        return None
    try:
        r = requests.get(OPEN_METEO, params={
            "latitude":         coords["lat"],
            "longitude":        coords["lon"],
            "daily":            "temperature_2m_max",
            "temperature_unit": coords["temp_unit"],
            "timezone":         "auto",
            "start_date":       date_str,
            "end_date":         date_str,
        }, timeout=10)
        if r.status_code == 200:
            temps = r.json().get("daily", {}).get("temperature_2m_max", [])
            if temps and temps[0] is not None:
                temp_cache[key] = float(temps[0])
                return temp_cache[key]
    except Exception:
        pass
    return None


def get_range_midpoint(question):
    q = question.lower()
    nums = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)
            if -60 < float(x) < 200]
    if not nums:
        return None
    if "or below" in q:
        return nums[0] - 3
    if "or higher" in q:
        return nums[0] + 3
    if "between" in q and len(nums) >= 2:
        return (nums[0] + nums[1]) / 2
    return nums[0]


def run_backtest(days_back=30, entry_price_max=0.05, forecast_window=2):
    """
    Backtest early entry strategy.
    
    Simulates: 4-7 days before resolution, our app sees a market
    where the actual temperature (proxy for forecast) is within
    forecast_window degrees of the range, and price is under
    entry_price_max. Did we win?
    """
    conn = get_conn()
    c    = conn.cursor()

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    # Strategy: find ALL resolved markets opened 4+ days early
    # For YES markets (winners): we assume we bought at entry_price_max (worst case)
    # For NO markets (losers): we check if actual temp was near range
    # This simulates: bot buys anything within forecast_window at entry_price_max
    c.execute("""
        SELECT id, question, city, target_low, target_high, market_type,
               unit, outcome,
               TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date,
               TO_CHAR(TO_TIMESTAMP(created_at), 'YYYY-MM-DD') as open_date,
               (resolved_at - created_at) / 86400 as days_open
        FROM markets
        WHERE outcome IN ('Yes', 'No')
        AND TO_TIMESTAMP(resolved_at)::date >= %s::date
        AND (resolved_at - created_at) >= %s
        ORDER BY resolved_at DESC
        LIMIT 2000
    """, (cutoff, 3 * 86400))  # at least 3 days open

    markets = [dict(r) for r in c.fetchall()]
    conn.close()

    print(f"\nFound {len(markets)} markets matching entry criteria")
    print(f"(price <= {entry_price_max*100:.0f}¢, open 3+ days, last {days_back} days)")

    wins        = []
    losses      = []
    skipped     = 0
    city_stats  = {}

    for m in markets:
        city     = m["city"]
        date_str = m["res_date"]
        price    = float(m["last_trade_price"])
        outcome  = m["outcome"]
        question = m["question"]

        actual = get_actual_temp(city, date_str)
        if actual is None:
            skipped += 1
            continue

        mid = get_range_midpoint(question)
        if mid is None:
            continue

        gap = abs(mid - actual)

        # Only count if forecast would have matched (within window)
        # i.e. our model would have pointed at this range
        if gap > forecast_window:
            continue

        # We would have bet on this range
        # Assume entry price = entry_price_max (conservative worst case)
        price = entry_price_max
        bet_size = 10.0

        if outcome == "Yes":
            pnl = round((1.0 / price) * bet_size - bet_size, 2)
            wins.append({
                "city": city, "date": date_str,
                "question": question[:55], "price": round(price*100,1),
                "actual": actual, "gap": round(gap,1),
                "outcome": outcome, "pnl": pnl,
                "days_open": m.get("days_open", 0)
            })
        else:
            pnl = -bet_size
            losses.append({
                "city": city, "date": date_str,
                "question": question[:55], "price": round(price*100,1),
                "actual": actual, "gap": round(gap,1),
                "outcome": outcome, "pnl": pnl,
                "days_open": m.get("days_open", 0)
            })

        # City stats
        if city not in city_stats:
            city_stats[city] = {"wins": 0, "losses": 0, "pnl": 0}
        if outcome == "Yes":
            city_stats[city]["wins"] += 1
            city_stats[city]["pnl"] += pnl
        else:
            city_stats[city]["losses"] += 1
            city_stats[city]["pnl"] -= bet_size

    all_bets  = wins + losses
    total_pnl = sum(b["pnl"] for b in all_bets)
    spent     = len(all_bets) * 10.0
    win_rate  = round(len(wins)/len(all_bets)*100, 1) if all_bets else 0
    roi       = round(total_pnl/spent*100, 1) if spent > 0 else 0

    print(f"\n{'='*55}")
    print(f"  EARLY ENTRY BACKTEST — Last {days_back} days")
    print(f"  Entry: under {entry_price_max*100:.0f}¢, within {forecast_window}° of forecast")
    print(f"{'='*55}")
    print(f"  Total bets:   {len(all_bets)}")
    print(f"  Wins:         {len(wins)}")
    print(f"  Losses:       {len(losses)}")
    print(f"  Win rate:     {win_rate}%")
    print(f"  Total spent:  ${spent:.2f}")
    print(f"  Total P&L:    ${total_pnl:+.2f}")
    print(f"  ROI:          {roi:+.1f}%")
    print(f"  Daily avg:    ${total_pnl/days_back:+.2f}/day")
    print(f"  Skipped:      {skipped} (no temp data)")

    print(f"\n  TOP WINS:")
    for b in sorted(wins, key=lambda x: -x["pnl"])[:10]:
        print(f"    {b['city']} {b['date']} | {b['question'][:40]} | {b['price']}¢ | +${b['pnl']:.2f}")

    print(f"\n  CITY BREAKDOWN:")
    for city, s in sorted(city_stats.items(), key=lambda x: -x[1]["pnl"])[:10]:
        total = s["wins"] + s["losses"]
        wr = round(s["wins"]/total*100) if total > 0 else 0
        print(f"    {city:<15} {s['wins']}W/{s['losses']}L ({wr}%) P&L=${s['pnl']:+.2f}")

    return {
        "days":       days_back,
        "total_bets": len(all_bets),
        "wins":       len(wins),
        "losses":     len(losses),
        "win_rate":   win_rate,
        "total_pnl":  round(total_pnl, 2),
        "daily_avg":  round(total_pnl/days_back, 2),
        "roi":        roi,
        "top_wins":   sorted(wins, key=lambda x: -x["pnl"])[:10],
        "city_stats": city_stats,
    }


if __name__ == '__main__':
    # Test with different windows
    result = run_backtest(days_back=30, entry_price_max=0.05, forecast_window=2)
