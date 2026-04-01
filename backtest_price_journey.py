"""
backtest_price_journey.py

Shows the COMPLETE price journey for every market from open to resolution.

For each resolved market this shows:
- Exact open time in EST + opening price
- Every price tick with timestamp in EST
- When price started moving (our buy window)
- How long it stayed cheap
- Final resolution price + outcome

This is the data that shows us exactly when to buy and how long we have.
100% real data from Polymarket CLOB API + WU temps.
"""

import requests
import json
import re
import time
from datetime import datetime, timezone, timedelta, date
from data.database import get_conn
from forecast_logger import fetch_wu_temp

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
EST        = timezone(timedelta(hours=-5))

price_cache = {}


def get_full_price_history(market_id):
    """Get complete price history with all ticks."""
    if market_id in price_cache:
        return price_cache[market_id]

    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=15)
        if r.status_code != 200:
            return None

        tokens = r.json().get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not tokens:
            return None

        # fidelity=60 = hourly ticks
        r2 = requests.get(f"{CLOB_BASE}/prices-history", params={
            "market":   tokens[0],
            "interval": "all",
            "fidelity": 60,
        }, timeout=15)

        if r2.status_code != 200:
            return None

        history = r2.json().get("history", [])
        if history:
            price_cache[market_id] = sorted(history, key=lambda x: x["t"])
        return price_cache.get(market_id)

    except Exception as e:
        print(f"  [PRICE ERR] {market_id}: {e}")
    return None


def ts_to_est(ts):
    """Convert unix timestamp to EST string."""
    return datetime.fromtimestamp(int(ts), tz=EST).strftime("%Y-%m-%d %I:%M %p EST")


def get_range_midpoint(question):
    q    = question.lower()
    nums = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)
            if -60 < float(x) < 200]
    if not nums:
        return None
    if "or below" in q:
        return nums[0]
    if "or higher" in q:
        return nums[0]
    if "between" in q and len(nums) >= 2:
        return (nums[0] + nums[1]) / 2
    return nums[0]


def find_buy_window(ticks, open_price, threshold_multiplier=2.0):
    """
    Find when price started moving significantly.
    Returns the timestamp when price doubled from open.
    Everything before that = our buy window.
    """
    target = open_price * threshold_multiplier
    for tick in ticks:
        if float(tick["p"]) >= target:
            return tick["t"]
    return None


def run_price_journey_backtest(days_back=30, safety_nets=2, cities=None):
    """
    Full price journey backtest.
    Shows complete price history for every market.
    """
    conn = get_conn()
    c    = conn.cursor()

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    city_filter = ""
    params      = [cutoff]
    if cities:
        placeholders = ",".join(["%s"] * len(cities))
        city_filter  = f"AND city IN ({placeholders})"
        params.extend(cities)

    c.execute(f"""
        SELECT id, question, city, target_low, target_high,
               market_type, unit, outcome, resolved_at, created_at,
               TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date
        FROM markets
        WHERE outcome IN ('Yes', 'No')
        AND TO_TIMESTAMP(resolved_at)::date >= %s::date
        {city_filter}
        AND city IS NOT NULL
        ORDER BY city, resolved_at DESC
    """, params)

    all_markets = [dict(r) for r in c.fetchall()]
    conn.close()

    # Group by city + date
    by_city_date = {}
    for m in all_markets:
        key = f"{m['city']}_{m['res_date']}"
        if key not in by_city_date:
            by_city_date[key] = []
        by_city_date[key].append(m)

    print(f"Analyzing {len(by_city_date)} city/date combinations...")

    results    = []
    skipped_wu = 0

    for key, markets in list(by_city_date.items())[:150]:
        city        = markets[0]["city"]
        date_str    = markets[0]["res_date"]
        resolved_at = markets[0]["resolved_at"]

        # Get real WU temp
        wu_temp = fetch_wu_temp(city, date_str)
        if wu_temp is None:
            skipped_wu += 1
            continue

        # Find YES market (winner)
        yes_market = next((m for m in markets if m["outcome"] == "Yes"), None)
        if not yes_market:
            continue

        # Get full price history for winner
        history = get_full_price_history(yes_market["id"])
        if not history:
            time.sleep(0.3)
            continue

        # Opening info
        first_tick   = history[0]
        open_ts      = int(first_tick["t"])
        open_price   = float(first_tick["p"])
        open_dt_est  = datetime.fromtimestamp(open_ts, tz=EST)
        days_before  = round((resolved_at - open_ts) / 86400, 1)

        # Find when price started moving (2x open price)
        move_ts      = find_buy_window(history, open_price, 2.0)
        move_dt_est  = datetime.fromtimestamp(int(move_ts), tz=EST) if move_ts else None

        # Buy window = time from open until price doubled
        if move_ts:
            window_hours = round((int(move_ts) - open_ts) / 3600, 1)
        else:
            window_hours = round((resolved_at - open_ts) / 3600, 1)

        # Build hourly price journey (condensed)
        journey = []
        prev_price = None
        for tick in history:
            p = round(float(tick["p"]) * 100, 1)
            if prev_price is None or abs(p - prev_price) >= 2:  # only show when price changes 2¢+
                journey.append({
                    "time_est": ts_to_est(tick["t"]),
                    "price_c":  p,
                    "ts":       int(tick["t"]),
                })
                prev_price = p

        # Get safety net markets and their price journeys
        winner_mid = get_range_midpoint(yes_market["question"])
        bets       = []

        for offset in range(-safety_nets, safety_nets + 1):
            target = winner_mid + offset if winner_mid else None
            if target is None:
                continue

            target_market = None
            for m in markets:
                mid = get_range_midpoint(m["question"])
                if mid is not None and abs(mid - target) < 0.6:
                    target_market = m
                    break

            if not target_market:
                continue

            m_history = get_full_price_history(target_market["id"])
            if not m_history:
                time.sleep(0.2)
                continue

            m_open  = round(float(m_history[0]["p"]) * 100, 1)
            m_final = round(float(m_history[-1]["p"]) * 100, 1)

            bets.append({
                "range":       target,
                "question":    target_market["question"][:50],
                "open_price":  m_open,
                "final_price": m_final,
                "is_winner":   target_market["outcome"] == "Yes",
                "outcome":     target_market["outcome"],
            })

        total_spent = sum(
            10.0 if b["range"] == winner_mid else 5.0
            for b in bets
        )
        net_pnl = sum(
            (100.0 / b["open_price"] * (10.0 if b["range"] == winner_mid else 5.0) - (10.0 if b["range"] == winner_mid else 5.0))
            if b["is_winner"] else -(10.0 if b["range"] == winner_mid else 5.0)
            for b in bets
        )

        results.append({
            "city":           city,
            "date":           date_str,
            "wu_actual":      wu_temp,
            "open_time_est":  open_dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
            "open_day":       open_dt_est.strftime("%A"),
            "open_price_c":   round(open_price * 100, 1),
            "days_before":    days_before,
            "price_doubled_at": move_dt_est.strftime("%Y-%m-%d %I:%M %p EST") if move_dt_est else "never doubled",
            "buy_window_hours": window_hours,
            "final_price_c":  round(float(history[-1]["p"]) * 100, 1),
            "resolution_est": ts_to_est(resolved_at),
            "winner_range":   winner_mid,
            "winner_question": yes_market["question"][:60],
            "price_journey":  journey,
            "bets":           bets,
            "total_spent":    round(total_spent, 2),
            "net_pnl":        round(net_pnl, 2),
            "won_a_bet":      any(b["is_winner"] for b in bets),
        })

        time.sleep(0.2)

    # Stats
    wins       = [r for r in results if r["won_a_bet"]]
    total_pnl  = sum(r["net_pnl"] for r in results)
    total_spent = sum(r["total_spent"] for r in results)
    win_rate   = round(len(wins) / len(results) * 100, 1) if results else 0

    # Buy window analysis
    windows = [r["buy_window_hours"] for r in results if r["buy_window_hours"] > 0]
    avg_window = round(sum(windows)/len(windows), 1) if windows else 0

    print(f"\n{'='*60}")
    print(f"  PRICE JOURNEY BACKTEST — Last {days_back} days")
    print(f"{'='*60}")
    print(f"  Processed:       {len(results)} city/dates")
    print(f"  Win rate:        {win_rate}%")
    print(f"  Total P&L:       ${total_pnl:+.2f}")
    print(f"  Avg buy window:  {avg_window} hours before price doubles")
    print(f"  Skipped (no WU): {skipped_wu}")

    return {
        "days":            days_back,
        "processed":       len(results),
        "wins":            len(wins),
        "win_rate":        win_rate,
        "total_pnl":       round(total_pnl, 2),
        "total_spent":     round(total_spent, 2),
        "daily_avg":       round(total_pnl / days_back, 2),
        "avg_buy_window_hours": avg_window,
        "results":         sorted(results, key=lambda x: -x["net_pnl"]),
    }


if __name__ == '__main__':
    run_price_journey_backtest(days_back=30, safety_nets=2)
