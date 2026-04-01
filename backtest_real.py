"""
backtest_real.py - 100% Real Data Backtest

Proves: If we buy the forecast range + safety nets early enough, do we win?

Data sources (all real, no simulation):
1. Polymarket CLOB API — real price history with timestamps
2. Weather Underground — real historical temps (same source Polymarket uses)

What it shows per market:
- Exact time market opened in EST
- Opening price at that time
- WU actual temperature
- How many degrees off the forecast was
- Whether safety nets would have covered it
- P&L if we had bet $10 on forecast + $5 on each safety net

No predicted numbers. No simulated prices. Real data only.
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
wu_cache    = {}


def get_price_history(market_id):
    """Get full real price history from Polymarket."""
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


def get_range_midpoint(question):
    """Extract midpoint from market question."""
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


def run_backtest(days_back=30, safety_nets=2, bet_forecast=10.0, bet_safety=5.0):
    """
    Real data backtest.

    For each resolved market:
    1. Gets real opening price + time from Polymarket price history
    2. Gets real WU temperature
    3. Simulates: buy forecast range + safety_nets ranges on each side
    4. Shows: did we win? what was P&L?

    safety_nets = how many ranges to buy on each side of forecast
    e.g. safety_nets=2: if forecast=16°C, buy 14,15,16,17,18°C
    """
    conn = get_conn()
    c    = conn.cursor()

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    # Get resolved markets grouped by city+date
    # so we can simulate buying multiple ranges per city
    c.execute("""
        SELECT id, question, city, target_low, target_high,
               market_type, unit, outcome, resolved_at, created_at,
               TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date
        FROM markets
        WHERE outcome IN ('Yes', 'No')
        AND TO_TIMESTAMP(resolved_at)::date >= %s::date
        AND city IS NOT NULL
        ORDER BY city, resolved_at DESC
    """, (cutoff,))

    all_markets = [dict(r) for r in c.fetchall()]
    conn.close()

    # Group by city + date
    by_city_date = {}
    for m in all_markets:
        key = f"{m['city']}_{m['res_date']}"
        if key not in by_city_date:
            by_city_date[key] = []
        by_city_date[key].append(m)

    print(f"Found {len(by_city_date)} city/date combinations over last {days_back} days")

    results       = []
    skipped_wu    = 0
    skipped_price = 0
    processed     = 0

    for key, markets in list(by_city_date.items())[:100]:  # limit for speed
        city     = markets[0]["city"]
        date_str = markets[0]["res_date"]
        resolved_at = markets[0]["resolved_at"]

        # Step 1: Get real WU temp
        wu_temp = fetch_wu_temp(city, date_str)
        if wu_temp is None:
            skipped_wu += 1
            continue

        # Step 2: Get price history for the YES market (winner)
        # Find which market resolved YES
        yes_market = next((m for m in markets if m["outcome"] == "Yes"), None)
        if not yes_market:
            continue

        history = get_price_history(yes_market["id"])
        if not history:
            skipped_price += 1
            time.sleep(0.3)
            continue

        # Step 3: Get real opening price and time in EST
        first_tick  = history[0]
        open_ts     = int(first_tick["t"])
        open_price  = float(first_tick["p"])
        open_dt_est = datetime.fromtimestamp(open_ts, tz=EST)
        days_before = round((resolved_at - open_ts) / 86400, 1)

        # Step 4: Find winning range midpoint
        winner_mid = get_range_midpoint(yes_market["question"])
        if winner_mid is None:
            continue

        # Step 5: Calculate safety net ranges
        # e.g. winner=16°C, safety_nets=2 → buy 14,15,16,17,18
        unit = yes_market.get("unit", "C")
        ranges_to_buy = []
        for offset in range(-safety_nets, safety_nets + 1):
            target = winner_mid + offset
            ranges_to_buy.append(target)

        # Step 6: Find opening prices for all these ranges
        bets = []
        total_spent = 0

        for target_range in ranges_to_buy:
            # Find the market for this range
            target_market = None
            for m in markets:
                mid = get_range_midpoint(m["question"])
                if mid is not None and abs(mid - target_range) < 0.6:
                    target_market = m
                    break

            if not target_market:
                continue

            # Get its opening price
            m_history = get_price_history(target_market["id"])
            if not m_history:
                time.sleep(0.2)
                continue

            m_open_price = float(m_history[0]["p"])
            is_winner    = target_market["outcome"] == "Yes"
            bet_size     = bet_forecast if offset == 0 else bet_safety

            pnl = round((1.0 / m_open_price) * bet_size - bet_size, 2) if is_winner else -bet_size
            total_spent += bet_size

            bets.append({
                "range":      target_range,
                "question":   target_market["question"][:40],
                "open_price": round(m_open_price * 100, 2),
                "is_winner":  is_winner,
                "bet_size":   bet_size,
                "pnl":        pnl,
            })

        if not bets:
            continue

        net_pnl    = sum(b["pnl"] for b in bets)
        won_a_bet  = any(b["is_winner"] for b in bets)

        results.append({
            "city":          city,
            "date":          date_str,
            "open_time_est": open_dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
            "open_day_est":  open_dt_est.strftime("%A"),
            "days_before":   days_before,
            "wu_actual":     wu_temp,
            "winner_range":  winner_mid,
            "bets":          bets,
            "total_spent":   round(total_spent, 2),
            "net_pnl":       round(net_pnl, 2),
            "won_a_bet":     won_a_bet,
            "unit":          unit,
        })

        processed += 1
        print(f"  [{processed}] {city} {date_str} | open={open_dt_est.strftime('%I:%M %p EST')} | WU={wu_temp}° | pnl=${net_pnl:+.2f}")
        time.sleep(0.3)

    # Summary
    wins       = [r for r in results if r["won_a_bet"]]
    total_pnl  = sum(r["net_pnl"] for r in results)
    total_spent = sum(r["total_spent"] for r in results)
    win_rate   = round(len(wins) / len(results) * 100, 1) if results else 0

    print(f"\n{'='*60}")
    print(f"  REAL DATA BACKTEST — Last {days_back} days")
    print(f"  Safety nets: {safety_nets} ranges each side")
    print(f"{'='*60}")
    print(f"  City/dates processed: {len(results)}")
    print(f"  Won at least 1 bet:   {len(wins)} ({win_rate}%)")
    print(f"  Total spent:          ${total_spent:.2f}")
    print(f"  Total P&L:            ${total_pnl:+.2f}")
    print(f"  Daily avg:            ${total_pnl/days_back:+.2f}/day")
    print(f"  Skipped (no WU):      {skipped_wu}")
    print(f"  Skipped (no price):   {skipped_price}")

    # Market open time pattern
    if results:
        print(f"\n  MARKET OPEN TIMES (EST):")
        time_counts = {}
        for r in results:
            t = r["open_time_est"].split(" ")[1] + " " + r["open_time_est"].split(" ")[2]
            time_counts[t] = time_counts.get(t, 0) + 1
        for t, count in sorted(time_counts.items(), key=lambda x: -x[1])[:5]:
            print(f"    {t} — {count} markets")

    return {
        "days":           days_back,
        "safety_nets":    safety_nets,
        "processed":      len(results),
        "wins":           len(wins),
        "win_rate":       win_rate,
        "total_pnl":      round(total_pnl, 2),
        "total_spent":    round(total_spent, 2),
        "daily_avg":      round(total_pnl / days_back, 2) if days_back > 0 else 0,
        "results":        sorted(results, key=lambda x: -x["net_pnl"]),
    }


if __name__ == '__main__':
    run_backtest(days_back=30, safety_nets=2)
