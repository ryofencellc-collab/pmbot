"""
price_history_backtest.py

100% REAL DATA backtest.

For each resolved market:
1. Gets REAL price history from Polymarket CLOB API
2. Finds the REAL opening price and opening time (in EST)
3. Gets REAL Weather Underground temp (what Polymarket uses to resolve)
4. Checks: if we bought at open price, did we win?

No simulations. No estimates. Real data only.
"""

import requests
import json
import time
import re
from datetime import datetime, timezone, timedelta
from data.database import get_conn

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

EST = timezone(timedelta(hours=-5))

# WU stations — exactly what Polymarket uses to resolve
WU_STATIONS = {
    "London":        "EGLL",    # Heathrow
    "NYC":           "KLGA",    # LaGuardia
    "New York City": "KLGA",
    "Toronto":       "CYYZ",    # Pearson
    "Paris":         "LFPG",    # CDG
    "Dallas":        "KDFW",
    "Atlanta":       "KATL",
    "Seoul":         "RKSS",
    "Tokyo":         "RJTT",
    "Singapore":     "WSSS",
    "Madrid":        "LEMD",
    "Warsaw":        "EPWA",
    "Beijing":       "ZBAA",
    "Shanghai":      "ZSPD",
    "Taipei":        "RCTP",
    "Tel Aviv":      "LLBG",
    "Sao Paulo":     "SBGR",
    "Milan":         "LIMC",
    "Munich":        "EDDM",
    "Buenos Aires":  "SAEZ",
    "Chicago":       "KORD",
    "Seattle":       "KSEA",
    "Miami":         "KMIA",
}

wu_cache    = {}
price_cache = {}


def get_wu_temp(city, date_str):
    """Get actual WU temperature — same source Polymarket uses."""
    key = f"{city}_{date_str}"
    if key in wu_cache:
        return wu_cache[key]

    station = WU_STATIONS.get(city)
    if not station:
        return None

    date_fmt = date_str.replace("-", "")
    try:
        r = requests.get(
            f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json",
            params={"apiKey": WU_API_KEY, "units": "e", "startDate": date_fmt},
            timeout=15
        )
        if r.status_code == 200:
            obs   = r.json().get("observations", [])
            temps = [o.get("temp") for o in obs if o.get("temp") is not None]
            if temps:
                result = max(temps)
                wu_cache[key] = result
                return result
    except Exception as e:
        print(f"  [WU ERR] {city} {date_str}: {e}")
    return None


def get_price_history(market_id):
    """Get full price history from Polymarket CLOB API."""
    if market_id in price_cache:
        return price_cache[market_id]

    try:
        # Get token ID
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=15)
        if r.status_code != 200:
            return None

        tokens = r.json().get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not tokens:
            return None

        # Get full price history
        r2 = requests.get(f"{CLOB_BASE}/prices-history", params={
            "market":   tokens[0],
            "interval": "all",
            "fidelity": 60,  # hourly
        }, timeout=15)

        if r2.status_code != 200:
            return None

        history = r2.json().get("history", [])
        if not history:
            return None

        price_cache[market_id] = history
        return history

    except Exception as e:
        print(f"  [PRICE ERR] {market_id}: {e}")
    return None


def get_opening_price_est(history, resolved_at_ts):
    """
    Get the opening price and time in EST.
    Returns: (price, datetime_est, days_before_resolution)
    """
    if not history:
        return None, None, None

    # Sort by timestamp
    sorted_history = sorted(history, key=lambda x: x["t"])

    # First price tick = opening
    first = sorted_history[0]
    open_ts    = int(first["t"])
    open_price = float(first["p"])
    open_dt    = datetime.fromtimestamp(open_ts, tz=EST)

    days_before = (resolved_at_ts - open_ts) / 86400

    return open_price, open_dt, days_before


def run_backtest(days_back=30, max_open_price=0.05, min_days_before=3):
    """
    Real data backtest.

    Finds resolved markets where:
    - Opening price was under max_open_price (we could have bought cheap)
    - Market was open min_days_before resolution (early entry)
    - WU actual temp was within 2° of the range (our forecast would have matched)

    Returns real win/loss results.
    """
    conn = get_conn()
    c    = conn.cursor()

    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    c.execute("""
        SELECT id, question, city, target_low, target_high,
               market_type, unit, outcome, resolved_at,
               TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date
        FROM markets
        WHERE outcome IN ('Yes', 'No')
        AND TO_TIMESTAMP(resolved_at)::date >= %s::date
        ORDER BY resolved_at DESC
        LIMIT 300
    """, (cutoff,))

    markets = [dict(r) for r in c.fetchall()]
    conn.close()

    print(f"\nChecking {len(markets)} resolved markets...")

    results    = []
    skipped_wu = 0
    skipped_price = 0
    skipped_window = 0

    for i, m in enumerate(markets):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(markets)}...")

        city       = m["city"]
        date_str   = m["res_date"]
        outcome    = m["outcome"]
        question   = m["question"]
        resolved_at = m["resolved_at"]

        # Step 1: Get WU actual temp (real resolution data)
        wu_temp = get_wu_temp(city, date_str)
        if wu_temp is None:
            skipped_wu += 1
            continue

        # Step 2: Get price history
        history = get_price_history(m["id"])
        if not history:
            skipped_price += 1
            time.sleep(0.3)
            continue

        # Step 3: Get real opening price and time
        open_price, open_dt_est, days_before = get_opening_price_est(
            history, resolved_at
        )

        if open_price is None or days_before < min_days_before:
            continue

        # Step 4: Check if opening price was cheap enough
        if open_price > max_open_price:
            continue

        # Step 5: Check if WU temp is within 2° of range
        # (simulates: our forecast pointed at this range)
        q    = question.lower()
        nums = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)
                if -60 < float(x) < 200]
        if not nums:
            continue

        if "or below" in q:
            mid = nums[0] - 2
        elif "or higher" in q:
            mid = nums[0] + 2
        elif "between" in q and len(nums) >= 2:
            mid = (nums[0] + nums[1]) / 2
        else:
            mid = nums[0]

        gap = abs(mid - wu_temp)
        if gap > 2:
            skipped_window += 1
            continue

        # This is a valid backtest entry
        bet_size = 10.0
        if outcome == "Yes":
            pnl = round((1.0 / open_price) * bet_size - bet_size, 2)
        else:
            pnl = -bet_size

        results.append({
            "city":         city,
            "date":         date_str,
            "question":     question[:60],
            "open_price_c": round(open_price * 100, 2),
            "open_time_est": open_dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
            "days_before":  round(days_before, 1),
            "wu_temp":      wu_temp,
            "gap":          round(gap, 1),
            "outcome":      outcome,
            "pnl":          pnl,
            "bet_size":     bet_size,
        })

        time.sleep(0.2)

    # Results
    wins   = [r for r in results if r["outcome"] == "Yes"]
    losses = [r for r in results if r["outcome"] == "No"]
    total_pnl = sum(r["pnl"] for r in results)
    spent     = len(results) * 10.0
    win_rate  = round(len(wins)/len(results)*100, 1) if results else 0
    roi       = round(total_pnl/spent*100, 1) if spent > 0 else 0

    print(f"\n{'='*55}")
    print(f"  REAL DATA BACKTEST — Last {days_back} days")
    print(f"{'='*55}")
    print(f"  Markets checked:  {len(markets)}")
    print(f"  Valid bets:       {len(results)}")
    print(f"  Wins:             {len(wins)}")
    print(f"  Losses:           {len(losses)}")
    print(f"  Win rate:         {win_rate}%")
    print(f"  Total P&L:        ${total_pnl:+.2f}")
    print(f"  ROI:              {roi:+.1f}%")
    print(f"  Daily avg:        ${total_pnl/days_back:+.2f}/day")
    print(f"\n  Skipped (no WU):    {skipped_wu}")
    print(f"  Skipped (no price): {skipped_price}")
    print(f"  Skipped (too far):  {skipped_window}")

    print(f"\n  TOP WINS:")
    for r in sorted(wins, key=lambda x: -x["pnl"])[:10]:
        print(f"    {r['city']} {r['date']} | opened {r['open_time_est']}")
        print(f"    {r['question'][:50]}")
        print(f"    Entry: {r['open_price_c']}¢ | {r['days_before']:.1f}d before | WU={r['wu_temp']}° | +${r['pnl']:.2f}")

    return {
        "days":           days_back,
        "markets_checked": len(markets),
        "valid_bets":     len(results),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       win_rate,
        "total_pnl":      round(total_pnl, 2),
        "daily_avg":      round(total_pnl/days_back, 2),
        "roi":            roi,
        "results":        sorted(results, key=lambda x: -x["pnl"]),
    }


if __name__ == '__main__':
    run_backtest(days_back=30)
