"""
signals.py - Core signal engine.

Strategy:
  - Get NOAA forecast for Chicago
  - Record forecast in noaa_forecasts table
  - Calculate probability distribution across ALL 11 ranges
    using NOAA forecast + historical error margin
  - Find the 3 most mispriced ranges (market price << true prob)
  - Return all 3 as signals to bet

This means we bet 3 ranges per city per day, covering the
forecast ± error window. One will win. The payout vastly
exceeds the two losses because of low entry prices.
"""

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from data.database import get_conn

# Chicago only until other cities are verified
NOAA_FORECASTS = {
    "Chicago": "https://api.weather.gov/gridpoints/LOT/76,73/forecast",
}

# Max signals per city per day (the 3 best mispriced ranges)
MAX_SIGNALS_PER_CITY = 3

# Minimum edge to consider a signal (market price must be this far below true prob)
MIN_EDGE = 0.05


def get_noaa_forecast(city):
    """
    Get today's high temperature forecast from NOAA.
    Returns (temp_f, description) or (None, error_message).
    """
    url = NOAA_FORECASTS.get(city)
    if not url:
        return None, f"No NOAA URL for {city}"

    try:
        r = requests.get(url, timeout=15,
                         headers={"User-Agent": "PolyEdge/1.0"})
        if r.status_code != 200:
            return None, f"NOAA HTTP {r.status_code}"

        periods = r.json()["properties"]["periods"]
        today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Today's daytime period first
        for p in periods:
            if today in p.get("startTime", "") and p.get("isDaytime"):
                temp = float(p["temperature"])
                if p.get("temperatureUnit") == "C":
                    temp = temp * 9/5 + 32
                return temp, f"{temp}°F ({p.get('shortForecast', '')})"

        # Fallback: next daytime period
        for p in periods:
            if p.get("isDaytime"):
                temp = float(p["temperature"])
                return temp, f"{temp}°F (next daytime, {p.get('shortForecast', '')})"

        return None, "No daytime period in NOAA response"

    except Exception as e:
        return None, f"NOAA error: {e}"


def record_noaa_forecast(city, date_str, forecast_f):
    """
    Save today's NOAA forecast to DB for error tracking.
    actual_f and delta_f filled in later by evening session.
    """
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            INSERT INTO noaa_forecasts (city, date, forecast_f, recorded_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city, date) DO UPDATE SET
                forecast_f  = EXCLUDED.forecast_f,
                recorded_at = EXCLUDED.recorded_at
        """, (city, date_str, forecast_f, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[FORECAST LOG ERR] {e}")


def get_noaa_error_stats(city):
    """
    Calculate NOAA historical error stats for a city.
    Returns (mean_delta, std_delta, sample_count).
    mean_delta > 0 means NOAA typically forecasts HIGH (actual is lower).
    """
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            SELECT forecast_f - actual_f as delta
            FROM noaa_forecasts
            WHERE city=%s AND actual_f IS NOT NULL AND delta_f IS NOT NULL
        """, (city,))
        rows   = c.fetchall()
        conn.close()

        if not rows or len(rows) < 3:
            # Not enough data yet — use conservative defaults
            # Based on today: NOAA said 37°F, actual was 35°F → NOAA ran +2°F high
            return 2.0, 3.0, len(rows)

        deltas = [r["delta"] for r in rows]
        mean   = sum(deltas) / len(deltas)
        var    = sum((d - mean) ** 2 for d in deltas) / len(deltas)
        std    = var ** 0.5
        return mean, std, len(deltas)

    except Exception as e:
        print(f"[ERROR STATS ERR] {e}")
        return 2.0, 3.0, 0


def calc_range_probability(target_low, target_high, market_type,
                            forecast_f, mean_delta, std):
    """
    Calculate true probability that actual temp falls in this range.

    Adjusts NOAA forecast by historical mean error, then uses a
    normal distribution with std to spread probability across ranges.

    mean_delta = forecast - actual (positive = NOAA runs high)
    So adjusted forecast = forecast_f - mean_delta
    """
    # Adjust forecast for known NOAA bias
    adjusted = forecast_f - mean_delta

    # Use normal distribution centered on adjusted forecast
    # Approximate CDF with simple distance-based model
    # (avoids scipy dependency)
    def normal_cdf(x, mu, sigma):
        import math
        z = (x - mu) / (sigma if sigma > 0 else 1.0)
        return 0.5 * (1 + math.erf(z / (2 ** 0.5)))

    if market_type == "range":
        prob = normal_cdf(target_high + 0.5, adjusted, std) - \
               normal_cdf(target_low - 0.5, adjusted, std)
    elif market_type == "above":
        prob = 1.0 - normal_cdf(target_low - 0.5, adjusted, std)
    elif market_type == "below":
        prob = normal_cdf(target_high + 0.5, adjusted, std)
    else:
        prob = 0.05

    return round(max(0.001, min(0.999, prob)), 4)


def get_open_markets(city, target_date):
    """Get all markets for city resolving on target_date."""
    conn     = get_conn()
    c        = conn.cursor()
    date_dt  = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    ts_start = int(date_dt.timestamp())
    ts_end   = int((date_dt + timedelta(days=1)).timestamp())

    c.execute("""SELECT id, question, target_low, target_high, market_type, unit,
                        last_trade_price
                 FROM markets
                 WHERE city=%s
                 AND resolved_at >= %s AND resolved_at < %s
                 AND outcome IS NULL""",
              (city, ts_start, ts_end))
    markets = [dict(r) for r in c.fetchall()]
    conn.close()
    return markets


def get_live_price(market_id, stored_price):
    """
    Get live price from Polymarket API.
    Falls back to stored price if API fails.
    """
    try:
        r = requests.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
        if r.status_code == 200:
            price = float(r.json().get("lastTradePrice") or 0)
            if price > 0:
                return price
    except Exception:
        pass
    return stored_price if stored_price and stored_price > 0 else None


def scan_signals(target_date=None):
    """
    Main signal scanner.

    For each city:
    1. Get NOAA forecast and record it
    2. Get historical NOAA error stats
    3. Calculate true probability for EVERY range using error model
    4. Find the 3 ranges with highest edge (true_prob - market_price)
    5. Return those as signals

    Returns (signals_list, log_string).
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    signals = []
    log     = []
    log.append(f"=== SIGNAL SCAN {target_date} ===")
    log.append(f"Cities: {list(NOAA_FORECASTS.keys())}")
    log.append("")

    for city in NOAA_FORECASTS.keys():
        log.append(f"[{city}]")

        # Step 1: NOAA forecast
        forecast_f, forecast_note = get_noaa_forecast(city)
        log.append(f"  NOAA: {forecast_note}")
        if not forecast_f:
            log.append("  SKIP: No forecast")
            log.append("")
            continue

        # Record forecast for error tracking
        record_noaa_forecast(city, target_date, forecast_f)

        # Step 2: Historical error stats
        mean_delta, std, sample_count = get_noaa_error_stats(city)
        adjusted = forecast_f - mean_delta
        log.append(f"  Error model: mean_delta={mean_delta:+.1f}°F, std={std:.1f}°F, n={sample_count}")
        log.append(f"  Adjusted forecast: {adjusted:.1f}°F (NOAA={forecast_f}°F - bias={mean_delta:.1f}°F)")

        # Step 3: Get all markets for today
        markets = get_open_markets(city, target_date)
        log.append(f"  Open markets: {len(markets)}")
        if not markets:
            log.append("  SKIP: No markets in DB for today")
            log.append("")
            continue

        # Step 4: Score every range
        scored = []
        for m in markets:
            true_prob = calc_range_probability(
                m["target_low"], m["target_high"], m["market_type"],
                forecast_f, mean_delta, std
            )

            # Get live price
            price = get_live_price(m["id"], m["last_trade_price"])
            if not price:
                continue

            edge = true_prob - price
            ev   = true_prob * (1.0 / price) - 1.0 if price > 0 else 0

            scored.append({
                "city":        city,
                "market_id":   m["id"],
                "question":    m["question"],
                "forecast_f":  forecast_f,
                "adjusted_f":  adjusted,
                "target_low":  m["target_low"],
                "target_high": m["target_high"],
                "market_type": m["market_type"],
                "entry_price": price,
                "true_prob":   true_prob,
                "edge":        round(edge, 4),
                "ev":          round(ev, 4),
                "reasoning":   (f"NOAA={forecast_f}°F adjusted={adjusted:.1f}°F "
                               f"true_prob={true_prob:.3f} price={price:.4f} "
                               f"edge={edge:.3f} ev={ev:.2f}x"),
            })

        # Log all ranges
        log.append(f"  {'Range':<15} {'TrueProb':>9} {'Price':>7} {'Edge':>7} {'EV':>7}")
        for s in sorted(scored, key=lambda x: x["target_low"]):
            flag = " ← BET" if s["edge"] >= MIN_EDGE else ""
            log.append(f"  {s['question'][40:55]:<15} "
                       f"{s['true_prob']:>9.3f} "
                       f"{s['entry_price']:>7.4f} "
                       f"{s['edge']:>7.3f} "
                       f"{s['ev']:>7.2f}x{flag}")

        # Step 5: Top 3 by edge, only if edge >= MIN_EDGE
        top3 = sorted(
            [s for s in scored if s["edge"] >= MIN_EDGE],
            key=lambda x: x["edge"],
            reverse=True
        )[:MAX_SIGNALS_PER_CITY]

        if not top3:
            log.append(f"  SKIP: No ranges with edge >= {MIN_EDGE}")
        else:
            log.append(f"  ✅ {len(top3)} signals selected")
            signals.extend(top3)

        log.append("")
        time.sleep(0.3)

    log.append(f"Total signals: {len(signals)}")
    return signals, "\n".join(log)


if __name__ == '__main__':
    signals, log = scan_signals()
    print(log)
    print(f"\n{len(signals)} signals")
    for s in signals:
        print(f"  {s['city']} | {s['question'][40:]} | "
              f"price={s['entry_price']} edge={s['edge']} ev={s['ev']}x")
