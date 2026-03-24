"""
signals.py - Core signal engine for Chicago and London.

Strategy:
  - Chicago: NOAA forecast (°F), 2°F ranges
  - London:  Open-Meteo forecast (°C), exact degree markets

For each city:
1. Get forecast
2. Record in noaa_forecasts table
3. Calculate true probability across all ranges
4. Return top 3 most mispriced ranges by edge
"""

import math
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from data.database import get_conn

# ── City Configuration ────────────────────────────────────────────────────────

CITIES = {
    "Chicago": {
        "forecast_source": "noaa",
        "noaa_url":        "https://api.weather.gov/gridpoints/LOT/76,73/forecast",
        "unit":            "F",
        "slug":            "chicago",
    },
    "London": {
        "forecast_source": "open_meteo",
        "latitude":        51.5074,
        "longitude":       -0.1278,
        "timezone":        "Europe/London",
        "unit":            "C",
        "slug":            "london",
    },
}

MAX_SIGNALS_PER_CITY = 3
MIN_EDGE             = 0.10  # Minimum 10% edge — only high confidence bets


# ── Forecast Functions ────────────────────────────────────────────────────────

def get_noaa_forecast(city_config):
    """Get NOAA forecast for US cities. Returns (temp, note) or (None, error)."""
    try:
        r = requests.get(city_config["noaa_url"], timeout=15,
                         headers={"User-Agent": "PolyEdge/1.0"})
        if r.status_code != 200:
            return None, f"NOAA HTTP {r.status_code}"

        periods   = r.json()["properties"]["periods"]
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for p in periods:
            if today_str in p.get("startTime", "") and p.get("isDaytime"):
                temp = float(p["temperature"])
                if p.get("temperatureUnit") == "C":
                    temp = temp * 9/5 + 32
                return temp, f"{temp}°F ({p.get('shortForecast', '')})"

        for p in periods:
            if p.get("isDaytime"):
                return float(p["temperature"]), f"{p['temperature']}°F (next daytime)"

        return None, "No daytime period found"
    except Exception as e:
        return None, f"NOAA error: {e}"


def get_open_meteo_forecast(city_config):
    """Get Open-Meteo forecast for international cities. Returns (temp_c, note)."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":          city_config["latitude"],
                "longitude":         city_config["longitude"],
                "daily":             "temperature_2m_max",
                "temperature_unit":  "celsius",
                "timezone":          city_config["timezone"],
                "forecast_days":     2,
            },
            timeout=15
        )
        if r.status_code != 200:
            return None, f"Open-Meteo HTTP {r.status_code}"

        data  = r.json()
        times = data["daily"]["time"]
        temps = data["daily"]["temperature_2m_max"]

        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for t, temp in zip(times, temps):
            if t == today_str and temp is not None:
                return float(temp), f"{temp}°C (Open-Meteo)"

        # Fallback: first available
        if temps and temps[0] is not None:
            return float(temps[0]), f"{temps[0]}°C (Open-Meteo fallback)"

        return None, "No forecast available"
    except Exception as e:
        return None, f"Open-Meteo error: {e}"


def get_forecast(city, city_config):
    """Route to correct forecast source based on city config."""
    if city_config["forecast_source"] == "noaa":
        return get_noaa_forecast(city_config)
    elif city_config["forecast_source"] == "open_meteo":
        return get_open_meteo_forecast(city_config)
    return None, f"Unknown forecast source for {city}"


# ── Probability Model ─────────────────────────────────────────────────────────

def normal_cdf(x, mu, sigma):
    z = (x - mu) / (sigma if sigma > 0 else 1.0)
    return 0.5 * (1 + math.erf(z / (2 ** 0.5)))


def calc_range_probability(target_low, target_high, market_type,
                           forecast, mean_delta, std):
    """
    Calculate true probability for a range given forecast + error model.
    Works for both °F (Chicago) and °C (London) — same math, different scale.
    """
    adjusted = forecast - mean_delta

    if market_type == "range":
        prob = normal_cdf(target_high + 0.5, adjusted, std) - \
               normal_cdf(target_low  - 0.5, adjusted, std)
    elif market_type == "exact":
        # Exact degree market (London) — probability within ±0.5
        prob = normal_cdf(target_high + 0.5, adjusted, std) - \
               normal_cdf(target_low  - 0.5, adjusted, std)
    elif market_type == "above":
        prob = 1.0 - normal_cdf(target_low - 0.5, adjusted, std)
    elif market_type == "below":
        prob = normal_cdf(target_high + 0.5, adjusted, std)
    else:
        prob = 0.05

    return round(max(0.001, min(0.999, prob)), 4)


def get_error_stats(city):
    """Get historical forecast error stats from DB."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""SELECT forecast_f - actual_f as delta
                     FROM noaa_forecasts
                     WHERE city=%s AND actual_f IS NOT NULL
                     ORDER BY date DESC LIMIT 30""", (city,))
        rows = c.fetchall()
        conn.close()

        if not rows or len(rows) < 3:
            # Defaults based on observed accuracy:
            # Chicago NOAA: tends to run ~2F high, std ~3F
            # London Open-Meteo: very accurate, ~0.5C bias, std ~1.5C
            if city == "London":
                return 0.5, 1.5, len(rows)
            return 2.0, 3.0, len(rows)

        deltas = [r["delta"] for r in rows]
        mean   = sum(deltas) / len(deltas)
        var    = sum((d - mean)**2 for d in deltas) / len(deltas)
        std    = max(0.5, var**0.5)
        return mean, std, len(deltas)

    except Exception as e:
        print(f"[ERROR STATS ERR] {e}")
        return 0.5 if city == "London" else 2.0, 1.5 if city == "London" else 3.0, 0


def record_forecast(city, date_str, forecast):
    """Save forecast to DB for error model building."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""INSERT INTO noaa_forecasts (city, date, forecast_f, recorded_at)
                     VALUES (%s,%s,%s,%s)
                     ON CONFLICT (city, date) DO UPDATE SET
                         forecast_f  = EXCLUDED.forecast_f,
                         recorded_at = EXCLUDED.recorded_at""",
                  (city, date_str, forecast, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[RECORD FORECAST ERR] {e}")


def get_open_markets(city, target_date):
    """Get all open markets for a city on target_date."""
    conn     = get_conn()
    c        = conn.cursor()
    date_dt  = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    ts_start = int(date_dt.timestamp())
    ts_end   = int((date_dt + timedelta(days=1)).timestamp())

    c.execute("""SELECT id, question, target_low, target_high,
                        market_type, unit, last_trade_price
                 FROM markets
                 WHERE city=%s
                   AND resolved_at >= %s AND resolved_at < %s
                   AND outcome IS NULL""",
              (city, ts_start, ts_end))
    markets = [dict(r) for r in c.fetchall()]
    conn.close()
    return markets


def get_live_price(market_id, stored_price):
    """Get live price from Polymarket API, fall back to stored."""
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


# ── Main Scanner ──────────────────────────────────────────────────────────────

def scan_signals(target_date=None):
    """
    Scan all cities for trading signals.
    Returns (signals_list, log_string).
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    signals = []
    log     = []
    log.append(f"=== SIGNAL SCAN {target_date} ===")

    for city, city_config in CITIES.items():
        log.append(f"\n[{city}]")

        # Step 1: Get forecast
        forecast, note = get_forecast(city, city_config)
        unit           = city_config["unit"]
        log.append(f"  Forecast: {note}")

        if not forecast:
            log.append("  SKIP: No forecast")
            continue

        # Record forecast for error model
        record_forecast(city, target_date, forecast)

        # Step 2: Error stats
        mean_delta, std, n = get_error_stats(city)
        adjusted = forecast - mean_delta
        log.append(f"  Error model: bias={mean_delta:+.2f}°{unit} std={std:.2f}°{unit} n={n}")
        log.append(f"  Adjusted:    {adjusted:.1f}°{unit}")

        # Step 3: Get open markets
        markets = get_open_markets(city, target_date)
        log.append(f"  Open markets: {len(markets)}")
        if not markets:
            log.append("  SKIP: No markets in DB")
            continue

        # Step 4: Score all ranges
        scored = []
        for m in markets:
            true_prob = calc_range_probability(
                m["target_low"], m["target_high"], m["market_type"],
                forecast, mean_delta, std
            )
            price = get_live_price(m["id"], m["last_trade_price"])
            if not price:
                continue

            edge = true_prob - price
            ev   = true_prob * (1.0 / price) - 1.0

            scored.append({
                "city":        city,
                "market_id":   m["id"],
                "question":    m["question"],
                "forecast_f":  forecast,
                "target_low":  m["target_low"],
                "target_high": m["target_high"],
                "market_type": m["market_type"],
                "unit":        unit,
                "entry_price": price,
                "true_prob":   true_prob,
                "edge":        round(edge, 4),
                "ev":          round(ev, 4),
                "reasoning":   (f"Forecast={forecast}°{unit} adjusted={adjusted:.1f}°{unit} "
                               f"true_prob={true_prob:.3f} price={price:.4f} "
                               f"edge={edge:.3f} ev={ev:.2f}x"),
            })

        # Log all ranges
        log.append(f"  {'Range':<20} {'TrueProb':>9} {'Price':>7} {'Edge':>7} {'EV':>8}")
        for s in sorted(scored, key=lambda x: x["target_low"]):
            flag = " ← BET" if s["edge"] >= MIN_EDGE else ""
            q    = s["question"][-25:] if len(s["question"]) > 25 else s["question"]
            log.append(f"  {q:<20} {s['true_prob']:>9.3f} "
                       f"{s['entry_price']:>7.4f} {s['edge']:>7.3f} "
                       f"{s['ev']:>8.2f}x{flag}")

        # Step 5: Top signals by edge
        top = sorted(
            [s for s in scored if s["edge"] >= MIN_EDGE],
            key=lambda x: x["edge"],
            reverse=True
        )[:MAX_SIGNALS_PER_CITY]

        if not top:
            log.append(f"  SKIP: No ranges with edge >= {MIN_EDGE}")
        else:
            log.append(f"  ✅ {len(top)} signals")
            signals.extend(top)

        time.sleep(0.3)

    log.append(f"\nTotal signals: {len(signals)}")
    return signals, "\n".join(log)


if __name__ == '__main__':
    signals, log = scan_signals()
    print(log)
    print(f"\n{len(signals)} signals")
    for s in signals:
        print(f"  {s['city']} | {s['question'][-40:]} | "
              f"price={s['entry_price']} edge={s['edge']} ev={s['ev']}x")
