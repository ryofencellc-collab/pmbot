"""
signals.py - Core signal engine for all approved cities.

APPROVED cities (confirmed positive ROI from 30-day real backtest):
London, NYC, Toronto, Dallas, Atlanta, Seattle, Paris, Sao Paulo

Strategy:
- Get Open-Meteo forecast for each city
- Calculate true probability across all 11 ranges
- Find top 3 most mispriced ranges per city (edge >= 10%)
- Also find NO opportunities (ranges priced > 80% that forecast says are wrong)
"""

import math
import requests
import time
from datetime import datetime, timezone, timedelta
from data.database import get_conn

# ── Approved Cities ───────────────────────────────────────────────────────────
# Verified Open-Meteo accuracy from real data 7-day comparison
# Format: slug, lat, lon, timezone, unit (C or F), temp_unit for API

# APPROVED CITIES — confirmed positive ROI from 30-day real data backtest
# London +8% | NYC +4% | Sao Paulo +3.8% | Toronto +1.7% | Seattle +1.7%
# Dallas +1.2% | Atlanta +0.6% | Paris +0.4%
# REJECTED: Miami, Buenos Aires, Seoul, Tokyo, Tel Aviv, Warsaw, Beijing,
#            Taipei, Shanghai, Munich, Milan, Madrid, Singapore
CITIES = {
    "London":    {"slug": "london",    "lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",         "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5, "std": 1.5},
    "NYC":       {"slug": "nyc",       "lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",      "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 4.0, "std": 3.0},
    "Toronto":   {"slug": "toronto",   "lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",       "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5, "std": 2.0},
    "Dallas":    {"slug": "dallas",    "lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",       "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0, "std": 2.5},
    "Atlanta":   {"slug": "atlanta",   "lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",      "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0, "std": 2.5},
    "Seattle":   {"slug": "seattle",   "lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",   "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0, "std": 2.5},
    "Paris":     {"slug": "paris",     "lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",          "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5, "std": 1.5},
    "Sao Paulo": {"slug": "sao-paulo", "lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",     "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5, "std": 1.5},
}

MAX_SIGNALS_PER_CITY = 3
MIN_EDGE             = 0.10
NO_MIN_PRICE         = 0.75   # Buy NO when market is priced above this
NO_MIN_EDGE          = 0.10   # Minimum edge for NO bets too


# ── Math ──────────────────────────────────────────────────────────────────────

def normal_cdf(x, mu, sigma):
    z = (x - mu) / (sigma if sigma > 0 else 1.0)
    return 0.5 * (1 + math.erf(z / (2 ** 0.5)))


def calc_true_prob(target_low, target_high, market_type, forecast, mean_delta, std):
    adjusted = forecast - mean_delta
    if market_type in ("range", "exact"):
        prob = normal_cdf(target_high + 0.5, adjusted, std) - \
               normal_cdf(target_low  - 0.5, adjusted, std)
    elif market_type == "above":
        prob = 1.0 - normal_cdf(target_low - 0.5, adjusted, std)
    elif market_type == "below":
        prob = normal_cdf(target_high + 0.5, adjusted, std)
    else:
        prob = 0.05
    return round(max(0.001, min(0.999, prob)), 4)


# ── Forecast ──────────────────────────────────────────────────────────────────

def get_forecast(city_config):
    """Get Open-Meteo forecast for a city. Returns (temp, note) or (None, error)."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         city_config["lat"],
                "longitude":        city_config["lon"],
                "daily":            "temperature_2m_max",
                "temperature_unit": city_config["temp_unit"],
                "timezone":         city_config["tz"],
                "forecast_days":    2,
            },
            timeout=15
        )
        if r.status_code != 200:
            return None, f"Open-Meteo HTTP {r.status_code}"

        data      = r.json()
        times     = data["daily"]["time"]
        temps     = data["daily"]["temperature_2m_max"]
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for t, temp in zip(times, temps):
            if t == today_str and temp is not None:
                return float(temp), f"{temp}°{city_config['unit']} (Open-Meteo)"

        if temps and temps[0] is not None:
            return float(temps[0]), f"{temps[0]}°{city_config['unit']} (Open-Meteo fallback)"

        return None, "No forecast available"
    except Exception as e:
        return None, f"Open-Meteo error: {e}"


def record_forecast(city, date_str, forecast):
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


def already_bet_today(market_id, today):
    """Check if we already have a trade for this market today."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM paper_trades WHERE market_id=%s AND trade_date=%s",
                  (str(market_id), today))
        count = c.fetchone()["count"]
        conn.close()
        return count > 0
    except Exception:
        return False


# ── Main Scanner ──────────────────────────────────────────────────────────────

def scan_signals(target_date=None):
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    signals = []
    log     = []
    log.append(f"=== SIGNAL SCAN {target_date} ===")

    for city, config in CITIES.items():
        log.append(f"\n[{city}]")

        forecast, note = get_forecast(config)
        unit           = config["unit"]
        log.append(f"  Forecast: {note}")

        if not forecast:
            log.append("  SKIP: No forecast")
            continue

        record_forecast(city, target_date, forecast)

        mean_delta = config["mean_delta"]
        std        = config["std"]
        adjusted   = forecast - mean_delta
        log.append(f"  Bias={mean_delta:+.1f}°{unit} std={std:.1f}°{unit} adjusted={adjusted:.1f}°{unit}")

        markets = get_open_markets(city, target_date)
        log.append(f"  Open markets: {len(markets)}")
        if not markets:
            log.append("  SKIP: No markets in DB")
            continue

        scored = []
        for m in markets:
            # Skip if already bet today
            if already_bet_today(m["id"], target_date):
                continue

            true_prob = calc_true_prob(
                m["target_low"], m["target_high"], m["market_type"],
                forecast, mean_delta, std
            )
            price = get_live_price(m["id"], m["last_trade_price"])
            if not price:
                continue

            yes_edge = true_prob - price
            no_edge  = (1 - true_prob) - (1 - price)  # edge on NO side

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
                "yes_edge":    round(yes_edge, 4),
                "no_edge":     round(no_edge, 4),
                "ev":          round(true_prob * (1.0 / price) - 1.0, 4),
                "edge":        round(yes_edge, 4),
                "side":        "YES",
                "reasoning":   (f"Forecast={forecast}°{unit} adj={adjusted:.1f}°{unit} "
                               f"prob={true_prob:.3f} price={price:.4f} edge={yes_edge:.3f}"),
            })

        # YES signals — top 3 by edge
        yes_signals = sorted(
            [s for s in scored if s["yes_edge"] >= MIN_EDGE],
            key=lambda x: x["yes_edge"], reverse=True
        )[:MAX_SIGNALS_PER_CITY]

        # NO signals — ranges priced > 75% where our prob says < 50%
        no_signals = []
        for s in scored:
            if s["entry_price"] >= NO_MIN_PRICE and s["true_prob"] < 0.50:
                no_sig = s.copy()
                no_sig["side"]        = "NO"
                no_sig["edge"]        = round(s["no_edge"], 4)
                no_sig["entry_price"] = round(1 - s["entry_price"], 4)  # NO price
                no_sig["reasoning"]   = (f"NO BET: Forecast={forecast}°{unit} "
                                        f"true_prob={s['true_prob']:.3f} "
                                        f"market_yes={s['entry_price']:.3f} "
                                        f"(overpriced) no_edge={s['no_edge']:.3f}")
                if no_sig["edge"] >= NO_MIN_EDGE:
                    no_signals.append(no_sig)

        no_signals = sorted(no_signals, key=lambda x: x["edge"], reverse=True)[:MAX_SIGNALS_PER_CITY]

        city_signals = yes_signals + no_signals

        if city_signals:
            log.append(f"  ✅ {len(yes_signals)} YES + {len(no_signals)} NO signals")
            signals.extend(city_signals)
        else:
            log.append(f"  No signals")

        time.sleep(0.2)

    log.append(f"\nTotal signals: {len(signals)}")
    return signals, "\n".join(log)


if __name__ == '__main__':
    signals, log = scan_signals()
    print(log)
    print(f"\n{len(signals)} signals")
    for s in signals:
        print(f"  {s['city']} | {s['side']} | {s['question'][-40:]} | "
              f"price={s['entry_price']} edge={s['edge']} ev={s['ev']}x")
