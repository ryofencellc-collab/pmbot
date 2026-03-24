"""
backtest_all.py - Real backtest for all 21 approved cities.
Uses Open-Meteo historical forecasts + real CLOB entry prices.
Tests both YES and NO betting strategies.
"""

import math
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from data.database import get_conn

GAMMA_BASE         = "https://gamma-api.polymarket.com"
CLOB_BASE          = "https://clob.polymarket.com"
BET_SIZE           = 10.0
MAX_BETS_PER_DAY   = 6   # 3 YES + 3 NO per city
MIN_EDGE           = 0.10
NO_MIN_PRICE       = 0.75
STARTING_CAPITAL   = 10000.0
ENTRY_HOURS_BEFORE = 29

CITY_CONFIGS = {
    "London":       {"lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "NYC":          {"lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 4.0,  "std": 3.0},
    "Toronto":      {"lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",                "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 2.0},
    "Seoul":        {"lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",                     "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "Dallas":       {"lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",                "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0,  "std": 2.5},
    "Atlanta":      {"lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0,  "std": 2.5},
    "Miami":        {"lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 2.5,  "std": 3.0},
    "Seattle":      {"lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",            "unit": "F", "temp_unit": "fahrenheit", "mean_delta": 1.0,  "std": 2.5},
    "Paris":        {"lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",                   "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "Tokyo":        {"lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",                     "unit": "C", "temp_unit": "celsius",    "mean_delta": 1.0,  "std": 2.0},
    "Singapore":    {"lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",                 "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.0},
    "Madrid":       {"lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.3,  "std": 1.0},
    "Warsaw":       {"lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "Beijing":      {"lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.0},
    "Shanghai":     {"lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 1.0,  "std": 2.0},
    "Taipei":       {"lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",                    "unit": "C", "temp_unit": "celsius",    "mean_delta": 1.0,  "std": 2.0},
    "Tel Aviv":     {"lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",                 "unit": "C", "temp_unit": "celsius",    "mean_delta": 1.0,  "std": 1.5},
    "Sao Paulo":    {"lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",              "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "Milan":        {"lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",                    "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.0},
    "Munich":       {"lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",                  "unit": "C", "temp_unit": "celsius",    "mean_delta": 0.5,  "std": 1.5},
    "Buenos Aires": {"lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires", "unit": "C", "temp_unit": "celsius",    "mean_delta": 2.0,  "std": 2.0},
}


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


def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20,
                             headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30)
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(2 ** i)
    return None


def get_forecast(config, date_str):
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         config["lat"],
                "longitude":        config["lon"],
                "daily":            "temperature_2m_max",
                "temperature_unit": config["temp_unit"],
                "timezone":         config["tz"],
                "start_date":       date_str,
                "end_date":         date_str,
            },
            timeout=15
        )
        if r.status_code == 200:
            temps = r.json()["daily"]["temperature_2m_max"]
            return float(temps[0]) if temps and temps[0] is not None else None
    except Exception as e:
        print(f"  [FORECAST ERR] {e}")
    return None


def get_clob_token(market_id):
    mdata = safe_get(f"{GAMMA_BASE}/markets/{market_id}")
    if not mdata:
        return None
    tokens = mdata.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            return None
    return tokens[0] if tokens else None


def get_entry_price(clob_token, resolved_at):
    target_ts = resolved_at - (ENTRY_HOURS_BEFORE * 3600)
    hist = safe_get(f"{CLOB_BASE}/prices-history",
                    params={"market": clob_token, "interval": "all", "fidelity": 60})
    if not hist or not hist.get("history"):
        return None
    valid = [p for p in hist["history"] if p["t"] <= target_ts]
    if not valid:
        return None
    price = valid[-1]["p"]
    return float(price) if price and float(price) > 0 else None


def get_resolved_days(city):
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""
        SELECT
            TO_CHAR(TO_TIMESTAMP(m.resolved_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
            m.resolved_at,
            m.id,
            m.question,
            m.target_low,
            m.target_high,
            m.market_type,
            m.outcome
        FROM markets m
        WHERE m.city = %s AND m.outcome IS NOT NULL
        ORDER BY m.resolved_at DESC
    """, (city,))
    all_markets = c.fetchall()
    conn.close()

    days = {}
    for row in all_markets:
        d = row["date"]
        if d not in days:
            days[d] = {"date": d, "resolved_at": row["resolved_at"], "markets": []}
        days[d]["markets"].append(dict(row))

    return sorted(days.values(), key=lambda x: x["date"], reverse=True)


def run_city_backtest(city):
    config = CITY_CONFIGS[city]
    days   = get_resolved_days(city)

    if not days:
        return {
            "city": city, "error": f"No resolved {city} days in DB",
            "roi": 0, "win_rate": 0, "total_bets": 0,
            "final_capital": STARTING_CAPITAL, "trading_days": 0
        }

    print(f"\n[{city}] {len(days)} days")

    capital    = STARTING_CAPITAL
    all_trades = []
    log_lines  = [
        f"{city.upper()} BACKTEST — {len(days)} days",
        f"Forecast: Open-Meteo bias={config['mean_delta']:+.1f}°{config['unit']} std={config['std']:.1f}°{config['unit']}",
        f"Strategy: YES (edge≥10%) + NO (yes_price≥75%, true_prob<50%)",
        ""
    ]

    for day in days:
        date_str    = day["date"]
        resolved_at = day["resolved_at"]
        markets     = day["markets"]

        forecast = get_forecast(config, date_str)
        time.sleep(0.2)

        if forecast is None:
            continue

        winner = next((f"{m['market_type']}:{m['target_low']}-{m['target_high']}"
                      for m in markets if m["outcome"] == "Yes"), "unknown")

        scored = []
        for m in markets:
            true_prob = calc_true_prob(
                m["target_low"], m["target_high"], m["market_type"],
                forecast, config["mean_delta"], config["std"]
            )
            token = get_clob_token(m["id"])
            if not token:
                continue
            yes_price = get_entry_price(token, resolved_at)
            if not yes_price:
                continue

            yes_edge = true_prob - yes_price
            no_price = round(1 - yes_price, 4)
            no_edge  = (1 - true_prob) - no_price

            scored.append({
                "question":    m["question"],
                "target_low":  m["target_low"],
                "target_high": m["target_high"],
                "market_type": m["market_type"],
                "yes_price":   yes_price,
                "no_price":    no_price,
                "true_prob":   true_prob,
                "yes_edge":    yes_edge,
                "no_edge":     no_edge,
                "outcome":     m["outcome"],
            })
            time.sleep(0.15)

        # YES signals
        yes_signals = sorted(
            [s for s in scored if s["yes_edge"] >= MIN_EDGE],
            key=lambda x: x["yes_edge"], reverse=True
        )[:3]

        # NO signals
        no_signals = sorted(
            [s for s in scored if s["yes_price"] >= NO_MIN_PRICE
             and s["true_prob"] < 0.50 and s["no_edge"] >= MIN_EDGE],
            key=lambda x: x["no_edge"], reverse=True
        )[:3]

        all_signals = yes_signals + no_signals
        if not all_signals:
            continue

        day_pnl = wins = losses = 0

        for sig in all_signals:
            if capital < BET_SIZE:
                break

            is_yes   = sig in yes_signals
            outcome  = sig["outcome"]
            price    = sig["yes_price"] if is_yes else sig["no_price"]
            side     = "YES" if is_yes else "NO"

            if is_yes:
                won = outcome == "Yes"
            else:
                won = outcome == "No"

            if won:
                pnl = round(BET_SIZE * (1.0 / price - 1.0), 2)
                wins += 1
            else:
                pnl = -BET_SIZE
                losses += 1

            capital  += pnl
            day_pnl  += pnl

            all_trades.append({
                "date":      date_str,
                "city":      city,
                "question":  sig["question"],
                "side":      side,
                "price":     price,
                "true_prob": sig["true_prob"],
                "edge":      round(sig["yes_edge"] if is_yes else sig["no_edge"], 4),
                "outcome":   outcome,
                "won":       won,
                "pnl":       pnl,
                "forecast":  forecast,
            })

        log_lines.append(
            f"  {date_str}: {forecast:.1f}°{config['unit']} | "
            f"YES={len(yes_signals)} NO={len(no_signals)} | "
            f"W={wins} L={losses} pnl=${day_pnl:+.2f} capital=${capital:.2f}"
        )
        print(f"  {date_str}: forecast={forecast:.1f} — W={wins} L={losses} pnl=${day_pnl:+.2f}")

    total_bets = len(all_trades)
    wins_total = len([t for t in all_trades if t["won"]])
    total_pnl  = round(capital - STARTING_CAPITAL, 2)
    win_rate   = round(wins_total / total_bets * 100, 1) if total_bets > 0 else 0
    roi        = round(total_pnl / STARTING_CAPITAL * 100, 1)

    daily_pnl = {}
    for t in all_trades:
        daily_pnl[t["date"]] = round(daily_pnl.get(t["date"], 0) + t["pnl"], 2)

    best_day  = max(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None

    log_lines.extend([
        "", "=" * 50,
        f"RESULTS: {city.upper()}",
        "=" * 50,
        f"Trading days:  {len(days)}",
        f"Days w/signals:{len(daily_pnl)}",
        f"Total bets:    {total_bets}",
        f"Wins:          {wins_total}",
        f"Win rate:      {win_rate}%",
        f"ROI:           {roi}%",
        f"Final capital: ${capital:,.2f}",
        f"Best day:  {best_day[0]} ${best_day[1]:+.2f}" if best_day else "",
        f"Worst day: {worst_day[0]} ${worst_day[1]:+.2f}" if worst_day else "",
    ])

    return {
        "city":           city,
        "trading_days":   len(days),
        "days_w_signals": len(daily_pnl),
        "total_bets":     total_bets,
        "wins":           wins_total,
        "win_rate":       win_rate,
        "starting_capital": STARTING_CAPITAL,
        "final_capital":  round(capital, 2),
        "total_pnl":      total_pnl,
        "roi":            roi,
        "best_day":       {"date": best_day[0], "pnl": best_day[1]} if best_day else None,
        "worst_day":      {"date": worst_day[0], "pnl": worst_day[1]} if worst_day else None,
        "daily_pnl":      dict(sorted(daily_pnl.items())),
        "trades":         sorted(all_trades, key=lambda x: x["date"]),
        "log":            "\n".join(log_lines),
    }


def run_all_backtests():
    results  = {}
    approved = []
    rejected = []

    for city in CITY_CONFIGS.keys():
        print(f"\n{'='*50}\n  {city.upper()}\n{'='*50}")
        r = run_city_backtest(city)
        results[city] = r
        if "error" in r:
            print(f"  ERROR: {r['error']}")
        elif r["roi"] > 0 and r["total_bets"] >= 5:
            approved.append(city)
        else:
            rejected.append(city)

    print(f"\n{'='*60}")
    print(f"  FINAL SUMMARY — ALL CITIES")
    print(f"{'='*60}")
    print(f"  {'City':<15} {'ROI':>8} {'WinRate':>8} {'Bets':>6} {'Decision':>10}")
    print(f"  {'-'*55}")

    for city, r in sorted(results.items(), key=lambda x: x[1].get("roi", -999), reverse=True):
        if "error" in r:
            print(f"  {city:<15} {'N/A':>8} {'N/A':>8} {'0':>6} {'❌ NO DATA':>10}")
        else:
            decision = "✅ TRADE" if city in approved else "❌ SKIP"
            print(f"  {city:<15} {r['roi']:>7.1f}% {r['win_rate']:>7.1f}% {r['total_bets']:>6} {decision:>10}")

    print(f"\n  ✅ APPROVED ({len(approved)}): {', '.join(approved)}")
    print(f"  ❌ REJECTED ({len(rejected)}): {', '.join(rejected)}")
    print(f"{'='*60}")

    return {"results": results, "approved": approved, "rejected": rejected}


if __name__ == "__main__":
    run_all_backtests()
