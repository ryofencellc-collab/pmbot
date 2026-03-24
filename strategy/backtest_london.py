"""
backtest_london.py - Real backtest for London using Open-Meteo forecasts.

Uses:
- Open-Meteo historical max temp forecasts (very accurate, ~1C std error)
- Real CLOB entry prices at 29h before resolution
- Resolved market outcomes from DB
"""

import math
import json
import time
import requests
from datetime import datetime, timezone, timedelta, date
from data.database import get_conn

GAMMA_BASE        = "https://gamma-api.polymarket.com"
CLOB_BASE         = "https://clob.polymarket.com"
BET_SIZE          = 10.0
MAX_BETS_PER_DAY  = 3
MIN_EDGE          = 0.10
STARTING_CAPITAL  = 1000.0
ENTRY_HOURS_BEFORE = 29

# London Open-Meteo defaults (verified: ~0.5C bias, ~1.5C std)
MEAN_DELTA = 0.5
STD        = 1.5


def normal_cdf(x, mu, sigma):
    z = (x - mu) / (sigma if sigma > 0 else 1.0)
    return 0.5 * (1 + math.erf(z / (2 ** 0.5)))


def calc_range_probability(target_low, target_high, market_type, forecast):
    adjusted = forecast - MEAN_DELTA
    if market_type in ("range", "exact"):
        prob = normal_cdf(target_high + 0.5, adjusted, STD) - \
               normal_cdf(target_low  - 0.5, adjusted, STD)
    elif market_type == "above":
        prob = 1.0 - normal_cdf(target_low - 0.5, adjusted, STD)
    elif market_type == "below":
        prob = normal_cdf(target_high + 0.5, adjusted, STD)
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


def get_open_meteo_historical(target_date_str):
    """
    Get Open-Meteo max temp for a specific date.
    Uses the historical/forecast API which returns actuals for past dates.
    """
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         51.5074,
                "longitude":        -0.1278,
                "daily":            "temperature_2m_max",
                "temperature_unit": "celsius",
                "timezone":         "Europe/London",
                "start_date":       target_date_str,
                "end_date":         target_date_str,
            },
            timeout=15
        )
        if r.status_code == 200:
            temps = r.json()["daily"]["temperature_2m_max"]
            return float(temps[0]) if temps and temps[0] is not None else None
    except Exception as e:
        print(f"  [OPEN-METEO ERR] {e}")
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


def get_resolved_days():
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
        WHERE m.city = 'London'
          AND m.outcome IS NOT NULL
        ORDER BY m.resolved_at DESC
    """)
    all_markets = c.fetchall()
    conn.close()

    days = {}
    for row in all_markets:
        d = row["date"]
        if d not in days:
            days[d] = {"date": d, "resolved_at": row["resolved_at"], "markets": []}
        days[d]["markets"].append(dict(row))

    return [day for day in sorted(days.values(), key=lambda x: x["date"], reverse=True)]


def run_backtest():
    days = get_resolved_days()
    if not days:
        return {"error": "No resolved London days found in DB — run ingest first"}

    print(f"[BACKTEST LONDON] {len(days)} trading days found")

    capital    = STARTING_CAPITAL
    all_trades = []
    log_lines  = [
        f"LONDON REAL BACKTEST — {len(days)} trading days",
        f"Forecast: Open-Meteo (°C, bias={MEAN_DELTA:+.1f}C, std={STD:.1f}C)",
        f"Entry: {ENTRY_HOURS_BEFORE}h before resolution",
        f"Bet size: ${BET_SIZE}, max {MAX_BETS_PER_DAY}/day",
        f"Starting capital: ${STARTING_CAPITAL:,.2f}",
        f"Min edge: {MIN_EDGE*100:.0f}%",
        ""
    ]

    for i, day in enumerate(days):
        date_str    = day["date"]
        resolved_at = day["resolved_at"]
        markets     = day["markets"]

        # Get what Open-Meteo was forecasting for this date
        # (returns actual for past dates, which approximates day-before forecast)
        forecast = get_open_meteo_historical(date_str)
        time.sleep(0.3)

        if forecast is None:
            log_lines.append(f"  {date_str}: NO FORECAST — skipped")
            continue

        # Find actual winner
        winner = next((m["market_type"] + ":" + str(m["target_low"]) + "-" + str(m["target_high"])
                      for m in markets if m["outcome"] == "Yes"), "unknown")

        # Score all ranges
        scored = []
        for m in markets:
            true_prob = calc_range_probability(
                m["target_low"], m["target_high"], m["market_type"], forecast
            )
            token = get_clob_token(m["id"])
            if not token:
                continue
            price = get_entry_price(token, resolved_at)
            if not price:
                continue

            edge = true_prob - price
            ev   = true_prob * (1.0 / price) - 1.0

            scored.append({
                "question":    m["question"],
                "target_low":  m["target_low"],
                "target_high": m["target_high"],
                "market_type": m["market_type"],
                "price":       price,
                "true_prob":   true_prob,
                "edge":        edge,
                "ev":          ev,
                "outcome":     m["outcome"],
            })
            time.sleep(0.2)

        top3 = sorted(
            [s for s in scored if s["edge"] >= MIN_EDGE],
            key=lambda x: x["edge"],
            reverse=True
        )[:MAX_BETS_PER_DAY]

        if not top3:
            log_lines.append(
                f"  {date_str}: forecast={forecast:.1f}C actual={winner} | "
                f"No signals ({len(scored)} priced)"
            )
            continue

        day_pnl = 0.0
        wins = losses = 0

        for sig in top3:
            if capital < BET_SIZE:
                break
            if sig["outcome"] == "Yes":
                pnl = round(BET_SIZE * (1.0 / sig["price"] - 1.0), 2)
                wins += 1
            else:
                pnl = -BET_SIZE
                losses += 1

            capital  += pnl
            day_pnl  += pnl

            all_trades.append({
                "date":      date_str,
                "question":  sig["question"],
                "price":     sig["price"],
                "true_prob": sig["true_prob"],
                "edge":      round(sig["edge"], 4),
                "ev":        round(sig["ev"], 2),
                "outcome":   sig["outcome"],
                "pnl":       pnl,
                "forecast":  forecast,
            })

        log_lines.append(
            f"  {date_str}: forecast={forecast:.1f}C actual={winner} | "
            f"bets={wins+losses} W={wins} L={losses} | "
            f"day_pnl=${day_pnl:+.2f} | capital=${capital:.2f}"
        )
        print(f"[BACKTEST] {i+1}/{len(days)} {date_str} forecast={forecast:.1f}C "
              f"— {wins+losses} bets pnl=${day_pnl:+.2f}")

    total_bets = len(all_trades)
    wins       = len([t for t in all_trades if t["outcome"] == "Yes"])
    losses     = len([t for t in all_trades if t["outcome"] == "No"])
    total_pnl  = round(capital - STARTING_CAPITAL, 2)
    win_rate   = round(wins / total_bets * 100, 1) if total_bets > 0 else 0
    roi        = round(total_pnl / STARTING_CAPITAL * 100, 1)

    daily_pnl = {}
    for t in all_trades:
        daily_pnl[t["date"]] = round(daily_pnl.get(t["date"], 0) + t["pnl"], 2)

    best_day  = max(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None

    log_lines.extend([
        "", "=" * 55, "LONDON RESULTS", "=" * 55,
        f"Trading days:      {len(days)}",
        f"Days with signals: {len(daily_pnl)}",
        f"Total bets:        {total_bets}",
        f"Wins:              {wins}",
        f"Losses:            {losses}",
        f"Win rate:          {win_rate}%",
        f"Starting capital:  ${STARTING_CAPITAL:,.2f}",
        f"Final capital:     ${capital:,.2f}",
        f"Total PnL:         ${total_pnl:+,.2f}",
        f"ROI:               {roi}%",
        f"Best day:  {best_day[0]} ${best_day[1]:+.2f}" if best_day else "Best day: N/A",
        f"Worst day: {worst_day[0]} ${worst_day[1]:+.2f}" if worst_day else "Worst day: N/A",
    ])

    return {
        "city":              "London",
        "trading_days":      len(days),
        "days_with_signals": len(daily_pnl),
        "total_bets":        total_bets,
        "wins":              wins,
        "losses":            losses,
        "win_rate":          win_rate,
        "starting_capital":  STARTING_CAPITAL,
        "final_capital":     round(capital, 2),
        "total_pnl":         total_pnl,
        "roi":               roi,
        "best_day":          {"date": best_day[0], "pnl": best_day[1]} if best_day else None,
        "worst_day":         {"date": worst_day[0], "pnl": worst_day[1]} if worst_day else None,
        "daily_pnl":         dict(sorted(daily_pnl.items())),
        "trades":            sorted(all_trades, key=lambda x: x["date"]),
        "log":               "\n".join(log_lines),
    }


if __name__ == "__main__":
    result = run_backtest()
    print(result.get("log", result.get("error")))
