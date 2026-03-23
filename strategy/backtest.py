"""
backtest.py - Simulate trading against 30 days of resolved Chicago markets.

Uses REAL entry prices from CLOB price history API.
Entry price = price at 7AM Chicago time on the DAY BEFORE resolution.
This matches exactly when our morning session would have placed the bet.

Resolution time = noon UTC (12:00:00Z) per Polymarket rules.
Day-before 7AM Chicago = noon UTC - 29 hours = resolved_at - 104400 seconds.
"""

import math
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from data.database import get_conn

GAMMA_BASE       = "https://gamma-api.polymarket.com"
CLOB_BASE        = "https://clob.polymarket.com"

BET_SIZE         = 10.0
MAX_BETS_PER_DAY = 3
MIN_EDGE         = 0.05
STARTING_CAPITAL = 1000.0
NOAA_MEAN_DELTA  = 2.0
NOAA_STD         = 3.0
ENTRY_HOURS_BEFORE = 29


def normal_cdf(x, mu, sigma):
    z = (x - mu) / (sigma if sigma > 0 else 1.0)
    return 0.5 * (1 + math.erf(z / (2 ** 0.5)))


def calc_range_probability(target_low, target_high, market_type,
                           forecast_f, mean_delta, std):
    adjusted = forecast_f - mean_delta
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
        WHERE m.city = 'Chicago'
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

    result = []
    for date_str, day in sorted(days.items(), reverse=True):
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT max_temp_f FROM wu_temps WHERE city='Chicago' AND date=%s",
                  (date_str,))
        wu_row = c.fetchone()
        conn.close()
        if wu_row and wu_row["max_temp_f"]:
            day["actual_f"] = wu_row["max_temp_f"]
            result.append(day)

    return result


def run_backtest():
    days = get_resolved_days()
    if not days:
        return {"error": "No resolved days with WU temps found"}

    print(f"[BACKTEST] {len(days)} trading days found")

    capital    = STARTING_CAPITAL
    all_trades = []
    log_lines  = [
        f"CHICAGO BACKTEST — {len(days)} trading days",
        f"Entry: {ENTRY_HOURS_BEFORE}h before resolution (7AM day before)",
        f"Bet size: ${BET_SIZE} per signal, max {MAX_BETS_PER_DAY}/day",
        f"Starting capital: ${STARTING_CAPITAL:,.2f}",
        f"NOAA model: mean_delta=+{NOAA_MEAN_DELTA}F, std={NOAA_STD}F",
        ""
    ]

    for i, day in enumerate(days):
        date_str    = day["date"]
        actual_f    = day["actual_f"]
        resolved_at = day["resolved_at"]
        markets     = day["markets"]

        forecast_f = actual_f + NOAA_MEAN_DELTA

        scored = []
        for m in markets:
            true_prob = calc_range_probability(
                m["target_low"], m["target_high"], m["market_type"],
                forecast_f, NOAA_MEAN_DELTA, NOAA_STD
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
                "market_id":   m["id"],
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
                f"  {date_str}: forecast={forecast_f:.0f}F actual={actual_f:.0f}F | "
                f"No signals ({len(scored)} priced)"
            )
            continue

        day_pnl = 0.0
        wins    = 0
        losses  = 0

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
                "forecast":  forecast_f,
                "actual":    actual_f,
            })

        log_lines.append(
            f"  {date_str}: forecast={forecast_f:.0f}F actual={actual_f:.0f}F | "
            f"bets={wins+losses} W={wins} L={losses} | "
            f"day_pnl=${day_pnl:+.2f} | capital=${capital:.2f}"
        )
        print(f"[BACKTEST] {i+1}/{len(days)} {date_str} — "
              f"{wins+losses} bets pnl=${day_pnl:+.2f}")

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
        "",
        "=" * 55,
        "RESULTS",
        "=" * 55,
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
    print(result.get("log", result.get("error", "Unknown error")))
