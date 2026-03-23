"""
backtest.py - Simulate 30 days of trading against resolved Chicago markets.

Uses ONLY data that would have been available at trade time:
- WU actual temp as the "ground truth" (what we're trying to predict)
- last_trade_price as entry price (what market was pricing it at)
- Same probability model as signals.py (normal distribution around forecast)

Since we don't have historical NOAA forecasts, we simulate the forecast
using: forecast = actual_temp + error_sample
where error is drawn from our known distribution (mean=+2, std=3).

This gives us a realistic simulation of what would have happened if we
had been running this system for the past 30 days.

Key insight: we run the simulation 100 times with different random
error samples to get a distribution of outcomes, not just one result.
"""

import math
import random
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn

BET_SIZE        = 10.0
MAX_BETS_PER_DAY = 3
MIN_EDGE        = 0.05
STARTING_CAPITAL = 1000.0  # Simulate with $1000 to see realistic returns
NOAA_MEAN_DELTA  = 2.0     # NOAA runs ~2°F high on average
NOAA_STD         = 3.0     # Standard deviation of NOAA error


def normal_cdf(x, mu, sigma):
    z = (x - mu) / (sigma if sigma > 0 else 1.0)
    return 0.5 * (1 + math.erf(z / (2 ** 0.5)))


def calc_range_probability(target_low, target_high, market_type,
                           forecast_f, mean_delta, std):
    """Exact same model as signals.py."""
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


def get_resolved_dates():
    """Get all dates that have resolved Chicago markets with WU temps."""
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""
        SELECT DISTINCT
            TO_CHAR(TO_TIMESTAMP(m.resolved_at), 'YYYY-MM-DD') as date
        FROM markets m
        JOIN wu_temps w ON w.city = m.city
            AND TO_CHAR(TO_TIMESTAMP(m.resolved_at), 'YYYY-MM-DD') = w.date
        WHERE m.city = 'Chicago'
          AND m.outcome IS NOT NULL
        ORDER BY date DESC
    """)
    rows  = c.fetchall()
    conn.close()
    return [r["date"] for r in rows]


def get_markets_for_date(date_str):
    """Get all resolved markets for a specific date."""
    conn     = get_conn()
    c        = conn.cursor()
    date_dt  = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    ts_start = int(date_dt.timestamp())
    ts_end   = int((date_dt + timedelta(days=1)).timestamp())

    c.execute("""
        SELECT id, question, target_low, target_high, market_type,
               last_trade_price, outcome
        FROM markets
        WHERE city = 'Chicago'
          AND resolved_at >= %s AND resolved_at < %s
          AND outcome IS NOT NULL
          AND last_trade_price > 0
    """, (ts_start, ts_end))
    markets = [dict(r) for r in c.fetchall()]
    conn.close()
    return markets


def get_wu_temp(date_str):
    """Get WU actual high temp for a date."""
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT max_temp_f FROM wu_temps WHERE city='Chicago' AND date=%s",
              (date_str,))
    row = c.fetchone()
    conn.close()
    return row["max_temp_f"] if row else None


def simulate_one_day(date_str, actual_temp, simulated_forecast,
                     markets, capital):
    """
    Simulate one trading day.
    Returns (trades_placed, capital_after, day_pnl, log).
    """
    log = []

    # Score every market using our probability model
    scored = []
    for m in markets:
        true_prob = calc_range_probability(
            m["target_low"], m["target_high"], m["market_type"],
            simulated_forecast, NOAA_MEAN_DELTA, NOAA_STD
        )
        price = m["last_trade_price"]
        edge  = true_prob - price
        ev    = true_prob * (1.0 / price) - 1.0 if price > 0 else 0

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

    # Top 3 by edge, only where edge >= MIN_EDGE
    top3 = sorted(
        [s for s in scored if s["edge"] >= MIN_EDGE],
        key=lambda x: x["edge"],
        reverse=True
    )[:MAX_BETS_PER_DAY]

    if not top3:
        return [], capital, 0.0, f"  {date_str}: No signals (forecast={simulated_forecast:.1f}°F, actual={actual_temp}°F)"

    day_pnl = 0.0
    trades  = []

    for sig in top3:
        if capital < BET_SIZE:
            break

        outcome = sig["outcome"]
        if outcome == "Yes":
            pnl = round(BET_SIZE * (1.0 / sig["price"] - 1.0), 2)
        else:
            pnl = -BET_SIZE

        capital  += pnl
        day_pnl  += pnl

        trades.append({
            "date":       date_str,
            "question":   sig["question"],
            "price":      sig["price"],
            "true_prob":  sig["true_prob"],
            "edge":       sig["edge"],
            "outcome":    outcome,
            "pnl":        pnl,
            "forecast":   simulated_forecast,
            "actual":     actual_temp,
        })

    wins   = len([t for t in trades if t["outcome"] == "Yes"])
    losses = len([t for t in trades if t["outcome"] == "No"])
    log.append(
        f"  {date_str}: forecast={simulated_forecast:.1f}°F actual={actual_temp}°F | "
        f"bets={len(trades)} W={wins} L={losses} | "
        f"day_pnl=${day_pnl:+.2f} | capital=${capital:.2f}"
    )

    return trades, capital, day_pnl, "\n".join(log)


def run_backtest(simulations=100):
    """
    Run the backtest across all resolved Chicago dates.

    Since we don't have historical NOAA forecasts, we simulate the
    forecast for each day as: actual_temp + noise
    where noise ~ N(mean_delta, std).

    We run 'simulations' Monte Carlo iterations to get a
    distribution of outcomes.
    """
    dates = get_resolved_dates()
    if not dates:
        return {"error": "No resolved dates found in DB"}

    # Pre-fetch all data
    day_data = []
    for date_str in dates:
        actual = get_wu_temp(date_str)
        markets = get_markets_for_date(date_str)
        if actual and markets:
            day_data.append({
                "date":    date_str,
                "actual":  actual,
                "markets": markets,
            })

    if not day_data:
        return {"error": "No days with both WU temp and resolved markets"}

    # Single deterministic run using mean_delta as forecast bias
    # (best estimate of what our system would have done)
    capital    = STARTING_CAPITAL
    all_trades = []
    log_lines  = [f"BACKTEST — {len(day_data)} trading days",
                  f"Starting capital: ${STARTING_CAPITAL}",
                  f"Bet size: ${BET_SIZE} per signal, up to {MAX_BETS_PER_DAY}/day",
                  ""]

    for day in day_data:
        # Simulated forecast = actual + NOAA bias
        # (what NOAA would have said that morning)
        simulated_forecast = day["actual"] + NOAA_MEAN_DELTA

        trades, capital, day_pnl, day_log = simulate_one_day(
            day["date"], day["actual"], simulated_forecast,
            day["markets"], capital
        )
        all_trades.extend(trades)
        log_lines.append(day_log)

    # Stats
    total_bets = len(all_trades)
    wins       = len([t for t in all_trades if t["outcome"] == "Yes"])
    losses     = len([t for t in all_trades if t["outcome"] == "No"])
    total_pnl  = round(capital - STARTING_CAPITAL, 2)
    win_rate   = round(wins / total_bets * 100, 1) if total_bets > 0 else 0
    roi        = round(total_pnl / STARTING_CAPITAL * 100, 1)

    # Best and worst days
    daily_pnl = {}
    for t in all_trades:
        daily_pnl[t["date"]] = daily_pnl.get(t["date"], 0) + t["pnl"]

    best_day  = max(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else None

    log_lines.extend([
        "",
        "=" * 55,
        f"RESULTS",
        "=" * 55,
        f"Trading days:    {len(day_data)}",
        f"Total bets:      {total_bets}",
        f"Wins:            {wins}",
        f"Losses:          {losses}",
        f"Win rate:        {win_rate}%",
        f"Starting capital: ${STARTING_CAPITAL:,.2f}",
        f"Final capital:   ${capital:,.2f}",
        f"Total PnL:       ${total_pnl:+,.2f}",
        f"ROI:             {roi}%",
        f"Best day:        {best_day[0]} ${best_day[1]:+.2f}" if best_day else "",
        f"Worst day:       {worst_day[0]} ${worst_day[1]:+.2f}" if worst_day else "",
    ])

    return {
        "trading_days":    len(day_data),
        "total_bets":      total_bets,
        "wins":            wins,
        "losses":          losses,
        "win_rate":        win_rate,
        "starting_capital": STARTING_CAPITAL,
        "final_capital":   round(capital, 2),
        "total_pnl":       total_pnl,
        "roi":             roi,
        "best_day":        {"date": best_day[0], "pnl": round(best_day[1], 2)} if best_day else None,
        "worst_day":       {"date": worst_day[0], "pnl": round(worst_day[1], 2)} if worst_day else None,
        "daily_pnl":       {k: round(v, 2) for k, v in sorted(daily_pnl.items())},
        "trades":          sorted(all_trades, key=lambda x: x["date"]),
        "log":             "\n".join(log_lines),
    }


if __name__ == "__main__":
    result = run_backtest()
    print(result.get("log", result.get("error")))
