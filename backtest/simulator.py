"""
simulator.py — Blind day-by-day backtest. Runs all 3 models. Picks winner.
"""

import uuid
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from data.database import get_conn, init_db
from strategy.factors import scan_all_models

DEFAULT_CONFIG = {
    "starting_capital": 100.0,
    "principal":        100.0,
    "max_bets_per_day": 3,
    "min_signal_score": 0.05,
    "kelly_fraction":   0.25,
    "max_bet_pct":      0.20,
    "min_bet":          1.00,
    "max_bet":          500.00,
    "days_back":        1095,
}

MODELS = ["M1_crypto", "M2_weather", "M3_sports"]


def kelly_size(capital, signal, config):
    profit = capital - config["principal"]
    if profit < config["min_bet"]:
        return 0.0

    p = signal["factor_prob"]
    q = 1 - p
    b = (1.0 / signal["yes_price"]) - 1.0

    if b <= 0:
        return 0.0

    kelly = max(0.0, (p * b - q) / b)
    size  = kelly * config["kelly_fraction"] * capital
    size  = min(size, profit * config["max_bet_pct"])
    size  = max(config["min_bet"], min(config["max_bet"], size))
    size  = min(size, profit)
    return round(size, 2)


def resolve_pending(run_id, model, as_of_ts):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT bt.id, bt.entry_price, bt.size, m.outcome, m.resolved_at
        FROM backtest_trades bt
        JOIN markets m ON m.id = bt.market_id
        WHERE bt.run_id=? AND bt.model=? AND bt.outcome IS NULL
          AND m.resolved_at <= ? AND m.outcome IS NOT NULL
    ''', (run_id, model, as_of_ts))

    rows = c.fetchall()
    net  = 0.0

    for row in rows:
        tid, entry_price, size, outcome, resolved_at = row
        pnl           = size * (1.0 / entry_price - 1.0) if outcome == "Yes" else -size
        resolved_date = datetime.fromtimestamp(resolved_at, tz=timezone.utc).strftime('%Y-%m-%d')
        c.execute('''UPDATE backtest_trades SET outcome=?, pnl=?, resolved_date=? WHERE id=?''',
                  (outcome, round(pnl, 4), resolved_date, tid))
        net += pnl

    conn.commit()
    conn.close()
    return round(net, 4)


def run_model_backtest(model, run_id, config):
    end_dt   = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=config["days_back"])
    current  = start_dt
    capital  = config["starting_capital"]
    equity_curve = []
    day_count    = 0
    signal_days  = 0

    print(f"\n  [{model}] {start_dt.date()} → {end_dt.date()}")

    while current < end_dt:
        eval_ts = int(current.timestamp())
        day_str = current.strftime('%Y-%m-%d')

        pnl = resolve_pending(run_id, model, eval_ts)
        if pnl != 0:
            capital = max(capital + pnl, config["principal"] * 0.5)

        all_signals = scan_all_models(eval_ts, models=[model])
        signals     = [s for s in all_signals.get(model, [])
                       if s["signal_score"] >= config["min_signal_score"]]
        top         = signals[:config["max_bets_per_day"]]
        day_bets    = 0

        for sig in top:
            size = kelly_size(capital, sig, config)
            if size <= 0:
                continue

            conn = get_conn()
            conn.execute('''
                INSERT INTO backtest_trades
                (run_id, model, sim_date, market_id, question, entry_price,
                 size, capital_at_entry, signal_score, factor_data, outcome, pnl, resolved_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)
            ''', (run_id, model, day_str, sig["market_id"], sig["question"],
                  sig["yes_price"], size, round(capital, 2),
                  sig["signal_score"], sig.get("factor_data", "{}")))
            conn.commit()
            conn.close()

            capital  -= size
            day_bets += 1

        equity_curve.append({"date": day_str, "capital": round(capital, 2)})
        day_count += 1
        if day_bets > 0:
            signal_days += 1
            print(f"    {day_str} | ${capital:.2f} | bets={day_bets}")

        current += timedelta(days=1)

    resolve_pending(run_id, model, int(end_dt.timestamp()))

    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN outcome="Yes" THEN 1 ELSE 0 END) as wins,
               SUM(pnl) as total_pnl, MAX(pnl) as best, MIN(pnl) as worst
        FROM backtest_trades WHERE run_id=? AND model=? AND outcome IS NOT NULL
    ''', (run_id, model))
    s = dict(c.fetchone())

    c.execute('''SELECT sim_date, market_id, question, entry_price, size,
                        capital_at_entry, signal_score, factor_data, outcome, pnl
                 FROM backtest_trades WHERE run_id=? AND model=? ORDER BY sim_date''',
              (run_id, model))
    trades = [dict(r) for r in c.fetchall()]
    conn.close()

    total    = s["total"] or 1
    wins     = s["wins"] or 0
    pnl_sum  = s["total_pnl"] or 0
    final    = config["starting_capital"] + pnl_sum
    roi      = (final - config["starting_capital"]) / config["starting_capital"] * 100
    win_rate = wins / total

    caps   = [e["capital"] for e in equity_curve]
    rets   = np.diff(caps) / (np.array(caps[:-1]) + 1e-9)
    sharpe = float((np.mean(rets) / (np.std(rets) + 1e-9)) * np.sqrt(252)) if len(rets) > 1 else 0.0

    peak   = caps[0] if caps else 100
    max_dd = 0.0
    for cap in caps:
        peak   = max(peak, cap)
        max_dd = max(max_dd, (peak - cap) / peak)

    result = {
        "model": model, "run_id": run_id,
        "win_rate": round(win_rate, 4), "total_bets": total, "wins": wins,
        "total_pnl": round(pnl_sum, 2), "final_capital": round(final, 2),
        "roi": round(roi, 2), "best_trade": round(s["best"] or 0, 2),
        "worst_trade": round(s["worst"] or 0, 2),
        "sharpe": round(sharpe, 3), "max_drawdown": round(max_dd * 100, 2),
        "signal_coverage": round(signal_days / max(day_count, 1) * 100, 1),
        "equity_curve": equity_curve, "trades": trades,
    }

    conn = get_conn()
    conn.execute('''
        INSERT OR REPLACE INTO backtest_runs
        (run_id, model, started_at, completed_at, config,
         win_rate, total_bets, total_pnl, final_capital, roi, sharpe, max_drawdown)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (run_id, model, start_dt.isoformat(), datetime.now(timezone.utc).isoformat(),
          json.dumps(config), result["win_rate"], total, round(pnl_sum, 2),
          round(final, 2), round(roi, 2), result["sharpe"], result["max_drawdown"]))
    conn.commit()
    conn.close()
    return result


def run_all_backtests(config=None):
    if config is None:
        config = DEFAULT_CONFIG.copy()

    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM backtest_trades")
    conn.execute("DELETE FROM backtest_runs")
    conn.commit()
    conn.close()

    run_id      = str(uuid.uuid4())[:8]
    all_results = {}

    print(f"\n{'='*60}")
    print(f"  POLYEDGE 3-MODEL BACKTEST | run_id={run_id}")
    print(f"{'='*60}")

    for model in MODELS:
        all_results[model] = run_model_backtest(model, f"{run_id}_{model}", config)

    print(f"\n{'='*60}")
    print(f"  MODEL COMPARISON")
    print(f"{'='*60}")
    print(f"  {'Model':<14} {'WinRate':>8} {'Bets':>6} {'ROI':>8} {'Final$':>9} {'Sharpe':>7} {'MaxDD':>7}")
    print(f"  {'-'*65}")

    winner     = None
    best_score = -999

    for model, r in all_results.items():
        score = r["sharpe"] + r["roi"] / 100 - r["max_drawdown"] / 100
        if score > best_score:
            best_score = score
            winner     = model
        print(f"  {model:<14} {r['win_rate']*100:>7.1f}% {r['total_bets']:>6} "
              f"{r['roi']:>7.1f}% ${r['final_capital']:>8.2f} "
              f"{r['sharpe']:>7.3f} {r['max_drawdown']:>6.1f}%")

    print(f"\n  ✓ WINNER: {winner}")
    print(f"{'='*60}\n")

    all_results["winner"] = winner
    all_results["run_id"] = run_id
    return all_results


if __name__ == '__main__':
    run_all_backtests()
