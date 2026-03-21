"""
server.py — FastAPI backend.
Run: uvicorn server:app --host 0.0.0.0 --port 8000
"""

import sys
import os
import json
import threading
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from data.database import get_conn, init_db

app = FastAPI(title="PolyEdge API", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def startup():
    init_db()
    def _run():
        try:
            from data.ingest import fetch_weather_price_histories
            print("[STARTUP] Beginning price history load...")
            fetch_weather_price_histories()
            print("[STARTUP] Price history load complete")
        except Exception as e:
            print(f"[STARTUP ERR] {e}")
    threading.Thread(target=_run, daemon=True).start()
    print("[STARTUP] Background thread started")


@app.get("/health")
def health():
    conn    = get_conn()
    c       = conn.cursor()
    summary = {}
    for t in ["markets", "price_snapshots", "crypto_prices", "weather_data",
              "sports_lines", "backtest_trades", "live_trades"]:
        c.execute(f"SELECT COUNT(*) FROM {t}")
        summary[t] = c.fetchone()[0]
    conn.close()
    return {"status": "ok", "tables": summary}


@app.get("/backtest/summary")
def backtest_summary():
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''SELECT run_id, model, win_rate, total_bets, total_pnl,
                        final_capital, roi, sharpe, max_drawdown, completed_at
                 FROM backtest_runs ORDER BY roi DESC''')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/backtest/equity/{model}")
def backtest_equity(model: str):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT sim_date,
               SUM(SUM(COALESCE(pnl, 0))) OVER (ORDER BY sim_date) + 100 as capital
        FROM backtest_trades
        WHERE model=? AND outcome IS NOT NULL
        GROUP BY sim_date ORDER BY sim_date
    ''', (model,))
    rows = [{"date": r[0], "capital": round(r[1], 2)} for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/backtest/trades/{model}")
def backtest_trades(model: str, limit: int = 500):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''SELECT sim_date, question, entry_price, size, capital_at_entry,
                        signal_score, factor_data, outcome, pnl, resolved_date
                 FROM backtest_trades
                 WHERE model=? AND outcome IS NOT NULL
                 ORDER BY sim_date DESC LIMIT ?''', (model, limit))
    rows = []
    for r in c.fetchall():
        row = dict(r)
        try:
            row["factor_data"] = json.loads(row["factor_data"] or "{}")
        except Exception:
            row["factor_data"] = {}
        rows.append(row)
    conn.close()
    return rows


@app.get("/backtest/winner")
def backtest_winner():
    conn = get_conn()
    c    = conn.cursor()
    c.execute('SELECT model, roi, win_rate, final_capital FROM backtest_runs ORDER BY roi DESC LIMIT 1')
    row  = c.fetchone()
    conn.close()
    return dict(row) if row else {"model": None}


@app.get("/live/trades")
def live_trades():
    from live.bot import resolve_live
    resolve_live()
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''SELECT trade_date, model, question, entry_price, size,
                        capital_at_entry, signal_score, outcome, pnl, tx_hash
                 FROM live_trades ORDER BY id DESC''')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/live/summary")
def live_summary():
    from live.bot import resolve_live
    resolve_live()
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''SELECT COUNT(*) as total,
                        SUM(CASE WHEN outcome="Yes" THEN 1 ELSE 0 END) as wins,
                        SUM(COALESCE(pnl, 0)) as total_pnl
                 FROM live_trades WHERE outcome IS NOT NULL''')
    row  = dict(c.fetchone())
    conn.close()
    total = row["total"] or 1
    return {
        "total_bets": row["total"], "wins": row["wins"] or 0,
        "win_rate":   round((row["wins"] or 0) / total, 4),
        "total_pnl":  round(row["total_pnl"] or 0, 2),
    }


class BacktestConfig(BaseModel):
    starting_capital: float = 100.0
    principal:        float = 100.0
    max_bets_per_day: int   = 3
    min_signal_score: float = 0.05
    kelly_fraction:   float = 0.25
    days_back:        int   = 1095


@app.post("/run/ingest")
def trigger_ingest(days_back: int = 1095, bg: BackgroundTasks = None):
    def _run():
        from data.ingest import run_full_ingest
        run_full_ingest(days_back=days_back)
    if bg:
        bg.add_task(_run)
        return {"status": "ingestion started in background"}
    _run()
    return {"status": "complete"}


@app.post("/run/backtest")
def trigger_backtest(cfg: BacktestConfig, bg: BackgroundTasks = None):
    def _run():
        from backtest.simulator import run_all_backtests
        run_all_backtests(config=cfg.dict())
    if bg:
        bg.add_task(_run)
        return {"status": "backtest started in background"}
    _run()
    return {"status": "complete"}


@app.post("/run/live-day")
def trigger_live_day(model: str = None):
    from live.bot import run_live_day
    run_live_day(model=model)
    return {"status": "complete"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
