"""
server.py - FastAPI backend.
Serves the dashboard and handles all API calls.
"""

import sys
import os
import json
import threading
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from data.database import get_conn, init_db

app = FastAPI(title="PolyEdge", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def startup():
    init_db()
    # Create session_logs table if not exists
    conn = get_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS session_logs
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
         session_type TEXT, logged_at TEXT, content TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS paper_trades
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
         trade_date TEXT, market_id TEXT, question TEXT,
         city TEXT, entry_price REAL, noaa_forecast_f REAL,
         predicted_range TEXT, size REAL, capital_at_entry REAL,
         outcome TEXT, pnl REAL)""")
    conn.commit()
    conn.close()
    print("[SERVER] Ready")


# ── Health & Status ───────────────────────────────────────────────────────────

@app.get("/health")
def health():
    conn = get_conn()
    c    = conn.cursor()
    tables = {}
    for t in ["markets", "price_snapshots", "wu_temps", "paper_trades"]:
        try:
            c.execute(f"SELECT COUNT(*) FROM {t}")
            tables[t] = c.fetchone()[0]
        except Exception:
            tables[t] = 0
    conn.close()
    return {"status": "ok", "tables": tables}


@app.get("/test")
def run_test():
    """Full system test."""
    import warnings
    warnings.filterwarnings('ignore')
    from scheduler import run_system_test
    return run_system_test()


# ── Signals ───────────────────────────────────────────────────────────────────

@app.get("/signals")
def get_signals():
    """Get today's signals."""
    from strategy.signals import scan_signals
    from datetime import datetime, timezone
    today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    signals, log = scan_signals(today)
    return {"signals": signals, "log": log, "date": today}


@app.post("/morning")
def morning_session():
    """Manually trigger morning session."""
    from strategy.paper_trade import run_morning_session
    trades, log = run_morning_session()
    return {"trades": trades, "log": log}


@app.post("/evening")
def evening_session():
    """Manually trigger evening session."""
    from strategy.paper_trade import run_evening_session
    log = run_evening_session()
    return {"log": log}


# ── Performance ───────────────────────────────────────────────────────────────

@app.get("/performance")
def get_performance():
    from strategy.paper_trade import get_performance
    return get_performance()


@app.get("/trades")
def get_trades():
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT trade_date, city, question, entry_price, size,
                        noaa_forecast_f, predicted_range, outcome, pnl
                 FROM paper_trades ORDER BY trade_date DESC, id DESC""")
    trades = [dict(r) for r in c.fetchall()]
    conn.close()
    return trades


@app.get("/logs")
def get_logs():
    conn = get_conn()
    c    = conn.cursor()
    try:
        c.execute("""SELECT session_type, logged_at, content
                     FROM session_logs ORDER BY id DESC LIMIT 10""")
        logs = [dict(r) for r in c.fetchall()]
    except Exception:
        logs = []
    conn.close()
    return logs


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PolyEdge</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0a0a0f; color: #e8e8f0; font-family: -apple-system, sans-serif; }
  .header { background: #0f0f1a; border-bottom: 1px solid #1a1a2e; padding: 16px 20px;
            display: flex; align-items: center; justify-content: space-between; }
  .logo { font-size: 18px; font-weight: 700; }
  .logo span { color: #00ff88; }
  .status-bar { display: flex; gap: 8px; flex-wrap: wrap; padding: 12px 20px;
                background: #0f0f1a; border-bottom: 1px solid #1a1a2e; }
  .status-item { display: flex; align-items: center; gap: 6px; font-size: 12px;
                 padding: 4px 10px; background: #1a1a2e; border-radius: 20px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; }
  .dot-green { background: #00ff88; box-shadow: 0 0 6px #00ff88; }
  .dot-red   { background: #ff4444; box-shadow: 0 0 6px #ff4444; }
  .dot-gray  { background: #666; }
  .tabs { display: flex; border-bottom: 1px solid #1a1a2e; }
  .tab { padding: 12px 20px; font-size: 13px; cursor: pointer; border-bottom: 2px solid transparent;
         color: #666; transition: all 0.2s; }
  .tab.active { color: #00ff88; border-bottom-color: #00ff88; }
  .content { padding: 20px; max-width: 800px; margin: 0 auto; }
  .card { background: #0f0f1a; border: 1px solid #1a1a2e; border-radius: 12px;
          padding: 16px; margin-bottom: 16px; }
  .card-title { font-size: 11px; color: #666; letter-spacing: 0.1em;
                text-transform: uppercase; margin-bottom: 12px; }
  .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  .stat { text-align: center; }
  .stat-val { font-size: 24px; font-weight: 700; font-family: monospace; color: #00ff88; }
  .stat-label { font-size: 11px; color: #666; margin-top: 4px; }
  .signal { border: 1px solid #1a2e1a; background: #0d1a0d; border-radius: 10px;
            padding: 14px; margin-bottom: 12px; }
  .signal-city { font-size: 16px; font-weight: 700; color: #00ff88; }
  .signal-q { font-size: 13px; color: #aaa; margin: 6px 0; }
  .signal-row { display: flex; gap: 16px; font-size: 12px; margin-top: 8px; flex-wrap: wrap; }
  .signal-tag { background: #1a2e1a; padding: 3px 8px; border-radius: 4px; color: #00ff88; }
  .trade { b
