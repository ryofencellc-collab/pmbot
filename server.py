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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def run_scheduler():
    import time
    from datetime import datetime
    print("[SCHEDULER] Started")
    morning_ran = None
    evening_ran = None
    while True:
        now    = datetime.now()
        today  = now.strftime('%Y-%m-%d')
        hour   = now.hour
        minute = now.minute

        if hour == 7 and minute < 5 and morning_ran != today:
            try:
                from strategy.paper_trade import run_morning_session
                trades, log = run_morning_session()
                morning_ran = today
                print(f"[SCHEDULER] Morning done. {len(trades)} trades.")
            except Exception as e:
                print(f"[SCHEDULER] Morning error: {e}")

        elif hour == 20 and minute < 5 and evening_ran != today:
            try:
                from strategy.paper_trade import run_evening_session
                run_evening_session()
                evening_ran = today
                print("[SCHEDULER] Evening done.")
            except Exception as e:
                print(f"[SCHEDULER] Evening error: {e}")

        time.sleep(30)


@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    print("[SERVER] Ready — scheduler running")


@app.get("/health")
def health():
    conn = get_conn()
    c = conn.cursor()
    tables = {}
    for t in ["markets", "wu_temps", "paper_trades", "session_logs"]:
        try:
            c.execute(f"SELECT COUNT(*) FROM {t}")
            row = c.fetchone()
            tables[t] = row[0] if row else 0
        except Exception:
            tables[t] = 0
    conn.close()
    return {"status": "ok", "tables": tables}


@app.get("/test")
def run_test():
    import requests as req
    results = {}

    try:
        conn = get_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        results["database"] = {"status": "ok", "message": "Connected"}
    except Exception as e:
        results["database"] = {"status": "error", "message": str(e)}

    try:
        r = req.get("https://api.weather.gov/gridpoints/LOT/76,73/forecast",
                    timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
        temp = r.json()["properties"]["periods"][0]["temperature"]
        results["noaa"] = {"status": "ok", "message": f"Chicago: {temp}F"}
    except Exception as e:
        results["noaa"] = {"status": "error", "message": str(e)}

    try:
        r = req.get("https://gamma-api.polymarket.com/markets",
                    params={"limit": 1}, timeout=10)
        results["polymarket"] = {"status": "ok", "message": "Connected"}
    except Exception as e:
        results["polymarket"] = {"status": "error", "message": str(e)}

    return results


@app.get("/signals")
def get_signals():
    try:
        from strategy.signals import scan_signals
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        signals, log = scan_signals(today)
        return {"signals": signals, "log": log, "date": today}
    except Exception as e:
        return {"signals": [], "log": str(e), "date": ""}


@app.post("/morning")
def morning_session():
    try:
        from strategy.paper_trade import run_morning_session
        trades, log = run_morning_session()
        return {"trades": trades, "log": log}
    except Exception as e:
        return {"trades": [], "log": str(e)}


@app.post("/evening")
def evening_session():
    try:
        from strategy.paper_trade import run_evening_session
        log = run_evening_session()
        return {"log": log}
    except Exception as e:
        return {"log": str(e)}


@app.get("/performance")
def get_performance():
    try:
        from strategy.paper_trade import get_performance
        return get_performance()
    except Exception as e:
        return {"total_bets": 0, "wins": 0, "win_rate": 0,
                "total_pnl": 0, "final_capital": 100.0, "roi": 0,
                "best_trade": 0, "worst_trade": 0, "trades": []}


@app.get("/trades")
def get_trades():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT trade_date, city, question, entry_price, size, noaa_forecast_f, predicted_range, outcome, pnl FROM paper_trades ORDER BY trade_date DESC, id DESC")
        trades = [dict(r) for r in c.fetchall()]
        conn.close()
        return trades
    except Exception:
        return []


@app.get("/logs")
def get_logs():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT session_type, logged_at, content FROM session_logs ORDER BY id DESC LIMIT 10")
        logs = [dict(r) for r in c.fetchall()]
        conn.close()
        return logs
    except Exception:
        return []


@app.get("/", response_class=HTMLResponse)
def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path) as f:
        return f.read()


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
