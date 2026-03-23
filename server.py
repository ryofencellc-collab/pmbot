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

ingest_status = {"running": False, "done": False, "result": None}


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


def run_ingest_background():
    global ingest_status
    ingest_status["running"] = True
    ingest_status["done"] = False
    ingest_status["result"] = None
    try:
        from data.ingest import run_full_ingest
        run_full_ingest(days_back=30, days_ahead=7)
        conn = get_conn()
        c = conn.cursor()
        counts = {}
        for t in ["markets", "wu_temps", "paper_trades", "session_logs"]:
            c.execute(f"SELECT COUNT(*) as count FROM {t}")
            counts[t] = c.fetchone()["count"]
        conn.close()
        ingest_status["result"] = counts
        print(f"[INGEST] Done: {counts}")
    except Exception as e:
        ingest_status["result"] = {"error": str(e)}
        print(f"[INGEST] Error: {e}")
    finally:
        ingest_status["running"] = False
        ingest_status["done"] = True


@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    print("[SERVER] Ready — scheduler running")


@app.api_route("/health", methods=["GET", "POST", "HEAD"])
def health():
    conn = get_conn()
    c = conn.cursor()
    tables = {}
    for t in ["markets", "wu_temps", "paper_trades", "session_logs"]:
        try:
            c.execute(f"SELECT COUNT(*) as count FROM {t}")
            row = c.fetchone()
            tables[t] = row["count"] if row else 0
        except Exception:
            tables[t] = 0
    conn.close()
    return {"status": "ok", "tables": tables, "ingest": ingest_status}


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


@app.get("/ingest")
def run_ingest():
    global ingest_status
    if ingest_status["running"]:
        return {"status": "already_running", "ingest": ingest_status}
    threading.Thread(target=run_ingest_background, daemon=True).start()
    return {"status": "started", "message": "Ingest running in background. Check /health for progress."}


@app.get("/ingest/status")
def ingest_status_check():
    return ingest_status




@app.get("/debug/polymarket")
def debug_polymarket():
    import requests as req
    results = {}

    # Test 1: search by tag
    try:
        r = req.get("https://gamma-api.polymarket.com/markets", params={
            "limit": 5, "tag": "weather", "order": "endDate", "ascending": "false"
        }, timeout=15)
        results["tag_weather"] = [m.get("question","") for m in r.json()]
    except Exception as e:
        results["tag_weather"] = str(e)

    # Test 2: category weather
    try:
        r = req.get("https://gamma-api.polymarket.com/markets", params={
            "limit": 5, "category": "weather", "order": "endDate", "ascending": "false"
        }, timeout=15)
        results["category_weather"] = [m.get("question","") for m in r.json()]
    except Exception as e:
        results["category_weather"] = str(e)

    # Test 3: get events with weather tag
    try:
        r = req.get("https://gamma-api.polymarket.com/events", params={
            "limit": 5, "tag": "weather", "order": "endDate", "ascending": "false"
        }, timeout=15)
        results["events_weather"] = [m.get("title","") for m in r.json()]
    except Exception as e:
        results["events_weather"] = str(e)

    # Test 4: get events with temperature keyword
    try:
        r = req.get("https://gamma-api.polymarket.com/events", params={
            "limit": 5, "search": "temperature", "order": "endDate", "ascending": "false"
        }, timeout=15)
        results["events_search_temp"] = [m.get("title","") for m in r.json()]
    except Exception as e:
        results["events_search_temp"] = str(e)

    return results


@app.get("/debug/polymarket2")
def debug_polymarket2():
    import requests as req
    try:
        r = req.get("https://gamma-api.polymarket.com/markets", params={
            "limit": 5,
            "search": "temperature Chicago",
            "order": "endDate",
            "ascending": "false",
        }, timeout=15)
        data = r.json()
        return {
            "status": r.status_code,
            "count": len(data),
            "questions": [m.get("question", "") for m in data]
        }
    except Exception as e:
        return {"error": str(e)}

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


@app.api_route("/morning", methods=["GET", "POST"])
def morning_session():
    try:
        from strategy.paper_trade import run_morning_session
        trades, log = run_morning_session()
        return {"trades": trades, "log": log}
    except Exception as e:
        return {"trades": [], "log": str(e)}


@app.api_route("/evening", methods=["GET", "POST"])
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
