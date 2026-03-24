import sys
import os
import json
import threading
import time
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from data.database import get_conn, init_db

app = FastAPI(title="PolyEdge", version="4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ingest_status = {"running": False, "done": False, "result": None}


# ── Scheduler ─────────────────────────────────────────────────────────────────

def run_scheduler():
    """
    Runs forever in background thread.
    - 7:00 AM UTC: ingest + morning session
    - Every 30 min: check pending outcomes (real-time resolution)
    - 8:00 PM UTC: evening session log

    Uses DB session_logs to track whether morning already ran today
    so restarts don't cause missed or duplicate sessions.
    """
    from datetime import datetime, timezone
    print("[SCHEDULER] Started")

    last_outcome = None

    while True:
        now    = datetime.now(timezone.utc)
        today  = now.strftime('%Y-%m-%d')
        hour   = now.hour
        minute = now.minute

        # Morning: 7:00–7:05 AM UTC — ingest then trade
        if hour == 7 and minute < 5:
            # Check DB to see if morning already ran today (survives restarts)
            try:
                conn = get_conn()
                c    = conn.cursor()
                c.execute("""SELECT COUNT(*) as count FROM session_logs
                             WHERE session_type='morning'
                             AND logged_at LIKE %s""", (f"{today}%",))
                already_ran = c.fetchone()["count"] > 0
                conn.close()
            except Exception:
                already_ran = False

            if not already_ran:
                print("[SCHEDULER] Morning session starting...")
                # Run ingest first, wait for it to complete
                try:
                    from data.ingest import run_full_ingest
                    run_full_ingest(days_back=30, days_ahead=7)
                    print("[SCHEDULER] Ingest complete")
                except Exception as e:
                    print(f"[SCHEDULER] Ingest error: {e}")
                # Then run morning session
                try:
                    from strategy.paper_trade import run_morning_session
                    trades, log = run_morning_session()
                    print(f"[SCHEDULER] Morning done. {len(trades)} trades placed.")
                except Exception as e:
                    print(f"[SCHEDULER] Morning error: {e}")

        # Every 30 min: check outcomes in real time
        check_key = f"{today}-{hour}-{minute // 30}"
        if last_outcome != check_key:
            try:
                from strategy.paper_trade import check_pending_outcomes
                resolved = check_pending_outcomes()
                if resolved > 0:
                    print(f"[SCHEDULER] Resolved {resolved} trades")
                last_outcome = check_key
            except Exception as e:
                print(f"[SCHEDULER] Outcome check error: {e}")

        # Evening: 8:00–8:05 PM UTC — log summary
        if hour == 20 and minute < 5:
            try:
                conn = get_conn()
                c    = conn.cursor()
                c.execute("""SELECT COUNT(*) as count FROM session_logs
                             WHERE session_type='evening'
                             AND logged_at LIKE %s""", (f"{today}%",))
                already_ran = c.fetchone()["count"] > 0
                conn.close()
            except Exception:
                already_ran = False

            if not already_ran:
                try:
                    from strategy.paper_trade import run_evening_session
                    run_evening_session()
                    print("[SCHEDULER] Evening done.")
                except Exception as e:
                    print(f"[SCHEDULER] Evening error: {e}")

        time.sleep(30)


def run_ingest_background():
    global ingest_status
    ingest_status["running"] = True
    ingest_status["done"]    = False
    ingest_status["result"]  = None
    try:
        from data.ingest import run_full_ingest
        run_full_ingest(days_back=30, days_ahead=7)
        conn = get_conn()
        c    = conn.cursor()
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
        ingest_status["done"]    = True


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    # Auto-ingest on every startup so DB is never empty after a restart
    threading.Thread(target=run_ingest_background, daemon=True).start()
    print("[SERVER] Ready — scheduler + startup ingest running")


# ── Health ────────────────────────────────────────────────────────────────────

@app.api_route("/health", methods=["GET", "POST", "HEAD"])
def health():
    conn   = get_conn()
    c      = conn.cursor()
    tables = {}
    for t in ["markets", "wu_temps", "paper_trades", "session_logs", "noaa_forecasts"]:
        try:
            c.execute(f"SELECT COUNT(*) as count FROM {t}")
            row      = c.fetchone()
            tables[t] = row["count"] if row else 0
        except Exception:
            tables[t] = 0
    conn.close()
    return {"status": "ok", "tables": tables, "ingest": ingest_status}


# ── Ingest ────────────────────────────────────────────────────────────────────

@app.get("/ingest")
def run_ingest():
    global ingest_status
    if ingest_status["running"]:
        return {"status": "already_running", "ingest": ingest_status}
    threading.Thread(target=run_ingest_background, daemon=True).start()
    return {"status": "started", "message": "Ingest running. Watch /health for counts."}


@app.get("/ingest/status")
def ingest_status_check():
    return ingest_status


# ── Trading ───────────────────────────────────────────────────────────────────

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
        from strategy.paper_trade import check_pending_outcomes
        resolved = check_pending_outcomes()
        return {"resolved": resolved}
    except Exception as e:
        return {"resolved": 0, "error": str(e)}


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


@app.get("/trades")
def get_trades():
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""SELECT trade_date, city, question, entry_price, size,
                            noaa_forecast_f, predicted_range, outcome, pnl
                     FROM paper_trades ORDER BY trade_date DESC, id DESC""")
        trades = [dict(r) for r in c.fetchall()]
        conn.close()
        return trades
    except Exception:
        return []


@app.get("/performance")
def get_performance():
    try:
        from strategy.paper_trade import get_performance
        return get_performance()
    except Exception as e:
        return {"total_bets": 0, "wins": 0, "win_rate": 0,
                "total_pnl": 0, "final_capital": 100.0, "roi": 0,
                "best_trade": 0, "worst_trade": 0, "trades": [], "error": str(e)}


@app.get("/logs")
def get_logs():
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""SELECT session_type, logged_at, content
                     FROM session_logs ORDER BY id DESC LIMIT 20""")
        logs = [dict(r) for r in c.fetchall()]
        conn.close()
        return logs
    except Exception:
        return []


# ── Full System Debug ─────────────────────────────────────────────────────────

@app.get("/debug")
def debug_full():
    """
    Complete system diagnostic. Tests every layer.
    Run this any time something seems wrong — it tells you exactly what's broken.
    """
    import requests as req
    import re
    import math
    from datetime import date, datetime, timezone

    out = {}

    # ── 1. Database ───────────────────────────────────────────────────────────
    try:
        conn = get_conn()
        c    = conn.cursor()
        counts = {}
        for t in ["markets", "wu_temps", "paper_trades",
                  "session_logs", "noaa_forecasts"]:
            c.execute(f"SELECT COUNT(*) as count FROM {t}")
            counts[t] = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city='Chicago' AND outcome IS NULL")
        counts["chicago_open"] = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome IS NOT NULL")
        counts["resolved"] = c.fetchone()["count"]
        c.execute("SELECT version() as v")
        pg = c.fetchone()["v"]
        conn.close()
        out["1_database"] = {"status": "ok", "counts": counts, "postgres": pg}
    except Exception as e:
        out["1_database"] = {"status": "ERROR", "error": str(e)}
        return out

    # ── 2. Polymarket API ─────────────────────────────────────────────────────
    today = date.today()
    month = today.strftime("%B").lower()
    slug  = f"highest-temperature-in-chicago-on-{month}-{today.day}-{today.year}"
    try:
        r    = req.get("https://gamma-api.polymarket.com/events",
                       params={"slug": slug}, timeout=20,
                       headers={"User-Agent": "PolyEdge/1.0"})
        data = r.json() if r.status_code == 200 else []
        raw_markets = data[0].get("markets", []) if data else []
        out["2_polymarket_api"] = {
            "status":        "ok" if raw_markets else "ERROR — no markets",
            "slug":          slug,
            "http_status":   r.status_code,
            "markets_found": len(raw_markets),
            "titles":        [m.get("groupItemTitle") for m in raw_markets],
            "prices":        {m.get("groupItemTitle"): m.get("outcomePrices") for m in raw_markets},
        }
    except Exception as e:
        out["2_polymarket_api"] = {"status": "ERROR", "error": str(e)}

    # ── 3. NOAA Forecast ──────────────────────────────────────────────────────
    try:
        r = req.get("https://api.weather.gov/gridpoints/LOT/76,73/forecast",
                    timeout=15, headers={"User-Agent": "PolyEdge/1.0"})
        periods = r.json()["properties"]["periods"]
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        daytime   = [p for p in periods
                     if p.get("isDaytime") and today_str in p.get("startTime","")]
        fallback  = [p for p in periods if p.get("isDaytime")]
        period    = daytime[0] if daytime else (fallback[0] if fallback else None)
        out["3_noaa"] = {
            "status":        "ok" if period else "ERROR",
            "forecast_f":    period["temperature"] if period else None,
            "summary":       period["shortForecast"] if period else None,
            "period_start":  period["startTime"] if period else None,
        }
    except Exception as e:
        out["3_noaa"] = {"status": "ERROR", "error": str(e)}

    # ── 4. NOAA Error Model ───────────────────────────────────────────────────
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""SELECT city, date, forecast_f, actual_f, delta_f
                     FROM noaa_forecasts WHERE city='Chicago'
                     ORDER BY date DESC LIMIT 10""")
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        deltas = [r["delta_f"] for r in rows if r["delta_f"] is not None]
        mean   = sum(deltas)/len(deltas) if deltas else None
        out["4_error_model"] = {
            "status":        "ok",
            "sample_count":  len(deltas),
            "mean_delta":    round(mean, 2) if mean is not None else "not enough data",
            "note":          "positive = NOAA runs high vs actual",
            "history":       rows,
        }
    except Exception as e:
        out["4_error_model"] = {"status": "ERROR", "error": str(e)}

    # ── 5. Signal Engine ──────────────────────────────────────────────────────
    try:
        from strategy.signals import scan_signals
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        signals, log = scan_signals(today_str)
        out["5_signals"] = {
            "status":  "ok",
            "count":   len(signals),
            "signals": [{
                "question":    s["question"],
                "entry_price": s["entry_price"],
                "true_prob":   s["true_prob"],
                "edge":        s["edge"],
                "ev":          s["ev"],
            } for s in signals],
            "log": log,
        }
    except Exception as e:
        out["5_signals"] = {"status": "ERROR", "error": str(e)}

    # ── 6. Pending Trades ─────────────────────────────────────────────────────
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""SELECT trade_date, city, question, entry_price,
                            size, outcome, pnl
                     FROM paper_trades ORDER BY id DESC LIMIT 10""")
        trades = [dict(r) for r in c.fetchall()]
        c.execute("SELECT COUNT(*) as count FROM paper_trades WHERE outcome IS NULL")
        pending = c.fetchone()["count"]
        conn.close()
        out["6_trades"] = {
            "status":          "ok",
            "pending_count":   pending,
            "recent_trades":   trades,
        }
    except Exception as e:
        out["6_trades"] = {"status": "ERROR", "error": str(e)}

    # ── 7. Scheduler Status ───────────────────────────────────────────────────
    out["7_scheduler"] = {
        "status": "ok",
        "note":   "Scheduler runs: 7AM ingest+morning, every 30min outcome check, 8PM evening log",
        "ingest_background": ingest_status,
    }

    # ── Overall health ────────────────────────────────────────────────────────
    errors = [k for k, v in out.items() if isinstance(v, dict) and v.get("status","").startswith("ERROR")]
    out["0_summary"] = {
        "healthy": len(errors) == 0,
        "errors":  errors,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return out


# ── Backtest ─────────────────────────────────────────────────────────────────

@app.get("/backtest")
def run_backtest():
    """
    Simulate 30 days using WU actual temps as proxy forecast.
    Fast but uses simulated forecasts.
    """
    try:
        from strategy.backtest import run_backtest as _run
        result = _run()
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/backtest/real")
def run_backtest_real():
    """Real backtest Chicago using GFS MOS forecasts. Slow but 100% real."""
    try:
        from strategy.backtest_real import run_backtest as _run
        return _run()
    except Exception as e:
        return {"error": str(e)}


@app.get("/backtest/london")
def run_backtest_london():
    """Real backtest London using Open-Meteo forecasts."""
    try:
        from strategy.backtest_london import run_backtest as _run
        return _run()
    except Exception as e:
        return {"error": str(e)}


@app.get("/backtest/cities")
def run_backtest_cities():
    """Real backtest NYC, Buenos Aires, Seoul, Toronto. Takes 10-20 min."""
    try:
        from strategy.backtest_cities import run_all_backtests
        return run_all_backtests()
    except Exception as e:
        return {"error": str(e)}


@app.get("/backtest/city/{city_name}")
def run_backtest_city(city_name: str):
    """Real backtest for a single city. E.g. /backtest/city/Seoul"""
    try:
        from strategy.backtest_cities import run_city_backtest, CITY_CONFIGS
        # Handle URL encoding
        city = city_name.replace("-", " ").title()
        if city not in CITY_CONFIGS:
            return {"error": f"City '{city}' not found. Options: {list(CITY_CONFIGS.keys())}"}
        return run_city_backtest(city)
    except Exception as e:
        return {"error": str(e)}


# ── NOAA History Test ────────────────────────────────────────────────────────

@app.get("/debug/noaa-history")
def debug_noaa_history():
    """
    Test Iowa State Mesonet API for historical NOAA forecasts.
    KLOT = Chicago NWS office.
    If this works we can build a real error model from 30 days of real forecasts.
    """
    import requests as req
    from datetime import date, timedelta

    results = {}

    # Test 1: Iowa State Mesonet - NWS forecast archive
    try:
        r = req.get(
            "https://mesonet.agron.iastate.edu/api/1/nwstext.json",
            params={"station": "KLOT", "has_iemid": 0, "fmt": "json"},
            timeout=15, headers={"User-Agent": "PolyEdge/1.0"})
        results["mesonet_nwstext"] = {
            "status": r.status_code,
            "sample": str(r.text[:300]) if r.status_code == 200 else r.text[:200]
        }
    except Exception as e:
        results["mesonet_nwstext"] = {"status": "ERROR", "error": str(e)}

    # Test 2: Iowa State - AFD (Area Forecast Discussion) archive
    try:
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        r = req.get(
            f"https://mesonet.agron.iastate.edu/api/1/nwstext.json",
            params={"station": "KLOT", "fmt": "json",
                    "sdate": yesterday, "edate": yesterday},
            timeout=15, headers={"User-Agent": "PolyEdge/1.0"})
        results["mesonet_yesterday"] = {
            "status": r.status_code,
            "sample": str(r.text[:300]) if r.status_code == 200 else r.text[:200]
        }
    except Exception as e:
        results["mesonet_yesterday"] = {"status": "ERROR", "error": str(e)}

    # Test 3: NOAA climate data API - daily summaries (actual observed)
    try:
        r = req.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
            params={
                "datasetid": "GHCND",
                "stationid": "GHCND:USW00094846",  # O'Hare
                "startdate": "2026-02-21",
                "enddate": "2026-02-21",
                "datatypeid": "TMAX",
                "units": "standard",
                "limit": 5,
            },
            headers={"token": "YOUR_NCDC_TOKEN"},
            timeout=15)
        results["ncdc_climate"] = {
            "status": r.status_code,
            "note": "Needs free API token from ncdc.noaa.gov",
            "sample": str(r.text[:200])
        }
    except Exception as e:
        results["ncdc_climate"] = {"status": "ERROR", "error": str(e)}

    # Test 4: Open-Meteo - free historical weather API, no auth needed
    try:
        r = req.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": 41.9742,   # O'Hare coordinates
                "longitude": -87.9073,
                "start_date": "2026-02-21",
                "end_date": "2026-03-22",
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "America/Chicago",
            },
            timeout=15, headers={"User-Agent": "PolyEdge/1.0"})
        data = r.json() if r.status_code == 200 else {}
        results["open_meteo_history"] = {
            "status": r.status_code,
            "days": len(data.get("daily", {}).get("time", [])),
            "sample_dates": data.get("daily", {}).get("time", [])[:3],
            "sample_temps": data.get("daily", {}).get("temperature_2m_max", [])[:3],
        }
    except Exception as e:
        results["open_meteo_history"] = {"status": "ERROR", "error": str(e)}

    # Test 5: Open-Meteo forecast API - free, no auth
    try:
        r = req.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 41.9742,
                "longitude": -87.9073,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "America/Chicago",
                "forecast_days": 7,
            },
            timeout=15, headers={"User-Agent": "PolyEdge/1.0"})
        data = r.json() if r.status_code == 200 else {}
        results["open_meteo_forecast"] = {
            "status": r.status_code,
            "days": len(data.get("daily", {}).get("time", [])),
            "dates": data.get("daily", {}).get("time", []),
            "temps": data.get("daily", {}).get("temperature_2m_max", []),
        }
    except Exception as e:
        results["open_meteo_forecast"] = {"status": "ERROR", "error": str(e)}

    return results


# ── System Test (legacy) ──────────────────────────────────────────────────────

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
        r    = req.get("https://api.weather.gov/gridpoints/LOT/76,73/forecast",
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


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path) as f:
        return f.read()


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
