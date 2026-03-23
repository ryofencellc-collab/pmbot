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


@app.get("/debug")
def debug_full():
    """
    Full system diagnostic. Tests every layer end to end for Chicago only:
    1. DB connection + schema
    2. Polymarket API fetch for today slug
    3. Parse all 11 groupItemTitles
    4. Insert all 11 markets into DB
    5. Verify they were written by reading back
    6. NOAA forecast for Chicago
    Returns complete picture — no guessing.
    """
    import requests as req
    import json
    import re
    from datetime import date, datetime
    from data.database import get_conn

    out = {}

    # ── 1. DB connection ──────────────────────────────────────────────────────
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM markets")
        existing = c.fetchone()["count"]
        c.execute("SELECT version() as v")
        pg_version = c.fetchone()["v"]
        conn.close()
        out["1_db"] = {"status": "ok", "existing_markets": existing, "pg": pg_version}
    except Exception as e:
        out["1_db"] = {"status": "ERROR", "error": str(e)}
        return out  # no point continuing

    # ── 2. Polymarket API fetch ───────────────────────────────────────────────
    today = date.today()
    month = today.strftime("%B").lower()
    slug  = f"highest-temperature-in-chicago-on-{month}-{today.day}-{today.year}"
    try:
        r    = req.get("https://gamma-api.polymarket.com/events",
                       params={"slug": slug}, timeout=20,
                       headers={"User-Agent": "PolyEdge/1.0"})
        data = r.json()
        raw_markets = data[0].get("markets", []) if isinstance(data, list) and data else []
        out["2_api"] = {
            "status":       "ok",
            "slug":         slug,
            "http_status":  r.status_code,
            "events_found": len(data) if isinstance(data, list) else 0,
            "markets_found": len(raw_markets),
        }
    except Exception as e:
        out["2_api"] = {"status": "ERROR", "slug": slug, "error": str(e)}
        return out

    if not raw_markets:
        out["2_api"]["status"] = "ERROR — no markets in event"
        return out

    # ── 3. Parse groupItemTitles ──────────────────────────────────────────────
    def parse_title(title):
        t = title.strip().lower()
        m = re.match(r'^(\d+(?:\.\d+)?)\s*°?f?\s+or\s+(?:below|lower)$', t)
        if m:
            return {"market_type": "below", "target_low": -9999, "target_high": float(m.group(1))}
        m = re.match(r'^(\d+(?:\.\d+)?)\s*°?f?\s+or\s+(?:higher|above)$', t)
        if m:
            return {"market_type": "above", "target_low": float(m.group(1)), "target_high": 9999}
        m = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*°?f?$', t)
        if m:
            return {"market_type": "range", "target_low": float(m.group(1)), "target_high": float(m.group(2))}
        return None

    parse_results = []
    for m in raw_markets:
        title  = m.get("groupItemTitle", "")
        parsed = parse_title(title)
        parse_results.append({"title": title, "parsed": parsed, "ok": parsed is not None})

    failed_parses = [p for p in parse_results if not p["ok"]]
    out["3_parse"] = {
        "status":        "ok" if not failed_parses else "SOME FAILED",
        "total":         len(parse_results),
        "parsed_ok":     len([p for p in parse_results if p["ok"]]),
        "failed":        failed_parses,
        "all_titles":    [p["title"] for p in parse_results],
    }

    # ── 4. Insert all markets ─────────────────────────────────────────────────
    insert_results = []
    for m, p in zip(raw_markets, parse_results):
        if not p["ok"]:
            insert_results.append({"id": m.get("id"), "status": "SKIP — parse failed"})
            continue

        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        yes_price = float(prices[0]) if prices else 0.0

        outcome = None
        if m.get("closed"):
            if prices and str(prices[0]) == "1":   outcome = "Yes"
            elif len(prices) > 1 and str(prices[1]) == "1": outcome = "No"

        end_str = m.get("endDate", "")
        try:
            resolved_at = int(datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            resolved_at = 0

        start_str = m.get("startDate", "")
        try:
            created_at = int(datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            created_at = 0

        row = (
            str(m["id"]), m.get("question", ""), "Chicago",
            p["parsed"]["target_low"], p["parsed"]["target_high"],
            p["parsed"]["market_type"], "F",
            resolved_at, created_at, outcome,
            yes_price, float(m.get("liquidityNum") or 0)
        )

        try:
            conn = get_conn()
            c    = conn.cursor()
            c.execute("""
                INSERT INTO markets
                    (id, question, city, target_low, target_high,
                     market_type, unit, resolved_at, created_at,
                     outcome, last_trade_price, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    outcome          = EXCLUDED.outcome,
                    last_trade_price = EXCLUDED.last_trade_price,
                    volume           = EXCLUDED.volume
            """, row)
            conn.commit()
            conn.close()
            insert_results.append({"id": str(m["id"]), "title": p["title"], "status": "ok", "yes_price": yes_price})
        except Exception as e:
            insert_results.append({"id": str(m["id"]), "title": p["title"], "status": "ERROR", "error": str(e)})

    errors = [r for r in insert_results if r["status"] not in ("ok",)]
    out["4_insert"] = {
        "status":   "ok" if not errors else "SOME ERRORS",
        "inserted": len([r for r in insert_results if r["status"] == "ok"]),
        "errors":   errors,
        "detail":   insert_results,
    }

    # ── 5. Verify DB wrote correctly ──────────────────────────────────────────
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city='Chicago'")
        chicago_count = c.fetchone()["count"]
        c.execute("SELECT id, question, target_low, target_high, market_type, last_trade_price FROM markets WHERE city='Chicago' ORDER BY target_low LIMIT 5")
        sample = [dict(r) for r in c.fetchall()]
        conn.close()
        out["5_verify"] = {"status": "ok", "chicago_markets_in_db": chicago_count, "sample": sample}
    except Exception as e:
        out["5_verify"] = {"status": "ERROR", "error": str(e)}

    # ── 6. NOAA forecast ──────────────────────────────────────────────────────
    try:
        r = req.get("https://api.weather.gov/gridpoints/LOT/76,73/forecast",
                    timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
        periods = r.json()["properties"]["periods"]
        daytime = [p for p in periods if p.get("isDaytime")]
        out["6_noaa"] = {
            "status":   "ok",
            "forecast": daytime[0]["temperature"] if daytime else None,
            "unit":     daytime[0]["temperatureUnit"] if daytime else None,
            "summary":  daytime[0]["shortForecast"] if daytime else None,
        }
    except Exception as e:
        out["6_noaa"] = {"status": "ERROR", "error": str(e)}

    return out

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
