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

# Ensure cache table exists on startup
try:
    _conn = get_conn()
    _c = _conn.cursor()
    _c.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)
    _conn.commit()
    _conn.close()
    print("[STARTUP] Cache table ready")
except Exception as _e:
    print(f"[STARTUP] Cache table error: {_e}")

app = FastAPI(title="PolyEdge", version="4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ingest_status = {"running": False, "done": False, "result": None}
backtest_status = {"running": False, "done": False, "result": None}


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
                # Real money trading — completely independent
                # Log forecasts every morning — Option B
                try:
                    from forecast_logger import log_all_forecasts, fill_wu_actuals
                    log_all_forecasts()
                    fill_wu_actuals()
                    print("[SCHEDULER] Forecasts logged")
                except Exception as e:
                    print(f"[SCHEDULER] Forecast log error: {e}")

                try:
                    import os
                    if os.getenv("TRADING_MODE") == "real":
                        try:
                            from strategy.polymarket_client import is_real_mode, place_real_order, BET_SIZE
                            from strategy.early_entry import get_early_signals
                            if is_real_mode():
                                signals, _ = get_early_signals()
                                placed = 0
                                # Sort by forecast proximity — buy closest matches first
                                import re as _re
                                def _score(s):
                                    q = (s.get("question","")).lower()
                                    nums = [float(x) for x in _re.findall(r"-?\d+\.?\d*", q) if -50 < float(x) < 150]
                                    if not nums: return 0
                                    target = sum(nums)/len(nums)
                                    gap = abs(target - (s.get("forecast") or 0))
                                    return -gap  # negative so closest = highest
                                signals = sorted(signals, key=_score, reverse=True)
                                for sig in signals[:3]:  # TEST: 3 best bets only
                                    result = place_real_order(
                                        sig["market_id"],
                                        sig["question"],
                                        sig["city"],
                                        sig["entry_price"]
                                    )
                                    if result.get("success"):
                                        placed += 1
                                        # Log real trade to DB
                                        try:
                                            from data.database import get_conn as _gc
                                            _conn = _gc()
                                            _c    = _conn.cursor()
                                            _c.execute("""
                                                INSERT INTO paper_trades
                                                (trade_date, market_id, question, city, entry_price,
                                                 noaa_forecast_f, predicted_range, size, capital_at_entry)
                                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                            """, (
                                                __import__('datetime').date.today().isoformat(),
                                                sig["market_id"],
                                                sig["question"],
                                                sig["city"],
                                                sig["entry_price"],
                                                sig.get("forecast") or 0.0,
                                                f"REAL:${result.get('bet_size',1.0)}",
                                                result.get("bet_size", 1.0),
                                                0.0,
                                            ))
                                            _conn.commit()
                                            _conn.close()
                                        except Exception as _e:
                                            print(f"[REAL LOG ERR] {_e}")
                                    time.sleep(0.5)
                                print(f"[REAL] {placed} real money orders placed")
                        except Exception as e3:
                            print(f"[REAL] Error: {e3}")
                except Exception as e:
                    print(f"[SCHEDULER] Morning error: {e}")

        # Once per day at 7 AM UTC: log forecasts (rate limit protection)
        if minute < 5 and hour == 7:
            try:
                from forecast_logger import log_all_forecasts
                log_all_forecasts()
                try:
                    from forecast_logger import fill_wu_actuals
                    fill_wu_actuals()
                except Exception:
                    pass
                print(f"[SCHEDULER] Forecasts logged at {hour}:00 UTC")
            except Exception as e:
                print(f"[SCHEDULER] Forecast log error: {e}")

            # Rebuild signals cache
            try:
                import threading, json as _json
                def _rebuild_signals():
                    from strategy.early_entry import get_early_signals
                    from datetime import datetime, timezone as _tz, timedelta as _td
                    result = get_early_signals()
                    signals = result[0] if isinstance(result, tuple) else result
                    est_now = datetime.now(_tz(_td(hours=-4))).strftime("%Y-%m-%d %I:%M %p EST")
                    conn2 = get_conn()
                    c2 = conn2.cursor()
                    c2.execute("""INSERT INTO cache (key, value, updated_at) VALUES ('early_signals', %s, %s)
                        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at""",
                        (_json.dumps(signals), est_now))
                    conn2.commit(); conn2.close()
                    print(f"[SCHEDULER] Signals cached: {len(signals)} at {est_now}")
                threading.Thread(target=_rebuild_signals, daemon=True).start()
            except Exception as e:
                print(f"[SCHEDULER] Signals cache error: {e}")

            # Rebuild city cards cache
            try:
                from city_cards import get_all_city_cards
                import json as _json
                cards = get_all_city_cards(days_out=4)
                conn  = get_conn()
                c     = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TEXT
                    )
                """)
                from datetime import datetime, timezone as _tz, timedelta as _td
                est_now = datetime.now(_tz(_td(hours=-4))).strftime("%Y-%m-%d %I:%M %p EST")
                c.execute("""
                    INSERT INTO cache (key, value, updated_at)
                    VALUES ('city_cards', %s, %s)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """, (_json.dumps(cards), est_now))
                conn.commit()
                conn.close()
                print(f"[SCHEDULER] City cards cached at {est_now}")
            except Exception as e:
                print(f"[SCHEDULER] City cards cache error: {e}")

        # Every 30 min: paper scan + check outcomes
        check_key = f"{today}-{hour}"  # scan once per hour to stay within Open-Meteo free tier
        if last_outcome != check_key:
            # Run paper trading scan
            try:
                import threading as _thr
                def _paper_scan():
                    try:
                        from strategy.paper_trade import run_scan, init_tables
                        init_tables()
                        trades, summary = run_scan()
                        print(f"[SCHEDULER] Paper scan done — {trades} trades in {summary.get('cities_bought', [])}")
                    except Exception as e:
                        print(f"[SCHEDULER] Paper scan error: {e}")
                _thr.Thread(target=_paper_scan, daemon=True).start()
            except Exception as e:
                print(f"[SCHEDULER] Paper scan thread error: {e}")

            # Check outcomes on pending trades
            try:
                from strategy.paper_trade import check_outcomes
                resolved = check_outcomes()
                if resolved > 0:
                    print(f"[SCHEDULER] Resolved {resolved} trades")
                last_outcome = check_key
            except Exception as e:
                print(f"[SCHEDULER] Outcome check error: {e}")
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

        # Keep-alive self-ping every 5 minutes to prevent Railway from sleeping
        if minute % 5 == 0:
            try:
                import urllib.request
                port = os.environ.get("PORT", "8080")
                urllib.request.urlopen(
                    f"http://localhost:{port}/health",
                    timeout=5
                )
            except Exception:
                pass  # silent — just keeping the process warm

        time.sleep(30)


def run_backtest_all_background():
    global backtest_status
    backtest_status["running"] = True
    backtest_status["done"]    = False
    backtest_status["result"]  = None
    try:
        from strategy.backtest_all import run_all_backtests
        result = run_all_backtests()
        backtest_status["result"] = result
        print(f"[BACKTEST] Done")
    except Exception as e:
        backtest_status["result"] = {"error": str(e)}
        print(f"[BACKTEST] Error: {e}")
    finally:
        backtest_status["running"] = False
        backtest_status["done"]    = True


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


@app.get("/")
def serve_dashboard():
    """Serve the PolyEdge dashboard"""
    from fastapi.responses import HTMLResponse
    try:
        with open("dashboard.html", "r") as f:
            return HTMLResponse(content=f.read())
    except:
        return HTMLResponse(content="<h1>Dashboard not found</h1>")

@app.api_route("/health", methods=["GET", "POST", "HEAD"])
def health():
    try:
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
    except Exception as e:
        return {"status": "degraded", "error": str(e), "ingest": ingest_status}


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
                     FROM paper_trades
                     WHERE predicted_range LIKE 'REAL:%'
                     ORDER BY trade_date DESC, id DESC""")
        trades = [dict(r) for r in c.fetchall()]
        conn.close()
        return trades
    except Exception:
        return []


@app.get("/performance")
def get_performance():
    """Real money P&L only."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END) as total_pnl,
                   MAX(pnl) as best_trade,
                   MIN(pnl) as worst_trade
            FROM paper_trades
            WHERE predicted_range LIKE 'REAL:%'
        """)
        row = c.fetchone()
        c.execute("""
            SELECT trade_date, city, question, entry_price, size,
                   predicted_range, outcome, pnl
            FROM paper_trades
            WHERE predicted_range LIKE 'REAL:%'
            ORDER BY trade_date DESC, id DESC
        """)
        trades = [dict(r) for r in c.fetchall()]
        conn.close()

        total    = row["total"] or 0
        wins     = row["wins"] or 0
        total_pnl = float(row["total_pnl"] or 0)
        spent    = total * 1.0  # $1 per bet
        roi      = round((total_pnl / spent * 100), 2) if spent > 0 else 0

        return {
            "total_bets":    total,
            "wins":          wins,
            "win_rate":      round(wins / total * 100, 2) if total > 0 else 0,
            "total_pnl":     round(total_pnl, 2),
            "amount_spent":  spent,
            "roi":           roi,
            "best_trade":    float(row["best_trade"] or 0),
            "worst_trade":   float(row["worst_trade"] or 0),
            "trades":        trades,
            "mode":          "REAL",
        }
    except Exception as e:
        return {"total_bets": 0, "wins": 0, "win_rate": 0,
                "total_pnl": 0, "amount_spent": 0, "roi": 0,
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

# old /backtest endpoint removed — replaced by new comprehensive backtest above


@app.get("/market-times")
def market_times():
    """
    Checks real market open times from our DB for multiple cities.
    Converts to EST. Checks multiple days to confirm the pattern.
    This tells us EXACTLY when to place bets.
    """
    try:
        from datetime import datetime, timezone, timedelta
        EST = timezone(timedelta(hours=-4))

        conn = get_conn()
        c    = conn.cursor()

        # Get last 14 days of markets for London and NYC
        # Check created_at to find open time pattern
        c.execute("""
            SELECT city, question,
                   TO_CHAR(TO_TIMESTAMP(created_at), 'YYYY-MM-DD HH24:MI:SS') as open_utc,
                   TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date,
                   created_at,
                   (resolved_at - created_at) / 86400 as days_open
            FROM markets
            WHERE city IN ('London', 'NYC', 'New York City', 'Paris', 'Tokyo', 'Seoul')
            AND created_at > 0
            AND outcome IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 60
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Convert to EST and find pattern
        results = {}
        for r in rows:
            city = r["city"]
            if city not in results:
                results[city] = []

            ts     = r["created_at"]
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_est = dt_utc.astimezone(EST)

            results[city].append({
                "question":    r["question"][:50],
                "open_utc":    dt_utc.strftime("%Y-%m-%d %H:%M UTC"),
                "open_est":    dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
                "open_hour_utc": dt_utc.hour,
                "open_hour_est": dt_est.hour,
                "res_date":    r["res_date"],
                "days_open":   round(float(r["days_open"]), 1),
            })

        # Find the consistent open time per city
        summary = {}
        for city, markets in results.items():
            hours_utc = [m["open_hour_utc"] for m in markets]
            hours_est = [m["open_hour_est"] for m in markets]
            from collections import Counter
            most_common_utc = Counter(hours_utc).most_common(1)[0][0] if hours_utc else None
            most_common_est = Counter(hours_est).most_common(1)[0][0] if hours_est else None

            summary[city] = {
                "most_common_open_hour_utc": most_common_utc,
                "most_common_open_hour_est": most_common_est,
                "open_est_formatted": f"{most_common_est}:00 {'AM' if most_common_est < 12 else 'PM'} EST" if most_common_est is not None else "unknown",
                "sample_markets": markets[:5],
            }

        return {
            "summary": summary,
            "note": "Based on real created_at timestamps from our DB"
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


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
        city = city_name.replace("-", " ").title()
        if city not in CITY_CONFIGS:
            return {"error": f"City '{city}' not found. Options: {list(CITY_CONFIGS.keys())}"}
        return run_city_backtest(city)
    except Exception as e:
        return {"error": str(e)}


@app.get("/backtest/all")
def run_backtest_all():
    """Start full backtest in background. Check /backtest/all/status for results."""
    global backtest_status
    if backtest_status["running"]:
        return {"status": "already_running", "message": "Check /backtest/all/status"}
    threading.Thread(target=run_backtest_all_background, daemon=True).start()
    return {"status": "started", "message": "Backtest running in background. Check /backtest/all/status for results."}


@app.get("/backtest/all/status")
def backtest_all_status():
    """Check backtest progress and results."""
    return backtest_status


# ── Honda Civic Strategy Backtest ─────────────────────────────────────────────

honda_status = {"running": False, "done": False, "result": None}


def run_honda_background():
    global honda_status
    honda_status["running"] = True
    honda_status["done"]    = False
    honda_status["result"]  = None
    try:
        from strategy.backtest_honda import run_honda_backtest
        result = run_honda_backtest()
        honda_status["result"] = result
        print(f"[HONDA] Done")
    except Exception as e:
        honda_status["result"] = {"error": str(e)}
        print(f"[HONDA] Error: {e}")
    finally:
        honda_status["running"] = False
        honda_status["done"]    = True


@app.get("/early-signals")
def early_signals(refresh: bool = False):
    """
    Returns signals from cache instantly.
    Cache is rebuilt every 6 hours by scheduler.
    Pass ?refresh=true to trigger a background rebuild.
    """
    import json as _json
    import threading

    def rebuild_cache():
        try:
            import sys, os
            sys.path.insert(0, os.path.dirname(__file__))
            from strategy.early_entry import get_early_signals
            result = get_early_signals()
            signals = result[0] if isinstance(result, tuple) else result
            from datetime import datetime, timezone as _tz, timedelta as _td
            est_now = datetime.now(_tz(_td(hours=-4))).strftime("%Y-%m-%d %I:%M %p EST")
            conn = get_conn()
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
                )
            """)
            c.execute("""
                INSERT INTO cache (key, value, updated_at)
                VALUES ('early_signals', %s, %s)
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
            """, (_json.dumps(signals), est_now))
            conn.commit()
            conn.close()
            print(f"[SIGNALS] Cache rebuilt at {est_now} — {len(signals)} signals")
        except Exception as e:
            print(f"[SIGNALS] Cache rebuild error: {e}")

    # Trigger background rebuild if requested
    if refresh:
        t = threading.Thread(target=rebuild_cache, daemon=True)
        t.start()
        return {"status": "rebuilding", "message": "Signals rebuilding in background — check back in 3 minutes"}

    # Serve from cache
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT value, updated_at FROM cache WHERE key='early_signals'")
        row = c.fetchone()
        conn.close()
        if row:
            signals = _json.loads(row["value"])
            return {
                "signals":      signals,
                "total":        len(signals),
                "from_cache":   True,
                "updated_at":   row["updated_at"],
                "high_conf":    [s for s in signals if isinstance(s, dict) and s.get("confidence", 0) >= 80],
            }
    except Exception as e:
        print(f"[SIGNALS] Cache read error: {e}")

    # No cache yet — trigger build and tell user
    t = threading.Thread(target=rebuild_cache, daemon=True)
    t.start()

    # Also ensure cache table exists
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
            )
        """)
        conn.commit()
        conn.close()
    except Exception:
        pass

    return {
        "signals":    [],
        "total":      0,
        "from_cache": False,
        "status":     "building",
        "message":    "Building now — check back in 3 minutes"
    }


@app.api_route("/signals/save", methods=["GET", "POST"])
def save_signal(market_id: str, city: str, question: str, 
                entry_price: float, forecast: float, days_out: int):
    """Save a signal to track whether it hits or not."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS tracked_signals (
                id SERIAL PRIMARY KEY,
                saved_at TEXT,
                market_id TEXT,
                city TEXT,
                question TEXT,
                entry_price REAL,
                forecast REAL,
                days_out INT,
                outcome TEXT,
                pnl REAL,
                resolved_at TEXT
            )
        """)
        c.execute("""
            INSERT INTO tracked_signals 
            (saved_at, market_id, city, question, entry_price, forecast, days_out)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            __import__('datetime').date.today().isoformat(),
            market_id, city, question, entry_price, forecast, days_out
        ))
        conn.commit()
        conn.close()
        return {"status": "saved", "market_id": market_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/signals/tracked")
def get_tracked_signals():
    """Get all tracked signals with outcomes."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        
        # Create table if not exists
        c.execute("""
            CREATE TABLE IF NOT EXISTS tracked_signals (
                id SERIAL PRIMARY KEY,
                saved_at TEXT,
                market_id TEXT,
                city TEXT,
                question TEXT,
                entry_price REAL,
                forecast REAL,
                days_out INT,
                outcome TEXT,
                pnl REAL,
                resolved_at TEXT
            )
        """)

        # Auto-resolve any pending signals
        c.execute("""
            SELECT id, market_id, entry_price FROM tracked_signals 
            WHERE outcome IS NULL
        """)
        pending = c.fetchall()
        
        import requests as req
        for row in pending:
            try:
                r = req.get(f"https://gamma-api.polymarket.com/markets/{row['market_id']}", timeout=10)
                if r.status_code == 200:
                    data   = r.json()
                    closed = data.get("closed", False)
                    prices = data.get("outcomePrices", "[]")
                    if isinstance(prices, str):
                        import json as j
                        prices = j.loads(prices)
                    if closed and prices:
                        outcome = "Yes" if str(prices[0]) == "1" else "No"
                        pnl     = round((1.0 / row["entry_price"]) * 1.0 - 1.0, 2) if outcome == "Yes" else -1.0
                        c.execute("""
                            UPDATE tracked_signals 
                            SET outcome=%s, pnl=%s, resolved_at=%s 
                            WHERE id=%s
                        """, (outcome, pnl, __import__('datetime').date.today().isoformat(), row["id"]))
            except Exception:
                pass
        
        conn.commit()

        # Get all signals
        c.execute("""
            SELECT * FROM tracked_signals 
            ORDER BY saved_at DESC, id DESC
        """)
        signals = [dict(r) for r in c.fetchall()]

        # Stats
        resolved = [s for s in signals if s["outcome"]]
        wins     = [s for s in resolved if s["outcome"] == "Yes"]
        total_pnl = sum(s["pnl"] or 0 for s in resolved)

        conn.close()
        return {
            "signals":    signals,
            "total":      len(signals),
            "resolved":   len(resolved),
            "wins":       len(wins),
            "win_rate":   round(len(wins)/len(resolved)*100, 1) if resolved else 0,
            "total_pnl":  round(total_pnl, 2),
            "pending":    len(signals) - len(resolved),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Accuracy Backtest ─────────────────────────────────────────────────────

@app.get("/market-times")
def market_times():
    """
    Checks real market open times from our DB for multiple cities.
    Converts to EST. Checks multiple days to confirm the pattern.
    This tells us EXACTLY when to place bets.
    """
    try:
        from datetime import datetime, timezone, timedelta
        EST = timezone(timedelta(hours=-4))

        conn = get_conn()
        c    = conn.cursor()

        # Get last 14 days of markets for London and NYC
        # Check created_at to find open time pattern
        c.execute("""
            SELECT city, question,
                   TO_CHAR(TO_TIMESTAMP(created_at), 'YYYY-MM-DD HH24:MI:SS') as open_utc,
                   TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date,
                   created_at,
                   (resolved_at - created_at) / 86400 as days_open
            FROM markets
            WHERE city IN ('London', 'NYC', 'New York City', 'Paris', 'Tokyo', 'Seoul')
            AND created_at > 0
            AND outcome IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 60
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Convert to EST and find pattern
        results = {}
        for r in rows:
            city = r["city"]
            if city not in results:
                results[city] = []

            ts     = r["created_at"]
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_est = dt_utc.astimezone(EST)

            results[city].append({
                "question":    r["question"][:50],
                "open_utc":    dt_utc.strftime("%Y-%m-%d %H:%M UTC"),
                "open_est":    dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
                "open_hour_utc": dt_utc.hour,
                "open_hour_est": dt_est.hour,
                "res_date":    r["res_date"],
                "days_open":   round(float(r["days_open"]), 1),
            })

        # Find the consistent open time per city
        summary = {}
        for city, markets in results.items():
            hours_utc = [m["open_hour_utc"] for m in markets]
            hours_est = [m["open_hour_est"] for m in markets]
            from collections import Counter
            most_common_utc = Counter(hours_utc).most_common(1)[0][0] if hours_utc else None
            most_common_est = Counter(hours_est).most_common(1)[0][0] if hours_est else None

            summary[city] = {
                "most_common_open_hour_utc": most_common_utc,
                "most_common_open_hour_est": most_common_est,
                "open_est_formatted": f"{most_common_est}:00 {'AM' if most_common_est < 12 else 'PM'} EST" if most_common_est is not None else "unknown",
                "sample_markets": markets[:5],
            }

        return {
            "summary": summary,
            "note": "Based on real created_at timestamps from our DB"
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/journey")
def backtest_journey(days: int = 30, safety_nets: int = 2, city: str = None):
    """
    Complete price journey backtest.
    Shows every price tick from open to resolution in EST.
    Shows exactly how long the buy window is before price moves.
    100% real data — Polymarket CLOB + WU temps.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_price_journey import run_price_journey_backtest
        cities = [city] if city else None
        return run_price_journey_backtest(
            days_back=days,
            safety_nets=safety_nets,
            cities=cities,
        )
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/v2")
def backtest_v2(days: int = 30, safety_nets: int = 2):
    """
    100% Real Data Backtest v2.
    Uses real Polymarket price history + real WU temps.
    Tests: buy forecast range + safety_nets on each side.
    Shows exact market open times in EST.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_real import run_backtest
        return run_backtest(days_back=days, safety_nets=safety_nets)
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/forecast/log")
def get_forecast_log(city: str = None, days: int = 7):
    """
    Show logged forecasts with timestamps.
    Lets you see how model accuracy changes
    from 4 days out to 1 day out.
    """
    try:
        conn = get_conn()
        c    = conn.cursor()
        if city:
            c.execute("""
                SELECT * FROM forecast_log
                WHERE city = %s
                AND target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY target_date, logged_at_utc
            """, (city, days))
        else:
            c.execute("""
                SELECT * FROM forecast_log
                WHERE target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY city, target_date, logged_at_utc
                LIMIT 500
            """, (days,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return {"count": len(rows), "logs": rows}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/backtest/real")
def backtest_real(days: int = 30, max_price: float = 0.05, min_days: int = 3):
    """
    100% REAL DATA backtest.
    - Uses actual Polymarket price history (opening price + time in EST)
    - Uses actual Weather Underground temps (same source Polymarket uses)
    - No simulations. No estimates.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from price_history_backtest import run_backtest
        return run_backtest(
            days_back=days,
            max_open_price=max_price,
            min_days_before=min_days,
        )
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/early-entry")
def backtest_early_entry(days: int = 30, window: float = 2.0, max_price: float = 0.05):
    """
    Backtest our actual early entry strategy.
    Finds markets that were cheap (under max_price) AND 
    where actual temp was within window degrees of the range.
    Shows if we would have won.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_early_entry import run_backtest
        result = run_backtest(
            days_back=days,
            entry_price_max=max_price,
            forecast_window=window
        )
        return result
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/honda")
def backtest_honda(days: int = 30):
    """
    Backtest HondaCivic strategy:
    - FORECAST bets: $10 on range where forecast lands (10-60¢)
    - LOTTERY bets: $2 on extreme cheap ranges under 5¢
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_honda_v2 import run_backtest
        result = run_backtest(days_back=days)

        forecast = result["forecast_bets"]
        lottery  = result["lottery_bets"]
        all_bets = forecast + lottery

        f_wins = [b for b in forecast if b["outcome"] == "Yes"]
        l_wins = [b for b in lottery  if b["outcome"] == "Yes"]

        return {
            "days":           days,
            "forecast": {
                "bets":     len(forecast),
                "wins":     len(f_wins),
                "win_rate": round(len(f_wins)/len(forecast)*100, 1) if forecast else 0,
                "total_pnl": round(sum(b["pnl"] for b in forecast), 2),
                "spent":    round(sum(b["bet_size"] for b in forecast), 2),
                "daily":    round(sum(b["pnl"] for b in forecast)/days, 2),
                "results":  sorted(forecast, key=lambda x: -x["pnl"])[:20],
            },
            "lottery": {
                "bets":     len(lottery),
                "wins":     len(l_wins),
                "win_rate": round(len(l_wins)/len(lottery)*100, 1) if lottery else 0,
                "total_pnl": round(sum(b["pnl"] for b in lottery), 2),
                "spent":    round(sum(b["bet_size"] for b in lottery), 2),
                "daily":    round(sum(b["pnl"] for b in lottery)/days, 2),
                "results":  sorted(l_wins, key=lambda x: -x["pnl"])[:10],
            },
            "combined": {
                "total_pnl": result["total_pnl"],
                "total_spent": result["total_spent"],
                "daily_avg": result["daily_avg"],
            }
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/accuracy")
def backtest_accuracy(days: int = 7):
    """
    Backtest signal accuracy over the last N days.
    Uses historical Open-Meteo data to simulate what our signals would have been,
    then checks actual market outcomes to see win/loss rate.
    """
    import requests as req
    from datetime import date, timedelta
    import re

    try:
        conn = get_conn()
        c    = conn.cursor()

        # Get all resolved markets from last N days
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        c.execute("""
            SELECT m.id, m.question, m.city, m.target_low, m.target_high,
                   m.market_type, m.unit, m.outcome, m.last_trade_price,
                   TO_TIMESTAMP(m.resolved_at) as resolved_date
            FROM markets m
            WHERE m.outcome IN ('Yes', 'No')
            AND TO_TIMESTAMP(m.resolved_at)::date >= %s::date
            AND m.last_trade_price > 0
            AND m.last_trade_price <= 0.05
            ORDER BY m.resolved_at DESC
            LIMIT 200
        """, (cutoff,))
        markets = [dict(r) for r in c.fetchall()]
        conn.close()

        if not markets:
            return {"status": "no_data", "message": "No resolved markets found in last 7 days"}

        # City coordinates for historical forecast lookup
        CITY_COORDS = {
            "London":       {"lat": 51.5074,  "lon": -0.1278,   "temp_unit": "celsius"},
            "NYC":          {"lat": 40.7128,  "lon": -74.0060,  "temp_unit": "fahrenheit"},
            "Toronto":      {"lat": 43.6532,  "lon": -79.3832,  "temp_unit": "celsius"},
            "Paris":        {"lat": 48.8566,  "lon": 2.3522,    "temp_unit": "celsius"},
            "Dallas":       {"lat": 32.7767,  "lon": -96.7970,  "temp_unit": "fahrenheit"},
            "Atlanta":      {"lat": 33.7490,  "lon": -84.3880,  "temp_unit": "fahrenheit"},
            "Seattle":      {"lat": 47.6062,  "lon": -122.3321, "temp_unit": "fahrenheit"},
            "Miami":        {"lat": 25.7617,  "lon": -80.1918,  "temp_unit": "fahrenheit"},
            "Chicago":      {"lat": 41.8781,  "lon": -87.6298,  "temp_unit": "fahrenheit"},
            "New York City":{"lat": 40.7128,  "lon": -74.0060,  "temp_unit": "fahrenheit"},
        }

        results    = []
        would_bet  = 0
        would_win  = 0
        would_lose = 0
        total_pnl  = 0.0

        for m in markets[:50]:  # limit to 50 for speed
            city   = m.get("city", "")
            coords = CITY_COORDS.get(city)
            if not coords:
                continue

            # Get resolved date
            res_date = m["resolved_date"]
            if hasattr(res_date, 'date'):
                res_date = res_date.date()
            date_str = str(res_date)[:10]

            # Get historical actual temperature from Open-Meteo
            try:
                r = req.get("https://api.open-meteo.com/v1/forecast", params={
                    "latitude":         coords["lat"],
                    "longitude":        coords["lon"],
                    "daily":            "temperature_2m_max",
                    "temperature_unit": coords["temp_unit"],
                    "timezone":         "auto",
                    "start_date":       date_str,
                    "end_date":         date_str,
                    "models":           "gfs_global",
                }, timeout=10)
                if r.status_code != 200:
                    continue
                temps = r.json().get("daily", {}).get("temperature_2m_max", [])
                if not temps or temps[0] is None:
                    continue
                actual_temp = float(temps[0])
            except Exception:
                continue

            # Would our signal have selected this market?
            # Check if actual_temp is within FORECAST_WINDOW of market target
            target_low  = m["target_low"]
            target_high = m["target_high"]
            market_type = m["market_type"]
            WINDOW = 4

            if market_type == "exact":
                in_window = abs(actual_temp - target_low) <= WINDOW
            elif market_type == "range":
                mid = (target_low + target_high) / 2
                in_window = abs(mid - actual_temp) <= WINDOW
            elif market_type == "above":
                in_window = actual_temp >= target_low - WINDOW
            elif market_type == "below":
                in_window = actual_temp <= target_high + WINDOW
            else:
                in_window = False

            if not in_window:
                continue

            # We would have bet on this
            would_bet += 1
            price  = float(m["last_trade_price"])
            outcome = m["outcome"]

            if outcome == "Yes":
                would_win += 1
                pnl = round((1.0 / price) * 1.0 - 1.0, 2)
                total_pnl += pnl
            else:
                would_lose += 1
                pnl = -1.0
                total_pnl += pnl

            results.append({
                "city":     city,
                "question": m["question"][:60],
                "date":     date_str,
                "price":    round(price * 100, 1),
                "actual_temp": actual_temp,
                "outcome":  outcome,
                "pnl":      round(pnl, 2),
            })

        win_rate = round(would_win / would_bet * 100, 1) if would_bet > 0 else 0

        return {
            "days":        days,
            "markets_checked": len(markets),
            "would_have_bet":  would_bet,
            "wins":            would_win,
            "losses":          would_lose,
            "win_rate":        win_rate,
            "total_pnl_per_dollar": round(total_pnl, 2),
            "total_pnl_per_10":     round(total_pnl * 10, 2),
            "results":         results[:30],
        }

    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/signals-dashboard", response_class=HTMLResponse)
def signals_dashboard():
    """Live signals dashboard — best bets ranked by forecast match."""
    try:
        with open("signals_dashboard.html") as f:
            return f.read()
    except Exception as e:
        return f"<h1>Error: {e}</h1>"


@app.get("/morning/early")
def run_early_trades():
    """Place early entry paper trades manually."""
    try:
        from strategy.early_entry import place_early_trades
        result = place_early_trades()
        return result
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


# ── Reset / Clear Trades ──────────────────────────────────────────────────

@app.get("/admin/clear-paper-trades")
def clear_paper_trades():
    """Clear all paper trades to start fresh with real money tracking."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM paper_trades")
        count = c.fetchone()["count"]
        c.execute("DELETE FROM paper_trades")
        conn.commit()
        conn.close()
        return {"status": "cleared", "trades_removed": count, "message": "Ready for real money tracking"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/admin/clear-paper-only")
def clear_paper_only():
    """Clear only paper trades (EARLY: and paper), keep real money trades."""
    try:
        conn = get_conn()
        c    = conn.cursor()
        # Keep trades that have real order IDs (real money)
        # Paper trades have predicted_range starting with EARLY: or have no order_id
        c.execute("SELECT COUNT(*) as count FROM paper_trades WHERE predicted_range LIKE 'EARLY:%' OR predicted_range LIKE '%.0F' OR predicted_range LIKE '%.0-%'")
        count = c.fetchone()["count"]
        c.execute("DELETE FROM paper_trades WHERE predicted_range LIKE 'EARLY:%' OR predicted_range LIKE '%.0F' OR predicted_range LIKE '%.0-%'")
        conn.commit()
        conn.close()
        return {"status": "cleared", "paper_trades_removed": count, "message": "Paper trades cleared — real trades preserved"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Real Money Trading ────────────────────────────────────────────────────

@app.get("/trade/run-now")
def run_real_trades_now():
    """Manually trigger real money trades right now — no need to wait for 7AM."""
    try:
        import os
        if os.getenv("TRADING_MODE") != "real":
            return {"status": "error", "message": "TRADING_MODE is not real"}
        
        from strategy.polymarket_client import is_real_mode, place_real_order
        from strategy.early_entry import get_early_signals

        if not is_real_mode():
            return {"status": "error", "message": "Not in real mode"}

        signals, _ = get_early_signals()
        placed  = 0
        failed  = 0
        results = []

        # Sort by forecast proximity — best matches first
        import re as _re
        def _score(s):
            q = (s.get("question","")).lower()
            nums = [float(x) for x in _re.findall(r"-?\d+\.?\d*", q) if -50 < float(x) < 150]
            if not nums: return 0
            target = sum(nums)/len(nums)
            gap = abs(target - (s.get("forecast") or 0))
            return -gap
        signals = sorted(signals, key=_score, reverse=True)

        for sig in signals[:3]:
            result = place_real_order(
                sig["market_id"],
                sig["question"],
                sig["city"],
                sig["entry_price"]
            )
            if result.get("success"):
                placed += 1
                # Log to DB
                try:
                    import datetime
                    conn = get_conn()
                    c    = conn.cursor()
                    c.execute("""
                        INSERT INTO paper_trades
                        (trade_date, market_id, question, city, entry_price,
                         noaa_forecast_f, predicted_range, size, capital_at_entry)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        datetime.date.today().isoformat(),
                        sig["market_id"], sig["question"], sig["city"],
                        sig["entry_price"], sig.get("forecast") or 0.0,
                        f"REAL:${result.get('bet_size',1.0)}",
                        result.get("bet_size", 1.0), 0.0,
                    ))
                    conn.commit()
                    conn.close()
                except Exception as e:
                    print(f"[LOG ERR] {e}")
            else:
                failed += 1
            results.append({"city": sig["city"], "success": result.get("success"), "error": result.get("error","")})

        return {
            "status":  "done",
            "placed":  placed,
            "failed":  failed,
            "signals": len(signals),
            "results": results[:10],
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}


@app.get("/trade/test")
def test_real_connection():
    """Test real money connection without placing orders."""
    try:
        from strategy.polymarket_client import test_connection
        return test_connection()
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/trade/real")
def place_real_trade(market_id: str, city: str = "", question: str = "", price: float = 0.01):
    """
    Place a single real money trade on Polymarket.
    Usage: /trade/real?market_id=123456&city=London&question=...&price=0.005
    """
    try:
        from strategy.polymarket_client import place_real_order
        result = place_real_order(market_id, question, city, price)
        return result
    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "trace": traceback.format_exc()}


@app.get("/trade/mode")
def trading_mode():
    """Check current trading mode (paper or real)."""
    import os
    mode     = os.getenv("TRADING_MODE", "paper")
    wallet   = os.getenv("POLYMARKET_WALLET", "")
    bet_size = os.getenv("BET_SIZE_REAL", "1.0")
    has_key  = bool(os.getenv("POLYMARKET_PRIVATE_KEY", ""))
    return {
        "mode":       mode,
        "wallet":     wallet[:10] + "..." if wallet else "not set",
        "has_key":    has_key,
        "bet_size":   bet_size,
        "ready":      mode == "real" and has_key and bool(wallet),
    }


@app.get("/backtest/honda/test")
def test_honda():
    """Dry run — test Honda backtest logic on 5 markets before full run."""
    try:
        conn = get_conn()
        c = conn.cursor()

        # Get 3 Yes outcomes + 3 No outcomes with snapshots
        c.execute("""
            SELECT m.id, m.question, m.city, m.outcome, m.resolved_at, m.created_at
            FROM markets m
            WHERE m.outcome = 'Yes'
            AND EXISTS (SELECT 1 FROM price_snapshots p WHERE p.market_id = m.id)
            LIMIT 3
        """)
        yes_markets = [dict(r) for r in c.fetchall()]

        c.execute("""
            SELECT m.id, m.question, m.city, m.outcome, m.resolved_at, m.created_at
            FROM markets m
            WHERE m.outcome = 'No'
            AND EXISTS (SELECT 1 FROM price_snapshots p WHERE p.market_id = m.id)
            LIMIT 3
        """)
        no_markets = [dict(r) for r in c.fetchall()]
        markets = yes_markets + no_markets

        results = []
        for m in markets:
            c.execute("""
                SELECT timestamp, yes_price FROM price_snapshots
                WHERE market_id = %s ORDER BY timestamp ASC
            """, (m["id"],))
            history = [(r["timestamp"], r["yes_price"]) for r in c.fetchall()]

            resolved_at = m.get("resolved_at", 0)
            pre_res = [(t, p) for t, p in history if t < resolved_at] if resolved_at else history
            last_price = pre_res[-1][1] if pre_res else None
            first_price = history[0][1] if history else None
            min_price = min(p for t, p in history) if history else None

            # Filter to live trading prices only (exclude post-resolution 0.001/0.999)
            pre_res   = [(t, p) for t, p in history if 0.001 < p < 0.999]
            pre_min   = min(p for t, p in pre_res) if pre_res else None
            pre_max   = max(p for t, p in pre_res) if pre_res else None
            pre_first = pre_res[0][1] if pre_res else None

            results.append({
                "market_id":         m["id"],
                "city":              m["city"],
                "question":          m["question"][:60] if m["question"] else "",
                "outcome":           m["outcome"],
                "total_snapshots":   len(history),
                "pre_res_snapshots": len(pre_res),
                "pre_min_price":     pre_min,
                "pre_max_price":     pre_max,
                "pre_first_price":   pre_first,
                "arb_candidate":     pre_max is not None and pre_max >= 0.95,
                "spec_candidate":    pre_min is not None and pre_min <= 0.05,
                "mm_candidate":      pre_min is not None and pre_min <= 0.20 and pre_max is not None and pre_max >= 0.80,
            })

        conn.close()
        return {
            "status": "ok",
            "markets_tested": len(results),
            "results": results,
            "ready_for_backtest": len(results) > 0
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@app.get("/market-times")
def market_times():
    """
    Checks real market open times from our DB for multiple cities.
    Converts to EST. Checks multiple days to confirm the pattern.
    This tells us EXACTLY when to place bets.
    """
    try:
        from datetime import datetime, timezone, timedelta
        EST = timezone(timedelta(hours=-4))

        conn = get_conn()
        c    = conn.cursor()

        # Get last 14 days of markets for London and NYC
        # Check created_at to find open time pattern
        c.execute("""
            SELECT city, question,
                   TO_CHAR(TO_TIMESTAMP(created_at), 'YYYY-MM-DD HH24:MI:SS') as open_utc,
                   TO_CHAR(TO_TIMESTAMP(resolved_at), 'YYYY-MM-DD') as res_date,
                   created_at,
                   (resolved_at - created_at) / 86400 as days_open
            FROM markets
            WHERE city IN ('London', 'NYC', 'New York City', 'Paris', 'Tokyo', 'Seoul')
            AND created_at > 0
            AND outcome IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 60
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Convert to EST and find pattern
        results = {}
        for r in rows:
            city = r["city"]
            if city not in results:
                results[city] = []

            ts     = r["created_at"]
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_est = dt_utc.astimezone(EST)

            results[city].append({
                "question":    r["question"][:50],
                "open_utc":    dt_utc.strftime("%Y-%m-%d %H:%M UTC"),
                "open_est":    dt_est.strftime("%Y-%m-%d %I:%M %p EST"),
                "open_hour_utc": dt_utc.hour,
                "open_hour_est": dt_est.hour,
                "res_date":    r["res_date"],
                "days_open":   round(float(r["days_open"]), 1),
            })

        # Find the consistent open time per city
        summary = {}
        for city, markets in results.items():
            hours_utc = [m["open_hour_utc"] for m in markets]
            hours_est = [m["open_hour_est"] for m in markets]
            from collections import Counter
            most_common_utc = Counter(hours_utc).most_common(1)[0][0] if hours_utc else None
            most_common_est = Counter(hours_est).most_common(1)[0][0] if hours_est else None

            summary[city] = {
                "most_common_open_hour_utc": most_common_utc,
                "most_common_open_hour_est": most_common_est,
                "open_est_formatted": f"{most_common_est}:00 {'AM' if most_common_est < 12 else 'PM'} EST" if most_common_est is not None else "unknown",
                "sample_markets": markets[:5],
            }

        return {
            "summary": summary,
            "note": "Based on real created_at timestamps from our DB"
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/journey")
def backtest_journey(days: int = 30, safety_nets: int = 2, city: str = None):
    """
    Complete price journey backtest.
    Shows every price tick from open to resolution in EST.
    Shows exactly how long the buy window is before price moves.
    100% real data — Polymarket CLOB + WU temps.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_price_journey import run_price_journey_backtest
        cities = [city] if city else None
        return run_price_journey_backtest(
            days_back=days,
            safety_nets=safety_nets,
            cities=cities,
        )
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/v2")
def backtest_v2(days: int = 30, safety_nets: int = 2):
    """
    100% Real Data Backtest v2.
    Uses real Polymarket price history + real WU temps.
    Tests: buy forecast range + safety_nets on each side.
    Shows exact market open times in EST.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_real import run_backtest
        return run_backtest(days_back=days, safety_nets=safety_nets)
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/forecast/log")
def get_forecast_log(city: str = None, days: int = 7):
    """
    Show logged forecasts with timestamps.
    Lets you see how model accuracy changes
    from 4 days out to 1 day out.
    """
    try:
        conn = get_conn()
        c    = conn.cursor()
        if city:
            c.execute("""
                SELECT * FROM forecast_log
                WHERE city = %s
                AND target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY target_date, logged_at_utc
            """, (city, days))
        else:
            c.execute("""
                SELECT * FROM forecast_log
                WHERE target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY city, target_date, logged_at_utc
                LIMIT 500
            """, (days,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return {"count": len(rows), "logs": rows}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/backtest/real")
def backtest_real(days: int = 30, max_price: float = 0.05, min_days: int = 3):
    """
    100% REAL DATA backtest.
    - Uses actual Polymarket price history (opening price + time in EST)
    - Uses actual Weather Underground temps (same source Polymarket uses)
    - No simulations. No estimates.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from price_history_backtest import run_backtest
        return run_backtest(
            days_back=days,
            max_open_price=max_price,
            min_days_before=min_days,
        )
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/early-entry")
def backtest_early_entry(days: int = 30, window: float = 2.0, max_price: float = 0.05):
    """
    Backtest our actual early entry strategy.
    Finds markets that were cheap (under max_price) AND 
    where actual temp was within window degrees of the range.
    Shows if we would have won.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest_early_entry import run_backtest
        result = run_backtest(
            days_back=days,
            entry_price_max=max_price,
            forecast_window=window
        )
        return result
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/backtest/honda")
def run_honda():
    """Start Honda Civic full strategy backtest in background."""
    global honda_status
    if honda_status["running"]:
        return {"status": "already_running", "message": "Check /backtest/honda/status"}
    threading.Thread(target=run_honda_background, daemon=True).start()
    return {"status": "started", "message": "Honda backtest running. Check /backtest/honda/status"}


@app.get("/backtest/honda/status")
def honda_backtest_status():
    """Check Honda backtest progress and results."""
    return honda_status


@app.get("/debug/db")
def debug_db():
    """Check what's actually in the DB."""
    try:
        conn = get_conn()
        c = conn.cursor()
        results = {}

        def count(query, params=None):
            if params:
                c.execute(query, params)
            else:
                c.execute(query)
            row = c.fetchone()
            return list(row.values())[0] if row else 0

        results["total_markets"] = count("SELECT COUNT(*) FROM markets")
        results["resolved"] = count("SELECT COUNT(*) FROM markets WHERE outcome IS NOT NULL")
        results["outcome_yes"] = count("SELECT COUNT(*) FROM markets WHERE outcome = %s", ("Yes",))
        results["outcome_no"] = count("SELECT COUNT(*) FROM markets WHERE outcome = %s", ("No",))
        results["has_price"] = count("SELECT COUNT(*) FROM markets WHERE last_trade_price > 0")
        results["resolved_with_price"] = count("SELECT COUNT(*) FROM markets WHERE outcome IS NOT NULL AND last_trade_price > 0")
        results["total_snapshots"] = count("SELECT COUNT(*) FROM price_snapshots")
        results["markets_with_snapshots"] = count("SELECT COUNT(DISTINCT market_id) FROM price_snapshots")

        c.execute("SELECT market_id FROM price_snapshots LIMIT 3")
        results["sample_snapshot_ids"] = [list(r.values())[0] for r in c.fetchall()]

        c.execute("SELECT id, outcome, last_trade_price FROM markets WHERE outcome IS NOT NULL LIMIT 3")
        results["sample_markets"] = [dict(r) for r in c.fetchall()]

        conn.close()
        return results
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@app.get("/backtest/all/{city_name}")
def run_backtest_all_city(city_name: str):
    """Real backtest for a single city with YES+NO. E.g. /backtest/all/London"""
    try:
        from strategy.backtest_all import run_city_backtest, CITY_CONFIGS
        # Handle various formats: "tel-aviv" -> "Tel Aviv", "sao-paulo" -> "Sao Paulo"
        city = city_name.replace("-", " ").title()
        # Fix special cases
        city = city.replace("Tel Aviv", "Tel Aviv")
        city = city.replace("Sao Paulo", "Sao Paulo")
        city = city.replace("Buenos Aires", "Buenos Aires")
        if city not in CITY_CONFIGS:
            # Try partial match
            matches = [c for c in CITY_CONFIGS.keys() 
                      if city.lower() in c.lower() or c.lower() in city.lower()]
            if len(matches) == 1:
                city = matches[0]
            else:
                return {"error": f"City not found. Options: {list(CITY_CONFIGS.keys())}"}
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


@app.get("/city-cards")
def city_cards_endpoint(days_out: int = 4, refresh: bool = False):
    """
    Get complete betting cards for all cities.
    Serves from cache instantly. Cache refreshes every 6 hours.
    Pass ?refresh=true to force rebuild.
    """
    import json as _json
    try:
        # Try cache first (unless forced refresh)
        if not refresh:
            try:
                conn = get_conn()
                c    = conn.cursor()
                c.execute("SELECT value, updated_at FROM cache WHERE key='city_cards'")
                row = c.fetchone()
                conn.close()
                if row:
                    return {
                        "days_out":   days_out,
                        "from_cache": True,
                        "updated_at": row["updated_at"],
                        "cards":      _json.loads(row["value"]),
                    }
            except Exception:
                pass

        # Cache miss or forced refresh — build fresh
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from city_cards import get_all_city_cards
        from datetime import datetime, timezone as _tz, timedelta as _td
        cards   = get_all_city_cards(days_out=days_out)
        est_now = datetime.now(_tz(_td(hours=-4))).strftime("%Y-%m-%d %I:%M %p EST")

        # Save to cache
        try:
            conn = get_conn()
            c    = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
                )
            """)
            c.execute("""
                INSERT INTO cache (key, value, updated_at)
                VALUES ('city_cards', %s, %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
            """, (_json.dumps(cards), est_now))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[CACHE] Save error: {e}")

        return {"days_out": days_out, "from_cache": False, "updated_at": est_now, "cards": cards}

    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/city-card/{city}")
def single_city_card(city: str, days_out: int = 4):
    """Get betting card for a single city."""
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from city_cards import get_city_card, ALL_CITIES
        config = ALL_CITIES.get(city)
        if not config:
            return {"error": f"City {city} not found"}
        return get_city_card(city, config, days_out=days_out)
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/forecast/log-now")
def log_forecasts_now():
    """Manually trigger forecast logging right now."""
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from forecast_logger import log_all_forecasts, fill_wu_actuals
        logged  = log_all_forecasts()
        filled  = fill_wu_actuals()
        return {"status": "done", "logged": logged, "message": "Forecasts saved with timestamp"}
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/forecast/history")
def forecast_history(city: str = None, days: int = 7):
    """
    Show full forecast history with timestamps.
    Shows how GFS/UKMO/MF changed over time for each city/date.
    This is how we prove model accuracy 4 days out.
    """
    try:
        conn = get_conn()
        c    = conn.cursor()

        if city:
            c.execute("""
                SELECT city, target_date, days_until_resolution,
                       logged_at_est, gfs_temp, ukmo_temp, mf_temp,
                       consensus_temp, spread, unit, wu_actual
                FROM forecast_log
                WHERE city = %s
                AND target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY city, target_date, logged_at_utc
            """, (city, days))
        else:
            c.execute("""
                SELECT city, target_date, days_until_resolution,
                       logged_at_est, gfs_temp, ukmo_temp, mf_temp,
                       consensus_temp, spread, unit, wu_actual
                FROM forecast_log
                WHERE target_date >= (CURRENT_DATE - %s)::TEXT
                ORDER BY city, target_date, logged_at_utc
                LIMIT 1000
            """, (days,))

        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Group by city+date to show forecast evolution
        grouped = {}
        for r in rows:
            key = f"{r['city']}_{r['target_date']}"
            if key not in grouped:
                grouped[key] = {
                    "city":        r["city"],
                    "target_date": r["target_date"],
                    "wu_actual":   r["wu_actual"],
                    "snapshots":   []
                }
            grouped[key]["snapshots"].append({
                "logged_at":    r["logged_at_est"],
                "days_out":     r["days_until_resolution"],
                "gfs":          r["gfs_temp"],
                "ukmo":         r["ukmo_temp"],
                "mf":           r["mf_temp"],
                "consensus":    r["consensus_temp"],
                "spread":       r["spread"],
            })

        return {
            "total_snapshots": len(rows),
            "city_dates":      len(grouped),
            "data":            list(grouped.values()),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/forecast/city-accuracy")
def forecast_city_accuracy(days: int = 30):
    """
    Test Open-Meteo accuracy vs WU actuals per city.
    Shows which cities to bet big on RIGHT NOW.
    Uses real historical data — no simulation.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from forecast_accuracy_test import run_accuracy_test
        return run_accuracy_test(days_back=days)
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/forecast/accuracy")
def forecast_accuracy():
    """
    Compare our logged forecasts vs WU actual temps.
    Shows which cities our models are most accurate in.
    Only uses real logged data — no simulation.
    """
    try:
        from forecast_logger import fetch_wu_temp
        conn = get_conn()
        c    = conn.cursor()

        # Get all logged forecasts where target_date has passed
        c.execute("""
            SELECT city, target_date, days_until_resolution,
                   gfs_temp, ukmo_temp, mf_temp, consensus_temp,
                   spread, unit, wu_actual, logged_at_est
            FROM forecast_log
            WHERE target_date < CURRENT_DATE::TEXT
            AND consensus_temp IS NOT NULL
            ORDER BY city, target_date, days_until_resolution
        """)
        logs = [dict(r) for r in c.fetchall()]
        conn.close()

        if not logs:
            return {
                "status": "no_data",
                "message": "No forecast logs yet. Logger started today — check back in a few days.",
                "tip": "The forecast_logger runs every morning and logs GFS+UKMO+MF for all cities."
            }

        # Fill in WU actuals if missing
        results  = []
        by_city  = {}

        for log in logs:
            city     = log["city"]
            date_str = log["target_date"]
            wu       = log["wu_actual"]

            if wu is None:
                wu = fetch_wu_temp(city, date_str)

            if wu is None:
                continue

            consensus = log["consensus_temp"]
            error     = round(abs(consensus - wu), 1)
            correct   = error <= 2  # within 2 degrees = accurate

            entry = {
                "city":       city,
                "date":       date_str,
                "days_out":   log["days_until_resolution"],
                "gfs":        log["gfs_temp"],
                "ukmo":       log["ukmo_temp"],
                "mf":         log["mf_temp"],
                "consensus":  consensus,
                "spread":     log["spread"],
                "wu_actual":  wu,
                "error":      error,
                "correct":    correct,
                "logged_at":  log["logged_at_est"],
            }
            results.append(entry)

            if city not in by_city:
                by_city[city] = []
            by_city[city].append(entry)

        # City accuracy summary
        city_summary = {}
        for city, entries in by_city.items():
            correct = [e for e in entries if e["correct"]]
            accuracy = round(len(correct)/len(entries)*100, 1) if entries else 0
            avg_error = round(sum(e["error"] for e in entries)/len(entries), 1) if entries else 0
            city_summary[city] = {
                "total_forecasts": len(entries),
                "correct":         len(correct),
                "accuracy":        accuracy,
                "avg_error":       avg_error,
                "bet_here":        accuracy >= 70,
            }

        # Sort cities by accuracy
        ranked = sorted(city_summary.items(), key=lambda x: -x[1]["accuracy"])

        return {
            "total_forecasts": len(results),
            "cities_tracked":  len(by_city),
            "overall_accuracy": round(sum(1 for r in results if r["correct"])/len(results)*100, 1) if results else 0,
            "city_rankings":   dict(ranked),
            "bet_these_cities": [c for c, s in ranked if s["bet_here"]],
            "avoid_these":     [c for c, s in ranked if not s["bet_here"]],
            "raw_results":     results,
        }

    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/paper/trades")
def paper_trades(limit: int = 100):
    """All paper trades with full detail — wins, losses, pending."""
    try:
        from strategy.paper_trade import get_performance
        return get_performance()
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/paper/scan-log")
def paper_scan_log(limit: int = 200):
    """Full audit trail of every scan decision."""
    try:
        from strategy.paper_trade import get_scan_log
        rows = get_scan_log(limit=limit)
        decisions = {}
        for r in rows:
            d = r.get("decision", "UNKNOWN")
            decisions[d] = decisions.get(d, 0) + 1
        return {
            "total_entries": len(rows),
            "decision_counts": decisions,
            "log": rows,
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/paper/run-now")
def paper_run_now():
    """Trigger a paper trading scan right now in background."""
    import threading
    def _run():
        try:
            from strategy.paper_trade import run_scan, init_tables
            init_tables()
            trades, summary = run_scan()
            print(f"[MANUAL SCAN] {trades} trades: {summary}")
        except Exception as e:
            print(f"[MANUAL SCAN ERR] {e}")
    threading.Thread(target=_run, daemon=True).start()
    return {"status": "scanning", "message": "Paper scan running in background — check /paper/trades in 3-5 minutes"}


@app.get("/paper/weekly-summary")
def weekly_summary():
    """Win rate, P&L, performance by city for the week."""
    try:
        from strategy.paper_trade import get_performance
        perf = get_performance()
        return {
            "week_summary": {
                "total_trades": perf["total_trades"],
                "wins":         perf["wins"],
                "losses":       perf["losses"],
                "pending":      perf["pending"],
                "win_rate":     perf["win_rate"],
                "total_pnl":    perf["total_pnl"],
            },
            "by_city":      perf["by_city"],
            "recent_trades": perf["trades"][:20],
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}










@app.get("/backtest/all-strategies")
def backtest_all_strategies():
    """
    Backtest 4 strategies simultaneously using real data only.
    
    Strategy 1: Cross-market arbitrage
      All ranges for one day must sum to ~100c. Find mispricings.
    
    Strategy 2: Momentum
      Buy ranges where price is rising strongly in last 24hrs.
    
    Strategy 3: Direction (day-over-day temp change)
      If yesterday was hot and today forecast is cooler,
      market may still overprice hot ranges due to anchoring.
    
    Strategy 4: Closing price gap
      Market closes at noon. WU posts at 2-5am next day.
      Find ranges where market closed cheap but temp hit the range.
    """
    import math
    from collections import defaultdict

    BET = 10

    try:
        conn = get_conn()
        c = conn.cursor()

        # ── Pull all price snapshots with market metadata ──
        c.execute("""
            SELECT
                ps.market_id,
                ps.timestamp,
                ps.yes_price,
                m.city,
                m.target_low,
                m.target_high,
                m.market_type,
                m.unit,
                m.outcome,
                m.resolved_at,
                LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10) as resolved_date
            FROM price_snapshots ps
            JOIN markets m ON m.id::text = ps.market_id
            WHERE m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            ORDER BY m.resolved_at DESC, ps.timestamp ASC
        """)
        snapshots = [dict(r) for r in c.fetchall()]

        # ── Pull WU actuals ──
        c.execute("SELECT city, date, max_temp_f FROM wu_temps WHERE city IN ('Atlanta','Dallas','NYC')")
        wu_map = {(r["city"], str(r["date"])[:10]): float(r["max_temp_f"]) for r in c.fetchall()}

        # ── Pull scan_log forecasts ──
        c.execute("""
            SELECT city, target_date, AVG(consensus) as fc, AVG(spread) as sp
            FROM scan_log
            WHERE days_out IN (1,2) AND consensus IS NOT NULL
            AND city IN ('Atlanta','Dallas','NYC')
            GROUP BY city, target_date
        """)
        fc_map = {(r["city"], str(r["target_date"])[:10]): 
                  {"fc": float(r["fc"]), "sp": float(r["sp"]) if r["sp"] else 0}
                  for r in c.fetchall()}

        conn.close()

    except Exception as e:
        return {"error": str(e)}

    # ── Organize snapshots by city+date+market ──
    # Group: city -> date -> market_id -> list of (timestamp, price)
    by_city_date = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    market_meta = {}

    for s in snapshots:
        city = s["city"]
        date = s["resolved_date"]
        mid  = s["market_id"]
        by_city_date[city][date][mid].append((s["timestamp"], float(s["yes_price"])))
        if mid not in market_meta:
            market_meta[mid] = {
                "city":       city,
                "date":       date,
                "lo":         float(s["target_low"]),
                "hi":         float(s["target_high"]),
                "mtype":      s["market_type"],
                "outcome":    s["outcome"],
                "resolved_at": s["resolved_at"],
            }

    # ═══════════════════════════════════════════════
    # STRATEGY 1: CROSS-MARKET ARBITRAGE
    # All exact ranges for one city/date must sum to ~100c
    # If sum != 100, someone is mispriced
    # ═══════════════════════════════════════════════
    s1_bets = []

    for city, dates in by_city_date.items():
        for date, markets in dates.items():
            wu = wu_map.get((city, date))
            if wu is None:
                continue

            # Get all exact-range markets for this city/date
            # Use price 24hrs before resolution
            resolved_at = None
            range_prices = {}

            for mid, ticks in markets.items():
                meta = market_meta[mid]
                if meta["mtype"] != "range":
                    continue
                if resolved_at is None:
                    resolved_at = meta["resolved_at"]

                # Get price 24h before resolution
                cutoff = meta["resolved_at"] - 86400
                before = [(t, p) for t, p in ticks if t <= cutoff]
                if not before:
                    continue
                price_24h = before[-1][1]  # latest before cutoff
                range_prices[mid] = {
                    "lo": meta["lo"], "hi": meta["hi"],
                    "price": price_24h,
                    "outcome": meta["outcome"],
                }

            if len(range_prices) < 5:
                continue

            # Check if prices sum correctly
            total = sum(v["price"] for v in range_prices.values())
            deviation = abs(total - 1.0)

            if deviation > 0.05:  # more than 5% off from 100%
                # Find which range is cheapest relative to others
                # The cheapest range relative to its neighbors may be underpriced
                ranges_sorted = sorted(range_prices.items(), key=lambda x: x[1]["lo"])

                for mid, info in ranges_sorted:
                    # Real outcome based on WU
                    real_win = info["lo"] <= wu < info["hi"]
                    real_out = "Yes" if real_win else "No"
                    db_out   = info["outcome"]

                    # Is this range underpriced? (price < 0.08 but neighbors are higher)
                    price_c = round(info["price"] * 100, 2)
                    if price_c < 0.5 or price_c > 40:
                        continue

                    s1_bets.append({
                        "city": city, "date": date,
                        "range": f"{info['lo']}-{info['hi']}",
                        "price_c": price_c,
                        "wu": wu,
                        "sum_deviation_pct": round(deviation * 100, 1),
                        "real_outcome": real_out,
                        "won": real_win,
                        "pnl": round((100/price_c - 1)*BET, 2) if real_win else -BET,
                    })

    s1_n = len(s1_bets)
    s1_wins = sum(1 for b in s1_bets if b["won"])
    s1_pnl = round(sum(b["pnl"] for b in s1_bets), 2)
    s1_wr = round(s1_wins/s1_n*100, 1) if s1_n else 0
    s1_ev = round(s1_pnl/s1_n, 2) if s1_n else 0

    # ═══════════════════════════════════════════════
    # STRATEGY 2: MOMENTUM
    # Buy ranges where price rose >50% in last 24hrs
    # Strong upward momentum = market gaining confidence
    # ═══════════════════════════════════════════════
    s2_bets = []

    for mid, meta in market_meta.items():
        if meta["mtype"] != "range":
            continue
        city = meta["city"]
        date = meta["date"]
        wu   = wu_map.get((city, date))
        if wu is None:
            continue

        ticks = by_city_date[city][date].get(mid, [])
        if len(ticks) < 4:
            continue

        ticks_sorted = sorted(ticks)
        resolved_at  = meta["resolved_at"]
        cutoff_24h   = resolved_at - 86400
        cutoff_48h   = resolved_at - 172800

        before_24h = [(t,p) for t,p in ticks_sorted if t <= cutoff_24h]
        before_48h = [(t,p) for t,p in ticks_sorted if t <= cutoff_48h]

        if not before_24h or not before_48h:
            continue

        price_24h = before_24h[-1][1]
        price_48h = before_48h[-1][1]

        if price_48h < 0.005:
            continue

        momentum = (price_24h - price_48h) / price_48h  # % change

        # Only buy if strong upward momentum AND price in buyable range
        price_c = round(price_24h * 100, 2)
        if momentum < 0.5 or price_c < 0.5 or price_c > 40:
            continue

        real_win = meta["lo"] <= wu < meta["hi"]

        s2_bets.append({
            "city": city, "date": date,
            "range": f"{meta['lo']}-{meta['hi']}",
            "price_48h_c": round(price_48h*100, 2),
            "price_24h_c": price_c,
            "momentum_pct": round(momentum*100, 1),
            "wu": wu,
            "real_outcome": "Yes" if real_win else "No",
            "won": real_win,
            "pnl": round((100/price_c-1)*BET, 2) if real_win else -BET,
        })

    s2_n = len(s2_bets)
    s2_wins = sum(1 for b in s2_bets if b["won"])
    s2_pnl = round(sum(b["pnl"] for b in s2_bets), 2)
    s2_wr = round(s2_wins/s2_n*100, 1) if s2_n else 0
    s2_ev = round(s2_pnl/s2_n, 2) if s2_n else 0

    # ═══════════════════════════════════════════════
    # STRATEGY 3: DIRECTION (anchoring bias)
    # If WU actual yesterday > forecast today by 5°F+,
    # market may overprice high ranges due to anchoring
    # Buy low ranges when temp is forecast to drop
    # ═══════════════════════════════════════════════
    s3_bets = []

    wu_by_city_date = defaultdict(dict)
    for (city, date), temp in wu_map.items():
        wu_by_city_date[city][date] = temp

    for mid, meta in market_meta.items():
        if meta["mtype"] != "range":
            continue
        city = meta["city"]
        date = meta["date"]
        wu_today = wu_map.get((city, date))
        if wu_today is None:
            continue

        fc_info = fc_map.get((city, date))
        if fc_info is None:
            continue

        # Get yesterday's actual temp
        from datetime import datetime, timedelta
        try:
            d = datetime.strptime(date, "%Y-%m-%d")
            yesterday = (d - timedelta(days=1)).strftime("%Y-%m-%d")
        except:
            continue

        wu_yesterday = wu_by_city_date[city].get(yesterday)
        if wu_yesterday is None:
            continue

        temp_drop = wu_yesterday - fc_info["fc"]  # positive = forecast is cooler than yesterday

        if temp_drop < 5:  # only act on significant drops
            continue

        # Market may still price high ranges expensively due to yesterday's heat
        # Buy the range our forecast actually points to
        ticks = by_city_date[city][date].get(mid, [])
        if not ticks:
            continue

        resolved_at = meta["resolved_at"]
        cutoff = resolved_at - 86400
        before = [(t,p) for t,p in sorted(ticks) if t <= cutoff]
        if not before:
            continue

        price_24h = before[-1][1]
        price_c = round(price_24h * 100, 2)
        if price_c < 0.5 or price_c > 40:
            continue

        # Check if this range contains our forecast
        fc = fc_info["fc"]
        if not (meta["lo"] <= fc < meta["hi"]):
            continue

        real_win = meta["lo"] <= wu_today < meta["hi"]

        s3_bets.append({
            "city": city, "date": date,
            "range": f"{meta['lo']}-{meta['hi']}",
            "wu_yesterday": wu_yesterday,
            "forecast_today": fc,
            "temp_drop": round(temp_drop, 1),
            "price_c": price_c,
            "wu_actual": wu_today,
            "real_outcome": "Yes" if real_win else "No",
            "won": real_win,
            "pnl": round((100/price_c-1)*BET, 2) if real_win else -BET,
        })

    s3_n = len(s3_bets)
    s3_wins = sum(1 for b in s3_bets if b["won"])
    s3_pnl = round(sum(b["pnl"] for b in s3_bets), 2)
    s3_wr = round(s3_wins/s3_n*100, 1) if s3_n else 0
    s3_ev = round(s3_pnl/s3_n, 2) if s3_n else 0

    # ═══════════════════════════════════════════════
    # STRATEGY 4: CLOSING PRICE GAP
    # Market closes at noon EST. WU posts next morning.
    # Find ranges that closed cheap but temp hit them.
    # These are markets where we could buy before close
    # knowing the temp is already in that range.
    # ═══════════════════════════════════════════════
    s4_bets = []

    for mid, meta in market_meta.items():
        if meta["mtype"] != "range":
            continue
        city = meta["city"]
        date = meta["date"]
        wu = wu_map.get((city, date))
        if wu is None:
            continue

        ticks = by_city_date[city][date].get(mid, [])
        if not ticks:
            continue

        resolved_at = meta["resolved_at"]
        # Market closes at noon = resolved_at - ~12hrs
        close_time = resolved_at - 43200
        # 2hrs before close = buying window
        buy_time   = close_time - 7200

        buy_ticks = [(t,p) for t,p in sorted(ticks) if t <= buy_time]
        if not buy_ticks:
            continue

        buy_price = buy_ticks[-1][1]
        buy_price_c = round(buy_price * 100, 2)

        if buy_price_c < 0.5 or buy_price_c > 35:
            continue

        # Did temp land in this range?
        real_win = meta["lo"] <= wu < meta["hi"]
        if not real_win:
            continue  # only count winning trades for this strategy
            # (we'd need real-time temp data to implement this live)

        # What was the price at market open vs buy time?
        open_price = sorted(ticks)[0][1] if ticks else None

        s4_bets.append({
            "city": city, "date": date,
            "range": f"{meta['lo']}-{meta['hi']}",
            "open_price_c": round(open_price*100, 2) if open_price else None,
            "buy_price_c": buy_price_c,
            "wu_actual": wu,
            "real_outcome": "Yes",
            "won": True,
            "pnl": round((100/buy_price_c-1)*BET, 2),
        })

    s4_n = len(s4_bets)
    s4_pnl = round(sum(b["pnl"] for b in s4_bets), 2)
    s4_ev = round(s4_pnl/s4_n, 2) if s4_n else 0

    # ═══════════════════════════════════════════════
    # COMPILE RESULTS
    # ═══════════════════════════════════════════════
    strategies = [
        {
            "strategy": "1. Cross-market arbitrage",
            "description": "Buy ranges when total prices deviate >5% from 100¢",
            "n_bets": s1_n,
            "wins": s1_wins,
            "win_rate": s1_wr,
            "total_pnl": s1_pnl,
            "ev_per_bet": s1_ev,
            "profitable": s1_ev > 0,
            "sample_bets": s1_bets[:5],
        },
        {
            "strategy": "2. Momentum",
            "description": "Buy ranges with >50% price rise in last 24hrs",
            "n_bets": s2_n,
            "wins": s2_wins,
            "win_rate": s2_wr,
            "total_pnl": s2_pnl,
            "ev_per_bet": s2_ev,
            "profitable": s2_ev > 0,
            "sample_bets": s2_bets[:5],
        },
        {
            "strategy": "3. Direction / anchoring",
            "description": "Buy forecast range when temp drops 5°F+ vs yesterday",
            "n_bets": s3_n,
            "wins": s3_wins,
            "win_rate": s3_wr,
            "total_pnl": s3_pnl,
            "ev_per_bet": s3_ev,
            "profitable": s3_ev > 0,
            "sample_bets": s3_bets[:5],
        },
        {
            "strategy": "4. Closing price gap",
            "description": "Buy winning ranges 2hrs before market close",
            "n_bets": s4_n,
            "wins": s4_n,
            "win_rate": 100.0 if s4_n else 0,
            "total_pnl": s4_pnl,
            "ev_per_bet": s4_ev,
            "profitable": s4_ev > 0,
            "note": "100% win rate by design - shows missed opportunities",
            "sample_bets": s4_bets[:5],
        },
    ]

    best = max((s for s in strategies if s["n_bets"] > 0),
               key=lambda x: x["ev_per_bet"], default=None)

    return {
        "strategies": strategies,
        "best_strategy": best["strategy"] if best else None,
        "best_ev_per_bet": best["ev_per_bet"] if best else None,
        "data_available": {
            "price_snapshots": len(snapshots),
            "markets_with_wu": len([m for m in market_meta
                                    if wu_map.get((market_meta[m]["city"],
                                                   market_meta[m]["date"]))]),
            "markets_with_forecast": len([m for m in market_meta
                                          if fc_map.get((market_meta[m]["city"],
                                                         market_meta[m]["date"]))]),
        },
    }

@app.get("/backtest/buy-no")
def backtest_buy_no():
    """
    Backtest: Buy 'No' on overpriced 'or higher' markets.
    
    Strategy: When Polymarket prices 'X or higher' at 75c+,
    but our forecast says temp will be BELOW X by gap_min degrees,
    buy 'No' at the cheap price.
    
    Uses ONLY real data:
    - price_snapshots: actual market prices at time of scan
    - wu_temps: actual WU temperatures (ground truth)
    - scan_log: our real forecasts (consensus, spread, days_out)
    - markets: market metadata and outcomes
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        # Step 1: Get all resolved 'or higher' markets with price history
        c.execute("""
            SELECT
                m.id::text as market_id,
                m.city,
                m.target_low as threshold,
                m.outcome as db_outcome,
                m.resolved_at,
                LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10) as resolved_date,
                -- Price 24hrs before resolution = our entry price for No
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 AND ps.timestamp <= m.resolved_at - 86400
                 ORDER BY ps.timestamp DESC LIMIT 1) as yes_price_24h,
                -- Price 48hrs before resolution
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 AND ps.timestamp <= m.resolved_at - 172800
                 ORDER BY ps.timestamp DESC LIMIT 1) as yes_price_48h,
                -- Earliest price
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 ORDER BY ps.timestamp ASC LIMIT 1) as open_price,
                -- WU actual temperature
                (SELECT max_temp_f FROM wu_temps w
                 WHERE w.city = m.city
                 AND w.date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 LIMIT 1) as wu_actual,
                -- Our forecast from scan_log (1-2 days out)
                (SELECT AVG(consensus) FROM scan_log sl
                 WHERE sl.city = m.city
                 AND sl.target_date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 AND sl.days_out IN (1, 2)
                 AND sl.consensus IS NOT NULL
                 LIMIT 1) as our_forecast,
                -- Our spread from scan_log
                (SELECT AVG(spread) FROM scan_log sl
                 WHERE sl.city = m.city
                 AND sl.target_date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 AND sl.days_out IN (1, 2)
                 AND sl.consensus IS NOT NULL
                 LIMIT 1) as our_spread
            FROM markets m
            WHERE m.market_type = 'above'
            AND m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            ORDER BY m.resolved_at DESC
        """)
        raw = [dict(r) for r in c.fetchall()]
        conn.close()

    except Exception as e:
        return {"error": str(e)}

    # Step 2: Build clean dataset with real outcomes
    dataset = []
    for r in raw:
        wu = float(r["wu_actual"]) if r["wu_actual"] else None
        thresh = float(r["threshold"])
        yes_24h = float(r["yes_price_24h"]) if r["yes_price_24h"] else None
        fc = float(r["our_forecast"]) if r["our_forecast"] else None
        spread = float(r["our_spread"]) if r["our_spread"] else None

        if wu is None or yes_24h is None:
            continue

        # Real outcome based on WU actual (not DB which is corrupted)
        real_outcome = "Yes" if wu >= thresh else "No"
        no_price_24h = round(1.0 - yes_24h, 4)

        row = {
            "market_id":   r["market_id"],
            "city":        r["city"],
            "date":        r["resolved_date"],
            "threshold":   thresh,
            "wu_actual":   wu,
            "real_outcome": real_outcome,
            "yes_price":   round(yes_24h * 100, 2),
            "no_price":    round(no_price_24h * 100, 2),
            "forecast":    round(fc, 1) if fc else None,
            "spread":      round(spread, 1) if spread else None,
            "gap":         round(thresh - fc, 1) if fc else None,
            # gap > 0 means our forecast is BELOW threshold (expect No)
            # gap < 0 means our forecast is ABOVE threshold (expect Yes)
        }
        dataset.append(row)

    # Step 3: Run backtest with multiple parameter combinations
    BET = 10  # $10 per bet

    param_sets = [
        {"min_yes_price": 75, "min_gap": 0,  "max_spread": 99, "label": "All: yes≥75¢ gap≥0"},
        {"min_yes_price": 75, "min_gap": 3,  "max_spread": 99, "label": "yes≥75¢ gap≥3°"},
        {"min_yes_price": 75, "min_gap": 5,  "max_spread": 99, "label": "yes≥75¢ gap≥5°"},
        {"min_yes_price": 75, "min_gap": 8,  "max_spread": 99, "label": "yes≥75¢ gap≥8°"},
        {"min_yes_price": 75, "min_gap": 10, "max_spread": 99, "label": "yes≥75¢ gap≥10°"},
        {"min_yes_price": 80, "min_gap": 5,  "max_spread": 99, "label": "yes≥80¢ gap≥5°"},
        {"min_yes_price": 80, "min_gap": 5,  "max_spread": 5,  "label": "yes≥80¢ gap≥5° spread≤5°"},
        {"min_yes_price": 90, "min_gap": 5,  "max_spread": 99, "label": "yes≥90¢ gap≥5°"},
        {"min_yes_price": 90, "min_gap": 10, "max_spread": 99, "label": "yes≥90¢ gap≥10°"},
        {"min_yes_price": 75, "min_gap": 5,  "max_spread": 3,  "label": "yes≥75¢ gap≥5° spread≤3°"},
    ]

    results = []
    for p in param_sets:
        qualifying = [
            r for r in dataset
            if r["yes_price"] >= p["min_yes_price"]
            and r["gap"] is not None
            and r["gap"] >= p["min_gap"]
            and r["spread"] is not None
            and r["spread"] <= p["max_spread"]
            and r["no_price"] >= 0.5  # must have real liquidity
        ]

        if not qualifying:
            results.append({
                "params": p["label"],
                "n_bets": 0,
                "note": "no qualifying markets"
            })
            continue

        n = len(qualifying)
        wins = sum(1 for r in qualifying if r["real_outcome"] == "No")
        losses = n - wins
        pnl_list = []
        for r in qualifying:
            if r["real_outcome"] == "No":
                pnl_list.append(round((100 / r["no_price"] - 1) * BET, 2))
            else:
                pnl_list.append(-BET)

        total_pnl = round(sum(pnl_list), 2)
        wr = round(wins / n * 100, 1)
        ev = round(total_pnl / n, 2)

        results.append({
            "params":     p["label"],
            "n_bets":     n,
            "wins":       wins,
            "losses":     losses,
            "win_rate":   wr,
            "total_pnl":  total_pnl,
            "ev_per_bet": ev,
            "profitable": ev > 0,
            "bets":       qualifying,
        })

    # Sort by EV
    results_sorted = sorted(
        [r for r in results if r.get("n_bets", 0) > 0],
        key=lambda x: -x["ev_per_bet"]
    )

    # Step 4: Best config detail
    best = results_sorted[0] if results_sorted else None

    # Step 5: Per-city breakdown for best config
    city_breakdown = {}
    if best and best.get("bets"):
        for r in best["bets"]:
            city = r["city"]
            if city not in city_breakdown:
                city_breakdown[city] = {"n": 0, "wins": 0, "pnl": 0}
            city_breakdown[city]["n"] += 1
            won = r["real_outcome"] == "No"
            if won:
                city_breakdown[city]["wins"] += 1
                city_breakdown[city]["pnl"] += round((100/r["no_price"]-1)*BET, 2)
            else:
                city_breakdown[city]["pnl"] -= BET
        for city in city_breakdown:
            n = city_breakdown[city]["n"]
            w = city_breakdown[city]["wins"]
            city_breakdown[city]["win_rate"] = round(w/n*100, 1)
            city_breakdown[city]["ev_per_bet"] = round(city_breakdown[city]["pnl"]/n, 2)

    return {
        "strategy":      "Buy No on overpriced or-higher markets",
        "bet_size":      f"${BET}",
        "total_markets_with_data": len(dataset),
        "markets_with_forecast":  sum(1 for r in dataset if r["forecast"] is not None),
        "summary":       results_sorted,
        "best_config":   {
            "params":      best["params"] if best else None,
            "n_bets":      best["n_bets"] if best else 0,
            "win_rate":    best["win_rate"] if best else 0,
            "total_pnl":   best["total_pnl"] if best else 0,
            "ev_per_bet":  best["ev_per_bet"] if best else 0,
        } if best else None,
        "city_breakdown": city_breakdown,
        "all_qualifying_bets": best["bets"] if best else [],
        "dataset_sample": dataset[:10],
    }

@app.get("/backtest/small-fish/dallas")
def small_fish_dallas():
    """
    Pull every Dallas or-higher market with price history.
    Shows: threshold, outcome, wu_actual, entry prices, max price.
    Answers: which markets reached 75c+, did they actually win, what were prices?
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        c.execute("""
            SELECT
                m.id::text as market_id,
                m.city,
                m.target_low as threshold,
                m.outcome,
                m.resolved_at,
                LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10) as resolved_date,
                COUNT(ps.id) as n_snapshots,
                ROUND(MAX(ps.yes_price)::numeric, 4) as max_price,
                ROUND(MIN(ps.yes_price)::numeric, 4) as min_price,
                -- earliest price (market open)
                (SELECT ROUND(yes_price::numeric,4) FROM price_snapshots ps2
                 WHERE ps2.market_id = m.id::text
                 ORDER BY ps2.timestamp ASC LIMIT 1) as open_price,
                -- price 48hrs before resolution
                (SELECT ROUND(yes_price::numeric,4) FROM price_snapshots ps2
                 WHERE ps2.market_id = m.id::text
                 AND ps2.timestamp <= m.resolved_at - 172800
                 ORDER BY ps2.timestamp DESC LIMIT 1) as price_48h_before,
                -- price 24hrs before resolution
                (SELECT ROUND(yes_price::numeric,4) FROM price_snapshots ps2
                 WHERE ps2.market_id = m.id::text
                 AND ps2.timestamp <= m.resolved_at - 86400
                 ORDER BY ps2.timestamp DESC LIMIT 1) as price_24h_before,
                -- wu actual
                (SELECT max_temp_f FROM wu_temps
                 WHERE city = m.city
                 AND date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 LIMIT 1) as wu_actual,
                -- our forecast
                (SELECT ROUND(AVG(consensus)::numeric,1) FROM scan_log
                 WHERE city = m.city
                 AND target_date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 AND days_out IN (1,2)
                 AND consensus IS NOT NULL) as forecast,
                (SELECT ROUND(AVG(spread)::numeric,1) FROM scan_log
                 WHERE city = m.city
                 AND target_date = LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10)
                 AND days_out IN (1,2)
                 AND consensus IS NOT NULL) as spread
            FROM markets m
            LEFT JOIN price_snapshots ps ON ps.market_id = m.id::text
            WHERE m.market_type = 'above'
            AND m.city = 'Dallas'
            AND m.outcome IS NOT NULL
            GROUP BY m.id, m.city, m.target_low, m.outcome, m.resolved_at
            ORDER BY m.resolved_at DESC
        """)

        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Annotate each row
        for r in rows:
            r["wu_actual"] = float(r["wu_actual"]) if r["wu_actual"] else None
            r["forecast"]  = float(r["forecast"])  if r["forecast"]  else None
            r["threshold"] = float(r["threshold"])

            # Verify outcome correctness
            if r["wu_actual"] is not None:
                expected = "Yes" if r["wu_actual"] >= r["threshold"] else "No"
                r["outcome_correct"] = (r["outcome"] == expected)
                r["expected_outcome"] = expected

            # Small fish opportunity check
            p24 = float(r["price_24h_before"]) if r["price_24h_before"] else None
            r["price_24h_c"] = round(p24 * 100, 1) if p24 else None
            r["qualifies_75_93c"] = (p24 is not None and 0.75 <= p24 <= 0.93)
            r["qualifies_90c_plus"] = (p24 is not None and p24 >= 0.90)

            if r["forecast"] and r["threshold"]:
                r["buffer"] = round(r["forecast"] - r["threshold"], 1)

        # Summary stats
        with_prices  = [r for r in rows if r["price_24h_before"]]
        reached_75c  = [r for r in with_prices if r["qualifies_75_93c"]]
        reached_90c  = [r for r in with_prices if r["qualifies_90c_plus"]]

        wins_at_75c  = [r for r in reached_75c if r["outcome"] == "Yes"]
        wins_at_90c  = [r for r in reached_90c if r["outcome"] == "Yes"]

        bad_outcomes = [r for r in rows if r.get("outcome_correct") == False]

        return {
            "total_markets":        len(rows),
            "with_price_history":   len(with_prices),
            "reached_75_93c":       len(reached_75c),
            "reached_90c_plus":     len(reached_90c),
            "wins_at_75_93c":       len(wins_at_75c),
            "win_rate_at_75_93c":   round(len(wins_at_75c)/len(reached_75c)*100,1) if reached_75c else 0,
            "wins_at_90c_plus":     len(wins_at_90c),
            "win_rate_at_90c_plus": round(len(wins_at_90c)/len(reached_90c)*100,1) if reached_90c else 0,
            "bad_outcomes_in_db":   len(bad_outcomes),
            "markets_75_93c":       reached_75c,
            "markets_90c_plus":     reached_90c,
            "bad_outcomes":         bad_outcomes[:5],
            "all_markets":          rows,
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/backtest/small-fish/diagnose")
def small_fish_diagnose():
    """
    Diagnose why no bets qualify in small fish backtest.
    Shows sample data from each step of the filter chain.
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        # 1. Sample of above markets with their price snapshots
        c.execute("""
            SELECT 
                m.id, m.city, m.target_low as threshold,
                m.outcome, m.resolved_at,
                LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10) as resolved_date,
                COUNT(ps.id) as snapshot_count,
                MAX(ps.yes_price) as max_price,
                MIN(ps.yes_price) as min_price,
                -- Price 1 day before resolution
                (SELECT yes_price FROM price_snapshots ps2
                 WHERE ps2.market_id = m.id::text
                 AND ps2.timestamp <= m.resolved_at - 86400
                 ORDER BY ps2.timestamp DESC LIMIT 1) as entry_price_1d
            FROM markets m
            LEFT JOIN price_snapshots ps ON ps.market_id = m.id::text
            WHERE m.market_type = 'above'
            AND m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            GROUP BY m.id, m.city, m.target_low, m.outcome, m.resolved_at
            ORDER BY m.resolved_at DESC
            LIMIT 20
        """)
        sample_markets = [dict(r) for r in c.fetchall()]

        # 2. Check price ranges in snapshots
        c.execute("""
            SELECT 
                m.city,
                COUNT(*) as n_markets,
                AVG(ps_stats.max_p) as avg_max_price,
                COUNT(CASE WHEN ps_stats.max_p >= 0.75 THEN 1 END) as markets_reached_75c,
                COUNT(CASE WHEN ps_stats.max_p >= 0.90 THEN 1 END) as markets_reached_90c
            FROM markets m
            JOIN (
                SELECT market_id, MAX(yes_price) as max_p
                FROM price_snapshots
                GROUP BY market_id
            ) ps_stats ON ps_stats.market_id = m.id::text
            WHERE m.market_type = 'above'
            AND m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            GROUP BY m.city
        """)
        price_stats = [dict(r) for r in c.fetchall()]

        # 3. Check forecast availability
        c.execute("""
            SELECT city, COUNT(DISTINCT target_date) as dates_with_forecasts
            FROM scan_log
            WHERE days_out IN (1,2)
            AND consensus IS NOT NULL
            AND city IN ('Atlanta', 'Dallas', 'NYC')
            GROUP BY city
        """)
        forecast_coverage = [dict(r) for r in c.fetchall()]

        # 4. Check WU coverage
        c.execute("""
            SELECT city, COUNT(*) as wu_days
            FROM wu_temps
            WHERE city IN ('Atlanta', 'Dallas', 'NYC')
            GROUP BY city
        """)
        wu_coverage = [dict(r) for r in c.fetchall()]

        # 5. Find markets that have BOTH price history AND forecasts AND wu
        c.execute("""
            SELECT 
                m.id::text as market_id,
                m.city,
                m.target_low as threshold,
                m.outcome,
                LEFT(CAST(TO_TIMESTAMP(m.resolved_at) AS TEXT), 10) as resolved_date,
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 AND ps.timestamp <= m.resolved_at - 86400
                 ORDER BY ps.timestamp DESC LIMIT 1) as entry_price_1d
            FROM markets m
            WHERE m.market_type = 'above'
            AND m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            AND EXISTS (
                SELECT 1 FROM price_snapshots ps
                WHERE ps.market_id = m.id::text
            )
            ORDER BY m.resolved_at DESC
            LIMIT 30
        """)
        matched_markets = []
        for row in c.fetchall():
            r = dict(row)
            resolved_date = r["resolved_date"]
            city = r["city"]
            
            # Check forecast
            c2 = conn.cursor()
            c2.execute("""
                SELECT AVG(consensus) as consensus, AVG(spread) as spread
                FROM scan_log
                WHERE city=%s AND target_date=%s AND days_out IN (1,2)
                AND consensus IS NOT NULL
            """, (city, resolved_date))
            fc = c2.fetchone()
            r["forecast"] = round(float(fc["consensus"]), 1) if fc and fc["consensus"] else None
            r["spread"] = round(float(fc["spread"]), 1) if fc and fc["spread"] else None
            
            # Check WU
            c2.execute("SELECT max_temp_f FROM wu_temps WHERE city=%s AND date=%s", 
                      (city, resolved_date))
            wu = c2.fetchone()
            r["wu_actual"] = float(wu["max_temp_f"]) if wu else None
            r["entry_price_c"] = round(float(r["entry_price_1d"])*100, 1) if r["entry_price_1d"] else None
            
            matched_markets.append(r)

        conn.close()

        return {
            "sample_markets": sample_markets[:10],
            "price_stats_by_city": price_stats,
            "forecast_coverage": forecast_coverage,
            "wu_coverage": wu_coverage,
            "matched_markets_sample": matched_markets[:20],
            "key_question": "How many markets have entry_price_1d + forecast + wu_actual?"
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/backtest/small-fish")
def small_fish_backtest():
    """
    Backtest the small fish strategy using real market data.
    
    Strategy: Buy "or higher" markets already priced at 75-93¢
    when ALL models forecast >= threshold + buffer degrees.
    
    Uses:
    - price_snapshots table: real historical market prices
    - markets table: market outcomes and metadata  
    - wu_temps table: actual WU temperatures
    - scan_log table: our forecast data per date
    """
    import math

    results = []
    skipped_reasons = {}

    try:
        conn = get_conn()
        c = conn.cursor()

        # Pull all resolved "or higher" markets with real price history
        c.execute("""
            SELECT 
                m.id as market_id,
                m.city,
                m.target_low as threshold,
                m.target_high,
                m.market_type,
                m.unit,
                m.outcome,
                m.last_trade_price,
                LEFT(m.resolved_at::text, 10) as resolved_date,
                -- Get the opening price (earliest snapshot)
                (SELECT yes_price FROM price_snapshots ps 
                 WHERE ps.market_id = m.id::text
                 ORDER BY ps.timestamp ASC LIMIT 1) as open_price,
                -- Get price 2 days before resolution (entry price simulation)
                -- resolved_at is Unix timestamp (bigint), subtract 2 days in seconds
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 AND ps.timestamp <= m.resolved_at - 172800
                 ORDER BY ps.timestamp DESC LIMIT 1) as entry_price_2d,
                -- Get price 1 day before resolution (86400 seconds)
                (SELECT yes_price FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text
                 AND ps.timestamp <= m.resolved_at - 86400
                 ORDER BY ps.timestamp DESC LIMIT 1) as entry_price_1d,
                -- Get the max price reached (to see if it ever hit 75¢+)
                (SELECT MAX(yes_price) FROM price_snapshots ps
                 WHERE ps.market_id = m.id::text) as max_price
            FROM markets m
            WHERE m.market_type = 'above'
            AND m.outcome IS NOT NULL
            AND m.city IN ('Atlanta', 'Dallas', 'NYC')
            ORDER BY m.resolved_at DESC
        """)
        above_markets = [dict(r) for r in c.fetchall()]

        # Get forecast data from scan_log for each market date
        c.execute("""
            SELECT DISTINCT city, target_date, 
                   AVG(consensus) as consensus,
                   AVG(spread) as spread,
                   AVG(gfs_temp) as gfs,
                   AVG(ukmo_temp) as ukmo,
                   AVG(mf_temp) as mf
            FROM scan_log
            WHERE days_out IN (1, 2)
            AND consensus IS NOT NULL
            GROUP BY city, target_date
        """)
        forecasts = {}
        for row in c.fetchall():
            forecasts[(row["city"], str(row["target_date"])[:10])] = {
                "consensus": float(row["consensus"]) if row["consensus"] else None,
                "spread":    float(row["spread"]) if row["spread"] else None,
                "gfs":       float(row["gfs"]) if row["gfs"] else None,
                "ukmo":      float(row["ukmo"]) if row["ukmo"] else None,
                "mf":        float(row["mf"]) if row["mf"] else None,
            }

        # Get WU actuals
        c.execute("SELECT city, date, max_temp_f FROM wu_temps")
        wu = {}
        for row in c.fetchall():
            wu[(row["city"], str(row["date"])[:10])] = float(row["max_temp_f"])

        conn.close()

    except Exception as e:
        return {"error": str(e)}

    # Test multiple parameter combinations
    param_sets = [
        {"buffer": 3, "spread_max": 2.0, "min_price": 0.75, "max_price": 0.93, "label": "buffer=3 spread=2"},
        {"buffer": 4, "spread_max": 2.0, "min_price": 0.75, "max_price": 0.93, "label": "buffer=4 spread=2"},
        {"buffer": 5, "spread_max": 2.0, "min_price": 0.75, "max_price": 0.93, "label": "buffer=5 spread=2"},
        {"buffer": 5, "spread_max": 3.0, "min_price": 0.75, "max_price": 0.93, "label": "buffer=5 spread=3"},
        {"buffer": 3, "spread_max": 3.0, "min_price": 0.70, "max_price": 0.95, "label": "buffer=3 spread=3 wide"},
        {"buffer": 7, "spread_max": 2.0, "min_price": 0.75, "max_price": 0.93, "label": "buffer=7 spread=2"},
        {"buffer": 10, "spread_max": 3.0, "min_price": 0.70, "max_price": 0.95, "label": "buffer=10 spread=3"},
    ]

    backtest_results = {}
    market_detail = []

    for params in param_sets:
        bets = []
        
        for m in above_markets:
            city         = m["city"]
            threshold    = float(m["threshold"])
            resolved_date = m.get("resolved_date", "")[:10]
            outcome      = m["outcome"]  # "Yes" or "No"
            
            # Get entry price (1 day before resolution)
            entry = m.get("entry_price_1d")
            if entry is None:
                entry = m.get("entry_price_2d")
            if entry is None:
                continue
                
            entry = float(entry)
            entry_c = round(entry * 100, 1)

            # Price filter
            if entry < params["min_price"] or entry > params["max_price"]:
                continue

            # Get forecast for this date
            fc = forecasts.get((city, resolved_date))
            if fc is None or fc["consensus"] is None:
                continue

            consensus = fc["consensus"]
            spread    = fc["spread"] or 0

            # Buffer filter: forecast must be >= threshold + buffer
            if consensus < threshold + params["buffer"]:
                continue

            # Spread filter
            if spread > params["spread_max"]:
                continue

            # Get actual temp
            actual = wu.get((city, resolved_date))
            if actual is None:
                continue

            # Calculate P&L
            bet_size = 50
            won = (outcome == "Yes")
            win_amt = round(bet_size * (1/entry - 1), 2)
            pnl = win_amt if won else -bet_size

            bets.append({
                "city":      city,
                "date":      resolved_date,
                "threshold": threshold,
                "entry_c":   entry_c,
                "consensus": round(consensus, 1),
                "spread":    round(spread, 1),
                "actual":    actual,
                "outcome":   outcome,
                "won":       won,
                "pnl":       pnl,
            })

        if not bets:
            backtest_results[params["label"]] = {
                "n_bets": 0,
                "note": "no qualifying bets"
            }
            continue

        n     = len(bets)
        wins  = sum(1 for b in bets if b["won"])
        total = round(sum(b["pnl"] for b in bets), 2)
        wr    = round(wins/n*100, 1)
        ev    = round(total/n, 2)

        backtest_results[params["label"]] = {
            "n_bets":      n,
            "wins":        wins,
            "losses":      n - wins,
            "win_rate":    wr,
            "total_pnl":   total,
            "ev_per_bet":  ev,
            "profitable":  ev > 0,
            "bets":        bets,
        }

    # Summary of all param sets
    summary = []
    for label, res in backtest_results.items():
        if res.get("n_bets", 0) > 0:
            summary.append({
                "params":     label,
                "n_bets":     res["n_bets"],
                "win_rate":   res["win_rate"],
                "ev_per_bet": res["ev_per_bet"],
                "total_pnl":  res["total_pnl"],
                "profitable": res["profitable"],
            })

    summary.sort(key=lambda x: -x["ev_per_bet"])

    # Best config detail
    best = summary[0] if summary else None
    best_bets = backtest_results.get(best["params"], {}).get("bets", []) if best else []

    return {
        "strategy":       "Small fish: buy high-priced or-higher markets",
        "bet_size":       "$50 per bet",
        "markets_tested": len(above_markets),
        "summary":        summary,
        "best_config":    best,
        "best_config_bets": best_bets,
        "total_above_markets_in_db": len(above_markets),
        "forecasts_available": len(forecasts),
        "wu_actuals_available": len(wu),
    }

@app.get("/backtest")
def run_backtest():
    """
    Full backtest using scan_log forecasts vs wu_temps actuals.
    Only uses 1-day-ahead and 2-day-ahead forecasts (days_out=1 or 2).
    Grid searches bias, std, edge threshold, spread limit, temp range.
    """
    import math
    from datetime import datetime

    # ── 1. Pull WU actuals ──
    wu = {}
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT city, date, max_temp_f FROM wu_temps
            WHERE city IN ('Atlanta', 'Dallas', 'NYC')
            ORDER BY city, date
        """)
        for row in c.fetchall():
            wu.setdefault(row["city"], {})[str(row["date"])[:10]] = float(row["max_temp_f"])
        conn.close()
    except Exception as e:
        return {"error": f"WU error: {e}"}

    # ── 2. Pull scan_log — only 1-day and 2-day ahead forecasts ──
    # Use the EARLIEST scan for each city+target_date+days_out combo
    # This simulates placing bets at market open
    scan_data = {}
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT city, target_date, days_out,
                   gfs_temp, ukmo_temp, mf_temp, consensus, spread,
                   MIN(scanned_at) as first_scan
            FROM scan_log
            WHERE days_out IN (1, 2)
            AND gfs_temp IS NOT NULL
            AND consensus IS NOT NULL
            AND city IN ('Atlanta', 'Dallas', 'NYC')
            GROUP BY city, target_date, days_out,
                     gfs_temp, ukmo_temp, mf_temp, consensus, spread
            ORDER BY city, target_date, days_out
        """)
        rows = c.fetchall()
        conn.close()

        # For each city+target_date, keep earliest scan per days_out
        seen = {}
        for row in rows:
            key = (row["city"], str(row["target_date"])[:10], row["days_out"])
            if key not in seen:
                seen[key] = {
                    "city":      row["city"],
                    "date":      str(row["target_date"])[:10],
                    "days_out":  row["days_out"],
                    "gfs":       float(row["gfs_temp"]) if row["gfs_temp"] else None,
                    "ukmo":      float(row["ukmo_temp"]) if row["ukmo_temp"] else None,
                    "mf":        float(row["mf_temp"]) if row["mf_temp"] else None,
                    "consensus": float(row["consensus"]),
                    "spread":    float(row["spread"]) if row["spread"] else 0.0,
                    "first_scan": row["first_scan"],
                }
        scan_data = list(seen.values())

    except Exception as e:
        return {"error": f"Scan log error: {e}"}

    # ── 3. Match forecasts to actuals ──
    matched = {}
    for fc in scan_data:
        city = fc["city"]
        date_str = fc["date"]
        actual = wu.get(city, {}).get(date_str)
        if actual is None:
            continue

        matched.setdefault(city, {}).setdefault(fc["days_out"], []).append({
            "date":      date_str,
            "days_out":  fc["days_out"],
            "forecast":  round(fc["consensus"], 1),
            "gfs":       round(fc["gfs"], 1) if fc["gfs"] else None,
            "ukmo":      round(fc["ukmo"], 1) if fc["ukmo"] else None,
            "spread":    round(fc["spread"], 1),
            "actual":    actual,
            "error":     round(abs(fc["consensus"] - actual), 1),
            "bias_raw":  round(fc["consensus"] - actual, 1),
        })

    if not matched:
        return {
            "error": "No matched data",
            "wu_counts":   {c: len(v) for c, v in wu.items()},
            "scan_rows":   len(scan_data),
            "sample_scan": scan_data[:3] if scan_data else [],
            "hint": "scan_log days_out may not match wu_temps dates",
        }

    # ── 4. Analysis per city per days_out ──
    def cdf(x):
        sign = 1 if x >= 0 else -1
        x = abs(x)
        t = 1.0/(1.0+0.3275911*x)
        return 0.5*(1-sign*(((((1.061405429*t-1.453152027)*t+1.421413741)*t
                              -0.284496736)*t+0.254829592)*t)*math.exp(-x*x))

    def tp(lo, hi, consensus, bias, std):
        c = consensus - bias
        if hi >= 999: return 1.0 - cdf((lo-c)/std)
        if lo <=-999: return cdf((hi+1-c)/std)
        return cdf((hi+1-c)/std) - cdf((lo-c)/std)

    results = {}

    for city, by_days in matched.items():
        city_results = {}

        for days_out, data in by_days.items():
            data.sort(key=lambda x: x["date"])
            n = len(data)
            if n < 5:
                city_results[f"{days_out}d"] = {"error": f"only {n} points"}
                continue

            avg_bias = round(sum(d["bias_raw"] for d in data)/n, 2)
            avg_err  = round(sum(d["error"] for d in data)/n, 2)
            std_calc = round(math.sqrt(sum(d["error"]**2 for d in data)/n), 2)
            acc_1f   = round(sum(1 for d in data if d["error"]<=1.0)/n*100, 1)
            acc_2f   = round(sum(1 for d in data if d["error"]<=2.0)/n*100, 1)

            # Range breakdown
            range_stats = {}
            for label, lo, hi in [
                ("all",  -999, 999), ("<70",  -999,70),
                ("70-75",  70,  75), ("75-80",  75,80),
                ("80-85",  80,  85), ("85-90",  85,90), ("90+", 90,999)
            ]:
                sub = [d for d in data if lo<=d["actual"]<hi]
                if len(sub) < 3: continue
                ns = len(sub)
                range_stats[label] = {
                    "n":      ns,
                    "bias":   round(sum(d["bias_raw"] for d in sub)/ns, 2),
                    "err":    round(sum(d["error"] for d in sub)/ns, 2),
                    "acc_2f": round(sum(1 for d in sub if d["error"]<=2.0)/ns*100, 1),
                }

            # Grid search
            best = []
            for bias in [-2,-1.5,-1,-0.5,0,0.5,1,1.5,2]:
                for std in [0.8,1.0,1.2,1.5,1.8,2.0,2.5]:
                    for emin in [0.20,0.25,0.30,0.35]:
                        for smax in [2.0,2.5,3.0,3.5]:
                            bets=wins=0; pnl=0.0
                            for d in data:
                                if d["spread"] > smax: continue
                                corr = d["forecast"] - bias
                                blo = math.floor(corr)
                                bhi = blo + 2
                                # Simulate realistic market price
                                # Market prices 2-degree range near consensus
                                # roughly 15-25¢ depending on how central
                                dist = abs(corr - (blo+1))
                                mkt = 0.15 + dist * 0.05
                                prob = tp(blo, bhi, d["forecast"], bias, std)
                                edge = prob - mkt
                                if edge < emin: continue
                                bets += 1
                                if blo <= d["actual"] < bhi:
                                    wins += 1
                                    pnl += (1/mkt-1)*10
                                else:
                                    pnl -= 10
                            if bets >= 5:
                                wr = round(wins/bets*100,1)
                                ev = round(pnl/bets,2)
                                if ev > 0:
                                    best.append({
                                        "bias":bias,"std":std,
                                        "edge_min":emin,"spread_max":smax,
                                        "n":bets,"wins":wins,
                                        "win_rate":wr,
                                        "pnl":round(pnl,2),
                                        "ev_per_bet":ev,
                                    })

            best.sort(key=lambda x: -x["ev_per_bet"])

            city_results[f"{days_out}d"] = {
                "n":          n,
                "dates":      f"{data[0]['date']} to {data[-1]['date']}",
                "avg_bias":   avg_bias,
                "avg_err":    avg_err,
                "std":        std_calc,
                "acc_1f":     acc_1f,
                "acc_2f":     acc_2f,
                "ranges":     range_stats,
                "best_configs": best[:5],
                "n_profitable": len(best),
            }

        results[city] = city_results

    # ── 5. Recommendation ──
    rec = {}
    for city, by_days in results.items():
        for days_key, stats in by_days.items():
            if isinstance(stats, dict) and stats.get("best_configs"):
                top = stats["best_configs"][0]
                rec[f"{city}_{days_key}"] = {
                    "bias":       top["bias"],
                    "std":        top["std"],
                    "edge_min":   top["edge_min"],
                    "spread_max": top["spread_max"],
                    "expected_win_rate": top["win_rate"],
                    "expected_ev_per_bet": top["ev_per_bet"],
                }

    return {
        "matched_counts": {c: {f"{d}d": len(v) for d,v in by_d.items()}
                           for c, by_d in matched.items()},
        "results":     results,
        "recommended_params": rec,
    }


@app.get("/discover-cities")
def discover_cities():
    """
    Auto-discovers all active Polymarket temperature markets.
    Tests known city slugs for today+1 and today+2.
    Returns full list with volume, unit, slug, and trading readiness.
    """
    import requests as req
    from datetime import date, timedelta

    # All known cities from Polymarket - expand this list as new ones appear
    CANDIDATE_CITIES = [
        # US - Fahrenheit
        ("Atlanta",      "atlanta",       33.749,  -84.388, "America/New_York",  "F", "KATL"),
        ("Dallas",       "dallas",        32.776,  -96.797, "America/Chicago",   "F", "KDAL"),
        ("NYC",          "nyc",           40.713,  -74.006, "America/New_York",  "F", "KLGA"),
        ("Houston",      "houston",       29.760,  -95.370, "America/Chicago",   "F", "KHOU"),
        ("Miami",        "miami",         25.762,  -80.192, "America/New_York",  "F", "KMIA"),
        ("Los Angeles",  "los-angeles",   34.052, -118.244, "America/Los_Angeles","F","KLAX"),
        ("Chicago",      "chicago",       41.878,  -87.630, "America/Chicago",   "F", "KORD"),
        ("Phoenix",      "phoenix",       33.448, -112.074, "America/Phoenix",   "F", "KPHX"),
        ("Seattle",      "seattle",       47.606, -122.332, "America/Los_Angeles","F","KSEA"),
        ("Denver",       "denver",        39.739, -104.984, "America/Denver",    "F", "KDEN"),
        # International - Celsius
        ("London",       "london",        51.507,   -0.128, "Europe/London",     "C", "EGLC"),
        ("Tokyo",        "tokyo",         35.690,  139.692, "Asia/Tokyo",        "C", "RJTT"),
        ("Istanbul",     "istanbul",      41.015,   28.979, "Europe/Istanbul",   "C", "LTBA"),
        ("Milan",        "milan",         45.465,    9.186, "Europe/Rome",       "C", "LIMC"),
        ("Tel Aviv",     "tel-aviv",      32.085,   34.782, "Asia/Jerusalem",    "C", "LLBG"),
        ("Sao Paulo",    "sao-paulo",    -23.550,  -46.633, "America/Sao_Paulo", "C", "SBGR"),
        ("Lagos",        "lagos",          6.455,    3.384, "Africa/Lagos",      "C", "DNMM"),
        ("Wuhan",        "wuhan",         30.593,  114.305, "Asia/Shanghai",     "C", "ZHHH"),
        ("Munich",       "munich",        48.135,   11.582, "Europe/Berlin",     "C", "EDDM"),
        ("Qingdao",      "qingdao",       36.067,  120.383, "Asia/Shanghai",     "C", "ZSQD"),
        ("Panama City",  "panama-city",    8.994,  -79.519, "America/Panama",   "C", "MPTO"),
        ("Madrid",       "madrid",        40.417,   -3.704, "Europe/Madrid",     "C", "LEMD"),
        ("Taipei",       "taipei",        25.048,  121.514, "Asia/Taipei",       "C", "RCTP"),
        ("Jakarta",      "jakarta",       -6.175,  106.827, "Asia/Jakarta",      "C", "WIII"),
        ("Cape Town",    "cape-town",    -33.926,   18.424, "Africa/Johannesburg","C","FACT"),
        ("Shanghai",     "shanghai",      31.230,  121.474, "Asia/Shanghai",     "C", "ZSPD"),
        ("Karachi",      "karachi",       24.861,   67.011, "Asia/Karachi",      "C", "OPKC"),
        ("Toronto",      "toronto",       43.651,  -79.347, "America/Toronto",   "C", "CYYZ"),
        ("Paris",        "paris",         48.857,    2.347, "Europe/Paris",      "C", "LFPG"),
        ("Mexico City",  "mexico-city",   19.433,  -99.133, "America/Mexico_City","C","MMMX"),
        ("Dubai",        "dubai",         25.204,   55.271, "Asia/Dubai",        "C", "OMDB"),
        ("Singapore",    "singapore",      1.352,  103.820, "Asia/Singapore",    "C", "WSSS"),
        ("Bangkok",      "bangkok",       13.756,  100.502, "Asia/Bangkok",      "C", "VTBS"),
        ("Sydney",       "sydney",       -33.869,  151.209, "Australia/Sydney",  "C", "YSSY"),
        ("Berlin",       "berlin",        52.520,   13.405, "Europe/Berlin",     "C", "EDDB"),
        ("Barcelona",    "barcelona",     41.385,    2.173, "Europe/Madrid",     "C", "LEBL"),
        ("Amsterdam",    "amsterdam",     52.374,    4.890, "Europe/Amsterdam",  "C", "EHAM"),
        ("New Orleans",  "new-orleans",   29.951,  -90.071, "America/Chicago",   "F", "KMSY"),
    ]

    today = date.today()
    results = []
    not_found = []

    for city_name, slug, lat, lon, tz, unit, station in CANDIDATE_CITIES:
        found = False
        for days_out in [1, 2]:
            test_date = today + timedelta(days=days_out)
            month = test_date.strftime("%B").lower()
            full_slug = f"highest-temperature-in-{slug}-on-{month}-{test_date.day}-{test_date.year}"
            try:
                r = req.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"slug": full_slug},
                    timeout=8,
                    headers={"User-Agent": "PolyEdge/1.0"}
                )
                if r.status_code == 200:
                    data = r.json()
                    if data and len(data) > 0 and data[0].get("markets"):
                        markets = data[0]["markets"]
                        vol = sum(float(m.get("volumeNum", 0) or 0) for m in markets)
                        active = sum(1 for m in markets if m.get("active"))
                        results.append({
                            "city": city_name,
                            "slug": slug,
                            "lat": lat,
                            "lon": lon,
                            "timezone": tz,
                            "unit": unit,
                            "wu_station": station,
                            "ranges": len(markets),
                            "active_ranges": active,
                            "volume_usd": round(vol),
                            "days_out": days_out,
                            "test_date": str(test_date),
                            "tradeable": False,  # needs 20+ days forecast data
                            "notes": "Add to forecast_logger to start collecting data"
                        })
                        found = True
                        break
            except Exception as e:
                pass

        if not found:
            not_found.append(city_name)

    # Sort by volume descending
    results.sort(key=lambda x: -x["volume_usd"])

    return {
        "checked_at": date.today().isoformat(),
        "total_found": len(results),
        "total_not_found": len(not_found),
        "active_cities": results,
        "no_market": not_found,
        "instructions": (
            "To add a city: copy its entry into CITY_ACCURACY in paper_trade.py "
            "and add to ALL_CITIES in forecast_logger.py. "
            "Wait 20+ days for accuracy baseline before trading."
        )
    }

@app.get("/migrate-dedup")
def migrate_dedup():
    """
    One-time migration: deduplicate market_ids and add unique constraint.
    Keeps the BEST trade per market_id (highest edge, or most recent).
    Safe to run multiple times.
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        # Step 1: Find all duplicate market_ids
        c.execute("""
            SELECT market_id, COUNT(*) as cnt
            FROM paper_trades
            GROUP BY market_id
            HAVING COUNT(*) > 1
        """)
        dupes = c.fetchall()
        removed = 0

        # Step 2: For each duplicate, keep the one with highest edge (or highest id)
        for row in dupes:
            mid = row["market_id"]
            c.execute("""
                DELETE FROM paper_trades
                WHERE market_id = %s
                AND id NOT IN (
                    SELECT id FROM paper_trades
                    WHERE market_id = %s
                    ORDER BY COALESCE(edge, 0) DESC, id DESC
                    LIMIT 1
                )
            """, (mid, mid))
            removed += c.rowcount

        # Step 3: Drop old constraint if exists
        c.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'paper_trades_market_id_trade_date_key'
                ) THEN
                    ALTER TABLE paper_trades
                    DROP CONSTRAINT paper_trades_market_id_trade_date_key;
                END IF;
            END$$;
        """)

        # Step 4: Add unique constraint on market_id alone
        c.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'paper_trades_market_id_key'
                ) THEN
                    ALTER TABLE paper_trades
                    ADD CONSTRAINT paper_trades_market_id_key UNIQUE (market_id);
                END IF;
            END$$;
        """)

        conn.commit()
        conn.close()
        return {
            "status": "✅ done",
            "duplicates_found": len(dupes),
            "rows_removed": removed,
            "message": "Duplicates cleaned, unique constraint on market_id applied"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/diagnostics")
def diagnostics():
    """
    Full system health check — one URL, no guessing.
    Shows scanner status, API health, ingest status, trade P&L, and recent scan decisions.
    """
    import time as _time
    from datetime import datetime, timezone, timedelta
    EST = timezone(timedelta(hours=-4))
    now_est = datetime.now(EST)

    out = {}

    # ── 1. SCANNER STATUS ──
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT scanned_at, decision, city, reason FROM scan_log ORDER BY id DESC LIMIT 1")
        last = c.fetchone()
        c.execute("SELECT COUNT(*) as n FROM scan_log WHERE scanned_at >= %s",
                  ((now_est - timedelta(hours=2)).strftime("%Y-%m-%d %I:%M %p EST"),))
        recent_scans = c.fetchone()["n"]
        conn.close()

        if last:
            last_scan_str = last["scanned_at"]
            # Parse and compute age
            try:
                from datetime import datetime as dt
                last_dt = dt.strptime(last_scan_str, "%Y-%m-%d %I:%M %p EST")
                last_dt = last_dt.replace(tzinfo=EST)
                age_min = int((now_est - last_dt).total_seconds() / 60)
                scanner_ok = age_min < 75  # should scan every hour
                out["scanner"] = {
                    "status": "✅ OK" if scanner_ok else "❌ STALE",
                    "last_scan": last_scan_str,
                    "minutes_ago": age_min,
                    "scans_last_2hrs": recent_scans,
                    "last_decision": last["decision"],
                    "last_city": last["city"],
                }
            except Exception as e:
                out["scanner"] = {"status": "⚠️ parse error", "last_scan": last_scan_str, "error": str(e)}
        else:
            out["scanner"] = {"status": "❌ NO SCANS EVER", "last_scan": None}
    except Exception as e:
        out["scanner"] = {"status": "❌ DB ERROR", "error": str(e)}

    # ── 2. OPEN-METEO API ──
    try:
        import requests as req
        r = req.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": 33.749, "longitude": -84.388,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "America/New_York",
            "forecast_days": 1,
        }, timeout=8)
        if r.status_code == 200 and "daily" in r.json():
            out["open_meteo"] = {"status": "✅ OK", "test_temp": r.json()["daily"]["temperature_2m_max"][0]}
        elif "limit exceeded" in r.text.lower():
            out["open_meteo"] = {"status": "❌ RATE LIMITED", "resets": "tomorrow UTC midnight"}
        else:
            out["open_meteo"] = {"status": f"❌ HTTP {r.status_code}"}
    except Exception as e:
        out["open_meteo"] = {"status": "❌ ERROR", "error": str(e)}

    # ── 3. POLYMARKET API ──
    try:
        import requests as req
        r = req.get("https://gamma-api.polymarket.com/events",
                    params={"slug": "highest-temperature-in-atlanta-on-may-11-2026"}, timeout=8)
        if r.status_code == 200:
            out["polymarket"] = {"status": "✅ OK"}
        else:
            out["polymarket"] = {"status": f"❌ HTTP {r.status_code}"}
    except Exception as e:
        out["polymarket"] = {"status": "❌ ERROR", "error": str(e)}

    # ── 4. INGEST STATUS ──
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM markets")
        total_markets = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM markets WHERE outcome IS NULL")
        open_markets = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM price_snapshots")
        snapshots = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM wu_temps")
        wu_rows = c.fetchone()["n"]
        conn.close()
        out["ingest"] = {
            "total_markets": total_markets,
            "open_markets": open_markets,
            "price_snapshots": snapshots,
            "wu_temp_rows": wu_rows,
        }
    except Exception as e:
        out["ingest"] = {"status": "❌ DB ERROR", "error": str(e)}

    # ── 5. PAPER TRADES P&L ──
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome='No' THEN 1 ELSE 0 END) as losses,
                COUNT(CASE WHEN outcome IS NULL THEN 1 END) as pending,
                ROUND(SUM(COALESCE(pnl,0))::numeric, 2) as pnl,
                COUNT(CASE WHEN wu_actual IS NOT NULL THEN 1 END) as with_wu_actual
            FROM paper_trades
        """)
        row = dict(c.fetchone())
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        wr = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
        out["trades"] = {
            "total": row["total"],
            "wins": wins,
            "losses": losses,
            "pending": row["pending"],
            "win_rate_pct": wr,
            "total_pnl": float(row["pnl"] or 0),
            "trades_with_wu_actual": row["with_wu_actual"],
            "wu_actual_coverage": f"{int(row['with_wu_actual'] or 0)}/{row['total']}",
        }
        conn.close()
    except Exception as e:
        out["trades"] = {"status": "❌ DB ERROR", "error": str(e)}

    # ── 6. LAST 10 SCAN DECISIONS ──
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT scanned_at, city, target_date, decision, reason, gfs_temp, consensus, spread
            FROM scan_log ORDER BY id DESC LIMIT 10
        """)
        out["recent_scans"] = [dict(r) for r in c.fetchall()]
        conn.close()
    except Exception as e:
        out["recent_scans"] = []

    # ── 7. PENDING BETS ──
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT id, city, target_date, question, entry_price_c, edge, bet_size
            FROM paper_trades WHERE outcome IS NULL ORDER BY id DESC
        """)
        out["pending_bets"] = [dict(r) for r in c.fetchall()]
        conn.close()
    except Exception as e:
        out["pending_bets"] = []

    # ── 8. CONTAINER UPTIME ──
    out["checked_at"] = now_est.strftime("%Y-%m-%d %I:%M %p EST")
    out["overall_status"] = "✅ ALL SYSTEMS GO" if (
        "✅" in out.get("scanner", {}).get("status", "") and
        "✅" in out.get("open_meteo", {}).get("status", "") and
        "✅" in out.get("polymarket", {}).get("status", "")
    ) else "⚠️ ISSUES DETECTED — see above"

    return out

@app.get("/run-tests")
def run_tests():
    """Run full variable test suite. Returns pass/fail for every assumption."""
    try:
        import sys, os
        sys.path.insert(0, "/app")
        from test_suite import run_all_tests
        return run_all_tests()
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@app.get("/test-openmeteo")
def test_openmeteo():
    """Test Open-Meteo API directly and show raw response."""
    import requests as _req
    from datetime import date as _d, timedelta as _td
    target = (_d.today() + _td(days=4)).strftime("%Y-%m-%d")
    results = {}
    models = {
        "gfs":  "gfs_global",
        "ukmo": "ukmo_global_deterministic_10km",
        "mf":   "meteofrance_arpege_world",
    }
    for name, code in models.items():
        try:
            r = _req.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": 33.749, "longitude": -84.388,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "America/New_York",
                "start_date": target, "end_date": target,
                "models": code,
            }, timeout=15)
            data = r.json()
            if "daily" in data:
                temps = data["daily"].get("temperature_2m_max", [])
                results[name] = {"status": "OK", "temp": temps[0] if temps else None}
            else:
                results[name] = {"status": "ERROR", "response": str(data)[:200]}
        except Exception as e:
            results[name] = {"status": "EXCEPTION", "error": str(e)}
    return {"target_date": target, "models": results}





@app.get("/backtest/bias-corrected")
def backtest_bias_corrected():
    """
    90-day backtest WITH bias correction applied.
    Shifts forecast down by real bias before checking which ranges to bet.
    This is what the system actually does when placing trades.
    """
    import requests as _req
    from datetime import date as _date, timedelta as _td
    import statistics as _stats

    CITIES = {
        "Atlanta": {"lat": 33.749, "lon": -84.388, "unit": "fahrenheit", "bias": 1.17},
        "Dallas":  {"lat": 32.776, "lon": -96.797, "unit": "fahrenheit", "bias": 1.37},
        "NYC":     {"lat": 40.713, "lon": -74.006, "unit": "fahrenheit", "bias": 1.13},
    }

    end_date   = _date.today() - _td(days=1)
    start_date = end_date - _td(days=89)

    results = {}

    for city, cfg in CITIES.items():
        try:
            fc_r = _req.get(
                "https://historical-forecast-api.open-meteo.com/v1/forecast",
                params={
                    "latitude":         cfg["lat"],
                    "longitude":        cfg["lon"],
                    "start_date":       str(start_date),
                    "end_date":         str(end_date),
                    "daily":            "temperature_2m_max",
                    "temperature_unit": cfg["unit"],
                    "models":           "gfs_seamless",
                    "timezone":         "auto",
                },
                timeout=20
            )
            fc_data = fc_r.json().get("daily", {})
            fc_dates = fc_data.get("time", [])
            fc_temps = fc_data.get("temperature_2m_max", [])

            ac_r = _req.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude":         cfg["lat"],
                    "longitude":        cfg["lon"],
                    "start_date":       str(start_date),
                    "end_date":         str(end_date),
                    "daily":            "temperature_2m_max",
                    "temperature_unit": cfg["unit"],
                    "timezone":         "auto",
                },
                timeout=20
            )
            ac_data = ac_r.json().get("daily", {})
            ac_dates = ac_data.get("time", [])
            ac_temps = ac_data.get("temperature_2m_max", [])
            ac_map = {d: t for d, t in zip(ac_dates, ac_temps) if t is not None}

            days_analyzed = 0
            single_wins = triple_wins = five_wins = seven_wins = 0
            errors_after = []
            miss_details = []
            day_details  = []

            for d, fc in zip(fc_dates, fc_temps):
                if fc is None or d not in ac_map:
                    continue
                actual  = ac_map[d]
                # Apply bias correction — shift forecast down
                corrected = fc - cfg["bias"]
                error_after = round(corrected - actual, 1)
                errors_after.append(error_after)
                days_analyzed += 1

                # Base range on CORRECTED forecast
                lo = int(corrected)
                hi = lo + 1

                hit_single = lo <= actual < hi
                hit_triple = (lo - 1) <= actual < (hi + 1)
                hit_five   = (lo - 2) <= actual < (hi + 2)
                hit_seven  = (lo - 3) <= actual < (hi + 3)

                if hit_single: single_wins += 1
                if hit_triple: triple_wins += 1
                if hit_five:   five_wins   += 1
                if hit_seven:  seven_wins  += 1

                day_details.append({
                    "date":      d,
                    "forecast":  fc,
                    "corrected": round(corrected, 1),
                    "actual":    actual,
                    "error":     error_after,
                    "hit_1":     hit_single,
                    "hit_3":     hit_triple,
                    "hit_5":     hit_five,
                })

                if not hit_five:
                    miss_details.append({
                        "date":      d,
                        "forecast":  fc,
                        "corrected": round(corrected, 1),
                        "actual":    actual,
                        "missed_by": round(abs(error_after), 1)
                    })

            if not errors_after:
                results[city] = {"status": "no data"}
                continue

            bias_after = round(sum(errors_after) / len(errors_after), 2)
            std_after  = round(_stats.stdev(errors_after), 2)

            within_1 = sum(1 for e in errors_after if abs(e) <= 1.0)
            within_2 = sum(1 for e in errors_after if abs(e) <= 2.0)
            within_3 = sum(1 for e in errors_after if abs(e) <= 3.0)

            s1  = round(single_wins / days_analyzed * 100, 1)
            s3  = round(triple_wins / days_analyzed * 100, 1)
            s5  = round(five_wins   / days_analyzed * 100, 1)
            s7  = round(seven_wins  / days_analyzed * 100, 1)

            # Profitability estimate
            # Assume average combined cost: 1-range=8¢, 3-range=20¢, 5-range=32¢
            # Payout = $1 per share, bet size = $10
            # Profit per win = (1/cost - 1) * bet_size
            est_costs = {"1": 0.08, "3": 0.20, "5": 0.32}
            profit_1 = round((s1/100) * (1/est_costs["1"] - 1) * 10 - (1 - s1/100) * 10, 2)
            profit_3 = round((s3/100) * (1/est_costs["3"] - 1) * 10 - (1 - s3/100) * 10, 2)
            profit_5 = round((s5/100) * (1/est_costs["5"] - 1) * 10 - (1 - s5/100) * 10, 2)

            if s5 >= 80:   rec = "✅ BET 5 RANGES — strong edge"
            elif s5 >= 70: rec = "✅ BET 5 RANGES — good edge"
            elif s3 >= 70: rec = "✅ BET 3 RANGES — good edge"
            elif s5 >= 60: rec = "⚠️ BET 5 RANGES — marginal, needs real price data"
            else:          rec = "❌ NOT READY — win rate too low"

            results[city] = {
                "days_analyzed":   days_analyzed,
                "bias_applied":    cfg["bias"],
                "residual_bias":   bias_after,
                "residual_std":    std_after,
                "win_rates": {
                    "1_range": f"{s1}%",
                    "3_range": f"{s3}%",
                    "5_range": f"{s5}%",
                    "7_range": f"{s7}%",
                },
                "error_distribution": {
                    "within_1f": f"{round(within_1/days_analyzed*100,1)}%",
                    "within_2f": f"{round(within_2/days_analyzed*100,1)}%",
                    "within_3f": f"{round(within_3/days_analyzed*100,1)}%",
                },
                "estimated_profit_per_10_bets": {
                    "1_range": f"${profit_1}",
                    "3_range": f"${profit_3}",
                    "5_range": f"${profit_5}",
                },
                "five_range_misses": sorted(miss_details, key=lambda x: x["missed_by"], reverse=True)[:5],
                "recommendation": rec,
            }

        except Exception as e:
            import traceback
            results[city] = {"status": f"error: {str(e)}", "trace": traceback.format_exc()[:300]}

    # Summary
    summary = {}
    for city, r in results.items():
        if "win_rates" in r:
            summary[city] = {
                "5_range_win_rate":    r["win_rates"]["5_range"],
                "3_range_win_rate":    r["win_rates"]["3_range"],
                "residual_std":        r["residual_std"],
                "est_profit_5_ranges": r["estimated_profit_per_10_bets"]["5_range"],
                "recommendation":      r["recommendation"],
            }

    return {
        "period":  f"{start_date} to {end_date}",
        "days":    90,
        "note":    "Bias correction applied — forecast shifted down by city bias before range selection",
        "summary": summary,
        "detail":  results,
    }

@app.get("/backtest/forecast-accuracy")
def backtest_forecast_accuracy():
    """
    90-day backtest of forecast accuracy per city.
    For each day, checks:
    - Single range: did actual land exactly on forecast range?
    - 3-range: did actual land within ±1F of forecast?
    - 5-range: did actual land within ±2F of forecast?
    Uses real Open-Meteo historical forecasts and archive actuals.
    No guessing. No fake data.
    """
    import requests as _req
    from datetime import date as _date, timedelta as _td

    CITIES = {
        "Atlanta": {"lat": 33.749, "lon": -84.388, "unit": "fahrenheit"},
        "Dallas":  {"lat": 32.776, "lon": -96.797, "unit": "fahrenheit"},
        "NYC":     {"lat": 40.713, "lon": -74.006, "unit": "fahrenheit"},
    }

    # 90 days back from yesterday (archive needs 1+ day lag)
    end_date   = _date.today() - _td(days=1)
    start_date = end_date - _td(days=89)

    results = {}

    for city, cfg in CITIES.items():
        try:
            # Fetch historical GFS forecasts (what model predicted each day)
            fc_r = _req.get(
                "https://historical-forecast-api.open-meteo.com/v1/forecast",
                params={
                    "latitude":         cfg["lat"],
                    "longitude":        cfg["lon"],
                    "start_date":       str(start_date),
                    "end_date":         str(end_date),
                    "daily":            "temperature_2m_max",
                    "temperature_unit": cfg["unit"],
                    "models":           "gfs_seamless",
                    "timezone":         "auto",
                },
                timeout=20
            )
            fc_data = fc_r.json().get("daily", {})
            fc_dates = fc_data.get("time", [])
            fc_temps = fc_data.get("temperature_2m_max", [])

            # Fetch actual observed temperatures
            ac_r = _req.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude":         cfg["lat"],
                    "longitude":        cfg["lon"],
                    "start_date":       str(start_date),
                    "end_date":         str(end_date),
                    "daily":            "temperature_2m_max",
                    "temperature_unit": cfg["unit"],
                    "timezone":         "auto",
                },
                timeout=20
            )
            ac_data = ac_r.json().get("daily", {})
            ac_dates = ac_data.get("time", [])
            ac_temps = ac_data.get("temperature_2m_max", [])

            # Align by date
            ac_map = {d: t for d, t in zip(ac_dates, ac_temps) if t is not None}

            days_analyzed = 0
            single_wins   = 0  # exact range hit
            triple_wins   = 0  # ±1F
            five_wins     = 0  # ±2F
            errors        = []  # signed errors (forecast - actual)
            miss_details  = []  # days we missed and by how much

            for d, fc in zip(fc_dates, fc_temps):
                if fc is None or d not in ac_map:
                    continue
                actual = ac_map[d]
                error  = round(fc - actual, 1)  # positive = model ran warm
                errors.append(error)
                days_analyzed += 1

                # Single range: forecast rounded to nearest int
                fc_range_lo = int(fc)
                fc_range_hi = fc_range_lo + 1

                # Did actual land in exact range?
                if fc_range_lo <= actual < fc_range_hi:
                    single_wins += 1
                    triple_wins += 1
                    five_wins   += 1
                # ±1F (3 ranges)
                elif (fc_range_lo - 1) <= actual < (fc_range_hi + 1):
                    triple_wins += 1
                    five_wins   += 1
                # ±2F (5 ranges)
                elif (fc_range_lo - 2) <= actual < (fc_range_hi + 2):
                    five_wins += 1
                else:
                    miss_details.append({
                        "date": d,
                        "forecast": fc,
                        "actual": actual,
                        "error": error,
                        "missed_by": round(abs(error), 1)
                    })

            if not errors:
                results[city] = {"status": "no data"}
                continue

            import statistics as _stats
            bias = round(sum(errors) / len(errors), 2)
            std  = round(_stats.stdev(errors), 2)

            # Error distribution
            within_1 = sum(1 for e in errors if abs(e) <= 1.0)
            within_2 = sum(1 for e in errors if abs(e) <= 2.0)
            within_3 = sum(1 for e in errors if abs(e) <= 3.0)

            results[city] = {
                "days_analyzed": days_analyzed,
                "bias_f": bias,
                "std_f":  std,
                "single_range_win_rate": f"{round(single_wins/days_analyzed*100,1)}%",
                "triple_range_win_rate": f"{round(triple_wins/days_analyzed*100,1)}%",
                "five_range_win_rate":   f"{round(five_wins/days_analyzed*100,1)}%",
                "error_distribution": {
                    "within_1f": f"{round(within_1/days_analyzed*100,1)}%",
                    "within_2f": f"{round(within_2/days_analyzed*100,1)}%",
                    "within_3f": f"{round(within_3/days_analyzed*100,1)}%",
                },
                "worst_misses": sorted(miss_details, key=lambda x: x["missed_by"], reverse=True)[:5],
                "recommendation": (
                    "✅ BET 3 RANGES" if triple_wins/days_analyzed >= 0.80
                    else "⚠️ BET 3 RANGES WITH CAUTION" if triple_wins/days_analyzed >= 0.65
                    else "❌ NEEDS MORE ANALYSIS"
                )
            }

        except Exception as e:
            import traceback
            results[city] = {"status": f"error: {str(e)}", "trace": traceback.format_exc()[:300]}

    # Overall summary
    summary = {}
    for city, r in results.items():
        if "triple_range_win_rate" in r:
            summary[city] = {
                "3_range_win_rate": r["triple_range_win_rate"],
                "bias": r["bias_f"],
                "std":  r["std_f"],
                "recommendation": r["recommendation"]
            }

    return {
        "period": f"{start_date} to {end_date}",
        "days": 90,
        "summary": summary,
        "detail": results
    }


@app.get("/price-history")
def price_history(city: str = None, days: int = 2):
    """
    Show how market prices moved hour by hour.
    Use this to find the optimal entry time.
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        # Create table if not exists yet
        c.execute("""
            CREATE TABLE IF NOT EXISTS price_log (
                id          SERIAL PRIMARY KEY,
                logged_at   TEXT,
                city        TEXT,
                target_date TEXT,
                days_out    INT,
                consensus   REAL,
                corrected   REAL,
                unit        TEXT,
                market_id   TEXT,
                question    TEXT,
                price_c     REAL,
                yes_price   REAL,
                volume      REAL
            )
        """)

        from datetime import date as _date, timedelta as _td
        since = str(_date.today() - _td(days=days))

        query = """
            SELECT logged_at, city, target_date, days_out,
                   consensus, corrected, unit,
                   question, price_c, volume
            FROM price_log
            WHERE target_date >= %s
        """
        params = [since]

        if city:
            query += " AND city = %s"
            params.append(city)

        query += " ORDER BY city, target_date, question, logged_at"

        c.execute(query, params)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Group by city → target_date → question → price over time
        grouped = {}
        for r in rows:
            key_city = r["city"]
            key_date = r["target_date"]
            key_q    = r["question"][:50] if r["question"] else ""

            if key_city not in grouped:
                grouped[key_city] = {}
            if key_date not in grouped[key_city]:
                grouped[key_city][key_date] = {}
            if key_q not in grouped[key_city][key_date]:
                grouped[key_city][key_date][key_q] = []

            grouped[key_city][key_date][key_q].append({
                "time":    r["logged_at"],
                "price_c": r["price_c"],
                "volume":  r["volume"],
            })

        # Summary: price range per range per day
        summary = {}
        for city_name, dates in grouped.items():
            summary[city_name] = {}
            for tdate, questions in dates.items():
                summary[city_name][tdate] = {}
                for q, prices in questions.items():
                    price_list = [p["price_c"] for p in prices if p["price_c"]]
                    if price_list:
                        summary[city_name][tdate][q] = {
                            "min_c":   min(price_list),
                            "max_c":   max(price_list),
                            "first_c": price_list[0],
                            "last_c":  price_list[-1],
                            "scans":   len(price_list),
                            "movement": round(price_list[-1] - price_list[0], 2),
                        }

        return {
            "total_rows": len(rows),
            "summary": summary,
            "raw": rows[:200],
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}

@app.get("/test-backtest-prices")
def test_backtest_prices():
    """
    Dig deeper into Polymarket historical price options.
    Try multiple approaches to get past market prices.
    """
    import requests as _requests
    results = {}

    condition_id = "0xdf06cad12a0ec7e34331fa8c35a8aa2f668a5fc5f7c4e9d961ac538d155ea357"
    token_id = "29185769007270931991136565589743280866291002679444898357035077087191303042599"

    # Approach 1: CLOB trades endpoint
    try:
        r = _requests.get(
            f"https://clob.polymarket.com/trades",
            params={"market": condition_id, "limit": 5},
            timeout=10
        )
        data = r.json()
        results["clob_trades"] = {
            "status": "✅" if data else "⚠️ empty",
            "count": len(data) if isinstance(data, list) else None,
            "sample": data[:2] if isinstance(data, list) and data else data
        }
    except Exception as e:
        results["clob_trades"] = {"status": f"❌ {str(e)[:100]}"}

    # Approach 2: CLOB price history with token ID instead
    try:
        r2 = _requests.get(
            f"https://clob.polymarket.com/prices-history",
            params={"market": token_id, "interval": "all", "fidelity": 60},
            timeout=10
        )
        data2 = r2.json()
        history = data2.get("history", [])
        results["clob_prices_by_token"] = {
            "status": "✅ available" if history else "⚠️ empty",
            "points": len(history),
            "first": history[0] if history else None,
            "last": history[-1] if history else None,
        }
    except Exception as e:
        results["clob_prices_by_token"] = {"status": f"❌ {str(e)[:100]}"}

    # Approach 3: Gamma market outcomePrices (snapshot)
    try:
        r3 = _requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": "highest-temperature-in-atlanta-on-april-1-2026"},
            timeout=10
        )
        data3 = r3.json()
        if data3 and data3[0].get("markets"):
            markets = data3[0]["markets"]
            prices = []
            for m in markets[:3]:
                prices.append({
                    "question": m.get("question", "")[:50],
                    "outcomePrices": m.get("outcomePrices"),
                    "volume": m.get("volume"),
                    "lastTradePrice": m.get("lastTradePrice"),
                    "bestBid": m.get("bestBid"),
                    "bestAsk": m.get("bestAsk"),
                })
            results["gamma_price_snapshot"] = {
                "status": "✅ found",
                "note": "These are FINAL prices, not historical",
                "markets": prices
            }
    except Exception as e:
        results["gamma_price_snapshot"] = {"status": f"❌ {str(e)[:100]}"}

    # Approach 4: Check if Polymarket has a timeseries endpoint
    try:
        r4 = _requests.get(
            f"https://clob.polymarket.com/timeseries",
            params={"market": condition_id},
            timeout=10
        )
        results["clob_timeseries"] = {
            "status_code": r4.status_code,
            "response": r4.json() if r4.status_code == 200 else r4.text[:200]
        }
    except Exception as e:
        results["clob_timeseries"] = {"status": f"❌ {str(e)[:100]}"}

    return results


@app.get("/archive-old-trades")
def archive_old_trades():
    """
    Move all pre-clean-system trades to paper_trades_archive.
    Clean system = trusted_city=True, placed after 2026-04-28.
    Safe to run multiple times.
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades_archive
            AS SELECT * FROM paper_trades WHERE 1=0
        """)

        # Move old/broken trades to archive
        c.execute("""
            INSERT INTO paper_trades_archive
            SELECT * FROM paper_trades
            WHERE trusted_city IS NULL
               OR trusted_city = false
               OR (trusted_city = true AND edge < 0.20 AND edge IS NOT NULL)
        """)
        archived = c.rowcount

        c.execute("""
            DELETE FROM paper_trades
            WHERE trusted_city IS NULL
               OR trusted_city = false
               OR (trusted_city = true AND edge < 0.20 AND edge IS NOT NULL)
        """)

        conn.commit()
        conn.close()
        return {
            "status": "✅ done",
            "archived": archived,
            "message": f"Moved {archived} old trades to paper_trades_archive. Clean system only remains."
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/dashboard")
def dashboard():
    """
    Clean trading dashboard — shows only clean system trades.
    Returns all data needed for the frontend dashboard.
    """
    try:
        conn = get_conn()
        c = conn.cursor()

        # Clean trades summary
        c.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome='No' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(pnl), 0) as total_pnl,
                COALESCE(SUM(bet_size), 0) as total_invested
            FROM paper_trades
            WHERE trusted_city = true
        """)
        summary = dict(c.fetchone())
        resolved = summary['wins'] + summary['losses']
        summary['win_rate'] = round(summary['wins'] / resolved * 100, 1) if resolved > 0 else 0

        # Pending trades with potential payout
        c.execute("""
            SELECT id, city, question, target_date, entry_price_c,
                   bet_size, edge, true_prob, market_prob,
                   placed_at, days_out, forecast_temp
            FROM paper_trades
            WHERE trusted_city = true AND outcome IS NULL
            ORDER BY placed_at DESC
        """)
        pending = []
        for row in c.fetchall():
            r = dict(row)
            if r['entry_price_c'] and r['entry_price_c'] > 0:
                potential = round(r['bet_size'] * (100 / r['entry_price_c'] - 1), 2)
            else:
                potential = 0
            r['potential_profit'] = potential
            pending.append(r)

        # Recent resolved trades
        c.execute("""
            SELECT id, city, question, target_date, entry_price_c,
                   bet_size, outcome, pnl, edge, resolved_at, wu_actual
            FROM paper_trades
            WHERE trusted_city = true AND outcome IS NOT NULL
            ORDER BY resolved_at DESC
            LIMIT 20
        """)
        recent = [dict(r) for r in c.fetchall()]

        # By city stats
        c.execute("""
            SELECT city,
                   COUNT(*) as bets,
                   SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
                   COALESCE(SUM(pnl), 0) as pnl,
                   COALESCE(AVG(edge), 0) as avg_edge
            FROM paper_trades
            WHERE trusted_city = true
            GROUP BY city
            ORDER BY pnl DESC
        """)
        by_city = [dict(r) for r in c.fetchall()]

        # Price history summary (last 2 days)
        try:
            c.execute("""
                SELECT city, target_date, question, 
                       MIN(price_c) as min_price,
                       MAX(price_c) as max_price,
                       (array_agg(price_c ORDER BY logged_at ASC))[1] as open_price,
                       (array_agg(price_c ORDER BY logged_at DESC))[1] as current_price,
                       COUNT(*) as scans
                FROM price_log
                WHERE logged_at >= NOW() - INTERVAL '48 hours'
                GROUP BY city, target_date, question
                ORDER BY city, target_date, min_price DESC
                LIMIT 50
            """)
            price_data = [dict(r) for r in c.fetchall()]
        except:
            price_data = []

        conn.close()

        # Calculate total potential if all pending win
        total_potential = sum(p['potential_profit'] for p in pending)
        pending_invested = sum(p['bet_size'] for p in pending)

        return {
            "summary": {
                **summary,
                "pending_invested": round(pending_invested, 2),
                "potential_if_all_win": round(total_potential + summary['total_pnl'], 2),
                "worst_case": round(summary['total_pnl'] - pending_invested, 2),
            },
            "pending": pending,
            "recent_resolved": recent,
            "by_city": by_city,
            "price_movement": price_data,
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}

@app.get("/test-backtest-apis")
def test_backtest_apis():
    """
    Test whether historical data APIs are available for backtesting.
    Checks:
    1. Polymarket CLOB price history for a known past Atlanta market
    2. Open-Meteo historical forecast API (forecast-as-of-that-day)
    3. Open-Meteo archive API (actual observed temperatures)
    """
    import requests as _requests
    from datetime import date as _date
    results = {}

    # ── TEST 1: Polymarket CLOB price history ──────────────────────────
    try:
        # Find April 1 Atlanta market
        r = _requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": "highest-temperature-in-atlanta-on-april-1-2026"},
            timeout=10
        )
        data = r.json()
        if data and isinstance(data, list) and data[0].get("markets"):
            m = data[0]["markets"][0]
            condition_id = m.get("conditionId")
            token_ids = m.get("clobTokenIds")

            results["polymarket_gamma"] = {
                "status": "✅ found",
                "question": m.get("question", "")[:60],
                "conditionId": condition_id,
                "clobTokenIds": token_ids,
                "volume": m.get("volume"),
                "created_at": m.get("createdAt"),
            }

            # Try CLOB price history
            if condition_id:
                try:
                    clob_r = _requests.get(
                        f"https://clob.polymarket.com/prices-history",
                        params={
                            "market": condition_id,
                            "interval": "all",
                            "fidelity": 60
                        },
                        timeout=10
                    )
                    clob_data = clob_r.json()
                    history = clob_data.get("history", [])
                    results["polymarket_clob_history"] = {
                        "status": "✅ available" if history else "⚠️ empty",
                        "points": len(history),
                        "first": history[0] if history else None,
                        "last": history[-1] if history else None,
                    }
                except Exception as e2:
                    results["polymarket_clob_history"] = {"status": f"❌ {str(e2)[:100]}"}
        else:
            results["polymarket_gamma"] = {"status": "❌ market not found"}
    except Exception as e:
        results["polymarket_gamma"] = {"status": f"❌ {str(e)[:100]}"}

    # ── TEST 2: Open-Meteo historical forecast API ─────────────────────
    # This returns what the model predicted on that day — not reanalysis
    try:
        r2 = _requests.get(
            "https://historical-forecast-api.open-meteo.com/v1/forecast",
            params={
                "latitude": 33.749,
                "longitude": -84.388,
                "start_date": "2026-04-01",
                "end_date": "2026-04-03",
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "models": "gfs_seamless",
                "timezone": "America/New_York",
            },
            timeout=15
        )
        data2 = r2.json()
        daily = data2.get("daily", {})
        dates = daily.get("time", [])
        temps = daily.get("temperature_2m_max", [])
        results["openmeteo_historical_forecast"] = {
            "status": "✅ available" if dates else "❌ empty",
            "description": "Archived GFS forecasts — what model predicted on that day",
            "days_returned": len(dates),
            "sample": [{"date": d, "forecast_max_f": t} for d, t in zip(dates[:3], temps[:3])],
        }
    except Exception as e:
        results["openmeteo_historical_forecast"] = {"status": f"❌ {str(e)[:100]}"}

    # ── TEST 3: Open-Meteo archive (actual observed temps) ─────────────
    try:
        r3 = _requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": 33.749,
                "longitude": -84.388,
                "start_date": "2026-04-01",
                "end_date": "2026-04-03",
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "America/New_York",
            },
            timeout=15
        )
        data3 = r3.json()
        daily3 = data3.get("daily", {})
        dates3 = daily3.get("time", [])
        temps3 = daily3.get("temperature_2m_max", [])
        results["openmeteo_archive_actuals"] = {
            "status": "✅ available" if dates3 else "❌ empty",
            "description": "Real observed temperatures — ground truth for backtest",
            "days_returned": len(dates3),
            "sample": [{"date": d, "actual_max_f": t} for d, t in zip(dates3[:3], temps3[:3])],
        }
    except Exception as e:
        results["openmeteo_archive_actuals"] = {"status": f"❌ {str(e)[:100]}"}

    # ── SUMMARY ────────────────────────────────────────────────────────
    all_pass = all(
        "✅" in str(v.get("status", ""))
        for v in results.values()
    )
    results["summary"] = {
        "backtest_viable": all_pass,
        "verdict": "✅ All APIs available — backtest can use 90 days of real data"
                   if all_pass else
                   "⚠️ Some APIs unavailable — check individual results above"
    }

    return results

@app.get("/db-migrate")
def db_migrate():
    """Add missing columns to paper_trades table."""
    try:
        conn = get_conn()
        c = conn.cursor()
        migrations = [
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS placed_at TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS target_date TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS days_out INT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS entry_price_c REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS forecast_temp REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS gfs_temp REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS ukmo_temp REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS mf_temp REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS spread REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS confidence REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS unit TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS bet_size REAL DEFAULT 10.0",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS resolved_at TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS wu_actual REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS true_prob REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS market_prob REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS edge REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS bias_used REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS std_used REAL",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS trusted_city BOOLEAN",

            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS scanned_at TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS days_out INT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS gfs_temp REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS ukmo_temp REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS mf_temp REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS consensus REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS spread REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS unit TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS decision TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS reason TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS market_id TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS question TEXT",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS price_c REAL",
            "ALTER TABLE scan_log ADD COLUMN IF NOT EXISTS trade_id INT",
        ]
        results = []
        for sql in migrations:
            try:
                c.execute(sql)
                results.append(f"✅ {sql.split('ADD COLUMN IF NOT EXISTS')[1].strip().split()[0]}")
            except Exception as e:
                results.append(f"⚠️ {e}")
        # Add unique index separately (not in the loop)
        try:
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_trades_market_date ON paper_trades(market_id, trade_date)")
            results.append("✅ unique index on paper_trades(market_id, trade_date)")
        except Exception as e:
            results.append(f"⚠️ index: {e}")

        conn.commit()
        conn.close()
        return {"status": "done", "migrations": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/system-test")
def system_test():
    """
    Full system health check — run before going live.
    Tests every component end to end with real data.
    """
    results = {}
    passed  = 0
    failed  = 0

    def check(name, fn):
        nonlocal passed, failed
        try:
            result = fn()
            results[name] = {"status": "✅ PASS", "detail": result}
            passed += 1
        except Exception as e:
            results[name] = {"status": "❌ FAIL", "detail": str(e)}
            failed += 1

    # 1. Database connection
    def test_db():
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM markets")
        n = c.fetchone()["n"]
        c.execute("""SELECT table_name FROM information_schema.tables
                     WHERE table_schema='public' ORDER BY table_name""")
        tables = [r["table_name"] for r in c.fetchall()]
        conn.close()
        return f"{n} markets, tables: {', '.join(tables)}"
    check("1_database", test_db)

    # 2. GFS forecast
    def test_gfs():
        import requests as _r
        from datetime import date as _date, timedelta as _td4
        target = (_date.today() + _td4(days=4)).strftime("%Y-%m-%d")
        r = _r.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": 47.6062, "longitude": -122.3321,
            "daily": "temperature_2m_max", "temperature_unit": "fahrenheit",
            "timezone": "America/Los_Angeles", "start_date": target,
            "end_date": target, "models": "gfs_global"
        }, timeout=15)
        data = r.json()
        if "daily" not in data:
            raise Exception(f"API error: {data.get('reason', data)}")
        temp = data["daily"]["temperature_2m_max"][0]
        return f"Seattle {target}: {temp}°F"
    check("2_gfs_forecast", test_gfs)

    # 3. UKMO forecast
    def test_ukmo():
        import requests as _r
        from datetime import date as _date, timedelta as _td4
        target = (_date.today() + _td4(days=4)).strftime("%Y-%m-%d")
        r = _r.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": 47.6062, "longitude": -122.3321,
            "daily": "temperature_2m_max", "temperature_unit": "fahrenheit",
            "timezone": "America/Los_Angeles", "start_date": target,
            "end_date": target, "models": "ukmo_global_deterministic_10km"
        }, timeout=15)
        data = r.json()
        if "daily" not in data:
            raise Exception(f"API error: {data.get('reason', data)}")
        temp = data["daily"]["temperature_2m_max"][0]
        return f"Seattle {target}: {temp}°F"
    check("3_ukmo_forecast", test_ukmo)

    # 4. Polymarket connection
    def test_polymarket():
        import requests as _r
        from datetime import date as _d, timedelta as _td
        target = _d.today() + _td(days=4)
        slug_date = target.strftime("%B-%-d").lower()
        slug = f"highest-temperature-in-seattle-on-{slug_date}-{target.year}"
        r = _r.get(f"https://gamma-api.polymarket.com/events",
                   params={"slug": slug}, timeout=15)
        data = r.json()
        if data and isinstance(data, list) and data:
            markets = data[0].get("markets", [])
            active = [m for m in markets if m.get("acceptingOrders")]
            return f"Seattle {target}: {len(active)} active ranges found"
        return "No market found yet (may not be open)"
    check("4_polymarket", test_polymarket)

    # 5. Signal logic
    def test_signals():
        from strategy.early_entry import range_near_forecast
        r1 = range_near_forecast("Will temp be 66°F on April 17?", 66.0, "F", 3)
        r2 = range_near_forecast("Will temp be 50°F on April 17?", 66.0, "F", 3)
        if r1 and not r2:
            return "Range matching working correctly"
        return f"WARNING: r1={r1} r2={r2} — check logic"
    check("5_signal_logic", test_signals)

    # 6. Scan log write
    def test_scan_log():
        from strategy.paper_trade import log_scan, init_tables
        init_tables()
        log_scan("TestCity", "2026-01-01", 4,
                 {"gfs": 60.0, "ukmo": 61.0, "meteofrance": 60.5,
                  "consensus": 60.5, "spread": 1.0, "unit": "F"},
                 "TEST", "System test entry")
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM scan_log WHERE city='TestCity'")
        n = c.fetchone()["n"]
        conn.close()
        return f"Scan log write/read working — {n} test entries"
    check("6_scan_log", test_scan_log)

    # 7. Paper trades table
    def test_paper_trades():
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM paper_trades")
        n = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as wins FROM paper_trades WHERE outcome='Yes'")
        wins = c.fetchone()["wins"]
        c.execute("SELECT COUNT(*) as pending FROM paper_trades WHERE outcome IS NULL")
        pending = c.fetchone()["pending"]
        conn.close()
        return f"{n} total trades, {wins} wins, {pending} pending"
    check("7_paper_trades", test_paper_trades)

    # 8. Signals cache
    def test_signals_cache():
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT updated_at, LENGTH(value) as size FROM cache WHERE key='early_signals'")
        row = c.fetchone()
        conn.close()
        if row:
            return f"Signals cached at {row['updated_at']}, size={row['size']} chars"
        return "No signals cache yet — hit /early-signals?refresh=true"
    check("8_signals_cache", test_signals_cache)

    # 9. Forecast log
    def test_forecast_log():
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM forecast_log")
        n = c.fetchone()["n"]
        c.execute("SELECT MAX(logged_at_est) as last FROM forecast_log")
        last = c.fetchone()["last"]
        conn.close()
        return f"{n} snapshots, last logged: {last}"
    check("9_forecast_log", test_forecast_log)

    # 10. Scheduler alive
    def test_scheduler():
        conn = get_conn()
        c = conn.cursor()
        c.execute("""SELECT logged_at_est FROM forecast_log
                     ORDER BY id DESC LIMIT 1""")
        row = c.fetchone()
        conn.close()
        if row:
            return f"Last scheduler run: {row['logged_at_est']}"
        return "No scheduler runs recorded yet"
    check("10_scheduler", test_scheduler)

    total = passed + failed
    status = "✅ SYSTEM READY" if failed == 0 else f"⚠️ {failed} CHECKS FAILED"

    return {
        "status":       status,
        "passed":       passed,
        "failed":       failed,
        "total_checks": total,
        "checks":       results,
        "tested_at":    __import__('datetime').datetime.now(
                            __import__('datetime').timezone.utc
                        ).strftime("%Y-%m-%d %I:%M %p UTC"),
    }


@app.get("/research/market-open-times")
def market_open_times():
    """
    Check real market open times from our DB.
    Shows created_at in EST for multiple cities/dates
    to find the exact pattern of when markets open.
    """
    from datetime import timezone, timedelta
    EST = timezone(timedelta(hours=-4))

    try:
        conn = get_conn()
        c    = conn.cursor()

        # Get last 30 days of markets with created_at
        # for our key cities — multiple dates to find pattern
        c.execute("""
            SELECT city, question,
                   TO_CHAR(TO_TIMESTAMP(created_at) AT TIME ZONE 'America/New_York', 
                           'YYYY-MM-DD HH12:MI AM') as open_est,
                   TO_CHAR(TO_TIMESTAMP(resolved_at) AT TIME ZONE 'America/New_York',
                           'YYYY-MM-DD HH12:MI AM') as resolve_est,
                   ROUND((resolved_at - created_at) / 86400.0, 1) as days_open,
                   outcome,
                   last_trade_price
            FROM markets
            WHERE city IN ('London', 'NYC', 'Paris', 'Tokyo', 'Seoul',
                          'Dallas', 'Miami', 'Toronto', 'Madrid', 'Munich')
            AND created_at IS NOT NULL
            AND resolved_at IS NOT NULL
            AND outcome IN ('Yes', 'No')
            ORDER BY created_at DESC
            LIMIT 100
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Group by city to find pattern
        by_city = {}
        for r in rows:
            city = r["city"]
            if city not in by_city:
                by_city[city] = []
            by_city[city].append(r)

        # Extract just the TIME part to find pattern
        summary = {}
        for city, markets in by_city.items():
            open_times = []
            for m in markets[:10]:
                if m["open_est"]:
                    # Extract just time portion
                    parts = m["open_est"].split(" ")
                    if len(parts) >= 3:
                        time_only = f"{parts[1]} {parts[2]}"
                        open_times.append({
                            "date":    parts[0],
                            "time":    time_only,
                            "days_open": m["days_open"],
                            "resolve_est": m["resolve_est"],
                        })
            summary[city] = open_times

        return {
            "summary": summary,
            "raw":     rows[:50],
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()[:500]}
