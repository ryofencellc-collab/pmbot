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

        # Every 6 hours: log all 3 model forecasts + rebuild city cards cache
        if minute < 5 and hour in [0, 6, 12, 18]:
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
                    est_now = datetime.now(_tz(_td(hours=-5))).strftime("%Y-%m-%d %I:%M %p EST")
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
                est_now = datetime.now(_tz(_td(hours=-5))).strftime("%Y-%m-%d %I:%M %p EST")
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
        check_key = f"{today}-{hour}-{minute // 60}"  # scan once per hour to stay within Open-Meteo free tier
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


@app.get("/market-times")
def market_times():
    """
    Checks real market open times from our DB for multiple cities.
    Converts to EST. Checks multiple days to confirm the pattern.
    This tells us EXACTLY when to place bets.
    """
    try:
        from datetime import datetime, timezone, timedelta
        EST = timezone(timedelta(hours=-5))

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
            est_now = datetime.now(_tz(_td(hours=-5))).strftime("%Y-%m-%d %I:%M %p EST")
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
        EST = timezone(timedelta(hours=-5))

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
        EST = timezone(timedelta(hours=-5))

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
        est_now = datetime.now(_tz(_td(hours=-5))).strftime("%Y-%m-%d %I:%M %p EST")

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
    EST = timezone(timedelta(hours=-5))

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
