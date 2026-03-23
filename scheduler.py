"""
scheduler.py - Runs morning and evening sessions automatically.
Start with: python3 scheduler.py
Runs forever. 7 AM = morning session. 8 PM = evening session.
"""

import time
import threading
from datetime import datetime, timezone
from strategy.paper_trade import run_morning_session, run_evening_session, save_log


def should_run_morning():
    now = datetime.now()
    return now.hour == 7 and now.minute == 0


def should_run_evening():
    now = datetime.now()
    return now.hour == 20 and now.minute == 0


def run_scheduler():
    print(f"[SCHEDULER] Started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("[SCHEDULER] Morning session: 7:00 AM")
    print("[SCHEDULER] Evening session: 8:00 PM")
    print("[SCHEDULER] Running... (Ctrl+C to stop)")

    morning_ran_today = None
    evening_ran_today = None

    while True:
        now   = datetime.now()
        today = now.strftime('%Y-%m-%d')

        # Morning session
        if should_run_morning() and morning_ran_today != today:
            print(f"\n[SCHEDULER] 🌅 Running morning session {today}...")
            try:
                trades, log = run_morning_session()
                morning_ran_today = today
                print(f"[SCHEDULER] Morning complete. {len(trades)} trades placed.")
            except Exception as e:
                error = f"Morning session error: {e}"
                print(f"[SCHEDULER] ❌ {error}")
                save_log("error", error)

        # Evening session
        if should_run_evening() and evening_ran_today != today:
            print(f"\n[SCHEDULER] 🌇 Running evening session {today}...")
            try:
                log = run_evening_session()
                evening_ran_today = today
                print(f"[SCHEDULER] Evening complete.")
            except Exception as e:
                error = f"Evening session error: {e}"
                print(f"[SCHEDULER] ❌ {error}")
                save_log("error", error)

        # Sleep 30 seconds between checks
        time.sleep(30)


def run_system_test():
    """
    Full system test. Tests every component.
    Returns dict of results.
    """
    import requests
    results = {}

    # Test 1: Database
    try:
        from data.database import get_conn
        conn = get_conn()
        conn.execute("SELECT 1")
        conn.close()
        results["database"] = {"status": "ok", "message": "Database connected"}
    except Exception as e:
        results["database"] = {"status": "error", "message": str(e)}

    # Test 2: NOAA API
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/LOT/76,73/forecast",
            timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
        if r.status_code == 200:
            periods = r.json()["properties"]["periods"]
            temp    = periods[0]["temperature"]
            results["noaa"] = {"status": "ok", "message": f"Chicago forecast: {temp}°F"}
        else:
            results["noaa"] = {"status": "error", "message": f"Status {r.status_code}"}
    except Exception as e:
        results["noaa"] = {"status": "error", "message": str(e)}

    # Test 3: Polymarket API
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 1}, timeout=10)
        if r.status_code == 200:
            results["polymarket"] = {"status": "ok", "message": "Polymarket API connected"}
        else:
            results["polymarket"] = {"status": "error", "message": f"Status {r.status_code}"}
    except Exception as e:
        results["polymarket"] = {"status": "error", "message": str(e)}

    # Test 4: WU API
    try:
        r = requests.get(
            "https://api.weather.com/v1/location/KORD:9:US/observations/historical.json",
            params={"apiKey": "e1f10a1e78da46f5b10a1e78da96f525",
                    "units": "e", "startDate": "20260321"},
            timeout=10, verify=False)
        if r.status_code == 200:
            results["wu_api"] = {"status": "ok", "message": "WU API connected"}
        else:
            results["wu_api"] = {"status": "error", "message": f"Status {r.status_code}"}
    except Exception as e:
        results["wu_api"] = {"status": "error", "message": str(e)}

    # Test 5: Signal scanner
    try:
        from strategy.signals import scan_signals
        signals, log = scan_signals()
        results["signals"] = {
            "status": "ok",
            "message": f"Scanner working. {len(signals)} signals found today."
        }
    except Exception as e:
        results["signals"] = {"status": "error", "message": str(e)}

    # Test 6: Data counts
    try:
        from data.database import get_conn
        conn = get_conn()
        c    = conn.cursor()
        counts = {}
        for table in ["markets", "price_snapshots", "wu_temps", "paper_trades"]:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = c.fetchone()[0]
        conn.close()
        results["data"] = {"status": "ok", "message": str(counts)}
    except Exception as e:
        results["data"] = {"status": "error", "message": str(e)}

    return results


if __name__ == '__main__':
    run_scheduler()
