"""
forecast_logger.py - Option B

Logs all 3 model forecasts with timestamps every time it runs.
This builds a historical record showing:
- What GFS/UKMO/MF said 4 days before resolution
- What they said 3 days before
- What they said 1 day before
- What WU actually recorded

After 2 weeks we can answer:
"Are forecasts accurate 4 days out or only accurate 1 day out?"
"""

import requests
import time
from datetime import datetime, timezone, timedelta, date
from data.database import get_conn

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
EST        = timezone(timedelta(hours=-5))

MODELS = {
    "gfs":         "gfs_global",
    "ukmo":        "ukmo_global_deterministic_10km",
    "meteofrance": "meteofrance_arpege_world",
}

ALL_CITIES = {
    "London":       {"lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",                  "unit": "C", "temp_unit": "celsius"},
    "NYC":          {"lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Toronto":      {"lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",                "unit": "C", "temp_unit": "celsius"},
    "Seoul":        {"lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",                     "unit": "C", "temp_unit": "celsius"},
    "Dallas":       {"lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",                "unit": "F", "temp_unit": "fahrenheit"},
    "Atlanta":      {"lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Miami":        {"lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",               "unit": "F", "temp_unit": "fahrenheit"},
    "Seattle":      {"lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",            "unit": "F", "temp_unit": "fahrenheit"},
    "Paris":        {"lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",                   "unit": "C", "temp_unit": "celsius"},
    "Tokyo":        {"lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",                     "unit": "C", "temp_unit": "celsius"},
    "Singapore":    {"lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",                 "unit": "C", "temp_unit": "celsius"},
    "Madrid":       {"lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",                  "unit": "C", "temp_unit": "celsius"},
    "Warsaw":       {"lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",                  "unit": "C", "temp_unit": "celsius"},
    "Beijing":      {"lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Shanghai":     {"lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",                  "unit": "C", "temp_unit": "celsius"},
    "Taipei":       {"lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",                    "unit": "C", "temp_unit": "celsius"},
    "Tel Aviv":     {"lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",                 "unit": "C", "temp_unit": "celsius"},
    "Sao Paulo":    {"lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",              "unit": "C", "temp_unit": "celsius"},
    "Milan":        {"lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",                    "unit": "C", "temp_unit": "celsius"},
    "Munich":       {"lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",                  "unit": "C", "temp_unit": "celsius"},
    "Buenos Aires": {"lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires", "unit": "C", "temp_unit": "celsius"},
}


def log_all_forecasts():
    """
    Log forecasts for all cities for next 7 days.
    Runs every time signals are checked.
    Timestamps everything in both UTC and EST.
    """
    now_utc = datetime.now(timezone.utc)
    now_est = datetime.now(EST)
    logged_at_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    logged_at_est = now_est.strftime("%Y-%m-%d %I:%M %p EST")
    today = date.today()

    conn  = get_conn()
    c     = conn.cursor()

    # Create table if needed
    c.execute("""
        CREATE TABLE IF NOT EXISTS forecast_log (
            id SERIAL PRIMARY KEY,
            logged_at_utc TEXT,
            logged_at_est TEXT,
            city TEXT,
            target_date TEXT,
            days_until_resolution INT,
            gfs_temp REAL,
            ukmo_temp REAL,
            mf_temp REAL,
            consensus_temp REAL,
            spread REAL,
            unit TEXT,
            wu_actual REAL,
            UNIQUE(city, target_date, logged_at_utc)
        )
    """)
    conn.commit()

    logged = 0

    for city, config in ALL_CITIES.items():
        for days_out in range(1, 8):
            target_date = today + timedelta(days=days_out)
            date_str    = target_date.strftime("%Y-%m-%d")
            results     = {}

            for model_name, model_code in MODELS.items():
                try:
                    r = requests.get(OPEN_METEO, params={
                        "latitude":         config["lat"],
                        "longitude":        config["lon"],
                        "daily":            "temperature_2m_max",
                        "temperature_unit": config["temp_unit"],
                        "timezone":         config["tz"],
                        "start_date":       date_str,
                        "end_date":         date_str,
                        "models":           model_code,
                    }, timeout=15)
                    if r.status_code == 200:
                        temps = r.json().get("daily", {}).get("temperature_2m_max", [])
                        if temps and temps[0] is not None:
                            results[model_name] = round(float(temps[0]), 1)
                except Exception:
                    pass
                time.sleep(0.1)

            if not results:
                continue

            vals      = list(results.values())
            consensus = round(sum(vals) / len(vals), 1)
            spread    = round(max(vals) - min(vals), 1) if len(vals) > 1 else 0.0

            try:
                c.execute("""
                    INSERT INTO forecast_log
                    (logged_at_utc, logged_at_est, city, target_date,
                     days_until_resolution, gfs_temp, ukmo_temp, mf_temp,
                     consensus_temp, spread, unit)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (city, target_date, logged_at_utc) DO NOTHING
                """, (
                    logged_at_utc, logged_at_est, city, date_str,
                    days_out,
                    results.get("gfs"),
                    results.get("ukmo"),
                    results.get("meteofrance"),
                    consensus, spread, config["unit"]
                ))
                logged += 1
            except Exception as e:
                print(f"  [LOG ERR] {city} {date_str}: {e}")

        conn.commit()

    conn.close()
    print(f"[FORECAST LOG] Logged {logged} forecasts at {logged_at_est}")
    return logged


def fill_wu_actuals():
    """
    After markets resolve, fill in the actual WU temperature
    so we can compare forecast vs actual over time.
    """
    from data.ingest import fetch_wu_history

    conn = get_conn()
    c    = conn.cursor()

    # Find logged forecasts where target_date has passed but wu_actual is null
    c.execute("""
        SELECT DISTINCT city, target_date
        FROM forecast_log
        WHERE wu_actual IS NULL
        AND target_date < CURRENT_DATE::TEXT
        ORDER BY target_date DESC
        LIMIT 50
    """)
    rows = c.fetchall()
    conn.close()

    print(f"[WU FILL] Filling actuals for {len(rows)} city/date pairs...")

    for row in rows:
        city      = row["city"]
        date_str  = row["target_date"]

        wu_temp = fetch_wu_temp(city, date_str)
        if wu_temp is None:
            continue

        conn = get_conn()
        c    = conn.cursor()
        c.execute("""
            UPDATE forecast_log
            SET wu_actual = %s
            WHERE city = %s AND target_date = %s
        """, (wu_temp, city, date_str))
        conn.commit()
        conn.close()
        print(f"  [WU] {city} {date_str} = {wu_temp}°")
        time.sleep(0.3)


def fetch_wu_temp(city, date_str):
    """Fetch WU historical temp for a city/date."""
    WU_STATIONS = {
        "London":        "EGLL",
        "NYC":           "KLGA",
        "New York City": "KLGA",
        "Toronto":       "CYYZ",
        "Paris":         "LFPG",
        "Dallas":        "KDFW",
        "Atlanta":       "KATL",
        "Seoul":         "RKSS",
        "Tokyo":         "RJTT",
        "Singapore":     "WSSS",
        "Madrid":        "LEMD",
        "Warsaw":        "EPWA",
        "Beijing":       "ZBAA",
        "Shanghai":      "ZSPD",
        "Taipei":        "RCTP",
        "Tel Aviv":      "LLBG",
        "Sao Paulo":     "SBGR",
        "Milan":         "LIMC",
        "Munich":        "EDDM",
        "Buenos Aires":  "SAEZ",
        "Chicago":       "KORD",
        "Seattle":       "KSEA",
        "Miami":         "KMIA",
    }

    station = WU_STATIONS.get(city)
    if not station:
        return None

    date_fmt = date_str.replace("-", "")
    try:
        r = requests.get(
            f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json",
            params={
                "apiKey":    "e1f10a1e78da46f5b10a1e78da96f525",
                "units":     "e",
                "startDate": date_fmt,
            },
            timeout=15
        )
        if r.status_code == 200:
            obs   = r.json().get("observations", [])
            temps = [o.get("temp") for o in obs if o.get("temp") is not None]
            if temps:
                return max(temps)
    except Exception as e:
        print(f"  [WU ERR] {city} {date_str}: {e}")
    return None


if __name__ == '__main__':
    print("Logging all forecasts...")
    n = log_all_forecasts()
    print(f"Done — {n} entries logged")
    print("\nFilling WU actuals...")
    fill_wu_actuals()
