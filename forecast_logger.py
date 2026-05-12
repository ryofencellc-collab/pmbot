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

# All 31 confirmed active Polymarket temperature cities (verified 2026-05-12)
# Logging daily forecasts for all — need 20+ days before trading each
ALL_CITIES = {
    # US — Fahrenheit
    "Atlanta":      {"lat": 33.749,  "lon":  -84.388, "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "Dallas":       {"lat": 32.776,  "lon":  -96.797, "tz": "America/Chicago",     "unit": "F", "temp_unit": "fahrenheit"},
    "NYC":          {"lat": 40.713,  "lon":  -74.006, "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "Denver":       {"lat": 39.739,  "lon": -104.984, "tz": "America/Denver",      "unit": "F", "temp_unit": "fahrenheit"},
    "Houston":      {"lat": 29.760,  "lon":  -95.370, "tz": "America/Chicago",     "unit": "F", "temp_unit": "fahrenheit"},
    "Miami":        {"lat": 25.762,  "lon":  -80.192, "tz": "America/New_York",    "unit": "F", "temp_unit": "fahrenheit"},
    "Chicago":      {"lat": 41.878,  "lon":  -87.630, "tz": "America/Chicago",     "unit": "F", "temp_unit": "fahrenheit"},
    "Seattle":      {"lat": 47.606,  "lon": -122.332, "tz": "America/Los_Angeles", "unit": "F", "temp_unit": "fahrenheit"},
    "Los Angeles":  {"lat": 34.052,  "lon": -118.244, "tz": "America/Los_Angeles", "unit": "F", "temp_unit": "fahrenheit"},
    # International — Celsius
    "Shanghai":     {"lat": 31.230,  "lon":  121.474, "tz": "Asia/Shanghai",       "unit": "C", "temp_unit": "celsius"},
    "Singapore":    {"lat":  1.352,  "lon":  103.820, "tz": "Asia/Singapore",      "unit": "C", "temp_unit": "celsius"},
    "London":       {"lat": 51.507,  "lon":   -0.128, "tz": "Europe/London",       "unit": "C", "temp_unit": "celsius"},
    "Munich":       {"lat": 48.135,  "lon":   11.582, "tz": "Europe/Berlin",       "unit": "C", "temp_unit": "celsius"},
    "Tokyo":        {"lat": 35.690,  "lon":  139.692, "tz": "Asia/Tokyo",          "unit": "C", "temp_unit": "celsius"},
    "Wuhan":        {"lat": 30.593,  "lon":  114.305, "tz": "Asia/Shanghai",       "unit": "C", "temp_unit": "celsius"},
    "Tel Aviv":     {"lat": 32.085,  "lon":   34.782, "tz": "Asia/Jerusalem",      "unit": "C", "temp_unit": "celsius"},
    "Madrid":       {"lat": 40.417,  "lon":   -3.704, "tz": "Europe/Madrid",       "unit": "C", "temp_unit": "celsius"},
    "Istanbul":     {"lat": 41.015,  "lon":   28.979, "tz": "Europe/Istanbul",     "unit": "C", "temp_unit": "celsius"},
    "Toronto":      {"lat": 43.651,  "lon":  -79.347, "tz": "America/Toronto",     "unit": "C", "temp_unit": "celsius"},
    "Taipei":       {"lat": 25.048,  "lon":  121.514, "tz": "Asia/Taipei",         "unit": "C", "temp_unit": "celsius"},
    "Jakarta":      {"lat": -6.175,  "lon":  106.827, "tz": "Asia/Jakarta",        "unit": "C", "temp_unit": "celsius"},
    "Mexico City":  {"lat": 19.433,  "lon":  -99.133, "tz": "America/Mexico_City", "unit": "C", "temp_unit": "celsius"},
    "Paris":        {"lat": 48.857,  "lon":    2.347, "tz": "Europe/Paris",        "unit": "C", "temp_unit": "celsius"},
    "Cape Town":    {"lat":-33.926,  "lon":   18.424, "tz": "Africa/Johannesburg", "unit": "C", "temp_unit": "celsius"},
    "Milan":        {"lat": 45.465,  "lon":    9.186, "tz": "Europe/Rome",         "unit": "C", "temp_unit": "celsius"},
    "Karachi":      {"lat": 24.861,  "lon":   67.011, "tz": "Asia/Karachi",        "unit": "C", "temp_unit": "celsius"},
    "Amsterdam":    {"lat": 52.374,  "lon":    4.890, "tz": "Europe/Amsterdam",    "unit": "C", "temp_unit": "celsius"},
    "Panama City":  {"lat":  8.994,  "lon":  -79.519, "tz": "America/Panama",      "unit": "C", "temp_unit": "celsius"},
    "Qingdao":      {"lat": 36.067,  "lon":  120.383, "tz": "Asia/Shanghai",       "unit": "C", "temp_unit": "celsius"},
    "Sao Paulo":    {"lat":-23.550,  "lon":  -46.633, "tz": "America/Sao_Paulo",   "unit": "C", "temp_unit": "celsius"},
    "Lagos":        {"lat":  6.455,  "lon":    3.384, "tz": "Africa/Lagos",        "unit": "C", "temp_unit": "celsius"},
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
