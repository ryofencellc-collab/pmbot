import requests
import time
import json
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
METEO_BASE = "https://archive-api.open-meteo.com"

WEATHER_CITIES = {
    "London":        {"lat": 51.5074,  "lon": -0.1278},
    "New York":      {"lat": 40.7128,  "lon": -74.0060},
    "Los Angeles":   {"lat": 34.0522,  "lon": -118.2437},
    "Chicago":       {"lat": 41.8781,  "lon": -87.6298},
    "Miami":         {"lat": 25.7617,  "lon": -80.1918},
    "Tokyo":         {"lat": 35.6762,  "lon": 139.6503},
    "Sydney":        {"lat": -33.8688, "lon": 151.2093},
    "Paris":         {"lat": 48.8566,  "lon": 2.3522},
    "Berlin":        {"lat": 52.5200,  "lon": 13.4050},
    "Dubai":         {"lat": 25.2048,  "lon": 55.2708},
    "Singapore":     {"lat": 1.3521,   "lon": 103.8198},
    "Seoul":         {"lat": 37.5665,  "lon": 126.9780},
    "Hong Kong":     {"lat": 22.3193,  "lon": 114.1694},
    "San Francisco": {"lat": 37.7749,  "lon": -122.4194},
    "Boston":        {"lat": 42.3601,  "lon": -71.0589},
}

WEATHER_KEYWORDS = [
    "temperature", "celsius", "fahrenheit", "highest temp",
    "lowest temp", "high temp", "low temp", "daily high",
    "daily low", "weather", "degrees", "exceed", "°c", "°f"
]


def safe_get(url, params=None, retries=3, delay=1.0):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                print("  [RATE LIMIT] sleeping 30s...")
                time.sleep(30)
            else:
                print(f"  [WARN] {r.status_code}")
        except Exception as e:
            print(f"  [ERR] attempt {i+1}: {e}")
        time.sleep(delay * (2 ** i))
    return None


def is_weather_market(question, category):
    q   = (question or "").lower()
    cat = (category or "").lower()
    if "weather" in cat:
        return True
    return any(kw in q for kw in WEATHER_KEYWORDS)


def fetch_weather_markets(days_back=1095):
    conn   = get_conn()
    c      = conn.cursor()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    offset = 0
    limit  = 500
    saved  = 0
    stop   = False

    print(f"\n[STEP 1] Pulling weather markets since {cutoff.date()}...")

    while not stop:
        data = safe_get(f"{GAMMA_BASE}/markets", params={
            "closed": "true", "limit": limit,
            "offset": offset, "order": "startDate", "ascending": "false"
        })
        if not data:
            break

        batch = 0
        for m in data:
            try:
                end_str = m.get("endDate") or m.get("endDateIso") or ""
                if not e
