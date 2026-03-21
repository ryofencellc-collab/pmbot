"""
ingest.py — Weather-only focused ingest.
Designed to resume automatically on every Railway restart.
Never duplicates data.
"""

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
                if not end_str:
                    continue
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                continue

            if end_dt < cutoff:
                stop = True
                continue

            question = m.get("question", "")
            category = m.get("category") or m.get("groupItemTitle") or ""

            if not is_weather_market(question, category):
                continue

            outcome = m.get("winnerOutcome") or m.get("resolvedOutcome")
            if not outcome and m.get("resolved"):
                outcome = "Yes"

            market_id = str(m.get("id") or m.get("conditionId") or "")
            if not market_id:
                continue

            try:
                start_str  = m.get("startDate", "")
                created_at = int(datetime.fromisoformat(
                    start_str.replace("Z", "+00:00")).timestamp()) if start_str else 0
            except Exception:
                created_at = 0

            try:
                c.execute('''
                    INSERT OR IGNORE INTO markets
                    (id, question, category, market_type, created_at, resolved_at,
                     end_date_iso, outcome, volume, liquidity)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (
                    market_id, question, category, "weather",
                    created_at, int(end_dt.timestamp()), end_str, outcome,
                    float(m.get("volume") or 0), float(m.get("liquidity") or 0)
                ))
                if c.rowcount > 0:
                    batch += 1
            except Exception:
                continue

        conn.commit()
        saved += batch
        print(f"  offset={offset} | saved={batch} | total={saved}")

        if len(data) < limit or stop:
            break

        offset += limit
        time.sleep(0.3)

    conn.close()
    print(f"\n[STEP 1] Done: {saved} weather markets\n")
    return saved


def fetch_weather_price_histories():
    """
    Pulls price history for weather markets.
    Skips markets already fetched.
    Safe to run multiple times — never duplicates.
    """
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT id FROM markets
        WHERE market_type = 'weather'
          AND outcome IS NOT NULL
          AND volume > 100
        ORDER BY volume DESC
    ''')
    market_ids = [r[0] for r in c.fetchall()]
    conn.close()

    print(f"[STEP 2] Price histories for {len(market_ids)} markets...")

    saved = 0
    for i, mid in enumerate(market_ids):
        # Skip if already fetched
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) FROM price_snapshots WHERE market_id=?", (mid,))
        exists = c.fetchone()[0] > 0
        conn.close()
        if exists:
            continue

        mdata = safe_get(f"{GAMMA_BASE}/markets/{mid}", delay=0.3)
        if not mdata:
            continue

        tokens = mdata.get("clobTokenIds")
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                tokens = []
        if not tokens:
            continue

        hist = safe_get(f"{CLOB_BASE}/prices-history", params={
            "market": tokens[0], "interval": "1h", "fidelity": 60
        }, delay=0.3)

        if not hist or "history" not in hist:
            continue

        rows = [
            (mid, int(p["t"]), float(p["p"]),
             round(1 - float(p["p"]), 6), 0.0)
            for p in hist["history"]
            if p.get("t") and p.get("p")
        ]

        if rows:
            conn = get_conn()
            conn.executemany('''
                INSERT OR IGNORE INTO price_snapshots
                (market_id, timestamp, yes_price, no_price, volume_at_time)
                VALUES (?,?,?,?,?)
            ''', rows)
            conn.commit()
            conn.close()
            saved += len(rows)

        if i % 100 == 0:
            print(f"  [{i}/{len(market_ids)}] done | {saved} snapshots saved")

        time.sleep(0.5)

    print(f"\n[STEP 2] Done: {saved} price snapshots\n")
    return saved


def fetch_weather_history(days_back=1095):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    conn       = get_conn()
    saved      = 0

    print(f"[STEP 3] Open-Meteo temps {start_date} → {end_date}...")

    for city, coords in WEATHER_CITIES.items():
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM weather_data WHERE city=?", (city,))
        if c.fetchone()[0] > 100:
            print(f"  {city}: already loaded")
            continue

        data = safe_get(f"{METEO_BASE}/v1/archive", params={
            "latitude":   coords["lat"],
            "longitude":  coords["lon"],
            "start_date": str(start_date),
            "end_date":   str(end_date),
            "daily":      "temperature_2m_max,temperature_2m_min",
            "timezone":   "UTC"
        })

        if not data or "daily" not in data:
            print(f"  {city}: no data")
            continue

        rows = [
            (city, d, h, l, h, l)
            for d, h, l in zip(
                data["daily"]["time"],
                data["daily"]["temperature_2m_max"],
                data["daily"]["temperature_2m_min"]
            )
            if h is not None and l is not None
        ]

        conn.executemany('''
            INSERT OR IGNORE INTO weather_data
            (city, date, forecast_high, forecast_low, actual_high, actual_low)
            VALUES (?,?,?,?,?,?)
        ''', rows)
        conn.commit()
        saved += len(rows)
        print(f"  {city}: {len(rows)} days")
        time.sleep(0.5)

    conn.close()
    print(f"\n[STEP 3] Done: {saved} city-days\n")
    return saved


def run_full_ingest(days_back=1095):
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE WEATHER INGEST — {days_back} days")
    print(f"{'='*55}\n")

    fetch_weather_markets(days_back=days_back)
    fetch_weather_price_histories()
    fetch_weather_history(days_back=days_back)

    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*55}")
    print(f"  INGEST COMPLETE — SUMMARY")
    print(f"{'='*55}")
    for table in ["markets", "price_snapshots", "weather_data"]:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<20} {c.fetchone()[0]:>10,} rows")
    conn.close()


if __name__ == '__main__':
    run_full_ingest(days_back=1095)
