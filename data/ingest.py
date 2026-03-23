"""
ingest.py - Pulls all data needed for backtest.
1. Weather Underground historical max temps (confirmed resolution source)
2. Polymarket weather markets with outcomes
3. Price snapshots for each market
"""

import requests
import time
import json
import re
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

# Confirmed WU stations matching Polymarket resolution
CITY_STATIONS = {
    "Chicago":       "KORD",
    "Dallas":        "KDFW",
    "Atlanta":       "KATL",
    "Miami":         "KMIA",
    "New York City": "KLGA",
    "Seattle":       "KSEA",
    "Boston":        "KBOS",
    "Los Angeles":   "KLAX",
    "San Francisco": "KSFO",
}

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"


def safe_get(url, params=None, retries=3, delay=1.0):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                print("  [RATE LIMIT] sleeping 30s...")
                time.sleep(30)
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(delay * (2 ** i))
    return None


# ── 1. Weather Underground Historical Temps ───────────────────────────────────

def fetch_wu_temps(days_back=120):
    """Pull historical max temps from WU for each city."""
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    conn       = get_conn()
    saved      = 0

    print(f"\n[WU] Fetching temps {start_date} → {end_date}...")

    for city, station in CITY_STATIONS.items():
        current = start_date
        city_saved = 0

        while current <= end_date:
            date_str = current.strftime('%Y-%m-%d')
            date_fmt = current.strftime('%Y%m%d')

            # Check if already have this
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM wu_temps WHERE city=? AND date=?", (city, date_str))
            if c.fetchone()[0] > 0:
                current += timedelta(days=1)
                continue

            r = requests.get(
                f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json",
                params={"apiKey": WU_API_KEY, "units": "e", "startDate": date_fmt},
                timeout=15
            )

            if r.status_code == 200:
                obs = r.json().get("observations", [])
                temps = [o.get("temp") for o in obs if o.get("temp")]
                if temps:
                    max_t = max(temps)
                    conn.execute(
                        "INSERT OR IGNORE INTO wu_temps (city, station, date, max_temp_f) VALUES (?,?,?,?)",
                        (city, station, date_str, max_t)
                    )
                    conn.commit()
                    city_saved += 1
                    saved += 1

            current += timedelta(days=1)
            time.sleep(0.3)

        print(f"  {city} ({station}): {city_saved} days saved")

    conn.close()
    print(f"[WU] Done: {saved} city-days\n")
    return saved


# ── 2. Polymarket Weather Markets ─────────────────────────────────────────────

def parse_market(question):
    """
    Parse Polymarket temperature question into structured data.
    Returns dict with city, target_low, target_high, unit, market_type
    
    Formats:
    - "between 76-77°F" → {low:76, high:77, unit:F, type:range}
    - "56°F or higher"  → {low:56, high:999, unit:F, type:above}
    - "63°F or below"   → {low:-999, high:63, unit:F, type:below}
    - "be 12°C"         → {low:12, high:12, unit:C, type:exact}
    - "18°C or higher"  → {low:18, high:999, unit:C, type:above}
    """
    q = question.lower()

    # Find city
    city = None
    for c in CITY_STATIONS.keys():
        if c.lower() in q:
            city = c
            break
    if not city:
        return None

    # Determine unit
    unit = "F" if "°f" in q or "fahrenheit" in q else "C"

    # Parse range formats
    # "between 76-77"
    m = re.search(r'between\s+(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)', q)
    if m:
        return {"city": city, "target_low": float(m.group(1)),
                "target_high": float(m.group(2)), "unit": unit, "market_type": "range"}

    # "56F or higher" / "18°C or higher"
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?[fc]?\s*or\s*higher', q)
    if m:
        return {"city": city, "target_low": float(m.group(1)),
                "target_high": 9999, "unit": unit, "market_type": "above"}

    # "63F or below" / "7°C or below"
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?[fc]?\s*or\s*below', q)
    if m:
        return {"city": city, "target_low": -9999,
                "target_high": float(m.group(1)), "unit": unit, "market_type": "below"}

    # "be 12°C" exact
    m = re.search(r'be\s+(\d+(?:\.\d+)?)\s*°?[fc]?\s+on', q)
    if m:
        t = float(m.group(1))
        return {"city": city, "target_low": t,
                "target_high": t, "unit": unit, "market_type": "exact"}

    return None


def fetch_polymarket_markets(days_back=120):
    """Pull weather markets for our tracked cities only."""
    conn   = get_conn()
    c      = conn.cursor()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    offset = 0
    limit  = 500
    saved  = 0
    stop   = False

    print(f"[POLY] Fetching markets since {cutoff.date()}...")

    while not stop:
        data = safe_get(f"{GAMMA_BASE}/markets", params={
            "closed": "true", "limit": limit, "offset": offset,
            "order": "startDate", "ascending": "false"
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
            parsed   = parse_market(question)
            if not parsed:
                continue

            # Get outcome from outcomePrices
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    prices = []
            outcome = None
            if prices:
                if str(prices[0]) == "1":
                    outcome = "Yes"
                elif str(prices[1]) == "1":
                    outcome = "No"

            market_id = str(m.get("id") or "")
            if not market_id:
                continue

            try:
                start_str  = m.get("startDate", "")
                created_at = int(datetime.fromisoformat(
                    start_str.replace("Z", "+00:00")).timestamp()) if start_str else 0
            except Exception:
                created_at = 0

            try:
                c.execute("""INSERT OR IGNORE INTO markets
                    (id, question, city, target_low, target_high, market_type,
                     unit, resolved_at, created_at, outcome, last_trade_price, volume)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (market_id, question, parsed["city"],
                     parsed["target_low"], parsed["target_high"],
                     parsed["market_type"], parsed["unit"],
                     int(end_dt.timestamp()), created_at, outcome,
                     float(m.get("lastTradePrice") or 0),
                     float(m.get("volume") or 0)))
                if c.rowcount > 0:
                    batch += 1
            except Exception as e:
                continue

        conn.commit()
        saved += batch
        print(f"  offset={offset} | saved={batch} | total={saved}")

        if len(data) < limit or stop:
            break

        offset += limit
        time.sleep(0.3)

    conn.close()
    print(f"[POLY] Done: {saved} markets\n")
    return saved


# ── 3. Price Histories ────────────────────────────────────────────────────────

def fetch_price_histories():
    """Pull price histories using interval=all to get full history."""
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT id FROM markets
                 WHERE outcome IS NOT NULL AND volume > 100
                 ORDER BY volume DESC""")
    market_ids = [r[0] for r in c.fetchall()]
    conn.close()

    print(f"[PRICES] Fetching price histories for {len(market_ids)} markets...")
    saved = 0

    for i, mid in enumerate(market_ids):
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) FROM price_snapshots WHERE market_id=?", (mid,))
        if c.fetchone()[0] > 0:
            conn.close()
            continue
        conn.close()

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
            "market": tokens[0], "interval": "all", "fidelity": 1
        }, delay=0.3)

        if not hist or "history" not in hist:
            continue

        rows = [(mid, int(p["t"]), float(p["p"]))
                for p in hist["history"] if p.get("t") and p.get("p")]

        if rows:
            conn = get_conn()
            conn.executemany(
                "INSERT OR IGNORE INTO price_snapshots (market_id, timestamp, yes_price) VALUES (?,?,?)",
                rows)
            conn.commit()
            conn.close()
            saved += len(rows)

        if i % 50 == 0:
            print(f"  [{i}/{len(market_ids)}] {saved} snapshots")
        time.sleep(0.3)

    print(f"[PRICES] Done: {saved} snapshots\n")
    return saved


# ── Main ──────────────────────────────────────────────────────────────────────

def run_full_ingest(days_back=120):
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE INGEST — {days_back} days")
    print(f"{'='*55}\n")

    fetch_wu_temps(days_back=days_back)
    fetch_polymarket_markets(days_back=days_back)
    fetch_price_histories()

    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*55}")
    print(f"  SUMMARY")
    print(f"{'='*55}")
    for table in ["markets", "price_snapshots", "wu_temps"]:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<20} {c.fetchone()[0]:>10,} rows")
    c.execute("SELECT COUNT(*) FROM markets WHERE outcome='Yes'")
    yes = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM markets WHERE outcome='No'")
    no  = c.fetchone()[0]
    print(f"\n  YES: {yes:,}  NO: {no:,}")
    conn.close()


if __name__ == '__main__':
    run_full_ingest(days_back=120)
