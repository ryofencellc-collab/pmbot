"""
ingest.py - Polymarket temperature market ingestion.

Confirmed working cities:
  - Chicago (°F, 2°F ranges, KORD)
  - London  (°C, exact degree, EGLC)

Both resolve at noon UTC. Both use WU as resolution source.
"""

import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

GAMMA_BASE = "https://gamma-api.polymarket.com"
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

# Confirmed working cities with verified slugs and WU stations
CITY_SLUGS = {
    "Chicago": "chicago",
    "London":  "london",
}

# WU stations — US cities use :9:US, international use country code
WU_STATIONS = {
    "Chicago": {"station": "KORD",  "country": "US",  "unit": "F"},
    "London":  {"station": "EGLC",  "country": "GB",  "unit": "C"},
}


def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20,
                             headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                print("  [RATE LIMIT] sleeping 30s...")
                time.sleep(30)
            else:
                print(f"  [HTTP {r.status_code}] {url}")
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(2 ** i)
    return None


def make_slug(city_slug, target_date):
    month = target_date.strftime("%B").lower()
    day   = str(target_date.day)
    year  = str(target_date.year)
    return f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"


def parse_group_title(title):
    """
    Parse groupItemTitle for both °F and °C markets.

    Chicago (°F):
      "33°F or below"  → below,  target_high=33
      "34-35°F"        → range,  target_low=34, target_high=35
      "52°F or higher" → above,  target_low=52

    London (°C):
      "9°C or below"   → below,  target_high=9
      "12°C"           → exact,  target_low=12, target_high=12
      "19°C or higher" → above,  target_low=19
    """
    t = title.strip().lower()

    # Detect unit
    unit = "C" if "°c" in t else "F"

    # "or below" / "or lower"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*°?[fc]?\s+or\s+(?:below|lower)$', t)
    if m:
        return {"market_type": "below", "unit": unit,
                "target_low": -9999, "target_high": float(m.group(1))}

    # "or higher" / "or above"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*°?[fc]?\s+or\s+(?:higher|above)$', t)
    if m:
        return {"market_type": "above", "unit": unit,
                "target_low": float(m.group(1)), "target_high": 9999}

    # "34-35°f" range (Chicago only)
    m = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*°?[fc]?$', t)
    if m:
        return {"market_type": "range", "unit": unit,
                "target_low": float(m.group(1)), "target_high": float(m.group(2))}

    # "12°c" exact (London)
    m = re.match(r'^(\d+(?:\.\d+)?)\s*°[fc]$', t)
    if m:
        val = float(m.group(1))
        return {"market_type": "exact", "unit": unit,
                "target_low": val, "target_high": val}

    return None


def fetch_event(city, city_slug, target_date):
    slug    = make_slug(city_slug, target_date)
    data    = safe_get(f"{GAMMA_BASE}/events", params={"slug": slug})
    unit    = WU_STATIONS[city]["unit"]

    if not data or not isinstance(data, list) or len(data) == 0:
        return []

    markets = data[0].get("markets", [])
    results = []

    for m in markets:
        title  = m.get("groupItemTitle", "")
        parsed = parse_group_title(title)
        if not parsed:
            print(f"  [SKIP] Cannot parse: '{title}'")
            continue

        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []

        yes_price = float(prices[0]) if prices else 0.0

        outcome = None
        if m.get("closed"):
            if prices and str(prices[0]) == "1":
                outcome = "Yes"
            elif len(prices) > 1 and str(prices[1]) == "1":
                outcome = "No"

        end_str = m.get("endDate", "")
        try:
            resolved_at = int(datetime.fromisoformat(
                end_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            resolved_at = 0

        start_str = m.get("startDate", "")
        try:
            created_at = int(datetime.fromisoformat(
                start_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            created_at = 0

        results.append({
            "id":               str(m["id"]),
            "question":         m.get("question", ""),
            "city":             city,
            "target_low":       parsed["target_low"],
            "target_high":      parsed["target_high"],
            "market_type":      parsed["market_type"],
            "unit":             parsed["unit"],
            "resolved_at":      resolved_at,
            "created_at":       created_at,
            "outcome":          outcome,
            "last_trade_price": yes_price,
            "volume":           float(m.get("liquidityNum") or 0),
        })

    return results


def fetch_polymarket_markets(days_ahead=7, days_back=30):
    today     = date.today()
    all_dates = [today + timedelta(days=i)
                 for i in range(-days_back, days_ahead + 1)]

    print(f"\n[POLY] {len(CITY_SLUGS)} cities x {len(all_dates)} dates...")
    saved = 0

    for city, city_slug in CITY_SLUGS.items():
        city_count = 0

        for target_date in all_dates:
            markets = fetch_event(city, city_slug, target_date)

            if not markets:
                time.sleep(0.2)
                continue

            conn = get_conn()
            try:
                c = conn.cursor()
                for m in markets:
                    try:
                        c.execute("""
                            INSERT INTO markets
                                (id, question, city, target_low, target_high,
                                 market_type, unit, resolved_at, created_at,
                                 outcome, last_trade_price, volume)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (id) DO UPDATE SET
                                outcome           = EXCLUDED.outcome,
                                last_trade_price  = EXCLUDED.last_trade_price,
                                volume            = EXCLUDED.volume
                        """, (
                            m["id"], m["question"], m["city"],
                            m["target_low"], m["target_high"],
                            m["market_type"], m["unit"],
                            m["resolved_at"], m["created_at"],
                            m["outcome"], m["last_trade_price"], m["volume"]
                        ))
                        city_count += 1
                        saved += 1
                    except Exception as e:
                        print(f"  [DB ERR] {city} {target_date}: {e}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  [COMMIT ERR] {city} {target_date}: {e}")
            finally:
                conn.close()

            time.sleep(0.2)

        print(f"  {city}: {city_count} markets upserted")

    print(f"[POLY] Done: {saved} total upserts\n")
    return saved


def fetch_wu_temps(days_back=30):
    """
    Fetch WU historical temps.
    US cities: uses imperial units (°F)
    International cities: uses metric units (°C) stored in max_temp_f column
    Note: for London we store °C in max_temp_f — signals.py knows to use it as °C
    """
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    saved      = 0

    print(f"\n[WU] Fetching temps {start_date} → {end_date}...")

    for city, info in WU_STATIONS.items():
        station  = info["station"]
        country  = info["country"]
        unit_sys = "e" if info["unit"] == "F" else "m"  # e=imperial, m=metric
        current    = start_date
        city_saved = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            date_fmt = current.strftime("%Y%m%d")

            conn = get_conn()
            c    = conn.cursor()
            c.execute("SELECT COUNT(*) as count FROM wu_temps "
                      "WHERE city=%s AND date=%s", (city, date_str))
            row = c.fetchone()
            conn.close()

            if row and row["count"] > 0:
                current += timedelta(days=1)
                continue

            # Build WU URL — international stations use country code
            if country == "US":
                wu_url = f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json"
            else:
                wu_url = f"https://api.weather.com/v1/location/{station}:9:{country}/observations/historical.json"

            try:
                r = requests.get(wu_url,
                    params={"apiKey": WU_API_KEY, "units": unit_sys,
                            "startDate": date_fmt},
                    timeout=15)
            except Exception as e:
                print(f"  [WU ERR] {city} {date_str}: {e}")
                current += timedelta(days=1)
                time.sleep(0.5)
                continue

            if r.status_code == 200:
                obs   = r.json().get("observations", [])
                temps = [o.get("temp") for o in obs if o.get("temp") is not None]
                if temps:
                    conn = get_conn()
                    c    = conn.cursor()
                    c.execute(
                        "INSERT INTO wu_temps (city, station, date, max_temp_f) "
                        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (city, station, date_str, max(temps))
                    )
                    conn.commit()
                    conn.close()
                    city_saved += 1
                    saved += 1
            elif r.status_code == 401:
                print(f"  [WU] 401 Unauthorized")
                break
            else:
                print(f"  [WU] HTTP {r.status_code} for {city} {date_str}")

            current += timedelta(days=1)
            time.sleep(0.4)

        print(f"  {city} ({station}): {city_saved} days saved")

    print(f"[WU] Done: {saved} city-days\n")
    return saved


def fetch_price_histories():
    """Pull CLOB price history for all resolved markets."""
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT id FROM markets
                 WHERE outcome IS NOT NULL
                 ORDER BY resolved_at DESC""")
    market_ids = [r["id"] for r in c.fetchall()]
    conn.close()

    print(f"[PRICES] Fetching price histories for {len(market_ids)} markets...")
    saved = 0

    for i, mid in enumerate(market_ids):
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM price_snapshots WHERE market_id=%s", (mid,))
        row  = c.fetchone()
        conn.close()
        if row and row["count"] > 0:
            continue

        mdata = safe_get(f"{GAMMA_BASE}/markets/{mid}")
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

        hist = safe_get("https://clob.polymarket.com/prices-history",
                        params={"market": tokens[0], "interval": "all", "fidelity": 60})
        if not hist or "history" not in hist:
            continue

        price_rows = [(mid, int(p["t"]), float(p["p"]))
                      for p in hist["history"] if p.get("t") and p.get("p") is not None]

        if price_rows:
            conn = get_conn()
            cur  = conn.cursor()
            cur.executemany(
                "INSERT INTO price_snapshots (market_id, timestamp, yes_price, no_price) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                [(r[0], r[1], r[2], round(1 - r[2], 4)) for r in price_rows])
            conn.commit()
            conn.close()
            saved += len(price_rows)

            # Update last_trade_price with price 29h before resolution
            conn = get_conn()
            c    = conn.cursor()
            c.execute("SELECT resolved_at FROM markets WHERE id=%s", (mid,))
            mrow = c.fetchone()
            conn.close()

            if mrow:
                target_ts = mrow["resolved_at"] - (29 * 3600)
                conn = get_conn()
                c    = conn.cursor()
                c.execute("""SELECT yes_price FROM price_snapshots
                             WHERE market_id=%s AND timestamp <= %s
                             ORDER BY timestamp DESC LIMIT 1""",
                          (mid, target_ts))
                prow = c.fetchone()
                if prow and prow["yes_price"] > 0:
                    c.execute("UPDATE markets SET last_trade_price=%s WHERE id=%s",
                              (prow["yes_price"], mid))
                    conn.commit()
                conn.close()

        if i % 20 == 0:
            print(f"  [{i}/{len(market_ids)}] {saved} snapshots saved")
        time.sleep(0.3)

    print(f"[PRICES] Done: {saved} snapshots")
    return saved


def run_full_ingest(days_back=30, days_ahead=7):
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE INGEST — {days_back} days back, {days_ahead} ahead")
    print(f"{'='*55}\n")

    fetch_polymarket_markets(days_ahead=days_ahead, days_back=days_back)
    fetch_wu_temps(days_back=days_back)
    fetch_price_histories()

    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*55}\n  SUMMARY\n{'='*55}")
    for table in ["markets", "wu_temps", "paper_trades"]:
        c.execute(f"SELECT COUNT(*) as count FROM {table}")
        print(f"  {table:<20} {c.fetchone()['count']:>10,} rows")
    for city in CITY_SLUGS.keys():
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city=%s AND outcome IS NULL", (city,))
        open_c = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city=%s AND outcome IS NOT NULL", (city,))
        resolved_c = c.fetchone()["count"]
        print(f"  {city}: {open_c} open, {resolved_c} resolved")
    conn.close()


if __name__ == "__main__":
    run_full_ingest(days_back=30, days_ahead=7)
