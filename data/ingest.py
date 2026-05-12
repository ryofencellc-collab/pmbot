"""
ingest.py - Polymarket temperature market ingestion for all 21 approved cities.
"""

import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

GAMMA_BASE = "https://gamma-api.polymarket.com"
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

# All 31 confirmed active Polymarket temperature cities (verified 2026-05-12)
CITY_SLUGS = {
    # US — Fahrenheit
    "Atlanta":      "atlanta",
    "Dallas":       "dallas",
    "NYC":          "nyc",
    "Denver":       "denver",
    "Houston":      "houston",
    "Miami":        "miami",
    "Chicago":      "chicago",
    "Seattle":      "seattle",
    "Los Angeles":  "los-angeles",
    # International — Celsius
    "Shanghai":     "shanghai",
    "Singapore":    "singapore",
    "London":       "london",
    "Munich":       "munich",
    "Tokyo":        "tokyo",
    "Wuhan":        "wuhan",
    "Tel Aviv":     "tel-aviv",
    "Madrid":       "madrid",
    "Istanbul":     "istanbul",
    "Toronto":      "toronto",
    "Taipei":       "taipei",
    "Jakarta":      "jakarta",
    "Mexico City":  "mexico-city",
    "Paris":        "paris",
    "Cape Town":    "cape-town",
    "Milan":        "milan",
    "Karachi":      "karachi",
    "Amsterdam":    "amsterdam",
    "Panama City":  "panama-city",
    "Qingdao":      "qingdao",
    "Sao Paulo":    "sao-paulo",
    "Lagos":        "lagos",
}

WU_STATIONS = {
    # US
    "Atlanta":      {"station": "KATL", "country": "US", "unit": "F"},
    "Dallas":       {"station": "KDAL", "country": "US", "unit": "F"},
    "NYC":          {"station": "KLGA", "country": "US", "unit": "F"},
    "Denver":       {"station": "KBKF", "country": "US", "unit": "F"},  # Buckley Space Force Base, Aurora CO
    "Houston":      {"station": "KHOU", "country": "US", "unit": "F"},
    "Miami":        {"station": "KMIA", "country": "US", "unit": "F"},
    "Chicago":      {"station": "KORD", "country": "US", "unit": "F"},
    "Seattle":      {"station": "KSEA", "country": "US", "unit": "F"},
    "Los Angeles":  {"station": "KLAX", "country": "US", "unit": "F"},
    # International
    "Shanghai":     {"station": "ZSPD", "country": "CN", "unit": "C"},
    "Singapore":    {"station": "WSSS", "country": "SG", "unit": "C"},
    "London":       {"station": "EGLC", "country": "GB", "unit": "C"},
    "Munich":       {"station": "EDDM", "country": "DE", "unit": "C"},
    "Tokyo":        {"station": "RJTT", "country": "JP", "unit": "C"},
    "Wuhan":        {"station": "ZHHH", "country": "CN", "unit": "C"},
    "Tel Aviv":     {"station": "LLBG", "country": "IL", "unit": "C"},
    "Madrid":       {"station": "LEMD", "country": "ES", "unit": "C"},
    "Istanbul":     {"station": "LTBA", "country": "TR", "unit": "C"},
    "Toronto":      {"station": "CYYZ", "country": "CA", "unit": "C"},
    "Taipei":       {"station": "RCTP", "country": "TW", "unit": "C"},
    "Jakarta":      {"station": "WIII", "country": "ID", "unit": "C"},
    "Mexico City":  {"station": "MMMX", "country": "MX", "unit": "C"},
    "Paris":        {"station": "LFPG", "country": "FR", "unit": "C"},
    "Cape Town":    {"station": "FACT", "country": "ZA", "unit": "C"},
    "Milan":        {"station": "LIMC", "country": "IT", "unit": "C"},
    "Karachi":      {"station": "OPKC", "country": "PK", "unit": "C"},
    "Amsterdam":    {"station": "EHAM", "country": "NL", "unit": "C"},
    "Panama City":  {"station": "MPTO", "country": "PA", "unit": "C"},
    "Qingdao":      {"station": "ZSQD", "country": "CN", "unit": "C"},
    "Sao Paulo":    {"station": "SBGR", "country": "BR", "unit": "C"},
    "Lagos":        {"station": "DNMM", "country": "NG", "unit": "C"},
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
    t    = title.strip().lower()
    unit = "C" if "°c" in t else "F"

    # "or below" / "or lower" (handles negatives)
    m = re.match(r'^(-?\d+(?:\.\d+)?)\s*°?[fc]?\s+or\s+(?:below|lower)$', t)
    if m:
        return {"market_type": "below", "unit": unit,
                "target_low": -9999, "target_high": float(m.group(1))}

    # "or higher" / "or above"
    m = re.match(r'^(-?\d+(?:\.\d+)?)\s*°?[fc]?\s+or\s+(?:higher|above)$', t)
    if m:
        return {"market_type": "above", "unit": unit,
                "target_low": float(m.group(1)), "target_high": 9999}

    # "34-35°f" range
    m = re.match(r'^(-?\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*°?[fc]?$', t)
    if m:
        return {"market_type": "range", "unit": unit,
                "target_low": float(m.group(1)), "target_high": float(m.group(2))}

    # "-4°c" or "12°c" exact
    m = re.match(r'^(-?\d+(?:\.\d+)?)\s*°[fc]$', t)
    if m:
        val = float(m.group(1))
        return {"market_type": "exact", "unit": unit,
                "target_low": val, "target_high": val}

    return None


def fetch_event(city, city_slug, target_date):
    slug = make_slug(city_slug, target_date)
    data = safe_get(f"{GAMMA_BASE}/events", params={"slug": slug})

    if not data or not isinstance(data, list) or len(data) == 0:
        return []

    markets = data[0].get("markets", [])
    results = []

    for m in markets:
        title  = m.get("groupItemTitle", "")
        parsed = parse_group_title(title)
        if not parsed:
            continue

        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []

        # Use lastTradePrice for actual market price (not resolution price)
        last_trade = float(m.get("lastTradePrice") or 0.0)
        yes_price = last_trade if last_trade > 0 else (float(prices[0]) if prices else 0.0)

        outcome = None
        if m.get("closed"):
            if prices and str(prices[0]) == "1":
                outcome = "Yes"
                yes_price = last_trade if last_trade > 0 else yes_price
            elif len(prices) > 1 and str(prices[1]) == "1":
                outcome = "No"
                yes_price = last_trade if last_trade > 0 else yes_price

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

    print(f"[POLY] Done: {saved} total\n")
    return saved


def fetch_wu_temps(days_back=30):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    saved      = 0

    print(f"\n[WU] Fetching temps {start_date} → {end_date}...")

    for city, info in WU_STATIONS.items():
        station  = info["station"]
        country  = info["country"]
        unit_sys = "e" if info["unit"] == "F" else "m"
        current    = start_date
        city_saved = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            date_fmt = current.strftime("%Y%m%d")

            conn = get_conn()
            c    = conn.cursor()
            c.execute("SELECT COUNT(*) as count FROM wu_temps WHERE city=%s AND date=%s",
                      (city, date_str))
            row = c.fetchone()
            conn.close()

            if row and row["count"] > 0:
                current += timedelta(days=1)
                continue

            if country == "US":
                wu_url = f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json"
            else:
                wu_url = f"https://api.weather.com/v1/location/{station}:9:{country}/observations/historical.json"

            try:
                r = requests.get(wu_url,
                    params={"apiKey": WU_API_KEY, "units": unit_sys, "startDate": date_fmt},
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
                print(f"  [WU] 401 — check API key")
                break

            current += timedelta(days=1)
            time.sleep(0.4)

        print(f"  {city} ({station}): {city_saved} days saved")

    print(f"[WU] Done: {saved} city-days\n")
    return saved


def fetch_price_histories():
    conn = get_conn()
    c    = conn.cursor()
    # Only fetch price history for markets resolved in last 90 days
    # Prevents fetching 4000+ historical markets every night
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp())
    c.execute("""SELECT id FROM markets 
                 WHERE outcome IS NOT NULL 
                 AND resolved_at > %s 
                 ORDER BY resolved_at DESC 
                 LIMIT 500""", (cutoff,))
    market_ids = [r["id"] for r in c.fetchall()]
    conn.close()

    print(f"[PRICES] Fetching histories for {len(market_ids)} markets...")
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
                "INSERT INTO price_snapshots (market_id, timestamp, yes_price) "
                "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                price_rows)
            conn.commit()
            conn.close()
            saved += len(price_rows)

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
            print(f"  [{i}/{len(market_ids)}] {saved} snapshots")
        time.sleep(0.3)

    print(f"[PRICES] Done: {saved} snapshots")
    return saved


def run_full_ingest(days_back=7, days_ahead=7):
    """
    Nightly ingest — reduced scope to protect API limits:
    - 3 cities only (Atlanta, Dallas, NYC)
    - 7 days back (not 30) for market data
    - Price history capped at last 90 days, 500 markets max
    """
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE INGEST — {len(CITY_SLUGS)} cities")
    print(f"  {days_back} days back, {days_ahead} days ahead")
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
    for city in list(CITY_SLUGS.keys())[:5]:
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city=%s AND outcome IS NULL", (city,))
        open_c = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) as count FROM markets WHERE city=%s AND outcome IS NOT NULL", (city,))
        res_c  = c.fetchone()["count"]
        print(f"  {city}: {open_c} open, {res_c} resolved")
    conn.close()


if __name__ == "__main__":
    run_full_ingest(days_back=30, days_ahead=7)
