"""
ingest.py - Pulls all data needed.
Paginates OPEN Polymarket markets (soonest resolving first) and filters
client-side by question text — avoids the broken server-side search param.
Pulls WU historical temps.
Pulls price histories for resolved markets.
"""

import requests
import time
import json
import re
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

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

# All city name variants that might appear in Polymarket questions
CITY_ALIASES = {
    "Chicago":       ["chicago"],
    "Dallas":        ["dallas"],
    "Atlanta":       ["atlanta"],
    "Miami":         ["miami"],
    "New York City": ["new york city", "new york", "nyc"],
    "Seattle":       ["seattle"],
    "Boston":        ["boston"],
    "Los Angeles":   ["los angeles"],
    "San Francisco": ["san francisco"],
}


def safe_get(url, params=None, retries=3, delay=1.0):
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
        time.sleep(delay * (2 ** i))
    return None


# ── 1. Polymarket Weather Markets (open-first pagination) ─────────────────────

def parse_market(question):
    """
    Parse a Polymarket temperature question into structured data.
    Returns dict with city, target_low, target_high, unit, market_type — or None.

    Handles all known question formats:
      "Will the high temperature in Chicago be between 45-50°F on March 24?"
      "Will the highest temperature in Chicago be 56°F or higher on March 25?"
      "Will the highest temperature in Chicago be 63°F or below on March 24?"
      "Will the high temperature in Chicago be 72°F on March 24?"
    """
    q    = question.lower()
    city = None

    # Match city — check longest aliases first to avoid "new york" vs "new york city"
    for canonical, aliases in sorted(CITY_ALIASES.items(),
                                     key=lambda x: max(len(a) for a in x[1]),
                                     reverse=True):
        for alias in aliases:
            if alias in q:
                city = canonical
                break
        if city:
            break

    if not city:
        return None

    # Determine unit
    unit = "F" if ("°f" in q or "fahrenheit" in q or "° f" in q) else "C"

    # ── Range: "between 45-50°f" or "between 45 and 50°f" ──────────────────
    m = re.search(r'between\s+(\d+(?:\.\d+)?)\s*(?:-|and)\s*(\d+(?:\.\d+)?)', q)
    if m:
        return {"city": city, "target_low": float(m.group(1)),
                "target_high": float(m.group(2)), "unit": unit,
                "market_type": "range"}

    # ── Above: "56°f or higher" / "56 or higher" ────────────────────────────
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*[fc]?\s*or\s+(?:higher|above|more)', q)
    if m:
        return {"city": city, "target_low": float(m.group(1)),
                "target_high": 9999, "unit": unit, "market_type": "above"}

    # ── Below: "63°f or below" / "63 or lower" ──────────────────────────────
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*[fc]?\s*or\s+(?:below|lower|less)', q)
    if m:
        return {"city": city, "target_low": -9999,
                "target_high": float(m.group(1)), "unit": unit,
                "market_type": "below"}

    # ── Exact: "be 72°f on" ──────────────────────────────────────────────────
    m = re.search(r'\btemperature\b.{0,30}be\s+(\d+(?:\.\d+)?)\s*°?\s*[fc]?\s+on', q)
    if m:
        t = float(m.group(1))
        return {"city": city, "target_low": t, "target_high": t,
                "unit": unit, "market_type": "exact"}

    # ── Fallback exact: any "NN°F" not already matched ───────────────────────
    m = re.search(r'(\d+(?:\.\d+)?)\s*°[fc]', q)
    if m:
        t = float(m.group(1))
        return {"city": city, "target_low": t, "target_high": t,
                "unit": unit, "market_type": "exact"}

    return None


def _save_market(conn, m, parsed):
    """Insert a single market row. Returns True if new row inserted."""
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
        elif len(prices) > 1 and str(prices[1]) == "1":
            outcome = "No"

    market_id = str(m.get("id") or "")
    if not market_id:
        return False

    end_str = m.get("endDate") or m.get("endDateIso") or ""
    try:
        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
    except Exception:
        return False

    start_str = m.get("startDate", "")
    try:
        created_at = int(datetime.fromisoformat(
            start_str.replace("Z", "+00:00")).timestamp()) if start_str else 0
    except Exception:
        created_at = 0

    try:
        c = conn.cursor()
        c.execute("""INSERT INTO markets
            (id, question, city, target_low, target_high, market_type,
             unit, resolved_at, created_at, outcome, last_trade_price, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                outcome          = EXCLUDED.outcome,
                last_trade_price = EXCLUDED.last_trade_price,
                volume           = EXCLUDED.volume""",
            (market_id, m.get("question", ""), parsed["city"],
             parsed["target_low"], parsed["target_high"],
             parsed["market_type"], parsed["unit"],
             int(end_dt.timestamp()), created_at, outcome,
             float(m.get("lastTradePrice") or 0),
             float(m.get("volume") or 0)))
        inserted = c.rowcount > 0
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"  [DB ERR] {e}")
        return False


def fetch_polymarket_markets(days_back=30):
    """
    Paginate open Polymarket markets ordered by endDate ascending
    (soonest resolving first).  Filter client-side by question text.
    Also pulls recently-closed markets for price history backfill.
    """
    conn   = get_conn()
    saved  = 0
    limit  = 100

    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days_back)

    print(f"\n[POLY] Fetching open + recent markets (days_back={days_back})...")

    # ── Pass 1: Open markets (no closed filter, soonest first) ───────────────
    print("  Pass 1: open markets...")
    offset = 0
    while True:
        data = safe_get(f"{GAMMA_BASE}/markets", params={
            "closed":    "false",      # open markets only
            "limit":     limit,
            "offset":    offset,
            "order":     "endDate",
            "ascending": "true",       # soonest resolving first → fast exit
        })

        if not data:
            break

        batch = 0
        for m in data:
            question = m.get("question", "")
            parsed   = parse_market(question)
            if not parsed:
                continue
            if _save_market(conn, m, parsed):
                batch += 1
                saved += 1

        print(f"    offset={offset} | found={batch} temp markets | total={saved}")

        if len(data) < limit:
            break   # last page

        offset += limit
        time.sleep(0.3)

    # ── Pass 2: Recently closed markets (for price history / backtest) ────────
    print("  Pass 2: recently closed markets...")
    offset = 0
    stop   = False
    while not stop:
        data = safe_get(f"{GAMMA_BASE}/markets", params={
            "closed":    "true",       # resolved markets
            "limit":     limit,
            "offset":    offset,
            "order":     "endDate",
            "ascending": "false",      # newest closed first
        })

        if not data:
            break

        batch = 0
        for m in data:
            end_str = m.get("endDate") or m.get("endDateIso") or ""
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                continue

            if end_dt < cutoff_dt:
                stop = True   # gone past our window — stop paginating
                break

            question = m.get("question", "")
            parsed   = parse_market(question)
            if not parsed:
                continue
            if _save_market(conn, m, parsed):
                batch += 1
                saved += 1

        print(f"    offset={offset} | found={batch} temp markets | total={saved}")

        if len(data) < limit:
            break

        offset += limit
        time.sleep(0.3)

    conn.close()
    print(f"[POLY] Done: {saved} markets saved/updated\n")
    return saved


# ── 2. Weather Underground Historical Temps ───────────────────────────────────

def fetch_wu_temps(days_back=30):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    saved      = 0

    print(f"\n[WU] Fetching temps {start_date} → {end_date}...")

    for city, station in CITY_STATIONS.items():
        current    = start_date
        city_saved = 0

        while current <= end_date:
            date_str = current.strftime('%Y-%m-%d')
            date_fmt = current.strftime('%Y%m%d')

            # Fresh connection each iteration (avoids closed-cursor issues)
            conn = get_conn()
            c    = conn.cursor()
            c.execute("SELECT COUNT(*) as count FROM wu_temps WHERE city=%s AND date=%s",
                      (city, date_str))
            row = c.fetchone()
            conn.close()

            if row and row["count"] > 0:
                current += timedelta(days=1)
                continue

            try:
                r = requests.get(
                    f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json",
                    params={"apiKey": WU_API_KEY, "units": "e", "startDate": date_fmt},
                    timeout=15
                )
            except Exception as e:
                print(f"  [WU ERR] {city} {date_str}: {e}")
                current += timedelta(days=1)
                time.sleep(0.5)
                continue

            if r.status_code == 200:
                obs   = r.json().get("observations", [])
                temps = [o.get("temp") for o in obs if o.get("temp") is not None]
                if temps:
                    max_t = max(temps)
                    conn  = get_conn()
                    c2    = conn.cursor()
                    c2.execute(
                        "INSERT INTO wu_temps (city, station, date, max_temp_f) "
                        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (city, station, date_str, max_t))
                    conn.commit()
                    conn.close()
                    city_saved += 1
                    saved += 1
            elif r.status_code == 401:
                print(f"  [WU] 401 Unauthorized — API key may be invalid")
                break  # no point retrying all dates
            else:
                print(f"  [WU] {r.status_code} for {city} {date_str}")

            current += timedelta(days=1)
            time.sleep(0.4)

        print(f"  {city} ({station}): {city_saved} days saved")

    print(f"[WU] Done: {saved} city-days\n")
    return saved


# ── 3. Price Histories ────────────────────────────────────────────────────────

def fetch_price_histories():
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT id FROM markets
                 WHERE outcome IS NOT NULL AND volume > 100
                 ORDER BY volume DESC""")
    rows       = c.fetchall()
    market_ids = [r["id"] for r in rows]
    conn.close()

    print(f"[PRICES] Fetching price histories for {len(market_ids)} markets...")
    saved = 0

    for i, mid in enumerate(market_ids):
        # Check if already have snapshots (RealDictCursor → use key name)
        conn = get_conn()
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM price_snapshots WHERE market_id=%s", (mid,))
        row  = c.fetchone()
        conn.close()
        if row and row["count"] > 0:
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
            "market": tokens[0], "interval": "all", "fidelity": 1
        }, delay=0.3)

        if not hist or "history" not in hist:
            continue

        price_rows = [(mid, int(p["t"]), float(p["p"]))
                      for p in hist["history"]
                      if p.get("t") and p.get("p")]

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

        if i % 20 == 0:
            print(f"  [{i}/{len(market_ids)}] {saved} snapshots")
        time.sleep(0.3)

    print(f"[PRICES] Done: {saved} snapshots\n")
    return saved


# ── Main ──────────────────────────────────────────────────────────────────────

def run_full_ingest(days_back=30):
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE INGEST — {days_back} days back")
    print(f"{'='*55}\n")

    fetch_polymarket_markets(days_back=days_back)
    fetch_wu_temps(days_back=days_back)
    fetch_price_histories()

    # Summary
    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*55}")
    print(f"  SUMMARY")
    print(f"{'='*55}")
    for table in ["markets", "price_snapshots", "wu_temps", "paper_trades"]:
        c.execute(f"SELECT COUNT(*) as count FROM {table}")
        print(f"  {table:<20} {c.fetchone()['count']:>10,} rows")
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome='Yes'")
    yes = c.fetchone()["count"]
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome='No'")
    no  = c.fetchone()["count"]
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome IS NULL")
    pending = c.fetchone()["count"]
    print(f"\n  YES: {yes:,}  NO: {no:,}  OPEN: {pending:,}")
    conn.close()


if __name__ == '__main__':
    run_full_ingest(days_back=30)
