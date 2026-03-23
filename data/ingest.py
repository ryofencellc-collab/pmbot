"""
ingest.py - Polymarket temperature market ingestion.

Strategy:
  - Hit /events?slug= directly for each city + date combination
  - Each event contains 11 child markets (one per temperature range)
  - Parse groupItemTitle for range: "34-35°F", "52°F or higher", "33°F or below"
  - Pull WU historical temps for the same cities/dates
  - No searching, no pagination, no timeouts

Slug format:  highest-temperature-in-{city}-on-{month}-{day}-{year}
Example:      highest-temperature-in-chicago-on-march-27-2026
"""

import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

GAMMA_BASE = "https://gamma-api.polymarket.com"
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

# City slug names exactly as Polymarket uses them
CITY_SLUGS = {
    "Chicago":       "chicago",
    "Dallas":        "dallas",
    "Atlanta":       "atlanta",
    "Miami":         "miami",
    "New York City": "new-york-city",
    "Seattle":       "seattle",
    "Boston":        "boston",
    "Los Angeles":   "los-angeles",
    "San Francisco": "san-francisco",
}

# WU stations — match Polymarket's resolution source exactly
WU_STATIONS = {
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


# ── Helpers ───────────────────────────────────────────────────────────────────

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
    """
    Build the exact Polymarket event slug for a city + date.
    Example: highest-temperature-in-chicago-on-march-27-2026
    """
    month = target_date.strftime("%B").lower()  # "march"
    day   = str(target_date.day)                # "27" (no leading zero)
    year  = str(target_date.year)               # "2026"
    return f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"


def parse_group_title(title):
    """
    Parse groupItemTitle into market_type, target_low, target_high.

    "33°F or below"  → below,  target_high=33,  target_low=-9999
    "34-35°F"        → range,  target_low=34,   target_high=35
    "52°F or higher" → above,  target_low=52,   target_high=9999

    Returns dict or None.
    """
    t = title.strip().lower()

    # "or below" / "or lower"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*°?f?\s+or\s+(?:below|lower)$', t)
    if m:
        return {"market_type": "below",
                "target_low":  -9999,
                "target_high": float(m.group(1))}

    # "or higher" / "or above"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*°?f?\s+or\s+(?:higher|above)$', t)
    if m:
        return {"market_type": "above",
                "target_low":  float(m.group(1)),
                "target_high": 9999}

    # "34-35°f" range
    m = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*°?f?$', t)
    if m:
        return {"market_type": "range",
                "target_low":  float(m.group(1)),
                "target_high": float(m.group(2))}

    return None


# ── 1. Fetch Polymarket Events ────────────────────────────────────────────────

def fetch_event(city, city_slug, target_date):
    """
    Fetch one event (all child markets) for a city + date.
    Returns list of market dicts ready to insert, or [].
    """
    slug = make_slug(city_slug, target_date)
    data = safe_get(f"{GAMMA_BASE}/events", params={"slug": slug})

    if not data or not isinstance(data, list) or len(data) == 0:
        return []

    event   = data[0]
    markets = event.get("markets", [])
    results = []

    for m in markets:
        title  = m.get("groupItemTitle", "")
        parsed = parse_group_title(title)
        if not parsed:
            print(f"  [SKIP] Cannot parse groupItemTitle: '{title}'")
            continue

        # YES price is outcomePrices[0]
        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []

        yes_price = float(prices[0]) if prices else 0.0

        # Outcome — only set when market is closed
        outcome = None
        if m.get("closed"):
            if prices and str(prices[0]) == "1":
                outcome = "Yes"
            elif len(prices) > 1 and str(prices[1]) == "1":
                outcome = "No"

        # resolved_at from endDate
        end_str = m.get("endDate", "")
        try:
            resolved_at = int(datetime.fromisoformat(
                end_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            resolved_at = 0

        # created_at from startDate
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
            "unit":             "F",
            "resolved_at":      resolved_at,
            "created_at":       created_at,
            "outcome":          outcome,
            "last_trade_price": yes_price,
            "volume":           float(m.get("liquidityNum") or 0),
        })

    return results


def fetch_polymarket_markets(days_ahead=7, days_back=30):
    """
    Fetch markets for all 9 cities across a date window.
    days_ahead: future markets to trade
    days_back:  past markets for outcome backfill
    Total API calls = 9 cities x (days_back + days_ahead + 1)
    """
    today     = date.today()
    all_dates = [today + timedelta(days=i)
                 for i in range(-days_back, days_ahead + 1)]

    total_calls = len(CITY_SLUGS) * len(all_dates)
    print(f"\n[POLY] {len(CITY_SLUGS)} cities x {len(all_dates)} dates "
          f"= {total_calls} API calls...")

    conn  = get_conn()
    saved = 0

    for city, city_slug in CITY_SLUGS.items():
        city_count = 0

        for target_date in all_dates:
            markets = fetch_event(city, city_slug, target_date)

            for m in markets:
                c = conn.cursor()
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
                    conn.commit()
                    city_count += 1
                    saved += 1
                except Exception as e:
                    conn.rollback()
                    print(f"  [DB ERR] {city} {target_date}: {e}")

            time.sleep(0.2)

        print(f"  {city}: {city_count} markets upserted")

    conn.close()
    print(f"[POLY] Done: {saved} total upserts\n")
    return saved


# ── 2. WU Historical Temps ────────────────────────────────────────────────────

def fetch_wu_temps(days_back=30):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    saved      = 0

    print(f"\n[WU] Fetching temps {start_date} → {end_date}...")

    for city, station in WU_STATIONS.items():
        current    = start_date
        city_saved = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            date_fmt = current.strftime("%Y%m%d")

            # Fresh connection per iteration — avoids stale cursor issues
            conn = get_conn()
            c    = conn.cursor()
            c.execute("SELECT COUNT(*) as count FROM wu_temps "
                      "WHERE city=%s AND date=%s", (city, date_str))
            row = c.fetchone()
            conn.close()

            if row and row["count"] > 0:
                current += timedelta(days=1)
                continue

            try:
                r = requests.get(
                    f"https://api.weather.com/v1/location/{station}:9:US"
                    f"/observations/historical.json",
                    params={"apiKey": WU_API_KEY, "units": "e",
                            "startDate": date_fmt},
                    timeout=15
                )
            except Exception as e:
                print(f"  [WU ERR] {city} {date_str}: {e}")
                current += timedelta(days=1)
                time.sleep(0.5)
                continue

            if r.status_code == 200:
                obs   = r.json().get("observations", [])
                temps = [o.get("temp") for o in obs
                         if o.get("temp") is not None]
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
                print(f"  [WU] 401 Unauthorized — check WU_API_KEY")
                break
            else:
                print(f"  [WU] HTTP {r.status_code} for {city} {date_str}")

            current += timedelta(days=1)
            time.sleep(0.4)

        print(f"  {city} ({station}): {city_saved} days saved")

    print(f"[WU] Done: {saved} city-days\n")
    return saved


# ── 3. Main ───────────────────────────────────────────────────────────────────

def run_full_ingest(days_back=30, days_ahead=7):
    init_db()
    print(f"\n{'='*55}")
    print(f"  POLYEDGE INGEST")
    print(f"  {days_back} days back, {days_ahead} days ahead")
    print(f"{'='*55}\n")

    fetch_polymarket_markets(days_ahead=days_ahead, days_back=days_back)
    fetch_wu_temps(days_back=days_back)

    # Summary
    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*55}")
    print(f"  SUMMARY")
    print(f"{'='*55}")
    for table in ["markets", "wu_temps", "paper_trades"]:
        c.execute(f"SELECT COUNT(*) as count FROM {table}")
        print(f"  {table:<20} {c.fetchone()['count']:>10,} rows")
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome='Yes'")
    yes = c.fetchone()["count"]
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome='No'")
    no  = c.fetchone()["count"]
    c.execute("SELECT COUNT(*) as count FROM markets WHERE outcome IS NULL")
    open_count = c.fetchone()["count"]
    print(f"\n  YES: {yes:,}  NO: {no:,}  OPEN: {open_count:,}")
    conn.close()


if __name__ == "__main__":
    run_full_ingest(days_back=30, days_ahead=7)
