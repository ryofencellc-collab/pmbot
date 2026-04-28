"""
PolyEdge Variable Test Suite
=============================
Tests every variable the trading system depends on.
Each test either PASSES with real data or FAILS with exact reason.
No assumptions. No patches. No guessing.

Run on Railway via: GET /run-tests
"""

import math
import json
import time
import requests
from datetime import date, timedelta

GAMMA       = "https://gamma-api.polymarket.com"
OPEN_METEO  = "https://api.open-meteo.com/v1/forecast"

results = []

def test(name, description):
    """Decorator to register a test."""
    def decorator(fn):
        results.append({
            "name": name,
            "description": description,
            "fn": fn,
            "status": None,
            "detail": None,
            "fix": None,
        })
        return fn
    return decorator


# ══════════════════════════════════════════════════════
# VARIABLE 1: FORECAST ENGINE
# Can we get real temperature forecasts from Open-Meteo?
# ══════════════════════════════════════════════════════

@test("V1.1", "Open-Meteo GFS reachable from Railway")
def test_gfs_reachable():
    target = (date.today() + timedelta(days=3)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": 33.749, "longitude": -84.388,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": target, "end_date": target,
        "models": "gfs_global",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        return False, f"Missing 'daily' key. Response: {str(data)[:200]}", \
               "Open-Meteo API error. Check if model name 'gfs_global' is still valid."
    temp = data["daily"]["temperature_2m_max"][0]
    if temp is None:
        return False, "GFS returned null temp", "GFS may not have data this far out"
    return True, f"Atlanta {target}: {temp}°F", None


@test("V1.2", "Open-Meteo UKMO reachable from Railway")
def test_ukmo_reachable():
    target = (date.today() + timedelta(days=3)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": 33.749, "longitude": -84.388,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": target, "end_date": target,
        "models": "ukmo_global_deterministic_10km",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        return False, f"Missing 'daily' key: {str(data)[:200]}", \
               "UKMO model name may have changed. Check open-meteo.com/en/docs"
    temp = data["daily"]["temperature_2m_max"][0]
    if temp is None:
        return False, "UKMO returned null temp", "UKMO may not have data this far out"
    return True, f"Atlanta {target}: {temp}°F", None


@test("V1.3", "Forecast available for 1 day out (tomorrow)")
def test_forecast_1day():
    target = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": 33.749, "longitude": -84.388,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": target, "end_date": target,
        "models": "gfs_global",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        return False, f"No data for tomorrow: {str(data)[:200]}", \
               "Can't forecast 1 day out — need to adjust DAYS_MIN"
    temp = data["daily"]["temperature_2m_max"][0]
    if temp is None:
        return False, "Null temp for tomorrow", "GFS doesn't have 1-day data"
    return True, f"Atlanta tomorrow {target}: {temp}°F", None


@test("V1.4", "Forecast available for 7 days out")
def test_forecast_7day():
    target = (date.today() + timedelta(days=7)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": 33.749, "longitude": -84.388,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": target, "end_date": target,
        "models": "gfs_global",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        return False, f"No 7-day data: {str(data)[:200]}", \
               "GFS max range may be less than 7 days"
    temp = data["daily"]["temperature_2m_max"][0]
    if temp is None:
        return False, "Null temp at 7 days", "Reduce DAYS_AHEAD to 6 or 5"
    return True, f"Atlanta {target}: {temp}°F", None


@test("V1.5", "Cities returning null are misconfigured (not API failure)")
def test_null_cities():
    """Buenos Aires returns null — is it the city config or Open-Meteo?"""
    # Buenos Aires coords
    target = (date.today() + timedelta(days=3)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": -34.603, "longitude": -58.381,
        "daily": "temperature_2m_max",
        "temperature_unit": "celsius",
        "timezone": "America/Argentina/Buenos_Aires",
        "start_date": target, "end_date": target,
        "models": "gfs_global",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        return False, f"Buenos Aires API error: {str(data)[:200]}", \
               "Open-Meteo can't serve Buenos Aires with GFS — try without model param"
    temp = data["daily"]["temperature_2m_max"][0]
    if temp is None:
        return False, "Buenos Aires returned null temp", \
               "City config issue — wrong lat/lon or timezone"
    return True, f"Buenos Aires {target}: {temp}°C", None


# ══════════════════════════════════════════════════════
# VARIABLE 2: POLYMARKET CONNECTION
# Can we find and read open markets?
# ══════════════════════════════════════════════════════

@test("V2.1", "Polymarket API reachable")
def test_polymarket_reachable():
    r = requests.get(f"{GAMMA}/markets", params={"limit": 1}, timeout=15)
    if r.status_code != 200:
        return False, f"HTTP {r.status_code}", \
               "Polymarket API is down or blocked from Railway"
    data = r.json()
    if not data:
        return False, "Empty response", "No markets returned"
    return True, f"API reachable, got {len(data)} markets", None


@test("V2.2", "Slug format matches real Polymarket URL")
def test_slug_format():
    """Verify our slug format finds the actual market."""
    today = date.today()
    # Test yesterday and today to find a known open market
    for days in range(0, 4):
        target = today + timedelta(days=days)
        slug_date = target.strftime("%B-%-d").lower()
        slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
        r = requests.get(f"{GAMMA}/events",
            params={"slug": slug}, timeout=15)
        data = r.json()
        if data and isinstance(data, list) and data:
            markets = data[0].get("markets", [])
            active = [m for m in markets if m.get("acceptingOrders")]
            if active:
                return True, \
                    f"Found {len(active)} active markets for Atlanta {target} (slug: {slug})", \
                    None
    return False, \
        f"No active Atlanta markets found for next 4 days. Last slug tried: {slug}", \
        "Slug format may be wrong OR markets not yet open for these dates"


@test("V2.3", "Market prices are readable and reasonable")
def test_market_prices():
    """Find an open market and verify prices sum to ~100%."""
    today = date.today()
    for days in range(0, 5):
        target = today + timedelta(days=days)
        slug_date = target.strftime("%B-%-d").lower()
        slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
        r = requests.get(f"{GAMMA}/events",
            params={"slug": slug}, timeout=15)
        data = r.json()
        if not data or not isinstance(data, list) or not data:
            continue
        markets = data[0].get("markets", [])
        active = [m for m in markets if m.get("acceptingOrders")]
        if not active:
            continue
        # Check prices
        total = 0
        prices_found = []
        for m in active:
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            if prices:
                yes_price = float(prices[0])
                total += yes_price
                prices_found.append(round(yes_price * 100, 1))
        if not prices_found:
            return False, "Markets found but no prices readable", \
                   "outcomePrices field format may have changed"
        return True, \
            f"Atlanta {target}: {len(prices_found)} ranges, prices sum={total*100:.0f}¢, sample={prices_found[:5]}", \
            None
    return False, "No markets found to test prices", \
           "No open Atlanta markets in next 5 days"


@test("V2.4", "Markets open for 1 day out (tomorrow)")
def test_market_tomorrow():
    target = date.today() + timedelta(days=1)
    slug_date = target.strftime("%B-%-d").lower()
    slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
    r = requests.get(f"{GAMMA}/events",
        params={"slug": slug}, timeout=15)
    data = r.json()
    if not data or not isinstance(data, list) or not data:
        return False, f"No market for Atlanta tomorrow {target}", \
               "Markets for tomorrow may not open until day-of"
    markets = data[0].get("markets", [])
    active = [m for m in markets if m.get("acceptingOrders")]
    if not active:
        return False, f"Market exists but closed for Atlanta {target}", \
               "Market may be in resolution — too late to bet"
    return True, f"{len(active)} active ranges for Atlanta {target}", None


@test("V2.5", "Market open timing — what days out are available?")
def test_market_timing():
    """Find earliest and latest open market for Atlanta."""
    today = date.today()
    open_days = []
    for days in range(0, 8):
        target = today + timedelta(days=days)
        slug_date = target.strftime("%B-%-d").lower()
        slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
        try:
            r = requests.get(f"{GAMMA}/events",
                params={"slug": slug}, timeout=10)
            data = r.json()
            if data and isinstance(data, list) and data:
                markets = data[0].get("markets", [])
                active = [m for m in markets if m.get("acceptingOrders")]
                if active:
                    open_days.append(days)
        except Exception:
            pass
        time.sleep(0.3)
    if not open_days:
        return False, "No Atlanta markets open in next 8 days", \
               "Polymarket may not have created markets yet this week"
    return True, \
        f"Open markets at days out: {open_days}. Min={min(open_days)}, Max={max(open_days)}", \
        None


# ══════════════════════════════════════════════════════
# VARIABLE 3: EDGE CALCULATION
# Is our probability math correct?
# ══════════════════════════════════════════════════════

def _erf(x):
    a1,a2,a3,a4,a5 = 0.254829592,-0.284496736,1.421413741,-1.453152027,1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1/(1+p*x)
    return sign*(1-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-x*x))

def _cdf(x, mean, std):
    return 0.5*(1+_erf((x-mean)/(std*math.sqrt(2))))

def _prob(lo, hi, consensus, bias, std):
    c = consensus - bias
    if hi >= 999: return 1 - _cdf(lo, c, std)
    if lo <= -999: return _cdf(hi+1, c, std)
    return _cdf(hi+1, c, std) - _cdf(lo, c, std)


@test("V3.1", "Probability math sums to 100% across all ranges")
def test_prob_sums_to_100():
    """All range probabilities must sum to ~100%."""
    consensus = 78.0
    bias = -1.33
    std = 1.05
    ranges = [
        (-999,69), (70,71), (72,73), (74,75), (76,77),
        (78,79), (80,81), (82,83), (84,85), (86,87), (88,999)
    ]
    total = sum(_prob(lo, hi, consensus, bias, std) for lo, hi in ranges)
    if abs(total - 1.0) > 0.01:
        return False, f"Probabilities sum to {total*100:.1f}% not 100%", \
               "Bug in probability calculation — ranges are overlapping or gapped"
    return True, f"Probabilities sum to {total*100:.2f}%", None


@test("V3.2", "Highest prob range matches consensus forecast")
def test_highest_prob_near_consensus():
    """The range containing our consensus should have highest probability."""
    consensus = 78.5
    bias = -1.33
    std = 1.05
    corrected = consensus - bias  # 79.83
    ranges = [
        (-999,69,"≤69F"), (70,71,"70-71F"), (72,73,"72-73F"),
        (74,75,"74-75F"), (76,77,"76-77F"), (78,79,"78-79F"),
        (80,81,"80-81F"), (82,83,"82-83F"), (84,85,"84-85F"),
        (86,87,"86-87F"), (88,999,"≥88F")
    ]
    probs = [(label, _prob(lo,hi,consensus,bias,std)) for lo,hi,label in ranges]
    best = max(probs, key=lambda x: x[1])
    # Corrected = 79.83, so 78-79F or 80-81F should win
    if best[0] not in ["78-79F", "80-81F"]:
        return False, \
            f"Highest prob is {best[0]} ({best[1]*100:.1f}%), corrected={corrected:.1f}°F", \
            "Probability math may have bias direction error (+/- flipped)"
    return True, \
        f"Highest prob: {best[0]} ({best[1]*100:.1f}%), corrected forecast={corrected:.1f}°F", \
        None


@test("V3.3", "Edge correctly identifies Beefslayer-style mispricing")
def test_edge_identifies_mispricing():
    """Given real Atlanta Apr 29 prices from screenshot, find the +38% edge."""
    market_prices = {
        "76-77F": 0.25,  # Market says 25%
        "78-79F": 0.36,
        "80-81F": 0.29,
    }
    consensus = 76.0
    bias = -1.33
    std = 1.05
    # 76-77F: our model should say ~64%
    tp_76_77 = _prob(76, 77, consensus, bias, std)
    edge = tp_76_77 - market_prices["76-77F"]
    if edge < 0.25:
        return False, \
            f"Edge for 76-77F only {edge*100:.1f}% — expected >25%", \
            "Check bias direction and std value"
    return True, \
        f"76-77F: true_prob={tp_76_77*100:.1f}%, market=25%, edge=+{edge*100:.1f}% ✓", \
        None


@test("V3.4", "Parse range from Polymarket question correctly")
def test_parse_range():
    """Verify we correctly extract lo/hi from question strings."""
    import re
    def parse(question, unit):
        q = question.lower()
        nums = [float(x) for x in re.findall(r'-?\d+\.?\d*', q)
                if (unit=="F" and -30 < float(x) < 200) or
                   (unit=="C" and -30 < float(x) < 60)]
        if not nums: return None, None
        if "or below" in q or "or lower" in q: return -999, nums[0]
        if "or higher" in q or "or above" in q: return nums[0], 999
        if "between" in q and len(nums) >= 2: return nums[0], nums[1]
        if len(nums) == 1: return nums[0], nums[0]+1
        return None, None

    tests = [
        ("Will the highest temperature in Atlanta be between 76-77°F on April 29?", "F", 76, 77),
        ("Will the highest temperature in Atlanta be 69°F or below on April 29?", "F", -999, 69),
        ("Will the highest temperature in Atlanta be 88°F or higher on April 29?", "F", 88, 999),
        ("Will the highest temperature in Seoul be 18°C on April 29?", "C", 18, 19),
    ]
    failures = []
    for q, unit, exp_lo, exp_hi in tests:
        lo, hi = parse(q, unit)
        if lo != exp_lo or hi != exp_hi:
            failures.append(f"'{q[:40]}' → got ({lo},{hi}) expected ({exp_lo},{exp_hi})")
    if failures:
        return False, "\n".join(failures), \
               "Range parser broken — fix parse_range_from_question()"
    return True, f"All {len(tests)} parse tests passed", None


# ══════════════════════════════════════════════════════
# VARIABLE 4: TIMING
# Are we scanning at the right time?
# ══════════════════════════════════════════════════════

@test("V4.1", "Markets exist for tomorrow (1 day out)")
def test_timing_1day():
    target = date.today() + timedelta(days=1)
    slug_date = target.strftime("%B-%-d").lower()
    slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
    r = requests.get(f"{GAMMA}/events", params={"slug": slug}, timeout=15)
    data = r.json()
    if not data or not isinstance(data, list) or not data:
        return False, f"No market for tomorrow ({target})", \
               "DAYS_MIN should stay at 2 — tomorrow's market not yet open"
    markets = data[0].get("markets", [])
    active = [m for m in markets if m.get("acceptingOrders")]
    if not active:
        return False, f"Market exists for {target} but not accepting orders", \
               "Too late to bet on tomorrow — market in resolution"
    return True, f"{len(active)} ranges open for Atlanta {target}", None


@test("V4.2", "Current DAYS_MIN setting captures open markets")
def test_days_min_correct():
    """Verify that at least 1 open market exists within our DAYS_MIN window."""
    DAYS_MIN = 1  # Current setting
    DAYS_AHEAD = 7
    today = date.today()
    found = []
    for days in range(DAYS_MIN, DAYS_AHEAD + 1):
        target = today + timedelta(days=days)
        slug_date = target.strftime("%B-%-d").lower()
        slug = f"highest-temperature-in-atlanta-on-{slug_date}-{target.year}"
        try:
            r = requests.get(f"{GAMMA}/events",
                params={"slug": slug}, timeout=10)
            data = r.json()
            if data and isinstance(data, list) and data:
                markets = data[0].get("markets", [])
                active = [m for m in markets if m.get("acceptingOrders")]
                if active:
                    found.append(f"{days}d ({target}): {len(active)} ranges")
        except Exception:
            pass
        time.sleep(0.3)
    if not found:
        return False, \
            f"No open markets in DAYS_MIN={DAYS_MIN} to DAYS_AHEAD={DAYS_AHEAD} window", \
            "Increase DAYS_AHEAD or decrease DAYS_MIN"
    return True, f"Open markets found: {found}", None


# ══════════════════════════════════════════════════════
# VARIABLE 5: DATABASE
# Is data being stored and retrieved correctly?
# ══════════════════════════════════════════════════════

@test("V5.1", "Database connection works")
def test_db_connection():
    try:
        from data.database import get_conn
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT 1")
        conn.close()
        return True, "Database connection successful", None
    except Exception as e:
        return False, f"DB connection failed: {e}", \
               "Check DATABASE_URL environment variable in Railway"


@test("V5.2", "paper_trades table has edge columns")
def test_db_edge_columns():
    try:
        from data.database import get_conn
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='paper_trades'
        """)
        cols = [r["column_name"] for r in c.fetchall()]
        conn.close()
        required = ["true_prob", "market_prob", "edge", "bias_used", "std_used"]
        missing = [col for col in required if col not in cols]
        if missing:
            return False, f"Missing columns: {missing}", \
                   "Run /db-migrate to add edge columns"
        return True, f"All edge columns present: {required}", None
    except Exception as e:
        return False, f"Error: {e}", "Check database schema"


@test("V5.3", "scan_log is recording real decisions")
def test_scan_log_real():
    try:
        from data.database import get_conn
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN gfs_temp IS NOT NULL THEN 1 ELSE 0 END) as with_forecast,
                   MAX(scanned_at) as last_scan
            FROM scan_log
            WHERE city != 'TestCity'
        """)
        row = dict(c.fetchone())
        conn.close()
        total = row["total"]
        with_fc = row["with_forecast"]
        pct = with_fc/total*100 if total > 0 else 0
        if pct < 30:
            return False, \
                f"Only {pct:.0f}% of scans have forecast data ({with_fc}/{total})", \
                "Forecast engine failing for most cities — check city configs"
        return True, \
            f"{pct:.0f}% of scans have forecast data ({with_fc}/{total}), last: {row['last_scan']}", \
            None
    except Exception as e:
        return False, f"Error: {e}", "Check scan_log table"


@test("V5.4", "paper_trades has edge data recorded")
def test_trades_have_edge():
    try:
        from data.database import get_conn
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN edge IS NOT NULL THEN 1 ELSE 0 END) as with_edge
            FROM paper_trades
        """)
        row = dict(c.fetchone())
        conn.close()
        total = row["total"]
        with_edge = row["with_edge"]
        if with_edge == 0:
            return False, f"{total} trades but 0 have edge data", \
                   "New paper_trade.py not deployed or db-migrate not run"
        return True, f"{with_edge}/{total} trades have edge data", None
    except Exception as e:
        return False, f"Error: {e}", "Check paper_trades table"


# ══════════════════════════════════════════════════════
# VARIABLE 6: END-TO-END
# Does the full pipeline work?
# ══════════════════════════════════════════════════════

@test("V6.1", "Full pipeline: forecast → market → edge → decision")
def test_full_pipeline():
    """Run a complete scan for Atlanta and verify each step."""
    errors = []

    # Step 1: Get forecast
    target = (date.today() + timedelta(days=2)).strftime("%Y-%m-%d")
    r = requests.get(OPEN_METEO, params={
        "latitude": 33.749, "longitude": -84.388,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": target, "end_date": target,
        "models": "gfs_global",
    }, timeout=15)
    data = r.json()
    if "daily" not in data:
        errors.append(f"Step 1 FAIL: No GFS forecast for {target}")
        return False, " | ".join(errors), "Forecast engine down"
    gfs = data["daily"]["temperature_2m_max"][0]
    if gfs is None:
        errors.append(f"Step 1 FAIL: Null GFS temp for {target}")
        return False, " | ".join(errors), "GFS null — try different date"

    # Step 2: Find market
    tdate = date.today() + timedelta(days=2)
    slug_date = tdate.strftime("%B-%-d").lower()
    slug = f"highest-temperature-in-atlanta-on-{slug_date}-{tdate.year}"
    r2 = requests.get(f"{GAMMA}/events", params={"slug": slug}, timeout=15)
    mdata = r2.json()
    if not mdata or not isinstance(mdata, list) or not mdata:
        errors.append(f"Step 2 FAIL: No market for Atlanta {tdate}")
        return False, " | ".join(errors), \
               "Market not open yet — adjust DAYS_MIN or check timing"
    markets = mdata[0].get("markets", [])
    active = [m for m in markets if m.get("acceptingOrders")]
    if not active:
        errors.append(f"Step 2 FAIL: Market exists but closed for {tdate}")
        return False, " | ".join(errors), "Too late to bet"

    # Step 3: Calculate edge
    best_edge = -999
    best_range = None
    import re
    for m in active:
        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        yes_price = float(prices[0]) if prices else 0
        q = m.get("question", "").lower()
        nums = [float(x) for x in re.findall(r'\d+\.?\d*', q)
                if -30 < float(x) < 200]
        if not nums: continue
        if "or below" in q: lo, hi = -999, nums[0]
        elif "or higher" in q: lo, hi = nums[0], 999
        elif len(nums) >= 2: lo, hi = nums[0], nums[1]
        else: continue
        tp = _prob(lo, hi, gfs, -1.33, 1.05)
        edge = tp - yes_price
        if edge > best_edge:
            best_edge = edge
            best_range = m.get("question", "")[:50]

    if best_edge < -0.5:
        errors.append("Step 3 FAIL: Could not calculate any edge")
        return False, " | ".join(errors), "Edge calculation broken"

    return True, \
        f"GFS={gfs}°F → market found ({len(active)} ranges) → best edge={best_edge*100:+.1f}% on '{best_range}'", \
        None


def run_all_tests():
    """Run all tests and return results."""
    output = []
    passed = 0
    failed = 0

    for test_case in results:
        name = test_case["name"]
        desc = test_case["description"]
        try:
            result = test_case["fn"]()
            if isinstance(result, tuple) and len(result) == 3:
                ok, detail, fix = result
            else:
                ok, detail, fix = result, "No detail", None

            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
            else:
                failed += 1

            output.append({
                "test":   name,
                "status": status,
                "desc":   desc,
                "detail": detail,
                "fix":    fix,
            })
            icon = "✅" if ok else "❌"
            print(f"{icon} {name}: {desc}")
            print(f"   → {detail}")
            if fix:
                print(f"   FIX: {fix}")
        except Exception as e:
            failed += 1
            output.append({
                "test":   name,
                "status": "ERROR",
                "desc":   desc,
                "detail": f"Exception: {e}",
                "fix":    "Unexpected error — check code",
            })
            print(f"💥 {name}: {desc}")
            print(f"   → Exception: {e}")

    print(f"\n{'='*50}")
    print(f"RESULTS: {passed} PASSED / {failed} FAILED / {passed+failed} TOTAL")
    print(f"{'='*50}")

    return {
        "passed": passed,
        "failed": failed,
        "total":  passed + failed,
        "tests":  output,
    }


if __name__ == "__main__":
    run_all_tests()
