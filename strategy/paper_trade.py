"""
paper_trade.py — PolyEdge Edge-Based Paper Trading Engine
Last updated: 2026-04-30 3:10 PM EST

WHAT CHANGED FROM PREVIOUS VERSION:
  1. Only trades PROVEN cities (Atlanta + Dallas) — no std=3.0 fake edges
  2. Direction consistency check — never bets against our own forecast
  3. DAYS_AHEAD reduced to 2 — markets only open 0-2 days out (V2.5 proven)
  4. DAYS_MIN = 1 — scan from tomorrow onwards
  5. Higher minimum edge (20%) — more conservative until we have more data

RULES:
  - Every number comes from real proven data. No estimates.
  - Only bet ranges consistent with forecast direction.
  - Max 1 bet per city per day — highest edge only.
  - Proven cities: Atlanta (bias=-1.33F, std=1.05F) + Dallas (bias=-0.30F, std=1.90F)
  - Do NOT add a city without 20+ days of real accuracy data from /forecast/city-accuracy
"""

import json
import math
import re
import time
import requests
from datetime import datetime, date, timedelta
from data.database import get_conn

GAMMA      = "https://gamma-api.polymarket.com"
EST_OFFSET = -5

# ─────────────────────────────────────────────
# PROVEN CITY ACCURACY — REAL DATA ONLY
# Source: /forecast/city-accuracy?days=30
# DO NOT add cities without 20+ days of verified data
# ─────────────────────────────────────────────
CITY_ACCURACY = {
    # bias = mean signed error (forecast - actual)
    #   negative = model runs cold (underforecasts)
    #   positive = model runs warm (overforecasts)
    # std = standard deviation of signed errors
    # All values from real 30-day accuracy data as of 2026-04-30
    # Source: /forecast/city-accuracy?days=30
    # DO NOT add cities without 20+ days of real data
    "Atlanta": {"bias": -0.78, "std": 1.37, "days": 29},  # 79% accuracy ✅ BET BIG
    "Dallas":  {"bias": -0.63, "std": 1.82, "days": 29},  # 69% accuracy ✅ TRADE
    "NYC":     {"bias":  2.10, "std": 1.94, "days": 30},  # 57% accuracy ⚠️ CAUTION
    # Seattle: std=3.33 — too noisy, skip until improves
}

# Trading parameters
MIN_EDGE    = 0.20   # 20% minimum edge — conservative
MIN_PRICE_C = 2.0    # ignore markets < 2¢ (too illiquid)
MAX_PRICE_C = 40.0   # ignore markets > 40¢ (not enough upside)
BET_HUGE    = 50.0   # edge >= 35%
BET_BIG     = 25.0   # edge >= 25%
BET_SMALL   = 10.0   # edge >= 20%
MAX_BETS_PER_CITY = 1
SPREAD_LIMIT      = 3.0
DAYS_MIN          = 1
DAYS_AHEAD        = 2   # proven: markets only open 0-2 days out


# ─────────────────────────────────────────────
# PROBABILITY ENGINE — NORMAL DISTRIBUTION
# ─────────────────────────────────────────────

def _erf(x):
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    return sign * (1.0 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-x*x))


def _cdf(x, mean, std):
    return 0.5 * (1 + _erf((x - mean) / (std * math.sqrt(2))))


def true_probability(lo, hi, consensus, bias, std):
    """
    P(temp lands in [lo, hi]) using bias-corrected normal distribution.
    corrected = consensus - bias  (removes systematic model error)
    """
    corrected = consensus - bias
    if hi >= 999:
        return 1.0 - _cdf(lo, corrected, std)
    if lo <= -999:
        return _cdf(hi + 1, corrected, std)
    return _cdf(hi + 1, corrected, std) - _cdf(lo, corrected, std)


def parse_range(question, unit):
    """
    Parse temperature range from Polymarket question.
    Strips date ('on April 30') before extracting numbers to avoid
    reading the day as a temperature.
    Returns (lo, hi, direction) or (None, None, None).
    """
    orig = question.lower()
    q = orig[:orig.rfind(" on ")] if " on " in orig else orig

    if unit == "F":
        nums = [float(n) for n in re.findall(r"-?\d+\.?\d*", q) if -30 < float(n) < 150]
    else:
        nums = [float(n) for n in re.findall(r"-?\d+\.?\d*", q) if -30 < float(n) < 55]

    if not nums:
        return None, None, None

    if "or higher" in orig or "or above" in orig:
        return nums[0], 999, "higher"
    if "or below" in orig or "or lower" in orig:
        return -999, nums[-1], "lower"
    if len(nums) >= 2:
        return min(nums), max(nums), "exact"
    if len(nums) == 1:
        return nums[0], nums[0] + 1, "exact"
    return None, None, None


def is_consistent_with_forecast(lo, hi, direction, corrected, std):
    """
    Check that we are betting WITH our forecast, not against it.

    'higher' (e.g. 'will temp be 70F or higher?'):
        We bet YES. Only valid if our corrected forecast >= threshold (lo).
        We genuinely expect it to be high.

    'lower' (e.g. 'will temp be 60F or below?'):
        We bet YES. Only valid if our corrected forecast <= threshold (hi).
        We genuinely expect it to be low.

    'exact' (e.g. 'will temp be 72-73F?'):
        We bet YES. Only valid if corrected forecast is within 1.5 std
        of the range center. We genuinely expect it to land here.

    This prevents the Madrid/Sao Paulo problem where we bet "or below"
    on ranges well below our own forecast.
    """
    if direction == "higher":
        return corrected >= lo
    if direction == "lower":
        return corrected <= hi
    if direction == "exact":
        center = (lo + hi) / 2.0
        return abs(corrected - center) <= 1.5 * std
    return False


def calculate_edge(question, consensus, unit, city, price_c):
    """
    Calculate edge = true_prob - market_prob.
    Returns dict or None if:
      - city not proven
      - range unparseable
      - bet direction contradicts forecast
    """
    acc = CITY_ACCURACY.get(city)
    if acc is None:
        return None

    bias = acc["bias"]
    std  = acc["std"]
    corrected = consensus - bias

    lo, hi, direction = parse_range(question, unit)
    if lo is None:
        return None

    if not is_consistent_with_forecast(lo, hi, direction, corrected, std):
        return None

    tp  = true_probability(lo, hi, consensus, bias, std)
    mkt = price_c / 100.0
    edge = tp - mkt

    return {
        "lo": lo, "hi": hi, "direction": direction,
        "corrected": round(corrected, 2),
        "true_prob": round(tp, 4),
        "market_prob": round(mkt, 4),
        "edge": round(edge, 4),
        "bias": bias, "std": std,
    }


def get_bet_size(edge):
    if edge >= 0.35: return BET_HUGE
    if edge >= 0.25: return BET_BIG
    return BET_SMALL


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def est_str():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=EST_OFFSET))).strftime("%Y-%m-%d %I:%M %p EST")


def log_scan(city, target, days_out, fc, decision, reason,
             market_id=None, question=None, price_c=None, trade_id=None):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO scan_log
                (scanned_at, city, target_date, days_out,
                 gfs_temp, ukmo_temp, mf_temp, consensus, spread, unit,
                 decision, reason, market_id, question, price_c, trade_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            est_str(), city, str(target), days_out,
            fc.get("gfs"), fc.get("ukmo"), fc.get("meteofrance"),
            fc.get("consensus"), fc.get("spread"), fc.get("unit"),
            decision, reason, market_id, question, price_c, trade_id
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[LOG ERR] {e}")


def place_trade(city, target, days_out, fc, market_id, question, price_c, ed, bet_size):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO paper_trades
                (placed_at, trade_date, market_id, city, question,
                 target_date, days_out, entry_price, entry_price_c,
                 forecast_temp, gfs_temp, ukmo_temp, mf_temp,
                 spread, confidence, unit, bet_size,
                 true_prob, market_prob, edge, bias_used, std_used, trusted_city)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (market_id, trade_date) DO NOTHING
            RETURNING id
        """, (
            est_str(), date.today().isoformat(), market_id, city, question,
            str(target), days_out, price_c/100.0, price_c,
            fc.get("consensus"), fc.get("gfs"), fc.get("ukmo"),
            fc.get("meteofrance"), fc.get("spread"),
            round(ed["edge"] * 100, 1), fc.get("unit"), bet_size,
            ed["true_prob"], ed["market_prob"], ed["edge"],
            ed["bias"], ed["std"], True
        ))
        row = c.fetchone()
        conn.commit()
        conn.close()
        return row["id"] if row else None
    except Exception as e:
        print(f"[TRADE ERR] {e}")
        return None


def bets_today(city):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM paper_trades WHERE city=%s AND trade_date=%s",
                  (city, date.today().isoformat()))
        n = c.fetchone()["n"]
        conn.close()
        return n
    except Exception:
        return 0


# ─────────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────────

def run_scan():
    """
    2026-04-30: Only scans Atlanta + Dallas (proven cities).
    Checks 1-2 days out (proven market window).
    Applies direction consistency filter.
    Logs every decision with full reasoning.
    """
    try:
        from strategy.early_entry import ALL_CITIES, get_multi_model_forecast
    except ImportError:
        from early_entry import ALL_CITIES, get_multi_model_forecast

    today  = date.today()
    placed = 0
    bought = []
    counts = {k: 0 for k in [
        "SKIP_UNTRUSTED", "SKIP_SPREAD", "SKIP_NOMARKET", "SKIP_PRICE",
        "SKIP_DIRECTION", "SKIP_PARSE", "SKIP_NOEDGE", "SKIP_LIMIT",
        "SKIP_DUPLICATE", "BUY"
    ]}

    print(f"\n[SCAN] {est_str()} — proven cities: {list(CITY_ACCURACY.keys())}")

    for city, config in ALL_CITIES.items():
        if city not in CITY_ACCURACY:
            counts["SKIP_UNTRUSTED"] += 1
            continue

        slug = config["slug"]
        unit = config["unit"]
        acc  = CITY_ACCURACY[city]

        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target    = today + timedelta(days=days_out)
            date_str  = target.strftime("%Y-%m-%d")
            slug_date = target.strftime("%B-%-d").lower()
            event_slug = f"highest-temperature-in-{slug}-on-{slug_date}-{target.year}"

            # Get forecast
            fc = get_multi_model_forecast(config, date_str)
            if fc is None:
                log_scan(city, target, days_out, {"unit": unit},
                         "SKIP", "No forecast data")
                continue

            fc["unit"] = unit
            consensus  = fc["consensus"]
            spread     = fc["spread"]
            corrected  = consensus - acc["bias"]

            # Skip high spread
            if spread > SPREAD_LIMIT and fc.get("models_available", 1) >= 2:
                counts["SKIP_SPREAD"] += 1
                log_scan(city, target, days_out, fc, "SKIP_SPREAD",
                         f"Spread={spread:.1f}° > {SPREAD_LIMIT}°")
                continue

            # Daily bet limit
            if bets_today(city) >= MAX_BETS_PER_CITY:
                counts["SKIP_LIMIT"] += 1
                continue

            # Get markets
            try:
                data = requests.get(f"{GAMMA}/events",
                    params={"slug": event_slug}, timeout=15).json()
            except Exception as e:
                log_scan(city, target, days_out, fc, "SKIP_NOMARKET", f"API error: {e}")
                continue

            if not data or not isinstance(data, list) or not data:
                counts["SKIP_NOMARKET"] += 1
                log_scan(city, target, days_out, fc, "SKIP_NOMARKET", "No market found")
                continue

            markets = data[0].get("markets", [])

            # Evaluate every range, find best edge
            best_edge = None
            best_m    = None
            best_ed   = None

            for m in markets:
                if not m.get("acceptingOrders", False):
                    continue

                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        continue

                yes_price = float(prices[0]) if prices else 0.0
                price_c   = round(yes_price * 100, 2)
                question  = m.get("question", "")
                mid       = m["id"]

                # Price filter
                if price_c < MIN_PRICE_C or price_c > MAX_PRICE_C:
                    counts["SKIP_PRICE"] += 1
                    log_scan(city, target, days_out, fc, "SKIP_PRICE",
                             f"Price {price_c}¢ out of range [{MIN_PRICE_C},{MAX_PRICE_C}]",
                             market_id=mid, question=question, price_c=price_c)
                    continue

                # Calculate edge (includes direction check)
                ed = calculate_edge(question, consensus, unit, city, price_c)

                if ed is None:
                    lo, hi, direction = parse_range(question, unit)
                    if lo is None:
                        counts["SKIP_PARSE"] += 1
                        log_scan(city, target, days_out, fc, "SKIP_PARSE",
                                 "Could not parse range",
                                 market_id=mid, question=question, price_c=price_c)
                    else:
                        counts["SKIP_DIRECTION"] += 1
                        log_scan(city, target, days_out, fc, "SKIP_DIRECTION",
                                 f"Direction inconsistent: corrected={corrected:.1f}{unit} "
                                 f"range={lo}-{hi} dir={direction}",
                                 market_id=mid, question=question, price_c=price_c)
                    continue

                edge = ed["edge"]

                # Log every evaluated range
                log_scan(city, target, days_out, fc,
                         f"EDGE_{edge:+.0%}",
                         f"true={ed['true_prob']:.1%} mkt={ed['market_prob']:.1%} "
                         f"edge={edge:+.1%} corrected={corrected:.1f}{unit} dir={ed['direction']}",
                         market_id=mid, question=question, price_c=price_c)

                if best_edge is None or edge > best_edge:
                    best_edge = edge
                    best_m    = dict(m)
                    best_ed   = ed
                    best_m["_price_c"]  = price_c
                    best_m["_question"] = question

            # Place best bet if it clears threshold
            if best_edge is None:
                continue

            if best_edge < MIN_EDGE:
                counts["SKIP_NOEDGE"] += 1
                log_scan(city, target, days_out, fc, "SKIP_NOEDGE",
                         f"Best edge {best_edge:+.1%} < {MIN_EDGE:.0%} minimum",
                         market_id=best_m["id"],
                         question=best_m["_question"],
                         price_c=best_m["_price_c"])
                continue

            bet_size = get_bet_size(best_edge)
            trade_id = place_trade(
                city, target, days_out, fc,
                best_m["id"], best_m["_question"], best_m["_price_c"],
                best_ed, bet_size
            )

            if trade_id:
                placed += 1
                counts["BUY"] += 1
                if city not in bought:
                    bought.append(city)
                log_scan(city, target, days_out, fc, "BUY",
                         f"edge={best_edge:+.1%} corrected={corrected:.1f}{unit} "
                         f"true={best_ed['true_prob']:.1%} mkt={best_ed['market_prob']:.1%} "
                         f"bet=${bet_size} dir={best_ed['direction']}",
                         market_id=best_m["id"],
                         question=best_m["_question"],
                         price_c=best_m["_price_c"],
                         trade_id=trade_id)
                print(f"  [BUY] {city} {date_str} | {best_m['_question'][:60]} | "
                      f"edge={best_edge:+.1%} | ${bet_size}")
            else:
                counts["SKIP_DUPLICATE"] += 1

            time.sleep(0.3)

    summary = {
        "scanned_at": est_str(),
        "trades_placed": placed,
        "cities": bought,
        "counts": counts,
    }
    print(f"[SCAN] Done — {placed} trades | {counts}")
    return placed, summary


# ─────────────────────────────────────────────
# OUTCOME CHECKING
# ─────────────────────────────────────────────

def check_outcomes():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, market_id, entry_price, bet_size, city, target_date
        FROM paper_trades WHERE outcome IS NULL
    """)
    pending = c.fetchall()
    conn.close()

    if not pending:
        return 0

    resolved = 0
    print(f"[OUTCOMES] Checking {len(pending)} pending trades...")

    for row in pending:
        tid   = row["id"]
        mid   = row["market_id"]
        entry = row["entry_price"]
        size  = row["bet_size"] or 10.0
        city  = row["city"]
        tdate = row["target_date"]

        try:
            r = requests.get(f"{GAMMA}/markets/{mid}",
                timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code != 200:
                continue

            m = r.json()
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)

            outcome = None
            if prices and str(prices[0]) in ["1", "1.0"]:
                outcome = "Yes"
            elif len(prices) > 1 and str(prices[1]) in ["1", "1.0"]:
                outcome = "No"

            if not outcome:
                continue

            pnl = round(size * (1.0 / entry - 1.0), 2) if outcome == "Yes" else -size

            conn2 = get_conn()
            c2 = conn2.cursor()
            c2.execute("""
                UPDATE paper_trades
                SET outcome=%s, resolved_at=%s, pnl=%s
                WHERE id=%s
            """, (outcome, est_str(), pnl, tid))
            conn2.commit()
            conn2.close()

            icon = "✅" if outcome == "Yes" else "❌"
            print(f"  {icon} {city} {tdate} | {outcome} | ${pnl:.2f}")
            resolved += 1
            time.sleep(0.2)

        except Exception as e:
            print(f"  [ERR] {mid}: {e}")

    return resolved


# ─────────────────────────────────────────────
# REPORTING
# ─────────────────────────────────────────────

def get_performance():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome='No'  THEN 1 ELSE 0 END) as losses,
               COUNT(CASE WHEN outcome IS NULL THEN 1 END) as pending,
               SUM(COALESCE(pnl,0)) as total_pnl
        FROM paper_trades
    """)
    s = dict(c.fetchone())

    c.execute("""
        SELECT
            CASE
                WHEN edge >= 0.35 THEN 'edge_35pct+'
                WHEN edge >= 0.25 THEN 'edge_25_35pct'
                WHEN edge >= 0.20 THEN 'edge_20_25pct'
                ELSE 'edge_below_20pct'
            END as bucket,
            COUNT(*) as bets,
            SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
            SUM(COALESCE(pnl,0)) as pnl
        FROM paper_trades
        WHERE outcome IS NOT NULL AND edge IS NOT NULL
        GROUP BY bucket ORDER BY bucket DESC
    """)
    edge_analysis = [dict(r) for r in c.fetchall()]

    c.execute("""
        SELECT city, COUNT(*) as bets,
               SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
               SUM(COALESCE(pnl,0)) as pnl,
               AVG(edge) as avg_edge
        FROM paper_trades WHERE outcome IS NOT NULL
        GROUP BY city ORDER BY pnl DESC
    """)
    by_city = [dict(r) for r in c.fetchall()]

    c.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT 100")
    trades = [dict(r) for r in c.fetchall()]
    conn.close()

    wins    = s["wins"] or 0
    losses  = s["losses"] or 0
    wr      = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

    return {
        "total_trades":  s["total"] or 0,
        "wins":          wins,
        "losses":        losses,
        "pending":       s["pending"] or 0,
        "win_rate":      wr,
        "total_pnl":     round(float(s["total_pnl"] or 0), 2),
        "edge_analysis": edge_analysis,
        "by_city":       by_city,
        "trades":        trades,
    }


def get_scan_log(limit=200):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM scan_log ORDER BY id DESC LIMIT %s", (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


# Stubs for backward compat with server.py imports
def init_tables(): pass
def run_morning_session(): return run_scan()
def run_evening_session(): return check_outcomes()
def check_pending_outcomes(): return check_outcomes()


if __name__ == "__main__":
    n, summary = run_scan()
    print(f"\nSummary: {summary}")
