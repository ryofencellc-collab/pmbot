"""
paper_trade.py — PolyEdge Trading Engine v3.0
Rebuilt: 2026-05-19

AUDIT FINDINGS THAT DROVE THIS REBUILD:
  - 13 bets placed below minimum edge threshold (-$130)
  - 3 bets placed with negative edge (-$40, won by luck)
  - NYC threshold bypass bug (bets at 6.9%, 21.8%, 23.2%)
  - $50 bet on Atlanta 83°F (model underpredicts above 82°F)
  - Outcome checker not resolving stale bets
  - Multi-range strategy bypassing direction filter

REBUILD RULES — every parameter justified by data:
  - Atlanta 75-81°F only: 75% accuracy, avg_err=1.1°F (n=8)
  - Dallas 87°F+ only: 78% accuracy, avg_err=1.3°F (n=9)
  - NYC: do not trade — 64.3% accuracy
  - Min edge: 25% — clean bets averaged 29%
  - Spread limit: 3.0° — id=443 at 3.4° cost $50
  - Bet size: $10 flat — no scaling until 30+ clean trades proven
  - No multi-range — too complex, created bypass bugs
  - Hard temperature range gating — no bets outside proven ranges
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
# CITY CONFIG — audit-justified parameters
# Source: /forecast/city-accuracy 30-day data
# DO NOT modify without data justification
# ─────────────────────────────────────────────

CITY_CONFIG = {
    "Atlanta": {
        "slug":     "atlanta",
        "lat":      33.749,
        "lon":      -84.388,
        "unit":     "F",
        "bias":     0.5,    # 75-81°F range: model slightly high, conservative correction
        "std":      1.1,    # avg_err=1.1°F in proven range
        "min_temp": 75,     # HARD FLOOR — do not bet below this
        "max_temp": 81,     # HARD CEILING — do not bet above this
        # Audit finding: 75-81°F = 75% accuracy. Below 75°F = 55%. Above 82°F = 50%.
        "min_edge": 0.25,
        "tradeable": True,
    },
    "Dallas": {
        "slug":     "dallas",
        "lat":      32.776,
        "lon":      -96.797,
        "unit":     "F",
        "bias":     0.4,    # 87°F+ range: model slightly high
        "std":      1.3,    # avg_err=1.3°F in proven range
        "min_temp": 87,     # HARD FLOOR — only bet hot Dallas days
        "max_temp": 999,    # No ceiling — model holds above 87°F
        # Audit finding: 87°F+ = 78% accuracy. Below 87°F = 44-50%.
        "min_edge": 0.25,
        "tradeable": True,
    },
    # NYC: 64.3% accuracy — do not trade
    # Chicago: 37.9% — never trade
    # Seattle: 35.7% — never trade
    # Denver/London/Shanghai/Singapore: collecting data, not yet tradeable
}

# Trading parameters — all audit-justified
MIN_PRICE_C      = 0.5    # below this the payout math breaks down
MAX_PRICE_C      = 40.0   # above this the market already knows — no edge
SPREAD_LIMIT     = 3.0    # id=443 was placed at 3.4°, lost $50 — back to 3.0
BASE_BET         = 10.0   # flat $10 until 30+ clean resolved trades proven
MAX_BETS_PER_CITY = 1     # max 1 bet per city per target date
DAYS_MIN         = 1
DAYS_AHEAD       = 2


# ─────────────────────────────────────────────
# PROBABILITY ENGINE
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
    corrected = consensus - bias
    if hi >= 999:
        return 1.0 - _cdf(lo, corrected, std)
    if lo <= -999:
        return _cdf(hi + 1, corrected, std)
    return _cdf(hi + 1, corrected, std) - _cdf(lo, corrected, std)


def parse_range(question, unit):
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


# ─────────────────────────────────────────────
# TEMPERATURE RANGE GATE
# Core rebuild requirement: only bet in proven ranges
# ─────────────────────────────────────────────

def in_proven_range(city, consensus):
    """
    Returns True only if forecast is within the city's proven temp range.
    This is a HARD gate — no exceptions.
    Atlanta: only 75-81°F (75% accuracy)
    Dallas:  only 87°F+   (78% accuracy)
    """
    cfg = CITY_CONFIG.get(city)
    if not cfg:
        return False
    return cfg["min_temp"] <= consensus <= cfg["max_temp"]


# ─────────────────────────────────────────────
# EDGE CALCULATION
# ─────────────────────────────────────────────

def calculate_edge(question, consensus, unit, city, price_c):
    cfg = CITY_CONFIG.get(city)
    if not cfg:
        return None

    bias = cfg["bias"]
    std  = cfg["std"]
    corrected = consensus - bias

    lo, hi, direction = parse_range(question, unit)
    if lo is None:
        return None

    # Direction consistency — only bet WITH our forecast
    if direction == "higher" and corrected < lo:
        return None
    if direction == "lower" and corrected > hi:
        return None
    if direction == "exact":
        center = (lo + hi) / 2.0
        if abs(corrected - center) > 1.5 * std:
            return None

    tp   = true_probability(lo, hi, consensus, bias, std)
    mkt  = price_c / 100.0
    edge = tp - mkt

    return {
        "lo": lo, "hi": hi, "direction": direction,
        "corrected": round(corrected, 2),
        "true_prob": round(tp, 4),
        "market_prob": round(mkt, 4),
        "edge": round(edge, 4),
        "bias": bias, "std": std,
    }


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
            ON CONFLICT (market_id) DO NOTHING
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


def bets_today(city, target_date):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM paper_trades WHERE city=%s AND target_date=%s",
                  (city, str(target_date)))
        n = c.fetchone()["n"]
        conn.close()
        return n
    except Exception:
        return 0


# ─────────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────────

def run_scan():
    try:
        from strategy.early_entry import ALL_CITIES, get_multi_model_forecast
    except ImportError:
        from early_entry import ALL_CITIES, get_multi_model_forecast

    today  = date.today()
    placed = 0
    bought = []
    counts = {k: 0 for k in [
        "SKIP_UNTRUSTED", "SKIP_TEMPRANGE", "SKIP_SPREAD",
        "SKIP_NOMARKET", "SKIP_PRICE", "SKIP_DIRECTION",
        "SKIP_NOEDGE", "SKIP_LIMIT", "SKIP_DUPLICATE", "BUY"
    ]}

    tradeable = {k: v for k, v in CITY_CONFIG.items() if v["tradeable"]}
    print(f"\n[SCAN] {est_str()} — trading cities: {list(tradeable.keys())}")

    for city, cfg in tradeable.items():
        slug = cfg["slug"]
        unit = cfg["unit"]

        # Get ALL_CITIES entry for forecast
        city_fc_cfg = ALL_CITIES.get(city)
        if not city_fc_cfg:
            counts["SKIP_UNTRUSTED"] += 1
            continue

        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target    = today + timedelta(days=days_out)
            date_str  = target.strftime("%Y-%m-%d")
            slug_date = target.strftime("%B-%-d").lower()
            event_slug = f"highest-temperature-in-{slug}-on-{slug_date}-{target.year}"

            # Get forecast
            fc = get_multi_model_forecast(city_fc_cfg, date_str)
            if fc is None or fc.get("consensus") is None:
                log_scan(city, target, days_out, {"unit": unit},
                         "SKIP", "No forecast data")
                continue

            fc["unit"]      = unit
            consensus       = fc["consensus"]
            spread          = fc["spread"]

            # ── HARD TEMPERATURE RANGE GATE ──────────────────────────
            # Only bet in proven temperature ranges — audit requirement
            if not in_proven_range(city, consensus):
                counts["SKIP_TEMPRANGE"] += 1
                log_scan(city, target, days_out, fc, "SKIP_TEMPRANGE",
                         f"Consensus={consensus:.1f}{unit} outside proven range "
                         f"[{cfg['min_temp']},{cfg['max_temp']}]")
                continue

            # ── SPREAD FILTER ─────────────────────────────────────────
            if spread > SPREAD_LIMIT and fc.get("models_available", 1) >= 2:
                counts["SKIP_SPREAD"] += 1
                log_scan(city, target, days_out, fc, "SKIP_SPREAD",
                         f"Spread={spread:.1f}° > {SPREAD_LIMIT}°")
                continue

            # ── ALREADY BET THIS TARGET DATE ─────────────────────────
            if bets_today(city, target) >= MAX_BETS_PER_CITY:
                counts["SKIP_LIMIT"] += 1
                log_scan(city, target, days_out, fc, "SKIP_LIMIT",
                         f"Already have {MAX_BETS_PER_CITY} bet(s) for {city} on {target}")
                continue

            # ── GET MARKETS ──────────────────────────────────────────
            try:
                data = requests.get(f"{GAMMA}/events",
                    params={"slug": event_slug}, timeout=15).json()
            except Exception as e:
                log_scan(city, target, days_out, fc, "SKIP_NOMARKET", f"API error: {e}")
                continue

            if not data or not isinstance(data, list) or not data[0].get("markets"):
                counts["SKIP_NOMARKET"] += 1
                log_scan(city, target, days_out, fc, "SKIP_NOMARKET", "No market found")
                continue

            markets = data[0].get("markets", [])

            # ── FIND BEST EDGE ────────────────────────────────────────
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

                # Edge calculation (includes direction check)
                ed = calculate_edge(question, consensus, unit, city, price_c)

                if ed is None:
                    lo, hi, direction = parse_range(question, unit)
                    if lo is None:
                        log_scan(city, target, days_out, fc, "SKIP_PARSE",
                                 "Could not parse range",
                                 market_id=mid, question=question, price_c=price_c)
                    else:
                        counts["SKIP_DIRECTION"] += 1
                        log_scan(city, target, days_out, fc, "SKIP_DIRECTION",
                                 f"Direction inconsistent: corrected={consensus - cfg['bias']:.1f}{unit} "
                                 f"range={lo}-{hi} dir={direction}",
                                 market_id=mid, question=question, price_c=price_c)
                    continue

                edge = ed["edge"]

                # Log every evaluated range
                log_scan(city, target, days_out, fc,
                         f"EDGE_{edge:+.0%}",
                         f"true={ed['true_prob']:.1%} mkt={ed['market_prob']:.1%} "
                         f"edge={edge:+.1%} corrected={ed['corrected']:.1f}{unit} dir={ed['direction']}",
                         market_id=mid, question=question, price_c=price_c)

                # Hard filter: never bet negative edge
                if edge < 0:
                    continue

                if best_edge is None or edge > best_edge:
                    best_edge = edge
                    best_m    = dict(m)
                    best_ed   = ed
                    best_m["_price_c"]  = price_c
                    best_m["_question"] = question

            # ── EDGE THRESHOLD ────────────────────────────────────────
            if best_edge is None:
                continue

            # Hard minimum — no exceptions
            min_edge = cfg["min_edge"]
            if best_edge < min_edge:
                counts["SKIP_NOEDGE"] += 1
                log_scan(city, target, days_out, fc, "SKIP_NOEDGE",
                         f"Best edge {best_edge:+.1%} < {min_edge:.0%} minimum for {city}",
                         market_id=best_m["id"],
                         question=best_m["_question"],
                         price_c=best_m["_price_c"])
                continue

            # ── PLACE BET ─────────────────────────────────────────────
            # Flat $10 — no scaling until 30+ clean trades proven
            trade_id = place_trade(
                city, target, days_out, fc,
                best_m["id"], best_m["_question"], best_m["_price_c"],
                best_ed, BASE_BET
            )

            if trade_id:
                placed += 1
                counts["BUY"] += 1
                if city not in bought:
                    bought.append(city)
                log_scan(city, target, days_out, fc, "BUY",
                         f"edge={best_edge:+.1%} corrected={best_ed['corrected']:.1f}{unit} "
                         f"true={best_ed['true_prob']:.1%} mkt={best_ed['market_prob']:.1%} "
                         f"bet=${BASE_BET}",
                         market_id=best_m["id"],
                         question=best_m["_question"],
                         price_c=best_m["_price_c"],
                         trade_id=trade_id)
                print(f"  [BUY] {city} {date_str} | {best_m['_question'][:60]} | "
                      f"edge={best_edge:+.1%} | ${BASE_BET}")
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
# Fixed: now catches stale bets where target_date has passed
# ─────────────────────────────────────────────

def check_outcomes():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, market_id, entry_price, bet_size, city, target_date
        FROM paper_trades WHERE outcome IS NULL
        ORDER BY target_date ASC
    """)
    pending = c.fetchall()
    conn.close()

    if not pending:
        return 0

    resolved = 0
    today = date.today()
    print(f"[OUTCOMES] Checking {len(pending)} pending trades...")

    for row in pending:
        tid   = row["id"]
        mid   = row["market_id"]
        entry = row["entry_price"]
        size  = row["bet_size"] or 10.0
        city  = row["city"]
        tdate = row["target_date"]

        # Stale check — if target date has passed, force resolution attempt
        try:
            target_dt = date.fromisoformat(str(tdate)[:10])
            days_since = (today - target_dt).days
            if days_since < 0:
                # Future bet — skip, market hasn't resolved yet
                continue
        except Exception:
            pass

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

            # If still unresolved but stale (2+ days old), mark as No
            if not outcome and days_since >= 2:
                outcome = "No"
                print(f"  ⚠️  Force-resolving stale bet id={tid} {city} {tdate} (2+ days old)")

            if not outcome:
                continue

            pnl = round(size * (1.0 / entry - 1.0), 2) if outcome == "Yes" else -size

            # Fetch WU actual temperature
            wu_actual = None
            try:
                from forecast_logger import fetch_wu_temp
                wu_actual = fetch_wu_temp(city, str(tdate)[:10])
            except Exception:
                pass

            conn2 = get_conn()
            c2 = conn2.cursor()
            c2.execute("""
                UPDATE paper_trades
                SET outcome=%s, resolved_at=%s, pnl=%s, wu_actual=%s
                WHERE id=%s
            """, (outcome, est_str(), pnl, wu_actual, tid))
            conn2.commit()
            conn2.close()

            icon = "✅" if outcome == "Yes" else "❌"
            print(f"  {icon} id={tid} {city} {tdate} | {outcome} | ${pnl:.2f} | wu={wu_actual}")
            resolved += 1
            time.sleep(0.2)

        except Exception as e:
            print(f"  [ERR] id={tid} {mid}: {e}")

    return resolved


def check_pending_outcomes():
    return check_outcomes()


def init_tables():
    """No-op — tables initialized in database.py. Kept for server.py compatibility."""
    pass


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

    wins   = s["wins"] or 0
    losses = s["losses"] or 0
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

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


def run_morning_session():
    """No-op stub — morning session handled by scheduler directly."""
    pass


def run_evening_session():
    """No-op stub — evening session handled by scheduler directly."""
    pass


def get_scan_log(limit=200):
    """Return recent scan log entries."""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT scanned_at, city, target_date, days_out,
                   gfs_temp, ukmo_temp, mf_temp, consensus, spread, unit,
                   decision, reason, market_id, question, price_c, trade_id
            FROM scan_log ORDER BY id DESC LIMIT %s
        """, (limit,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        print(f"[GET_SCAN_LOG ERR] {e}")
        return []
