"""
paper_trade.py - Edge-based paper trading engine.

FUNDAMENTAL RULE: Every number comes from a real source.
No estimations. No guessing. No made-up numbers.

BEEFSLAYER METHOD:
  1. Get real 3-model forecast consensus
  2. Apply real bias correction (from 30-day historical accuracy data)
  3. Calculate TRUE probability for each range using normal distribution
  4. Compare to REAL Polymarket market price
  5. Edge = true_prob - market_price
  6. Only bet when edge > MIN_EDGE threshold
  7. Max 1 range per city per day (highest edge only)
  8. Bet size scales with edge size

PROVEN Variables (from real 30-day Atlanta/Dallas data):
  Atlanta: bias=-1.33F, std=1.05F (75% accuracy, 28 days)
  Dallas:  bias=-0.30F, std=1.90F (71% accuracy, 28 days)
  Others:  use conservative defaults until proven

NO REAL MONEY until paper trading proves edge > 15% wins at 70%+ rate.
"""

import json
import math
import time
import re
import requests
from datetime import datetime, timezone, date, timedelta
from data.database import get_conn

GAMMA      = "https://gamma-api.polymarket.com"
EST_OFFSET = -5

# ─────────────────────────────────────────────
# PROVEN accuracy data from real 30-day backtest
# Source: /forecast/city-accuracy?days=30
# bias = mean signed error (forecast - actual)
# std  = standard deviation of signed errors
# Only cities with 20+ days of real data are trusted
# ─────────────────────────────────────────────
CITY_ACCURACY = {
    "Atlanta":      {"bias": -1.33, "std": 1.05, "days": 28, "trusted": True},
    "Dallas":       {"bias": -0.30, "std": 1.90, "days": 28, "trusted": True},
    "NYC":          {"bias": -1.50, "std": 2.10, "days": 29, "trusted": False},  # avoid
    "Seattle":      {"bias": -0.80, "std": 2.20, "days": 29, "trusted": False},  # caution
    "Miami":        {"bias": -1.20, "std": 1.40, "days":  3, "trusted": False},  # too few days
    # International cities — no proven data yet, use conservative defaults
    "London":       {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Paris":        {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Tokyo":        {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Seoul":        {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Beijing":      {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Shanghai":     {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Singapore":    {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Toronto":      {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Warsaw":       {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Madrid":       {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Munich":       {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Milan":        {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Taipei":       {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Tel Aviv":     {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Buenos Aires": {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
    "Sao Paulo":    {"bias": 0.0,  "std": 3.0,  "days":  0, "trusted": False},
}

# Edge thresholds — minimum edge required to place a bet
# Trusted cities (proven data): lower bar because model is proven
# Untrusted cities: higher bar because model accuracy unknown
MIN_EDGE_TRUSTED   = 0.12   # 12% edge required for proven cities
MIN_EDGE_UNTRUSTED = 0.20   # 20% edge required for unproven cities

# Bet sizing by edge (paper money)
BET_SIZE_HUGE  = 50.0   # edge > 30%
BET_SIZE_BIG   = 25.0   # edge > 20%
BET_SIZE_SMALL = 10.0   # edge > threshold

# Max 1 bet per city per day — BeefSlayer method
MAX_BETS_PER_CITY_PER_DAY = 1

# Price limits — ignore dead or overpriced markets
MIN_PRICE_C = 1.0   # ignore < 1¢
MAX_PRICE_C = 35.0  # ignore > 35¢

# Days out window
DAYS_MIN   = 2
DAYS_AHEAD = 7

# Model spread limit — skip if models disagree too much
SPREAD_LIMIT = 3.0


# ─────────────────────────────────────────────
# PROBABILITY ENGINE
# All math based on normal distribution
# Inputs are real accuracy data (no estimates)
# ─────────────────────────────────────────────

def _erf(x):
    """Error function approximation."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return sign * y


def _normal_cdf(x, mean, std):
    """Probability that a normal random variable is <= x."""
    return 0.5 * (1 + _erf((x - mean) / (std * math.sqrt(2))))


def true_probability(low, high, consensus, bias, std):
    """
    TRUE probability that actual temp lands in [low, high].

    Formula:
      corrected = consensus - bias  (adjust for systematic model error)
      P = CDF(high+1) - CDF(low)   (using real std dev from accuracy data)

    All inputs come from real data — no estimates.
    """
    corrected = consensus - bias
    if high >= 999:
        return 1.0 - _normal_cdf(low, corrected, std)
    if low <= -999:
        return _normal_cdf(high + 1, corrected, std)
    return _normal_cdf(high + 1, corrected, std) - _normal_cdf(low, corrected, std)


def parse_range_from_question(question, unit):
    """
    Extract temperature range [low, high] from Polymarket question.
    Returns (low, high) or (None, None) if can't parse.
    """
    q = question.lower()
    nums = []
    for n in re.findall(r'-?\d+\.?\d*', q):
        v = float(n)
        if unit == "F" and -30 < v < 150:
            nums.append(v)
        elif unit == "C" and -30 < v < 60:
            nums.append(v)

    if not nums:
        return None, None

    if "or below" in q or "or lower" in q:
        return -999, nums[0]
    if "or higher" in q or "or above" in q:
        return nums[0], 999
    if "between" in q and len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], nums[0] + 1
    return None, None


def calculate_edge(question, consensus, unit, city, market_price_c):
    """
    Calculate edge = true_probability - market_implied_probability.

    Returns dict with:
      true_prob: our calculated probability
      market_prob: what Polymarket implies
      edge: the difference
      low/high: the range
    """
    acc = CITY_ACCURACY.get(city, {"bias": 0.0, "std": 3.0, "trusted": False})
    bias = acc["bias"]
    std  = acc["std"]

    low, high = parse_range_from_question(question, unit)
    if low is None:
        return None

    tp = true_probability(low, high, consensus, bias, std)
    market_prob = market_price_c / 100.0
    edge = tp - market_prob

    return {
        "low": low,
        "high": high,
        "true_prob": round(tp, 4),
        "market_prob": round(market_prob, 4),
        "edge": round(edge, 4),
        "bias": bias,
        "std": std,
        "trusted": acc["trusted"],
    }


def get_bet_size(edge):
    """Bet size scales with edge confidence."""
    if edge >= 0.30:
        return BET_SIZE_HUGE
    if edge >= 0.20:
        return BET_SIZE_BIG
    return BET_SIZE_SMALL


# ─────────────────────────────────────────────
# DATABASE FUNCTIONS
# ─────────────────────────────────────────────

def est_str():
    from datetime import timezone as tz, timedelta as td
    return datetime.now(tz(td(hours=EST_OFFSET))).strftime("%Y-%m-%d %I:%M %p EST")


def init_tables():
    """Create all tables needed."""
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS scan_log (
            id           SERIAL PRIMARY KEY,
            scanned_at   TEXT NOT NULL,
            city         TEXT NOT NULL,
            target_date  TEXT NOT NULL,
            days_out     INT,
            gfs_temp     REAL,
            ukmo_temp    REAL,
            mf_temp      REAL,
            consensus    REAL,
            spread       REAL,
            unit         TEXT,
            decision     TEXT NOT NULL,
            reason       TEXT,
            market_id    TEXT,
            question     TEXT,
            price_c      REAL,
            trade_id     INT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id              SERIAL PRIMARY KEY,
            placed_at       TEXT NOT NULL,
            trade_date      TEXT NOT NULL,
            market_id       TEXT NOT NULL,
            city            TEXT NOT NULL,
            question        TEXT NOT NULL,
            target_date     TEXT NOT NULL,
            days_out        INT,
            entry_price     REAL NOT NULL,
            entry_price_c   REAL NOT NULL,
            forecast_temp   REAL,
            gfs_temp        REAL,
            ukmo_temp       REAL,
            mf_temp         REAL,
            spread          REAL,
            confidence      REAL,
            unit            TEXT,
            bet_size        REAL DEFAULT 10.0,
            true_prob       REAL,
            market_prob     REAL,
            edge            REAL,
            bias_used       REAL,
            std_used        REAL,
            trusted_city    BOOLEAN,
            outcome         TEXT,
            resolved_at     TEXT,
            wu_actual       REAL,
            pnl             REAL,
            UNIQUE(market_id, trade_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            id           SERIAL PRIMARY KEY,
            summary_date TEXT UNIQUE,
            total_bets   INT,
            wins         INT,
            losses       INT,
            win_rate     REAL,
            total_pnl    REAL,
            cities_bet   TEXT,
            created_at   TEXT
        )
    """)

    conn.commit()
    conn.close()


def log_scan(city, target_date, days_out, fc, decision, reason,
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
            est_str(), city, target_date, days_out,
            fc.get("gfs"), fc.get("ukmo"), fc.get("meteofrance"),
            fc.get("consensus"), fc.get("spread"), fc.get("unit"),
            decision, reason, market_id, question, price_c, trade_id
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[SCAN LOG ERR] {e}")


def place_paper_trade(city, target_date, days_out, fc, market_id,
                      question, price_c, edge_data, bet_size):
    """Record a paper trade with full edge data."""
    trade_date = date.today().isoformat()
    entry      = price_c / 100.0

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
            est_str(), trade_date, market_id, city, question,
            target_date, days_out, entry, price_c,
            fc.get("consensus"), fc.get("gfs"), fc.get("ukmo"),
            fc.get("meteofrance"), fc.get("spread"),
            round(edge_data["edge"] * 100, 1), fc.get("unit"), bet_size,
            edge_data["true_prob"], edge_data["market_prob"],
            edge_data["edge"], edge_data["bias"],
            edge_data["std"], edge_data["trusted"]
        ))
        row = c.fetchone()
        conn.commit()
        conn.close()
        return row["id"] if row else None
    except Exception as e:
        print(f"[TRADE ERR] {e}")
        return None


def get_bets_today(city):
    """How many bets placed today for this city?"""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as cnt FROM paper_trades
            WHERE city=%s AND trade_date=%s
        """, (city, date.today().isoformat()))
        row = c.fetchone()
        conn.close()
        return row["cnt"] if row else 0
    except Exception:
        return 0


# ─────────────────────────────────────────────
# MAIN SCAN — EDGE-BASED
# ─────────────────────────────────────────────

def run_scan():
    """
    Full scan using real edge calculation.

    For each city/date:
      1. Get real 3-model forecast
      2. Skip if models disagree
      3. For each open market range:
         a. Calculate true probability (bias-corrected normal distribution)
         b. Compare to market price
         c. Calculate edge
      4. Pick the SINGLE highest-edge range per city per day
      5. Only bet if edge > minimum threshold
      6. Log everything — every range, every edge, every decision

    Returns (trades_placed, summary)
    """
    try:
        from strategy.early_entry import ALL_CITIES, get_multi_model_forecast
    except ImportError:
        from early_entry import ALL_CITIES, get_multi_model_forecast

    today         = date.today()
    trades_placed = 0
    cities_bought = []

    counts = {
        "SKIP_SPREAD":   0,
        "SKIP_NOMARKET": 0,
        "SKIP_PRICE":    0,
        "SKIP_NOEDGE":   0,
        "SKIP_LIMIT":    0,
        "SKIP_PARSE":    0,
        "BUY":           0,
    }

    print(f"\n[PAPER EDGE] Scan started at {est_str()}")

    for city, config in ALL_CITIES.items():
        slug = config["slug"]
        unit = config["unit"]
        acc  = CITY_ACCURACY.get(city, {"bias": 0.0, "std": 3.0, "trusted": False})
        min_edge = MIN_EDGE_TRUSTED if acc["trusted"] else MIN_EDGE_UNTRUSTED

        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target_date = today + timedelta(days=days_out)
            date_str    = target_date.strftime("%Y-%m-%d")
            slug_date   = target_date.strftime("%B-%-d").lower()
            event_slug  = f"highest-temperature-in-{slug}-on-{slug_date}-{target_date.year}"

            # Get real forecast
            fc = get_multi_model_forecast(config, date_str)
            if fc is None:
                log_scan(city, date_str, days_out, {"unit": unit},
                         "SKIP", "No forecast data available")
                continue

            fc["unit"] = unit
            consensus  = fc["consensus"]
            spread     = fc["spread"]

            # Skip if models disagree
            if spread > SPREAD_LIMIT and fc.get("models_available", 1) >= 2:
                counts["SKIP_SPREAD"] += 1
                log_scan(city, date_str, days_out, fc,
                         "SKIP_SPREAD",
                         f"Models disagree: spread={spread}° > {SPREAD_LIMIT}°")
                continue

            # Check daily bet limit for this city
            if get_bets_today(city) >= MAX_BETS_PER_CITY_PER_DAY:
                counts["SKIP_LIMIT"] += 1
                continue

            # Get Polymarket markets
            try:
                data = requests.get(f"{GAMMA}/events",
                    params={"slug": event_slug}, timeout=15).json()
            except Exception as e:
                log_scan(city, date_str, days_out, fc,
                         "SKIP_NOMARKET", f"API error: {e}")
                continue

            if not data or not isinstance(data, list) or not data:
                counts["SKIP_NOMARKET"] += 1
                log_scan(city, date_str, days_out, fc,
                         "SKIP_NOMARKET", "No market found on Polymarket")
                continue

            markets = data[0].get("markets", [])

            # Calculate edge for EVERY range, find the best one
            best_edge    = None
            best_market  = None
            best_edge_data = None

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
                market_id = m["id"]

                # Price filter
                if price_c < MIN_PRICE_C or price_c > MAX_PRICE_C:
                    counts["SKIP_PRICE"] += 1
                    log_scan(city, date_str, days_out, fc,
                             "SKIP_PRICE",
                             f"Price {price_c}¢ outside range ({MIN_PRICE_C}¢-{MAX_PRICE_C}¢)",
                             market_id=market_id, question=question, price_c=price_c)
                    continue

                # Calculate real edge
                edge_data = calculate_edge(question, consensus, unit, city, price_c)
                if edge_data is None:
                    counts["SKIP_PARSE"] += 1
                    log_scan(city, date_str, days_out, fc,
                             "SKIP_PARSE", "Could not parse range from question",
                             market_id=market_id, question=question, price_c=price_c)
                    continue

                edge = edge_data["edge"]

                # Log the edge calculation for every range
                log_scan(city, date_str, days_out, fc,
                         f"EDGE_{edge:+.1%}",
                         f"true={edge_data['true_prob']:.1%} mkt={edge_data['market_prob']:.1%} edge={edge:+.1%}",
                         market_id=market_id, question=question, price_c=price_c)

                # Track best edge for this city/date
                if best_edge is None or edge > best_edge:
                    best_edge      = edge
                    best_market    = m
                    best_edge_data = edge_data
                    best_market["_price_c"]   = price_c
                    best_market["_question"]  = question

            # After checking all ranges — place bet on best edge only
            if best_edge is None:
                continue

            if best_edge < min_edge:
                counts["SKIP_NOEDGE"] += 1
                log_scan(city, date_str, days_out, fc,
                         "SKIP_NOEDGE",
                         f"Best edge {best_edge:+.1%} < minimum {min_edge:.0%} for {city}",
                         market_id=best_market["id"],
                         question=best_market["_question"],
                         price_c=best_market["_price_c"])
                continue

            # Place the bet
            bet_size = get_bet_size(best_edge)
            trade_id = place_paper_trade(
                city, date_str, days_out, fc,
                best_market["id"],
                best_market["_question"],
                best_market["_price_c"],
                best_edge_data,
                bet_size
            )

            if trade_id:
                trades_placed += 1
                counts["BUY"] += 1
                if city not in cities_bought:
                    cities_bought.append(city)
                log_scan(city, date_str, days_out, fc,
                         "BUY",
                         f"edge={best_edge:+.1%} true={best_edge_data['true_prob']:.1%} mkt={best_edge_data['market_prob']:.1%} bet=${bet_size}",
                         market_id=best_market["id"],
                         question=best_market["_question"],
                         price_c=best_market["_price_c"],
                         trade_id=trade_id)
                print(f"  [BUY] {city} {date_str} | edge={best_edge:+.1%} | {best_market['_question'][:50]} | {best_market['_price_c']}¢ | ${bet_size}")
            else:
                log_scan(city, date_str, days_out, fc,
                         "SKIP_DUPLICATE",
                         "Already traded this market today",
                         market_id=best_market["id"],
                         question=best_market["_question"],
                         price_c=best_market["_price_c"])

            time.sleep(0.1)

    summary = {
        "scanned_at":    est_str(),
        "trades_placed": trades_placed,
        "cities_bought": cities_bought,
        "counts":        counts,
    }
    print(f"[PAPER EDGE] Scan done — {trades_placed} trades in {len(cities_bought)} cities")
    print(f"  Counts: {counts}")
    return trades_placed, summary


def check_outcomes():
    """Check all pending trades against real Polymarket resolution."""
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
        tid       = row["id"]
        market_id = row["market_id"]
        entry     = row["entry_price"]
        size      = row["bet_size"] or 10.0
        city      = row["city"]

        try:
            r = requests.get(f"{GAMMA}/markets/{market_id}",
                timeout=10, headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code != 200:
                continue

            m      = r.json()
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

            wu_actual = None
            try:
                from forecast_logger import fetch_wu_temp
                wu_actual = fetch_wu_temp(city, row["target_date"])
            except Exception:
                pass

            conn2 = get_conn()
            c2 = conn2.cursor()
            c2.execute("""
                UPDATE paper_trades
                SET outcome=%s, resolved_at=%s, wu_actual=%s, pnl=%s
                WHERE id=%s
            """, (outcome, est_str(), wu_actual, pnl, tid))
            conn2.commit()
            conn2.close()

            icon = "✅" if outcome == "Yes" else "❌"
            print(f"  {icon} {city} | {outcome} | pnl=${pnl:.2f} | wu={wu_actual}°")
            resolved += 1

        except Exception as e:
            print(f"  [ERR] {city} {market_id}: {e}")

    return resolved


def get_performance():
    """Full performance report with edge analysis."""
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN outcome='No' THEN 1 ELSE 0 END) as losses,
            COUNT(CASE WHEN outcome IS NULL THEN 1 END) as pending,
            SUM(COALESCE(pnl,0)) as total_pnl
        FROM paper_trades
    """)
    s = dict(c.fetchone())

    # Edge analysis — do high-edge bets win more?
    c.execute("""
        SELECT
            CASE
                WHEN edge >= 0.25 THEN 'edge_25pct+'
                WHEN edge >= 0.15 THEN 'edge_15_25pct'
                WHEN edge >= 0.05 THEN 'edge_05_15pct'
                ELSE 'edge_below_5pct'
            END as edge_bucket,
            COUNT(*) as bets,
            SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
            SUM(COALESCE(pnl,0)) as pnl
        FROM paper_trades
        WHERE outcome IS NOT NULL AND edge IS NOT NULL
        GROUP BY edge_bucket
        ORDER BY edge_bucket DESC
    """)
    edge_analysis = [dict(r) for r in c.fetchall()]

    c.execute("""
        SELECT city,
               COUNT(*) as bets,
               SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
               SUM(COALESCE(pnl,0)) as pnl,
               AVG(edge) as avg_edge
        FROM paper_trades
        WHERE outcome IS NOT NULL
        GROUP BY city ORDER BY pnl DESC
    """)
    by_city = [dict(r) for r in c.fetchall()]

    c.execute("""
        SELECT * FROM paper_trades
        ORDER BY id DESC LIMIT 100
    """)
    trades = [dict(r) for r in c.fetchall()]
    conn.close()

    total    = s["total"] or 0
    wins     = s["wins"] or 0
    losses   = s["losses"] or 0
    pending  = s["pending"] or 0
    pnl      = float(s["total_pnl"] or 0)
    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

    return {
        "total_trades":  total,
        "wins":          wins,
        "losses":        losses,
        "pending":       pending,
        "win_rate":      win_rate,
        "total_pnl":     round(pnl, 2),
        "edge_analysis": edge_analysis,
        "by_city":       by_city,
        "trades":        trades,
    }


def get_scan_log(limit=200):
    """Full audit trail."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM scan_log
        ORDER BY id DESC LIMIT %s
    """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


if __name__ == '__main__':
    init_tables()
    trades, summary = run_scan()
    print(f"\nSummary: {summary}")
