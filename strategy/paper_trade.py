"""
paper_trade.py - Real paper trading engine.

Uses REAL Polymarket prices and REAL forecast data.
No simulated numbers. Every decision is logged in detail.
At end of week: full audit trail of every scan, every decision, every outcome.

Flow:
1. Get real forecast from GFS + UKMO + MF
2. Find exact matching range on Polymarket at real price
3. Record paper trade with full detail
4. Track to resolution with real WU temp
5. Log every decision — buy OR skip with reason
"""

import json
import time
import re
import requests
from datetime import datetime, timezone, date, timedelta
from data.database import get_conn

GAMMA      = "https://gamma-api.polymarket.com"
BET_SIZE   = 10.0
EST_OFFSET = -5  # EST = UTC-5

CITY_OPEN_TIMES = {
    "Seattle":      "06:00 EST",
    "Dallas":       "06:00 EST",
    "Chicago":      "06:00 EST",
    "Atlanta":      "06:00 EST",
    "Miami":        "06:00 EST",
    "NYC":          "15:15 EST",
    "London":       "13:53 EST",
    "Paris":        "13:52 EST",
    "Toronto":      "16:06 EST",
}


def est_now():
    return datetime.now(timezone.utc).replace(tzinfo=timezone.utc)


def est_str():
    from datetime import timezone as tz, timedelta as td
    return datetime.now(tz(td(hours=EST_OFFSET))).strftime("%Y-%m-%d %I:%M %p EST")


def init_tables():
    """Create all tables needed for paper trading + audit log."""
    conn = get_conn()
    c = conn.cursor()

    # Scan log — every decision made, every city, every scan
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

    # Paper trades — full detail on every trade placed
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
            outcome         TEXT,
            resolved_at     TEXT,
            wu_actual       REAL,
            pnl             REAL,
            UNIQUE(market_id, trade_date)
        )
    """)

    # Weekly summary — auto-computed at end of each day
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
    """Record every scan decision to the audit log."""
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
                      question, price_c):
    """Record a paper trade. Returns trade ID or None if duplicate."""
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
                 spread, confidence, unit, bet_size)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (market_id, trade_date) DO NOTHING
            RETURNING id
        """, (
            est_str(), trade_date, market_id, city, question,
            target_date, days_out, entry, price_c,
            fc.get("consensus"), fc.get("gfs"), fc.get("ukmo"),
            fc.get("meteofrance"), fc.get("spread"),
            fc.get("confidence", 0), fc.get("unit"), BET_SIZE
        ))
        row = c.fetchone()
        conn.commit()
        conn.close()
        return row["id"] if row else None
    except Exception as e:
        print(f"[TRADE ERR] {e}")
        return None


def run_scan():
    """
    Full scan — all cities, all days out.
    Uses real Polymarket prices + real model forecasts.
    Logs every decision in detail.
    Returns (trades_placed, scan_summary)
    """
    try:
        from strategy.early_entry import ALL_CITIES, get_multi_model_forecast, range_near_forecast
        from strategy.early_entry import MAX_PRICE, MIN_PRICE, DAYS_MIN, DAYS_AHEAD
        from strategy.early_entry import FORECAST_WINDOW, CONSENSUS_WINDOW
    except ImportError:
        from early_entry import ALL_CITIES, get_multi_model_forecast, range_near_forecast
        from early_entry import MAX_PRICE, MIN_PRICE, DAYS_MIN, DAYS_AHEAD
        from early_entry import FORECAST_WINDOW, CONSENSUS_WINDOW

    today         = date.today()
    trades_placed = 0
    skipped_spread = 0
    skipped_price  = 0
    skipped_range  = 0
    skipped_nomarket = 0
    cities_bought  = []

    print(f"\n[PAPER] Scan started at {est_str()}")

    for city, config in ALL_CITIES.items():
        slug = config["slug"]
        unit = config["unit"]

        for days_out in range(DAYS_MIN, DAYS_AHEAD + 1):
            target_date = today + timedelta(days=days_out)
            date_str    = target_date.strftime("%Y-%m-%d")
            slug_date   = target_date.strftime("%B-%-d").lower()
            event_slug  = f"highest-temperature-in-{slug}-on-{slug_date}-{target_date.year}"

            # Get real 3-model forecast
            fc = get_multi_model_forecast(config, date_str)
            if fc is None:
                log_scan(city, date_str, days_out, {"unit": unit},
                         "SKIP", "No forecast data available")
                continue

            fc["unit"] = unit
            consensus  = fc["consensus"]
            spread     = fc["spread"]

            # Skip if models disagree too much
            if spread > CONSENSUS_WINDOW and fc["models_available"] >= 2:
                skipped_spread += 1
                log_scan(city, date_str, days_out, fc,
                         "SKIP_SPREAD",
                         f"Models disagree: spread={spread}° > {CONSENSUS_WINDOW}°")
                continue

            # Find market on Polymarket
            try:
                data = requests.get(f"{GAMMA}/events",
                    params={"slug": event_slug}, timeout=15).json()
            except Exception as e:
                log_scan(city, date_str, days_out, fc,
                         "SKIP_NOMARKET", f"Polymarket API error: {e}")
                continue

            if not data or not isinstance(data, list) or not data:
                skipped_nomarket += 1
                log_scan(city, date_str, days_out, fc,
                         "SKIP_NOMARKET", "No market found on Polymarket")
                continue

            markets = data[0].get("markets", [])
            bought_this = False

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
                if yes_price < MIN_PRICE or yes_price > MAX_PRICE:
                    skipped_price += 1
                    log_scan(city, date_str, days_out, fc,
                             "SKIP_PRICE",
                             f"Price {price_c}¢ outside range ({MIN_PRICE*100}¢-{MAX_PRICE*100}¢)",
                             market_id=market_id, question=question, price_c=price_c)
                    continue

                # Range filter — is this range near our forecast?
                if not range_near_forecast(question, consensus, unit, FORECAST_WINDOW):
                    skipped_range += 1
                    log_scan(city, date_str, days_out, fc,
                             "SKIP_RANGE",
                             f"Range too far from forecast {consensus}°{unit}",
                             market_id=market_id, question=question, price_c=price_c)
                    continue

                # Confidence score
                fc["confidence"] = round(
                    max(0, (CONSENSUS_WINDOW - spread) / CONSENSUS_WINDOW * 100), 1)

                # Place paper trade
                trade_id = place_paper_trade(
                    city, date_str, days_out, fc,
                    market_id, question, price_c)

                if trade_id:
                    trades_placed += 1
                    bought_this = True
                    if city not in cities_bought:
                        cities_bought.append(city)
                    log_scan(city, date_str, days_out, fc,
                             "BUY",
                             f"forecast={consensus}°{unit} spread={spread}° conf={fc['confidence']}%",
                             market_id=market_id, question=question,
                             price_c=price_c, trade_id=trade_id)
                    print(f"  [BUY] {city} {date_str} | {question[:50]} | {price_c}¢")
                else:
                    log_scan(city, date_str, days_out, fc,
                             "SKIP_DUPLICATE",
                             "Already traded this market today",
                             market_id=market_id, question=question, price_c=price_c)

            time.sleep(0.1)

    summary = {
        "scanned_at":       est_str(),
        "trades_placed":    trades_placed,
        "cities_bought":    cities_bought,
        "skipped_spread":   skipped_spread,
        "skipped_price":    skipped_price,
        "skipped_range":    skipped_range,
        "skipped_nomarket": skipped_nomarket,
    }
    print(f"[PAPER] Scan done — {trades_placed} trades placed in {len(cities_bought)} cities")
    return trades_placed, summary


def check_outcomes():
    """
    Check all pending trades against real Polymarket prices.
    Records real outcome + real WU temp.
    """
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
        size      = row["bet_size"]
        city      = row["city"]

        try:
            r = requests.get(
                f"{GAMMA}/markets/{market_id}",
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

            # Get WU actual temp
            wu_actual = None
            try:
                from forecast_logger import fetch_wu_temp
                wu_actual = fetch_wu_temp(city, row["target_date"])
            except Exception:
                pass

            conn = get_conn()
            c2 = conn.cursor()
            c2.execute("""
                UPDATE paper_trades
                SET outcome=%s, resolved_at=%s, wu_actual=%s, pnl=%s
                WHERE id=%s
            """, (outcome, est_str(), wu_actual, pnl, tid))
            conn.commit()
            conn.close()

            icon = "✅" if outcome == "Yes" else "❌"
            print(f"  {icon} {city} | {outcome} | pnl=${pnl:.2f} | wu={wu_actual}°")
            resolved += 1

        except Exception as e:
            print(f"  [ERR] {city} {market_id}: {e}")

    return resolved


def get_performance():
    """Full performance report — all trades, wins, losses, P&L."""
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

    c.execute("""
        SELECT city,
               COUNT(*) as bets,
               SUM(CASE WHEN outcome='Yes' THEN 1 ELSE 0 END) as wins,
               SUM(COALESCE(pnl,0)) as pnl
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

    total   = s["total"] or 0
    wins    = s["wins"] or 0
    losses  = s["losses"] or 0
    pending = s["pending"] or 0
    pnl     = float(s["total_pnl"] or 0)
    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

    return {
        "total_trades": total,
        "wins":         wins,
        "losses":       losses,
        "pending":      pending,
        "win_rate":     win_rate,
        "total_pnl":    round(pnl, 2),
        "by_city":      by_city,
        "trades":       trades,
    }


def get_scan_log(limit=200):
    """Full audit trail of every scan decision."""
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
