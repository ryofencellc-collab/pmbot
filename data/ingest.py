"""
ingest.py — Pull all data needed for 3-year backtest.

Sources:
  1. Polymarket Gamma API    — resolved markets + price history
  2. Binance API             — BTC/ETH hourly OHLCV (free, no auth)
  3. Open-Meteo API          — historical weather forecasts + actuals (free)
  4. The Odds API            — sports lines (free tier: 500 req/month)
"""

import requests
import sqlite3
import time
import json
import os
from datetime import datetime, timedelta, timezone, date
from data.database import get_conn, init_db

GAMMA_BASE   = "https://gamma-api.polymarket.com"
CLOB_BASE    = "https://clob.polymarket.com"
BINANCE_BASE = "https://api.binance.com"
METEO_BASE   = "https://archive-api.open-meteo.com"
ODDS_KEY     = os.getenv("ODDS_API_KEY", "")

WEATHER_CITIES = {
    "London":      {"lat": 51.5074, "lon": -0.1278},
    "New York":    {"lat": 40.7128, "lon": -74.0060},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298},
    "Miami":       {"lat": 25.7617, "lon": -80.1918},
    "Tokyo":       {"lat": 35.6762, "lon": 139.6503},
    "Sydney":      {"lat": -33.8688, "lon": 151.2093},
}


def safe_get(url, params=None, retries=3, delay=1.0):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                print("  [RATE LIMIT] sleeping 30s...")
                time.sleep(30)
            else:
                print(f"  [WARN] {r.status_code} — {url}")
        except Exception as e:
            print(f"  [ERR] attempt {i+1}: {e}")
        time.sleep(delay * (2 ** i))
    return None


def ts_to_date(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def classify_market_type(question, category):
    q   = (question or "").lower()
    cat = (category or "").lower()
    if any(w in q for w in ["temperature", "celsius", "fahrenheit", "°c", "°f", "weather", "high temp", "low temp"]):
        return "weather"
    if any(w in q for w in ["bitcoin", "btc", "ethereum", "eth", "crypto", "price above", "price below", "reach $", "hit $"]):
        return "crypto_daily"
    if any(w in cat for w in ["sports", "basketball", "football", "soccer", "baseball", "nba", "nfl", "mlb", "nhl"]):
        return "sports"
    if any(w in q for w in ["win", "beat", "vs", "game", "match", "series"]):
        return "sports"
    return "other"


def fetch_polymarket_markets(days_back=1095):
    conn   = get_conn()
    c      = conn.cursor()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    offset = 0
    limit  = 500
    saved  = 0

    print(f"\n[INGEST] Polymarket markets since {cutoff.date()} ...")

    while True:
        data = safe_get(f"{GAMMA_BASE}/markets", params={
            "closed": "true", "limit": limit,
            "offset": offset, "order": "startDate", "ascending": "false"
        })
        if not data:
            break

        batch_saved = 0
        stop_early  = False

        for m in data:
            try:
                end_str = m.get("endDate") or m.get("endDateIso") or ""
                if not end_str:
                    continue
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                continue

            if end_dt < cutoff:
                stop_early = True
                continue

            outcome = m.get("winnerOutcome") or m.get("resolvedOutcome")
            if not outcome and m.get("resolved"):
                outcome = "Yes"

            market_id   = str(m.get("id") or m.get("conditionId") or "")
            question    = m.get("question", "")
            category    = m.get("category") or m.get("groupItemTitle") or "unknown"
            market_type = classify_market_type(question, category)

            try:
                start_str  = m.get("startDate", "")
                created_at = int(datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp()) if start_str else 0
            except Exception:
                created_at = 0

            if not market_id:
                continue

            try:
                c.execute('''
                    INSERT OR IGNORE INTO markets
                    (id, question, category, market_type, created_at, resolved_at,
                     end_date_iso, outcome, volume, liquidity)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (
                    market_id, question, category, market_type,
                    created_at, int(end_dt.timestamp()), end_str,
                    outcome, float(m.get("volume") or 0), float(m.get("liquidity") or 0)
                ))
                if c.rowcount > 0:
                    batch_saved += 1
            except Exception:
                continue

        conn.commit()
        saved += batch_saved
        print(f"  offset={offset} | batch={len(data)} | saved={batch_saved} | total={saved}")

        if len(data) < limit or stop_early:
            break

        offset += limit
        time.sleep(0.4)

    conn.close()
    print(f"[INGEST] Polymarket: {saved} markets saved\n")
    return saved


def fetch_price_histories(limit=1000):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('SELECT id FROM markets WHERE volume > 500 ORDER BY volume DESC LIMIT ?', (limit,))
    market_ids = [r[0] for r in c.fetchall()]
    conn.close()

    print(f"[INGEST] Fetching price history for {len(market_ids)} markets...")

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
            "market": tokens[0], "interval": "1h", "fidelity": 60
        }, delay=0.3)

        if not hist or "history" not in hist:
            continue

        rows = [
            (mid, int(p.get("t", 0)), float(p.get("p", 0)),
             round(1 - float(p.get("p", 0)), 6), 0.0)
            for p in hist["history"] if p.get("t") and p.get("p")
        ]

        if rows:
            conn = get_conn()
            conn.executemany('''
                INSERT OR IGNORE INTO price_snapshots
                (market_id, timestamp, yes_price, no_price, volume_at_time)
                VALUES (?,?,?,?,?)
            ''', rows)
            conn.commit()
            conn.close()

        if i % 50 == 0:
            print(f"  [{i}/{len(market_ids)}] price histories fetched")
        time.sleep(0.25)

    print("[INGEST] Price histories complete\n")


def fetch_crypto_ohlcv(symbol="BTC", days_back=1095):
    pair     = f"{symbol}USDT"
    end_ts   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ts = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)
    conn     = get_conn()
    saved    = 0

    print(f"[INGEST] Binance {pair} hourly OHLCV...")

    current_end = end_ts
    while current_end > start_ts:
        data = safe_get(f"{BINANCE_BASE}/api/v3/klines", params={
            "symbol": pair, "interval": "1h",
            "endTime": current_end, "limit": 1000
        })
        if not data:
            break

        rows = [(symbol, int(c[0]) // 1000,
                 float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5]))
                for c in data]

        conn.executemany('''
            INSERT OR IGNORE INTO crypto_prices
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?)
        ''', rows)
        conn.commit()
        saved      += len(rows)
        current_end = int(data[0][0]) - 1
        print(f"  {symbol}: {saved} candles | oldest={ts_to_date(int(data[0][0])//1000)}")
        time.sleep(0.1)

        if current_end <= start_ts:
            break

    conn.close()
    print(f"[INGEST] Crypto {symbol}: {saved} candles\n")


def fetch_weather_history(days_back=1095):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    conn       = get_conn()
    saved      = 0

    print(f"[INGEST] Weather history {start_date} → {end_date}...")

    for city, coords in WEATHER_CITIES.items():
        data = safe_get(f"{METEO_BASE}/v1/archive", params={
            "latitude": coords["lat"], "longitude": coords["lon"],
            "start_date": str(start_date), "end_date": str(end_date),
            "daily": "temperature_2m_max,temperature_2m_min", "timezone": "UTC"
        })
        if not data or "daily" not in data:
            print(f"  [SKIP] {city}")
            continue

        dates = data["daily"]["time"]
        highs = data["daily"]["temperature_2m_max"]
        lows  = data["daily"]["temperature_2m_min"]

        rows = [(city, d, h, l, h, l)
                for d, h, l in zip(dates, highs, lows) if h is not None and l is not None]

        conn.executemany('''
            INSERT OR IGNORE INTO weather_data
            (city, date, forecast_high, forecast_low, actual_high, actual_low)
            VALUES (?,?,?,?,?,?)
        ''', rows)
        conn.commit()
        saved += len(rows)
        print(f"  {city}: {len(rows)} days")
        time.sleep(0.5)

    conn.close()
    print(f"[INGEST] Weather: {saved} city-days\n")


def fetch_sports_odds(days_back=90):
    if not ODDS_KEY:
        print("[INGEST] Sports: No ODDS_API_KEY — skipping. Get free key at the-odds-api.com\n")
        return

    sports = ["basketball_nba", "americanfootball_nfl", "soccer_epl", "baseball_mlb"]
    conn   = get_conn()
    saved  = 0

    for sport in sports:
        data = safe_get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/odds-history/",
            params={"apiKey": ODDS_KEY, "regions": "us", "markets": "h2h", "daysFrom": min(days_back, 90)}
        )
        if not data:
            continue

        for game in data:
            home    = game.get("home_team", "")
            away    = game.get("away_team", "")
            gdate   = game.get("commence_time", "")[:10]
            game_id = f"{sport}_{home}_{away}_{gdate}".replace(" ", "_")

            home_prob = away_prob = None
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    if market["key"] == "h2h":
                        outcomes = {o["name"]: o["price"] for o in market["outcomes"]}
                        if home in outcomes and away in outcomes:
                            h_imp = 1.0 / outcomes[home]
                            a_imp = 1.0 / outcomes[away]
                            total = h_imp + a_imp
                            home_prob = round(h_imp / total, 4)
                            away_prob = round(a_imp / total, 4)
                        break
                if home_prob:
                    break

            if not home_prob:
                continue

            try:
                conn.execute('''
                    INSERT OR IGNORE INTO sports_lines
                    (game_id, sport, home_team, away_team, game_date, vegas_home_prob, vegas_away_prob)
                    VALUES (?,?,?,?,?,?,?)
                ''', (game_id, sport, home, away, gdate, home_prob, away_prob))
                saved += 1
            except Exception:
                continue

        conn.commit()
        time.sleep(1.0)

    conn.close()
    print(f"[INGEST] Sports: {saved} games\n")


def run_full_ingest(days_back=1095):
    init_db()
    print(f"\n{'='*60}")
    print(f"  POLYEDGE DATA INGESTION — {days_back} days back")
    print(f"{'='*60}\n")

    fetch_polymarket_markets(days_back=days_back)
    fetch_price_histories(limit=2000)
    fetch_crypto_ohlcv("BTC", days_back=days_back)
    fetch_crypto_ohlcv("ETH", days_back=days_back)
    fetch_weather_history(days_back=days_back)
    fetch_sports_odds(days_back=min(days_back, 90))

    conn = get_conn()
    c    = conn.cursor()
    print(f"\n{'='*60}")
    print(f"  INGEST COMPLETE")
    print(f"{'='*60}")
    for table in ["markets", "price_snapshots", "crypto_prices", "weather_data", "sports_lines"]:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<20} {c.fetchone()[0]:>10,} rows")
    conn.close()


if __name__ == '__main__':
    run_full_ingest(days_back=1095)
