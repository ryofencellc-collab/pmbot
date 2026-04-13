import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=5)
    conn.autocommit = False
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()

    # Core tables
    c.execute("CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY)")
    c.execute("CREATE TABLE IF NOT EXISTS price_snapshots (id SERIAL PRIMARY KEY, market_id TEXT, timestamp BIGINT, yes_price REAL, UNIQUE(market_id, timestamp))")
    c.execute("CREATE TABLE IF NOT EXISTS wu_temps (id SERIAL PRIMARY KEY, city TEXT, station TEXT, date TEXT, max_temp_f REAL, UNIQUE(city, date))")
    c.execute("CREATE TABLE IF NOT EXISTS session_logs (id SERIAL PRIMARY KEY, session_type TEXT, logged_at TEXT, content TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_trades (id SERIAL PRIMARY KEY, sim_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, wu_actual_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS noaa_forecasts (id SERIAL PRIMARY KEY, city TEXT, date TEXT, forecast_f REAL, actual_f REAL, delta_f REAL, recorded_at TEXT, UNIQUE(city, date))")

    # Forecast log
    c.execute("""CREATE TABLE IF NOT EXISTS forecast_log (
        id SERIAL PRIMARY KEY,
        logged_at_utc TEXT,
        logged_at_est TEXT,
        city TEXT,
        target_date TEXT,
        days_until_resolution INT,
        gfs_temp REAL,
        ukmo_temp REAL,
        mf_temp REAL,
        consensus_temp REAL,
        spread REAL,
        unit TEXT,
        wu_actual REAL,
        UNIQUE(city, target_date, logged_at_utc)
    )""")

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_forecast_log_city_date
        ON forecast_log(city, target_date)
    """)

    # Scan log — full audit trail of every decision
    c.execute("""CREATE TABLE IF NOT EXISTS scan_log (
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
    )""")

    # Paper trades — real prices, real markets, paper money
    c.execute("""CREATE TABLE IF NOT EXISTS paper_trades (
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
    )""")

    # Daily summary
    c.execute("""CREATE TABLE IF NOT EXISTS daily_summary (
        id           SERIAL PRIMARY KEY,
        summary_date TEXT UNIQUE,
        total_bets   INT,
        wins         INT,
        losses       INT,
        win_rate     REAL,
        total_pnl    REAL,
        cities_bet   TEXT,
        created_at   TEXT
    )""")

    # Cache table
    c.execute("""CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
    )""")

    # Migrations for markets table
    migrations = [
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS question TEXT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS city TEXT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS target_low REAL",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS target_high REAL",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS market_type TEXT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS unit TEXT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolved_at BIGINT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS created_at BIGINT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS outcome TEXT",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS last_trade_price REAL DEFAULT 0",
        "ALTER TABLE markets ADD COLUMN IF NOT EXISTS volume REAL DEFAULT 0",
    ]
    for sql in migrations:
        c.execute(sql)

    conn.commit()
    conn.close()
    print("[DB] PostgreSQL ready — all tables initialized")

if __name__ == '__main__':
    init_db()
