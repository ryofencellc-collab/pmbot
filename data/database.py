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
    c.execute("CREATE TABLE IF NOT EXISTS paper_trades (id SERIAL PRIMARY KEY, trade_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS session_logs (id SERIAL PRIMARY KEY, session_type TEXT, logged_at TEXT, content TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_trades (id SERIAL PRIMARY KEY, sim_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, wu_actual_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    # NOAA forecast tracking — records forecast vs actual for error model
    c.execute("""CREATE TABLE IF NOT EXISTS noaa_forecasts (
        id SERIAL PRIMARY KEY,
        city TEXT,
        date TEXT,
        forecast_f REAL,
        actual_f REAL,
        delta_f REAL,
        recorded_at TEXT,
        UNIQUE(city, date)
    )""")
    # Add missing columns to markets (safe migrations)
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
    print("[DB] PostgreSQL ready")

if __name__ == '__main__':
    init_db()
