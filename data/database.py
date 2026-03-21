import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY, question TEXT, category TEXT, market_type TEXT,
        created_at BIGINT, resolved_at BIGINT, end_date_iso TEXT, outcome TEXT,
        volume REAL DEFAULT 0, liquidity REAL DEFAULT 0)''')

    c.execute('''CREATE TABLE IF NOT EXISTS price_snapshots (
        id SERIAL PRIMARY KEY, market_id TEXT, timestamp BIGINT,
        yes_price REAL, no_price REAL, volume_at_time REAL DEFAULT 0,
        UNIQUE(market_id, timestamp))''')

    c.execute('''CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY, city TEXT, date TEXT,
        forecast_high REAL, forecast_low REAL, actual_high REAL, actual_low REAL,
        forecast_source TEXT DEFAULT 'open-meteo', UNIQUE(city, date))''')

    c.execute('''CREATE TABLE IF NOT EXISTS backtest_trades (
        id SERIAL PRIMARY KEY, run_id TEXT, model TEXT, sim_date TEXT,
        market_id TEXT, question TEXT, entry_price REAL, size REAL,
        capital_at_entry REAL, signal_score REAL, factor_data TEXT,
        outcome TEXT, pnl REAL, resolved_date TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS live_trades (
        id SERIAL PRIMARY KEY, trade_date TEXT, model TEXT, market_id TEXT,
        question TEXT, entry_price REAL, size REAL, capital_at_entry REAL,
        signal_score REAL, factor_data TEXT, outcome TEXT, pnl REAL, tx​​​​​​​​​​​​​​​​
