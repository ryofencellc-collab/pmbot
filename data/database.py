import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY, question TEXT, city TEXT, target_low REAL, target_high REAL, market_type TEXT, unit TEXT, resolved_at BIGINT, created_at BIGINT, outcome TEXT, last_trade_price REAL DEFAULT 0, volume REAL DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS price_snapshots (id SERIAL PRIMARY KEY, market_id TEXT, timestamp BIGINT, yes_price REAL, UNIQUE(market_id, timestamp))")
    c.execute("CREATE TABLE IF NOT EXISTS wu_temps (id SERIAL PRIMARY KEY, city TEXT, station TEXT, date TEXT, max_temp_f REAL, UNIQUE(city, date))")
    c.execute("CREATE TABLE IF NOT EXISTS paper_trades (id SERIAL PRIMARY KEY, trade_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS session_logs (id SERIAL PRIMARY KEY, session_type TEXT, logged_at TEXT, content TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_trades (id SERIAL PRIMARY KEY, sim_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, wu_actual_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    conn.commit()
    conn.close()
    print("[DB] PostgreSQL ready")

if __name__ == '__main__':
    init_db()
