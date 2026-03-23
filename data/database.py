import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'polymarket.db')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY,
        question TEXT,
        city TEXT,
        target_low REAL,
        target_high REAL,
        market_type TEXT,
        unit TEXT,
        resolved_at INTEGER,
        created_at INTEGER,
        outcome TEXT,
        last_trade_price REAL DEFAULT 0,
        volume REAL DEFAULT 0
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS price_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_id TEXT,
        timestamp INTEGER,
        yes_price REAL,
        UNIQUE(market_id, timestamp)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS wu_temps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        station TEXT,
        date TEXT,
        max_temp_f REAL,
        UNIQUE(city, date)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS noaa_forecasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        forecast_date TEXT,
        fetched_at INTEGER,
        high_temp_f REAL,
        UNIQUE(city, forecast_date, fetched_at)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS backtest_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sim_date TEXT,
        market_id TEXT,
        question TEXT,
        city TEXT,
        entry_price REAL,
        noaa_forecast_f REAL,
        wu_actual_f REAL,
        predicted_range TEXT,
        size REAL,
        capital_at_entry REAL,
        outcome TEXT,
        pnl REAL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date TEXT,
        market_id TEXT,
        question TEXT,
        city TEXT,
        entry_price REAL,
        noaa_forecast_f REAL,
        predicted_range TEXT,
        size REAL,
        capital_at_entry REAL,
        outcome TEXT,
        pnl REAL
    )""")

    conn.commit()
    conn.close()
    print("[DB] Ready")

if __name__ == '__main__':
    init_db()
