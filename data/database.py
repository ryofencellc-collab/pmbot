import sqlite3
import os

DB_PATH = os.environ.get(
    'DB_PATH',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'polymarket.db')
)

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY, question TEXT, category TEXT, market_type TEXT, created_at INTEGER, resolved_at INTEGER, end_date_iso TEXT, outcome TEXT, volume REAL DEFAULT 0, liquidity REAL DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS price_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, timestamp INTEGER, yes_price REAL, UNIQUE(market_id, timestamp))")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps ON price_snapshots(market_id, timestamp)")
    c.execute("CREATE TABLE IF NOT EXISTS weather_data (id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, date TEXT, forecast_high REAL, forecast_low REAL, actual_high REAL, actual_low REAL, UNIQUE(city, date))")
    c.execute("CREATE TABLE IF NOT EXISTS wu_temps (id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, station TEXT, date TEXT, max_temp_f REAL, UNIQUE(city, date))")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, model TEXT, sim_date TEXT, market_id TEXT, question TEXT, entry_price REAL, size REAL, capital_at_entry REAL, signal_score REAL, factor_data TEXT, outcome TEXT, pnl REAL, resolved_date TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS paper_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT, market_id TEXT, question TEXT, city TEXT, entry_price REAL, noaa_forecast_f REAL, predicted_range TEXT, size REAL, capital_at_entry REAL, outcome TEXT, pnl REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS session_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, session_type TEXT, logged_at TEXT, content TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS live_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT, model TEXT, market_id TEXT, question TEXT, entry_price REAL, size REAL, capital_at_entry REAL, signal_score REAL, factor_data TEXT, outcome TEXT, pnl REAL, tx_hash TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_runs (run_id TEXT PRIMARY KEY, model TEXT, started_at TEXT, completed_at TEXT, config TEXT, win_rate REAL, total_bets INTEGER, total_pnl REAL, final_capital REAL, roi REAL, sharpe REAL, max_drawdown REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS noaa_forecasts (id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, forecast_date TEXT, fetched_at INTEGER, high_temp_f REAL, UNIQUE(city, forecast_date, fetched_at))")
    conn.commit()
    conn.close()
    print("[DB] Ready")

if __name__ == '__main__':
    init_db()
