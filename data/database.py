import sqlite3
import os

DB_PATH = "/data/polymarket.db"

def get_conn():
    os.makedirs("/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    os.makedirs("/data", exist_ok=True)
    conn = get_conn()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY, question TEXT, category TEXT, market_type TEXT, created_at INTEGER, resolved_at INTEGER, end_date_iso TEXT, outcome TEXT, volume REAL DEFAULT 0, liquidity REAL DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS price_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, timestamp INTEGER, yes_price REAL, no_price REAL, volume_at_time REAL DEFAULT 0, UNIQUE(market_id, timestamp))")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps ON price_snapshots(market_id, timestamp)")
    c.execute("CREATE TABLE IF NOT EXISTS weather_data (id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, date TEXT, forecast_high REAL, forecast_low REAL, actual_high REAL, actual_low REAL, UNIQUE(city, date))")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, model TEXT, sim_date TEXT, market_id TEXT, question TEXT, entry_price REAL, size REAL, capital_at_entry REAL, signal_score REAL, factor_data TEXT, outcome TEXT, pnl REAL, resolved_date TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS live_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT, model TEXT, market_id TEXT, question TEXT, entry_price REAL, size REAL, capital_at_entry REAL, signal_score REAL, factor_data TEXT, outcome TEXT, pnl REAL, tx_hash TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS backtest_runs (run_id TEXT PRIMARY KEY, model TEXT, started_at TEXT, completed_at TEXT, config TEXT, win_rate REAL, total_bets INTEGER, total_pnl REAL, final_capital REAL, roi REAL, sharpe REAL, max_drawdown REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS crypto_prices (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timestamp INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL, UNIQUE(symbol, timestamp))")
    c.execute("CREATE TABLE IF NOT EXISTS sports_lines (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id TEXT UNIQUE, sport TEXT, home_team TEXT, away_team TEXT, game_date TEXT, vegas_home_prob REAL, vegas_away_prob REAL, outcome_winner TEXT)")
    conn.commit()
    conn.close()
    print("[DB] SQLite ready")

if __name__ == '__main__':
    init_db()
