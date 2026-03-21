"""
database.py — SQLite schema for PolyEdge.
All tables needed for: market data, factor data, backtest, live trading.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'polymarket.db')


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            id           TEXT PRIMARY KEY,
            question     TEXT NOT NULL,
            category     TEXT,
            market_type  TEXT,
            created_at   INTEGER,
            resolved_at  INTEGER,
            end_date_iso TEXT,
            outcome      TEXT,
            volume       REAL DEFAULT 0,
            liquidity    REAL DEFAULT 0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id      TEXT NOT NULL,
            timestamp      INTEGER NOT NULL,
            yes_price      REAL NOT NULL,
            no_price       REAL NOT NULL,
            volume_at_time REAL DEFAULT 0
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ps_market_ts ON price_snapshots(market_id, timestamp)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol    TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open      REAL,
            high      REAL,
            low       REAL,
            close     REAL,
            volume    REAL
        )
    ''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_cp_sym_ts ON crypto_prices(symbol, timestamp)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            city           TEXT NOT NULL,
            date           TEXT NOT NULL,
            forecast_high  REAL,
            forecast_low   REAL,
            actual_high    REAL,
            actual_low     REAL,
            forecast_source TEXT DEFAULT "open-meteo"
        )
    ''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_wd_city_date ON weather_data(city, date)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS sports_lines (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         TEXT NOT NULL,
            sport           TEXT,
            home_team       TEXT,
            away_team       TEXT,
            game_date       TEXT,
            vegas_home_prob REAL,
            vegas_away_prob REAL,
            outcome_winner  TEXT,
            source          TEXT DEFAULT "the-odds-api"
        )
    ''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_sl_game ON sports_lines(game_id)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS backtest_trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           TEXT NOT NULL,
            model            TEXT NOT NULL,
            sim_date         TEXT NOT NULL,
            market_id        TEXT NOT NULL,
            question         TEXT,
            entry_price      REAL NOT NULL,
            size             REAL NOT NULL,
            capital_at_entry REAL NOT NULL,
            signal_score     REAL,
            factor_data      TEXT,
            outcome          TEXT,
            pnl              REAL,
            resolved_date    TEXT
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bt_run ON backtest_trades(run_id)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS live_trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date       TEXT NOT NULL,
            model            TEXT NOT NULL,
            market_id        TEXT NOT NULL,
            question         TEXT,
            entry_price      REAL NOT NULL,
            size             REAL NOT NULL,
            capital_at_entry REAL NOT NULL,
            signal_score     REAL,
            factor_data      TEXT,
            outcome          TEXT,
            pnl              REAL,
            tx_hash          TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS backtest_runs (
            run_id        TEXT PRIMARY KEY,
            model         TEXT NOT NULL,
            started_at    TEXT,
            completed_at  TEXT,
            config        TEXT,
            win_rate      REAL,
            total_bets    INTEGER,
            total_pnl     REAL,
            final_capital REAL,
            roi           REAL,
            sharpe        REAL,
            max_drawdown  REAL
        )
    ''')

    conn.commit()
    conn.close()
    print("[DB] Schema initialized — polymarket.db ready")


if __name__ == '__main__':
    init_db()
