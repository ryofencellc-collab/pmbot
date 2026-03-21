"""
database.py — PostgreSQL version for Railway.
Persistent storage — survives restarts and redeploys.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:OHTgjWrTbsDYtKyuLrTqECtwJPWzeYQa@postgres.railway.internal:5432/railway")


def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn


def init_db():
    conn = get_conn()
    c    = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            id           TEXT PRIMARY KEY,
            question     TEXT NOT NULL,
            category     TEXT,
            market_type  TEXT,
            created_at   BIGINT,
            resolved_at  BIGINT,
            end_date_iso TEXT,
            outcome      TEXT,
            volume       REAL DEFAULT 0,
            liquidity    REAL DEFAULT 0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id             SERIAL PRIMARY KEY,
            market_id      TEXT NOT NULL,
            timestamp      BIGINT NOT NULL,
            yes_price      REAL NOT NULL,
            no_price       REAL NOT NULL,
            volume_at_time REAL DEFAULT 0,
            UNIQUE(market_id, timestamp)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ps_market_ts ON price_snapshots(market_id, timestamp)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id             SERIAL PRIMARY KEY,
            city           TEXT NOT NULL,
            date           TEXT NOT NULL,
            forecast_high  REAL,
            forecast_low   REAL,
            actual_high    REAL,
            actual_low     REAL,
            forecast_source TEXT DEFAULT 'open-meteo',
            UNIQUE(city, date)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS backtest_trades (
            id               SERIAL PRIMARY KEY,
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

    c.execute('''
        CREATE TABLE IF NOT EXISTS live_trades (
            id               SERIAL PRIMARY KEY,
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

    c.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id        SERIAL PRIMARY KEY,
            symbol    TEXT NOT NULL,
            timestamp BIGINT NOT NULL,
            open      REAL,
            high      REAL,
            low       REAL,
            close     REAL,
            volume    REAL,
            UNIQUE(symbol, timestamp)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS sports_lines (
            id              SERIAL PRIMARY KEY,
            game_id         TEXT UNIQUE NOT NULL,
            sport           TEXT,
            home_team       TEXT,
            away_team       TEXT,
            game_date       TEXT,
            vegas_home_prob REAL,
            vegas_away_prob REAL,
            outcome_winner  TEXT,
            source          TEXT DEFAULT 'the-odds-api'
        )
    ''')

    conn.commit()
    conn.close()
    print("[DB] PostgreSQL schema initialized")


if __name__ == '__main__':
    init_db()
