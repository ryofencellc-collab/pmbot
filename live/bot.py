"""
bot.py — Live 10-day trading bot.
Dry-run mode when no API keys set.
"""

import os
import json
import time
import hmac
import hashlib
import requests
from datetime import datetime, timezone
from data.database import get_conn
from strategy.factors import scan_all_models
from backtest.simulator import kelly_size, DEFAULT_CONFIG

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

API_KEY    = os.getenv("POLY_API_KEY", "")
SECRET     = os.getenv("POLY_SECRET", "")
PASSPHRASE = os.getenv("POLY_PASSPHRASE", "")
DRY_RUN    = not bool(API_KEY)


def _sign(method, path, body=""):
    ts  = str(int(time.time() * 1000))
    msg = ts + method.upper() + path + body
    sig = hmac.new(SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {
        "POLY-API-KEY":    API_KEY,
        "POLY-SIGNATURE":  sig,
        "POLY-TIMESTAMP":  ts,
        "POLY-PASSPHRASE": PASSPHRASE,
        "Content-Type":    "application/json",
    }


def get_balance():
    if DRY_RUN:
        conn = get_conn()
        c    = conn.cursor()
        c.execute('SELECT capital_at_entry, size FROM live_trades ORDER BY id DESC LIMIT 1')
        row  = c.fetchone()
        conn.close()
        if row:
            return max(row[0] - row[1], DEFAULT_CONFIG["principal"])
        return DEFAULT_CONFIG["starting_capital"]
    try:
        r = requests.get(f"{CLOB_BASE}/balance", headers=_sign("GET", "/balance"), timeout=10)
        return float(r.json().get("balance", 0))
    except Exception:
        return 0.0


def get_clob_token(market_id):
    try:
        r      = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=10)
        data   = r.json()
        tokens = data.get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        return tokens[0] if tokens else None
    except Exception:
        return None


def place_order(token_id, price, size):
    if DRY_RUN:
        return f"DRY_RUN_{int(time.time())}"
    body = json.dumps({
        "tokenID": token_id, "price": str(round(price, 4)),
        "side": "BUY", "size": str(round(size, 2)),
        "orderType": "LIMIT", "timeInForce": "GTC",
    })
    try:
        r    = requests.post(f"{CLOB_BASE}/order", headers=_sign("POST", "/order", body), data=body, timeout=10)
        data = r.json()
        return data.get("orderID") or data.get("transactionHash")
    except Exception as e:
        print(f"  [ORDER ERR] {e}")
        return None


def run_live_day(model=None, config=None):
    if config is None:
        config = DEFAULT_CONFIG.copy()

    if model is None:
        conn = get_conn()
        c    = conn.cursor()
        c.execute('SELECT model FROM backtest_runs ORDER BY roi DESC LIMIT 1')
        row  = c.fetchone()
        conn.close()
        model = row[0] if row else "M2_weather"
        print(f"  [BOT] Using winning model: {model}")

    now     = datetime.now(timezone.utc)
    day_str = now.strftime('%Y-%m-%d')
    eval_ts = int(now.timestamp())

    print(f"\n[LIVE] {day_str} | Model: {model} | {'DRY RUN' if DRY_RUN else 'LIVE'}")

    balance = get_balance()
    config["starting_capital"] = balance
    print(f"  Balance: ${balance:.2f}")

    signals_by_model = scan_all_models(eval_ts, models=[model])
    signals          = [s for s in signals_by_model.get(model, [])
                        if s["signal_score"] >= config["min_signal_score"]]
    top              = signals[:config["max_bets_per_day"]]

    print(f"  Signals: {len(signals)} found, top {len(top)} selected")

    conn = get_conn()
    for sig in top:
        size = kelly_size(balance, sig, config)
        if size <= 0:
            continue

        print(f"\n  → {sig['question'][:65]}")
        print(f"    YES @ {sig['yes_price']:.3f} | edge={sig['edge']:.3f} | size=${size:.2f}")

        token_id = get_clob_token(sig["market_id"])
        tx       = place_order(token_id, sig["yes_price"], size) if token_id else None

        conn.execute('''
            INSERT INTO live_trades
            (trade_date, model, market_id, question, entry_price, size,
             capital_at_entry, signal_score, factor_data, outcome, pnl, tx_hash)
            VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,?)
        ''', (day_str, model, sig["market_id"], sig["question"],
              sig["yes_price"], size, round(balance, 2),
              sig["signal_score"], sig.get("factor_data", "{}"), tx))
        balance -= size

    conn.commit()
    conn.close()
    print(f"\n[LIVE] Done. Balance: ${balance:.2f}")


def resolve_live():
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT lt.id, lt.entry_price, lt.size, m.outcome
        FROM live_trades lt
        JOIN markets m ON m.id = lt.market_id
        WHERE lt.outcome IS NULL AND m.outcome IS NOT NULL
    ''')
    rows     = c.fetchall()
    resolved = 0
    for row in rows:
        tid, entry_price, size, outcome = row
        pnl = size * (1.0 / entry_price - 1.0) if outcome == "Yes" else -size
        c.execute('UPDATE live_trades SET outcome=?, pnl=? WHERE id=?',
                  (outcome, round(pnl, 4), tid))
        resolved += 1
    conn.commit()
    conn.close()
    print(f"[LIVE] Resolved {resolved} trades")


if __name__ == '__main__':
    run_live_day()
