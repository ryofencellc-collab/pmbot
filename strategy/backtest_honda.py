"""
backtest_honda.py - Backtest HondaCivic's FULL strategy using real data.

THREE strategies backtested:

STRATEGY 1 — ARBITRAGE (near-certain NO bets)
  Buy NO at 99-100¢ on ranges clearly outside the forecast
  Win rate: ~100%, profit: 0.1-1¢ per share × thousands of shares
  Entry condition: yes_price >= 95¢ AND forecast clearly outside range

STRATEGY 2 — SPECULATION (early YES entry)
  Buy YES early (0.05-5¢) when market opens 2-4 days before resolution
  Sell or hold to resolution when price jumps to 80-99¢
  Entry condition: yes_price <= 5¢ AND forecast matches range

STRATEGY 3 — MARKET MAKING (buy low sell high)
  Buy YES at 3-20¢, sell YES at 80-99¢ on same market
  Collect the spread
  Entry condition: yes_price <= 20¢, exit when price >= 80¢

All using REAL price histories + REAL outcomes. No predictions.
"""

import json
import time
import requests
from datetime import datetime, timezone, timedelta
from data.database import get_conn, init_db

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG = {
    "starting_capital":     10000.0,
    "bet_size":             10.0,       # $ per trade
    "arb_min_yes_price":    0.95,       # buy NO when yes >= 95¢
    "arb_profit_per_share": 0.001,      # min profit per share (0.1¢)
    "spec_max_entry":       0.05,       # buy YES when price <= 5¢
    "spec_min_exit":        0.80,       # sell YES when price >= 80¢
    "mm_max_entry":         0.20,       # buy YES when price <= 20¢
    "mm_min_exit":          0.80,       # sell YES when price >= 80¢
}

GAMMA  = "https://gamma-api.polymarket.com"
CLOB   = "https://clob.polymarket.com"


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30)
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(1.5 * (i + 1))
    return None


def get_price_history(clob_token):
    """Get full price history for a market token."""
    data = safe_get(f"{CLOB}/prices-history", params={
        "market": clob_token, "interval": "all", "fidelity": 60
    })
    if data and "history" in data:
        return [(p["t"], float(p["p"])) for p in data["history"] if p.get("t") and p.get("p")]
    return []


def get_price_at(history, timestamp):
    """Get price closest to timestamp."""
    if not history:
        return None
    best = None
    for t, p in history:
        if t <= timestamp:
            best = p
        else:
            break
    return best


def get_min_price(history, start_ts, end_ts):
    """Get minimum price in a time window."""
    prices = [p for t, p in history if start_ts <= t <= end_ts]
    return min(prices) if prices else None


def get_max_price(history, start_ts, end_ts):
    """Get maximum price in a time window."""
    prices = [p for t, p in history if start_ts <= t <= end_ts]
    return max(prices) if prices else None


def get_first_price_above(history, threshold, after_ts):
    """Get first timestamp+price where price crossed threshold after a time."""
    for t, p in history:
        if t >= after_ts and p >= threshold:
            return t, p
    return None, None


# ── Strategy 1: Arbitrage (Buy NO at 99-100¢) ────────────────────────────────

def backtest_arbitrage(markets, capital):
    """
    Buy NO on ranges priced at 95-100¢ that lose.
    Real entry: buy NO at yes_price close to 100¢
    Real profit: (1.0 - entry_no_price) per share
    """
    print("\n" + "="*55)
    print("STRATEGY 1: ARBITRAGE (Buy NO at 95-100¢)")
    print("="*55)

    trades  = []
    wins    = 0
    losses  = 0
    total_pnl = 0.0

    for m in markets:
        if m.get("outcome") is None:
            continue

        market_id   = m["id"]
        outcome     = m["outcome"]
        history     = m.get("price_history", [])
        resolved_at = m.get("resolved_at", 0)

        if not history or len(history) < 2:
            continue

        # Filter to PRE-resolution snapshots only
        # The last snapshot is post-resolution (1.0 or 0.0)
        # We want prices from the live trading period
        if resolved_at:
            pre_res = [(t, p) for t, p in history if t < resolved_at]
        else:
            # No resolved_at — exclude last 2 snapshots (post-resolution)
            pre_res = history[:-2]

        if not pre_res:
            continue

        # HondaCivic buys NO on ranges priced >= 95¢ during trading
        # Peak price = when market was most confident (close to resolution)
        # This is the price he would have bought NO at
        peak_yes_price = max(p for t, p in pre_res)

        if peak_yes_price < CONFIG["arb_min_yes_price"]:
            continue

        # NO price at that moment
        no_price = round(1.0 - peak_yes_price, 4)
        if no_price <= 0:
            no_price = 0.001

        bet_size = CONFIG["bet_size"]
        shares   = bet_size / no_price

        # WIN: outcome is "No" — range lost as expected
        # LOSE: outcome is "Yes" — range actually won, we lose our NO bet
        if outcome == "No":
            payout = shares * 1.0
            pnl    = payout - bet_size
            wins  += 1
        else:
            pnl    = -bet_size
            losses += 1

        total_pnl += pnl
        capital   += pnl

        trades.append({
            "market_id":       market_id,
            "question":        m.get("question", "")[:70],
            "city":            m.get("city", ""),
            "peak_yes_price":  peak_yes_price,
            "no_price":        no_price,
            "bet_size":        bet_size,
            "shares":          round(shares, 1),
            "outcome":         outcome,
            "won":             outcome == "No",
            "pnl":             round(pnl, 4),
            "strategy":        "ARB_NO",
        })

    total = wins + losses
    win_rate = wins / total * 100 if total > 0 else 0
    roi = total_pnl / CONFIG["starting_capital"] * 100

    print(f"  Total trades:  {total}")
    print(f"  Wins:          {wins}")
    print(f"  Losses:        {losses}")
    print(f"  Win rate:      {win_rate:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  ROI:           {roi:.1f}%")
    print(f"  Final capital: ${capital:.2f}")

    if losses > 0:
        print(f"\n  ⚠️  LOSSES — markets where YES was priced high but WON:")
        for t in trades:
            if not t["won"]:
                print(f"    {t['city']}: {t['question'][:60]} | yes={t['yes_price']} | pnl=${t['pnl']:.2f}")

    return trades, capital, {"wins": wins, "losses": losses,
                             "win_rate": win_rate, "pnl": total_pnl, "roi": roi}


# ── Strategy 2: Speculation (Early YES entry) ─────────────────────────────────

def backtest_speculation(markets, capital):
    """
    Buy YES early when price is 0.1-5¢ (market just opened).
    Hold to resolution.
    Win condition: outcome == "Yes"
    """
    print("\n" + "="*55)
    print("STRATEGY 2: SPECULATION (Buy YES at 0.1-5¢ early)")
    print("="*55)

    trades  = []
    wins    = 0
    losses  = 0
    total_pnl = 0.0

    for m in markets:
        if m.get("outcome") is None:
            continue

        market_id  = m["id"]
        outcome    = m["outcome"]
        resolved_at = m.get("resolved_at", 0)
        created_at  = m.get("created_at", 0)

        if not resolved_at or not created_at:
            continue

        # Get price history
        tokens = m.get("clob_token_ids")
        if not tokens:
            continue
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                continue
        if not tokens:
            continue

        history = m.get("price_history", [])
        if not history:
            continue

        # Entry: minimum price in PRE-resolution snapshots
        if resolved_at:
            pre_res = [(t, p) for t, p in history if t < resolved_at]
        else:
            pre_res = history[:-2] if len(history) > 2 else history

        if not pre_res:
            continue

        entry_price = min(p for t, p in pre_res) if pre_res else None

        if not entry_price or entry_price > CONFIG["spec_max_entry"]:
            continue

        # We found a cheap entry — simulate buying
        bet_size = CONFIG["bet_size"]
        shares   = bet_size / entry_price

        if outcome == "Yes":
            payout = shares * 1.0
            pnl    = payout - bet_size
            wins  += 1
        else:
            pnl    = -bet_size
            losses += 1

        total_pnl += pnl
        capital   += pnl

        trades.append({
            "market_id":   market_id,
            "question":    m.get("question", "")[:70],
            "city":        m.get("city", ""),
            "entry_price": entry_price,
            "bet_size":    bet_size,
            "shares":      round(shares, 1),
            "outcome":     outcome,
            "won":         outcome == "Yes",
            "pnl":         round(pnl, 4),
            "multiplier":  round(1.0 / entry_price, 1),
            "strategy":    "SPEC_YES",
        })

    total = wins + losses
    win_rate = wins / total * 100 if total > 0 else 0
    roi = total_pnl / CONFIG["starting_capital"] * 100

    print(f"  Total trades:  {total}")
    print(f"  Wins:          {wins}")
    print(f"  Losses:        {losses}")
    print(f"  Win rate:      {win_rate:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  ROI:           {roi:.1f}%")
    print(f"  Final capital: ${capital:.2f}")

    if wins > 0:
        best = max([t for t in trades if t["won"]], key=lambda x: x["pnl"])
        print(f"\n  Best win: {best['city']} {best['question'][:50]}")
        print(f"    Entry: {best['entry_price']}¢ | Multiplier: {best['multiplier']}x | P&L: ${best['pnl']:.2f}")

    return trades, capital, {"wins": wins, "losses": losses,
                             "win_rate": win_rate, "pnl": total_pnl, "roi": roi}


# ── Strategy 3: Market Making (Buy low, sell high) ────────────────────────────

def backtest_market_making(markets, capital):
    """
    Buy YES at 3-20¢, sell YES when price reaches 80¢+.
    Uses real price history to find entry and exit points.
    """
    print("\n" + "="*55)
    print("STRATEGY 3: MARKET MAKING (Buy 3-20¢, Sell 80¢+)")
    print("="*55)

    trades    = []
    wins      = 0
    losses    = 0
    no_exit   = 0
    total_pnl = 0.0

    for m in markets:
        if m.get("outcome") is None:
            continue

        market_id   = m["id"]
        outcome     = m["outcome"]
        resolved_at = m.get("resolved_at", 0)
        created_at  = m.get("created_at", 0)

        if not resolved_at or not created_at:
            continue

        tokens = m.get("clob_token_ids")
        if not tokens:
            continue
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                continue
        if not tokens:
            continue

        history = m.get("price_history", [])
        if len(history) < 5:
            continue

        # Use pre-resolution prices only
        if resolved_at:
            pre_res = [(t, p) for t, p in history if t < resolved_at]
        else:
            pre_res = history[:-2] if len(history) > 2 else history

        if not pre_res:
            continue

        # Entry: minimum price in pre-resolution window
        entry_price = min(p for t, p in pre_res)
        if not entry_price or entry_price <= 0 or entry_price > CONFIG["mm_max_entry"]:
            continue

        entry_ts = next(t for t, p in pre_res if p == entry_price)

        # Exit: first price >= 80¢ after entry
        after_entry = [(t, p) for t, p in pre_res if t >= entry_ts]
        exit_price  = None
        exit_ts     = None
        for t, p in after_entry:
            if p >= CONFIG["mm_min_exit"]:
                exit_price = p
                exit_ts    = t
                break

        bet_size = CONFIG["bet_size"]
        shares   = bet_size / entry_price

        if exit_price and exit_ts and exit_ts < resolved_at:
            # Successfully sold at 80¢+
            payout = shares * exit_price
            pnl    = payout - bet_size
            wins  += 1
        elif outcome == "Yes":
            # Held to resolution — won at $1
            payout = shares * 1.0
            pnl    = payout - bet_size
            wins  += 1
            no_exit += 1
        else:
            # Never reached 80¢, outcome was No — lost
            pnl    = -bet_size
            losses += 1

        total_pnl += pnl
        capital   += pnl

        trades.append({
            "market_id":   market_id,
            "question":    m.get("question", "")[:70],
            "city":        m.get("city", ""),
            "entry_price": entry_price,
            "exit_price":  exit_price or (1.0 if outcome == "Yes" else 0.0),
            "bet_size":    bet_size,
            "shares":      round(shares, 1),
            "outcome":     outcome,
            "won":         pnl > 0,
            "pnl":         round(pnl, 4),
            "multiplier":  round((exit_price or (1.0 if outcome == "Yes" else 0)) / entry_price, 1),
            "strategy":    "MM_YES",
        })

        time.sleep(0.2)

    total = wins + losses
    win_rate = wins / total * 100 if total > 0 else 0
    roi = total_pnl / CONFIG["starting_capital"] * 100

    print(f"  Total trades:  {total}")
    print(f"  Wins:          {wins} ({no_exit} held to resolution)")
    print(f"  Losses:        {losses}")
    print(f"  Win rate:      {win_rate:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  ROI:           {roi:.1f}%")
    print(f"  Final capital: ${capital:.2f}")

    return trades, capital, {"wins": wins, "losses": losses,
                             "win_rate": win_rate, "pnl": total_pnl, "roi": roi}


# ── Main ──────────────────────────────────────────────────────────────────────

def run_honda_backtest():
    init_db()
    conn = get_conn()
    c    = conn.cursor()

    # Get ALL resolved markets
    c.execute("""
        SELECT id, question, city, target_low, target_high,
               market_type, unit, resolved_at, created_at,
               outcome, last_trade_price, volume
        FROM markets
        WHERE outcome IS NOT NULL
        ORDER BY resolved_at DESC
    """)
    markets = [dict(r) for r in c.fetchall()]
    print(f"  Found {len(markets)} resolved markets")

    # Load ALL price snapshots from DB
    c.execute("""
        SELECT market_id, timestamp, yes_price
        FROM price_snapshots
        ORDER BY market_id, timestamp ASC
    """)
    price_data = {}
    for row in c.fetchall():
        mid = row["market_id"]
        if mid not in price_data:
            price_data[mid] = []
        price_data[mid].append((row["timestamp"], row["yes_price"]))
    print(f"  Found price history for {len(price_data)} markets")

    conn.close()

    # Attach price history to each market
    for m in markets:
        m["price_history"] = price_data.get(m["id"], [])

    print(f"\n{'='*55}")
    print(f"  HONDA CIVIC FULL STRATEGY BACKTEST")
    print(f"  {len(markets)} resolved markets")
    print(f"  Starting capital: ${CONFIG['starting_capital']:,.2f}")
    print(f"{'='*55}")

    capital = CONFIG["starting_capital"]

    # Run all 3 strategies
    arb_trades,  capital, arb_stats  = backtest_arbitrage(markets, capital)
    spec_trades, capital, spec_stats = backtest_speculation(markets, capital)
    mm_trades,   capital, mm_stats   = backtest_market_making(markets, capital)

    all_trades = arb_trades + spec_trades + mm_trades
    total_pnl  = arb_stats["pnl"] + spec_stats["pnl"] + mm_stats["pnl"]
    total_wins = arb_stats["wins"] + spec_stats["wins"] + mm_stats["wins"]
    total_bets = len(all_trades)
    win_rate   = total_wins / total_bets * 100 if total_bets > 0 else 0

    print(f"\n{'='*55}")
    print(f"  COMBINED RESULTS — ALL 3 STRATEGIES")
    print(f"{'='*55}")
    print(f"  Strategy 1 (Arbitrage):      ${arb_stats['pnl']:>10.2f}  WR={arb_stats['win_rate']:.1f}%  ({arb_stats['wins']+arb_stats['losses']} trades)")
    print(f"  Strategy 2 (Speculation):    ${spec_stats['pnl']:>10.2f}  WR={spec_stats['win_rate']:.1f}%  ({spec_stats['wins']+spec_stats['losses']} trades)")
    print(f"  Strategy 3 (Market Making):  ${mm_stats['pnl']:>10.2f}  WR={mm_stats['win_rate']:.1f}%  ({mm_stats['wins']+mm_stats['losses']} trades)")
    print(f"{'='*55}")
    print(f"  Total bets:    {total_bets}")
    print(f"  Total wins:    {total_wins}")
    print(f"  Win rate:      {win_rate:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  Final capital: ${capital:.2f}")
    print(f"  ROI:           {total_pnl/CONFIG['starting_capital']*100:.1f}%")
    print(f"{'='*55}")

    return {
        "total_bets":    total_bets,
        "total_wins":    total_wins,
        "win_rate":      round(win_rate, 1),
        "total_pnl":     round(total_pnl, 2),
        "final_capital": round(capital, 2),
        "roi":           round(total_pnl / CONFIG["starting_capital"] * 100, 1),
        "strategies": {
            "arbitrage":     arb_stats,
            "speculation":   spec_stats,
            "market_making": mm_stats,
        },
        "trades": all_trades,
    }


if __name__ == '__main__':
    run_honda_backtest()
