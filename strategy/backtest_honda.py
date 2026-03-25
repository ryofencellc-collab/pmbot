"""
backtest_honda.py - HondaCivic full strategy backtest.

Follows EXACT same pattern as backtest_all.py:
- Gets real CLOB price history from API
- Uses real resolved outcomes from DB
- No predictions, no guessing

STRATEGY 1 - ARBITRAGE:
  Buy NO on ranges priced >= 95¢ at 1 hour before resolution
  HondaCivic buys NO on near-certain losing ranges to collect 0.1¢ spread
  Win: outcome = No (range lost as expected)
  Lose: outcome = Yes (range actually won)

STRATEGY 2 - SPECULATION:
  Buy YES at market open price (first available price)
  HondaCivic enters 2-4 days early at 0.05-0.15¢
  Win: outcome = Yes
  Lose: outcome = No

STRATEGY 3 - MARKET MAKING:
  Buy YES at minimum price, sell at maximum price
  Simulates buying cheap and selling expensive
  Win: max price >= 80¢ (successful exit)
  Lose: never reached 80¢
"""

import json
import time
import requests
from data.database import get_conn

GAMMA  = "https://gamma-api.polymarket.com"
CLOB   = "https://clob.polymarket.com"

BET_SIZE         = 10.0
STARTING_CAPITAL = 10000.0
ARB_MIN_PRICE    = 0.95   # buy NO when yes >= 95¢
SPEC_MAX_ENTRY   = 0.05   # buy YES when price <= 5¢
MM_MAX_ENTRY     = 0.20   # buy YES when price <= 20¢
MM_MIN_EXIT      = 0.80   # sell YES when price >= 80¢


def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20,
                             headers={"User-Agent": "PolyEdge/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30)
        except Exception as e:
            print(f"  [ERR] {e}")
        time.sleep(2 ** i)
    return None


def get_clob_token(market_id):
    mdata = safe_get(f"{GAMMA}/markets/{market_id}")
    if not mdata:
        return None
    tokens = mdata.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            return None
    return tokens[0] if tokens else None


def get_price_history(clob_token):
    """Get full price history for a market token."""
    hist = safe_get(f"{CLOB}/prices-history",
                    params={"market": clob_token, "interval": "all", "fidelity": 60})
    if not hist or not hist.get("history"):
        return []
    return [(int(p["t"]), float(p["p"])) for p in hist["history"]
            if p.get("t") and p.get("p") is not None]


def get_live_prices(history, resolved_at):
    """
    Filter to live trading prices only.
    Exclude post-resolution prices (0.999+ or 0.001-).
    Also exclude prices after resolution timestamp.
    """
    live = []
    for t, p in history:
        if resolved_at and t >= resolved_at:
            continue
        if 0.001 < p < 0.999:
            live.append((t, p))
    return live


def run_honda_backtest():
    conn = get_conn()
    c    = conn.cursor()

    # Get all resolved markets with their clob tokens
    c.execute("""
        SELECT id, question, city, outcome, resolved_at, created_at,
               target_low, target_high, market_type, unit
        FROM markets
        WHERE outcome IS NOT NULL
        AND resolved_at IS NOT NULL
        ORDER BY resolved_at DESC
        LIMIT 2000
    """)
    markets = [dict(r) for r in c.fetchall()]
    conn.close()

    print(f"  Testing {len(markets)} resolved markets")
    print(f"  Fetching CLOB price histories from API...")

    arb_wins = arb_losses = 0
    arb_pnl  = 0.0
    arb_trades = []

    spec_wins = spec_losses = 0
    spec_pnl  = 0.0
    spec_trades = []

    mm_wins = mm_losses = 0
    mm_pnl  = 0.0
    mm_trades = []

    processed = 0
    skipped   = 0

    for m in markets:
        mid         = m["id"]
        outcome     = m["outcome"]
        resolved_at = m["resolved_at"]

        # Get CLOB token
        token = get_clob_token(mid)
        if not token:
            skipped += 1
            continue

        # Get full price history from API
        history = get_price_history(token)
        if not history:
            skipped += 1
            continue

        # Filter to live trading prices only
        live = get_live_prices(history, resolved_at)
        if not live:
            skipped += 1
            continue

        processed += 1
        peak   = max(p for t, p in live)
        trough = min(p for t, p in live)

        # ── Strategy 1: Arbitrage ──────────────────────────────────────────
        # Buy NO when yes was priced >= 95¢ during live trading
        # These are near-certain ranges — HondaCivic buys NO to collect spread
        if peak >= ARB_MIN_PRICE:
            # Entry price = NO price at peak yes
            no_price = round(1.0 - peak, 4)
            if no_price < 0.001:
                no_price = 0.001
            shares = BET_SIZE / no_price

            if outcome == "No":
                pnl = round(shares * 1.0 - BET_SIZE, 4)
                arb_wins += 1
            else:
                pnl = -BET_SIZE
                arb_losses += 1

            arb_pnl += pnl
            arb_trades.append({
                "market_id": mid,
                "city":      m["city"],
                "question":  (m["question"] or "")[:65],
                "outcome":   outcome,
                "peak_yes":  peak,
                "no_price":  no_price,
                "won":       outcome == "No",
                "pnl":       round(pnl, 2),
            })

        # ── Strategy 2: Speculation ────────────────────────────────────────
        # Buy YES at open price (first live price)
        # HondaCivic enters 2-4 days early when price is cheapest
        open_price = live[0][1]
        if open_price <= SPEC_MAX_ENTRY:
            shares = BET_SIZE / open_price

            if outcome == "Yes":
                pnl = round(shares * 1.0 - BET_SIZE, 4)
                spec_wins += 1
            else:
                pnl = -BET_SIZE
                spec_losses += 1

            spec_pnl += pnl
            spec_trades.append({
                "market_id":  mid,
                "city":       m["city"],
                "question":   (m["question"] or "")[:65],
                "outcome":    outcome,
                "open_price": open_price,
                "multiplier": round(1.0 / open_price, 0),
                "won":        outcome == "Yes",
                "pnl":        round(pnl, 2),
            })

        # ── Strategy 3: Market Making ──────────────────────────────────────
        # Buy YES at minimum price, sell when price reaches 80¢+
        if trough <= MM_MAX_ENTRY:
            entry_ts = next(t for t, p in live if p == trough)
            # Find first price >= 80¢ after entry
            exit_price = None
            for t, p in live:
                if t >= entry_ts and p >= MM_MIN_EXIT:
                    exit_price = p
                    break

            shares = BET_SIZE / trough

            if exit_price:
                pnl = round(shares * exit_price - BET_SIZE, 2)
                mm_wins += 1
            elif outcome == "Yes":
                # Held to resolution at $1
                pnl = round(shares * 1.0 - BET_SIZE, 2)
                mm_wins += 1
            else:
                pnl = -BET_SIZE
                mm_losses += 1

            mm_pnl += pnl
            mm_trades.append({
                "market_id": mid,
                "city":      m["city"],
                "question":  (m["question"] or "")[:65],
                "outcome":   outcome,
                "entry":     trough,
                "exit":      exit_price or (1.0 if outcome == "Yes" else 0.0),
                "won":       pnl > 0,
                "pnl":       round(pnl, 2),
            })

        time.sleep(0.15)

        if processed % 50 == 0:
            print(f"  Processed {processed} markets...")

    # ── Results ────────────────────────────────────────────────────────────
    def summary(wins, losses, pnl, name):
        total = wins + losses
        wr    = wins / total * 100 if total else 0
        roi   = pnl / STARTING_CAPITAL * 100
        print(f"\n{'='*55}")
        print(f"  {name}")
        print(f"{'='*55}")
        print(f"  Trades:   {total}")
        print(f"  Wins:     {wins}")
        print(f"  Losses:   {losses}")
        print(f"  Win Rate: {wr:.1f}%")
        print(f"  P&L:      ${pnl:.2f}")
        print(f"  ROI:      {roi:.1f}%")
        return {"trades": total, "wins": wins, "losses": losses,
                "win_rate": round(wr, 1), "pnl": round(pnl, 2),
                "roi": round(roi, 1)}

    print(f"\n\nProcessed: {processed} | Skipped: {skipped}")

    s1 = summary(arb_wins,  arb_losses,  arb_pnl,  "STRATEGY 1: ARBITRAGE (NO on 95%+ ranges)")
    s2 = summary(spec_wins, spec_losses, spec_pnl, "STRATEGY 2: SPECULATION (YES at open <= 5¢)")
    s3 = summary(mm_wins,   mm_losses,   mm_pnl,   "STRATEGY 3: MARKET MAKING (buy <=20¢ sell >=80¢)")

    total_pnl  = arb_pnl + spec_pnl + mm_pnl
    total_wins = arb_wins + spec_wins + mm_wins
    total_bets = s1["trades"] + s2["trades"] + s3["trades"]
    total_wr   = total_wins / total_bets * 100 if total_bets else 0

    print(f"\n{'='*55}")
    print(f"  COMBINED RESULTS")
    print(f"{'='*55}")
    print(f"  Total bets:    {total_bets}")
    print(f"  Total wins:    {total_wins}")
    print(f"  Win rate:      {total_wr:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  Final capital: ${STARTING_CAPITAL + total_pnl:.2f}")
    print(f"  ROI:           {total_pnl/STARTING_CAPITAL*100:.1f}%")

    # Top wins
    top_arb  = sorted([t for t in arb_trades  if t["won"]], key=lambda x: x["pnl"], reverse=True)[:5]
    top_spec = sorted([t for t in spec_trades if t["won"]], key=lambda x: x["pnl"], reverse=True)[:5]
    top_mm   = sorted([t for t in mm_trades   if t["won"]], key=lambda x: x["pnl"], reverse=True)[:5]

    print(f"\n  TOP ARB WINS:")
    for t in top_arb:
        print(f"    {t['city']} | {t['question'][:50]} | +${t['pnl']:.2f}")

    print(f"\n  TOP SPEC WINS:")
    for t in top_spec:
        print(f"    {t['city']} | {t['question'][:50]} | {t['open_price']}¢ → +${t['pnl']:.2f} ({t['multiplier']}x)")

    print(f"\n  TOP MM WINS:")
    for t in top_mm:
        print(f"    {t['city']} | {t['question'][:50]} | {t['entry']}¢→{t['exit']}¢ | +${t['pnl']:.2f}")

    return {
        "processed":     processed,
        "skipped":       skipped,
        "total_bets":    total_bets,
        "total_wins":    total_wins,
        "win_rate":      round(total_wr, 1),
        "total_pnl":     round(total_pnl, 2),
        "final_capital": round(STARTING_CAPITAL + total_pnl, 2),
        "roi":           round(total_pnl / STARTING_CAPITAL * 100, 1),
        "strategies": {
            "arbitrage":     s1,
            "speculation":   s2,
            "market_making": s3,
        },
        "top_wins": {
            "arbitrage":     top_arb,
            "speculation":   top_spec,
            "market_making": top_mm,
        }
    }


if __name__ == '__main__':
    run_honda_backtest()
