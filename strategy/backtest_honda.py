"""
backtest_honda.py - HondaCivic full strategy backtest using real data.

Uses price snapshots already in DB (302k+ snapshots across 4,474 markets).
Filters to live trading prices only: 0.001 < price < 0.999

STRATEGY 1 - ARBITRAGE:
  Buy NO on ranges priced >= 95¢ during trading that resolve "No"
  Win: collect spread. Lose: range actually won.

STRATEGY 2 - SPECULATION:
  Buy YES early when price <= 5¢. Hold to resolution.
  Win: YES resolved. Lose: NO resolved.

STRATEGY 3 - MARKET MAKING:
  Buy YES at <= 20¢, sell when price reaches >= 80¢.
  Win: price hit 80¢+ before resolution. Lose: never hit 80¢.
"""

from data.database import get_conn, init_db

BET_SIZE        = 10.0
STARTING_CAP    = 10000.0
ARB_MIN         = 0.95   # buy NO when peak yes >= 95¢
SPEC_MAX_ENTRY  = 0.05   # buy YES when price <= 5¢
MM_MAX_ENTRY    = 0.20   # buy YES when price <= 20¢
MM_MIN_EXIT     = 0.80   # sell YES when price >= 80¢


def run_honda_backtest():
    init_db()
    conn = get_conn()
    c    = conn.cursor()

    # ── Load all resolved markets ──────────────────────────────────────────
    c.execute("""
        SELECT id, question, city, outcome, resolved_at, created_at
        FROM markets
        WHERE outcome IS NOT NULL
        ORDER BY resolved_at DESC
    """)
    markets = [dict(r) for r in c.fetchall()]
    print(f"  Loaded {len(markets)} resolved markets")

    # ── Load all price snapshots ───────────────────────────────────────────
    c.execute("""
        SELECT market_id, timestamp, yes_price
        FROM price_snapshots
        ORDER BY market_id, timestamp ASC
    """)
    snap_map = {}
    for row in c.fetchall():
        mid = row["market_id"]
        if mid not in snap_map:
            snap_map[mid] = []
        snap_map[mid].append((row["timestamp"], row["yes_price"]))
    conn.close()
    print(f"  Loaded snapshots for {len(snap_map)} markets")

    # ── Run strategies ─────────────────────────────────────────────────────
    arb_wins = arb_losses = 0
    arb_pnl  = 0.0
    arb_trades = []

    spec_wins = spec_losses = 0
    spec_pnl  = 0.0
    spec_trades = []

    mm_wins = mm_losses = 0
    mm_pnl  = 0.0
    mm_trades = []

    for m in markets:
        mid     = m["id"]
        outcome = m["outcome"]
        history = snap_map.get(mid, [])

        # Live trading prices only — exclude post-resolution 0/1 settlements
        live = [(t, p) for t, p in history if 0.001 < p < 0.999]
        if not live:
            continue

        peak  = max(p for t, p in live)
        trough= min(p for t, p in live)
        first = live[0][1]

        # ── Strategy 1: Arbitrage ──────────────────────────────────────────
        # Buy NO when yes peaked >= 95¢ during trading
        if peak >= ARB_MIN:
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
                "question":  (m["question"] or "")[:60],
                "outcome":   outcome,
                "peak_yes":  peak,
                "no_price":  no_price,
                "won":       outcome == "No",
                "pnl":       pnl,
                "strategy":  "ARB",
            })

        # ── Strategy 2: Speculation ────────────────────────────────────────
        # Buy YES when price was <= 5¢ early
        if trough <= SPEC_MAX_ENTRY:
            entry = trough
            shares = BET_SIZE / entry
            if outcome == "Yes":
                pnl = round(shares * 1.0 - BET_SIZE, 4)
                spec_wins += 1
            else:
                pnl = -BET_SIZE
                spec_losses += 1
            spec_pnl += pnl
            spec_trades.append({
                "market_id": mid,
                "city":      m["city"],
                "question":  (m["question"] or "")[:60],
                "outcome":   outcome,
                "entry":     entry,
                "multiplier":round(1.0 / entry, 1),
                "won":       outcome == "Yes",
                "pnl":       pnl,
                "strategy":  "SPEC",
            })

        # ── Strategy 3: Market Making ──────────────────────────────────────
        # Buy YES at <= 20¢, sell at >= 80¢
        if trough <= MM_MAX_ENTRY:
            entry    = trough
            entry_ts = next(t for t, p in live if p == entry)
            # Find first price >= 80¢ after entry
            exit_price = None
            for t, p in live:
                if t >= entry_ts and p >= MM_MIN_EXIT:
                    exit_price = p
                    break
            shares = BET_SIZE / entry
            if exit_price:
                pnl = round(shares * exit_price - BET_SIZE, 4)
                mm_wins += 1
            elif outcome == "Yes":
                pnl = round(shares * 1.0 - BET_SIZE, 4)
                mm_wins += 1
            else:
                pnl = -BET_SIZE
                mm_losses += 1
            mm_pnl += pnl
            mm_trades.append({
                "market_id": mid,
                "city":      m["city"],
                "question":  (m["question"] or "")[:60],
                "outcome":   outcome,
                "entry":     entry,
                "exit":      exit_price or (1.0 if outcome == "Yes" else 0.0),
                "won":       pnl > 0,
                "pnl":       pnl,
                "strategy":  "MM",
            })

    # ── Summary ────────────────────────────────────────────────────────────
    def stats(wins, losses, pnl, label):
        total = wins + losses
        wr    = wins / total * 100 if total > 0 else 0
        roi   = pnl / STARTING_CAP * 100
        print(f"\n{'='*50}")
        print(f"  {label}")
        print(f"{'='*50}")
        print(f"  Trades:    {total}")
        print(f"  Wins:      {wins}")
        print(f"  Losses:    {losses}")
        print(f"  Win rate:  {wr:.1f}%")
        print(f"  P&L:       ${pnl:.2f}")
        print(f"  ROI:       {roi:.1f}%")
        return {"wins": wins, "losses": losses, "win_rate": round(wr,1),
                "pnl": round(pnl,2), "roi": round(roi,1), "trades": total}

    print("\n\n" + "="*50)
    print("  HONDA CIVIC STRATEGY BACKTEST")
    print("="*50)

    s1 = stats(arb_wins,  arb_losses,  arb_pnl,  "STRATEGY 1: ARBITRAGE (NO on 95%+ ranges)")
    s2 = stats(spec_wins, spec_losses, spec_pnl, "STRATEGY 2: SPECULATION (YES at <5¢)")
    s3 = stats(mm_wins,   mm_losses,   mm_pnl,   "STRATEGY 3: MARKET MAKING (buy <20¢ sell >80¢)")

    total_pnl = arb_pnl + spec_pnl + mm_pnl
    total_wins = arb_wins + spec_wins + mm_wins
    total_bets = s1["trades"] + s2["trades"] + s3["trades"]
    total_wr   = total_wins / total_bets * 100 if total_bets > 0 else 0

    print(f"\n{'='*50}")
    print(f"  COMBINED")
    print(f"{'='*50}")
    print(f"  Total bets:    {total_bets}")
    print(f"  Total wins:    {total_wins}")
    print(f"  Win rate:      {total_wr:.1f}%")
    print(f"  Total P&L:     ${total_pnl:.2f}")
    print(f"  ROI:           {total_pnl/STARTING_CAP*100:.1f}%")
    print(f"  Final capital: ${STARTING_CAP + total_pnl:.2f}")

    # Top 10 winners per strategy
    top_arb  = sorted([t for t in arb_trades  if t["won"]], key=lambda x: x["pnl"], reverse=True)[:10]
    top_spec = sorted([t for t in spec_trades if t["won"]], key=lambda x: x["pnl"], reverse=True)[:10]
    top_mm   = sorted([t for t in mm_trades   if t["won"]], key=lambda x: x["pnl"], reverse=True)[:10]

    return {
        "total_bets":    total_bets,
        "total_wins":    total_wins,
        "win_rate":      round(total_wr, 1),
        "total_pnl":     round(total_pnl, 2),
        "final_capital": round(STARTING_CAP + total_pnl, 2),
        "roi":           round(total_pnl / STARTING_CAP * 100, 1),
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
