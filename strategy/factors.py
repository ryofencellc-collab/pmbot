"""
factors.py — Three causal factor models.

M1: Crypto Daily  — price distance + momentum vs market implied prob
M2: Weather       — forecast temp vs market implied threshold
M3: Sports        — Vegas line vs Polymarket price gap

NO LOOKAHEAD: every function only reads data with timestamp <= eval_timestamp.
"""

import re
import json
import numpy as np
from datetime import datetime, timezone
from data.database import get_conn


def _get_market(market_id):
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT * FROM markets WHERE id=?", (market_id,))
    row  = c.fetchone()
    conn.close()
    return dict(row) if row else None


def _get_yes_price(market_id, eval_timestamp):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT yes_price FROM price_snapshots
        WHERE market_id=? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    ''', (market_id, eval_timestamp))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else None


def _hours_to_resolution(market, eval_timestamp):
    resolved_at = market.get("resolved_at") or 0
    if not resolved_at:
        return 9999
    return (resolved_at - eval_timestamp) / 3600


# ── M1: CRYPTO ────────────────────────────────────────────────────────────────

def _parse_crypto_target(question):
    q = question.lower()
    symbol = "BTC" if any(x in q for x in ["bitcoin", "btc"]) else \
             "ETH" if any(x in q for x in ["ethereum", "eth"]) else None
    if not symbol:
        return None

    direction = "below" if any(x in q for x in ["below", "under", "dip", "drop", "fall"]) else "above"

    patterns = [
        r'\$(\d+(?:,\d+)*(?:\.\d+)?)\s*k\b',
        r'\$(\d+(?:,\d+)*(?:\.\d+)?)',
    ]
    for pat in patterns:
        matches = re.findall(pat, q)
        if matches:
            price_str = matches[-1].replace(",", "")
            target    = float(price_str)
            idx       = q.find(price_str)
            if idx >= 0 and "k" in q[idx:idx+10]:
                target *= 1000
            if target > 100:
                return (symbol, target, direction)
    return None


def _get_crypto_price_at(symbol, eval_timestamp):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT close FROM crypto_prices
        WHERE symbol=? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    ''', (symbol, eval_timestamp))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else None


def _get_crypto_momentum(symbol, eval_timestamp, hours=24):
    conn        = get_conn()
    c           = conn.cursor()
    lookback_ts = eval_timestamp - (hours * 3600)
    c.execute('''SELECT close FROM crypto_prices WHERE symbol=? AND timestamp <= ?
                 ORDER BY timestamp DESC LIMIT 1''', (symbol, eval_timestamp))
    cur = c.fetchone()
    c.execute('''SELECT close FROM crypto_prices WHERE symbol=? AND timestamp <= ?
                 ORDER BY timestamp DESC LIMIT 1''', (symbol, lookback_ts))
    past = c.fetchone()
    conn.close()
    if not cur or not past or past[0] == 0:
        return 0.0
    return (cur[0] - past[0]) / past[0]


def score_crypto_market(market_id, eval_timestamp):
    market = _get_market(market_id)
    if not market:
        return None

    question   = market.get("question", "")
    parsed     = _parse_crypto_target(question)
    if not parsed:
        return None

    symbol, target_price, direction = parsed
    hours_left = _hours_to_resolution(market, eval_timestamp)

    if hours_left <= 0 or hours_left > 12:
        return None

    yes_price = _get_yes_price(market_id, eval_timestamp)
    if yes_price is None or yes_price < 0.001 or yes_price > 0.60:
        return None

    current_price = _get_crypto_price_at(symbol, eval_timestamp)
    if not current_price:
        return None

    momentum_24h  = _get_crypto_momentum(symbol, eval_timestamp, 24)
    momentum_1h   = _get_crypto_momentum(symbol, eval_timestamp, 1)
    distance_pct  = (current_price - target_price) / target_price

    if direction == "above":
        if current_price >= target_price:
            prob = min(0.95, 0.5 + distance_pct * 2 + momentum_1h * 5)
        else:
            gap_pct = abs(distance_pct)
            prob    = min(0.5, max(0.01, 0.3 * np.exp(-gap_pct * 15) + momentum_24h * 0.3))
    else:
        if current_price <= target_price:
            prob = min(0.95, 0.5 + abs(distance_pct) * 2 - momentum_1h * 5)
        else:
            gap_pct = abs(distance_pct)
            prob    = min(0.5, max(0.01, 0.3 * np.exp(-gap_pct * 15) - momentum_24h * 0.3))

    prob = max(0.01, min(0.99, prob))
    edge = prob - yes_price

    if edge < 0.05:
        return None

    ev = prob * (1.0 / yes_price) - 1.0

    return {
        "market_id":    market_id,
        "question":     question,
        "model":        "M1_crypto",
        "yes_price":    round(yes_price, 4),
        "factor_prob":  round(prob, 4),
        "edge":         round(edge, 4),
        "ev":           round(ev, 4),
        "signal_score": round(ev * min(1.0, edge / 0.1), 4),
        "hours_left":   round(hours_left, 1),
        "factor_data":  json.dumps({
            "symbol": symbol, "current_price": round(current_price, 2),
            "target_price": target_price, "direction": direction,
            "distance_pct": round(distance_pct * 100, 2),
            "momentum_24h": round(momentum_24h * 100, 2),
            "momentum_1h":  round(momentum_1h * 100, 2),
            "hours_left":   round(hours_left, 1),
        })
    }


# ── M2: WEATHER ───────────────────────────────────────────────────────────────

def _parse_weather_question(question):
    q = question.lower()

    city = None
    for name in ["london", "new york", "los angeles", "chicago", "miami", "tokyo", "sydney"]:
        if name in q:
            city = name.title()
            break
    if not city:
        return None

    direction = "below" if any(x in q for x in ["below", "under", "not exceed", "less than"]) else "above"

    patterns = [
        r'(\d+(?:\.\d+)?)\s*°?\s*c\b',
        r'(\d+(?:\.\d+)?)\s*°?\s*f\b',
        r'(\d+(?:\.\d+)?)\s*degrees',
        r'(\d+(?:\.\d+)?)\s*celsius',
    ]
    threshold = None
    unit      = "C"
    for pat in patterns:
        m = re.search(pat, q)
        if m:
            threshold = float(m.group(1))
            unit      = "F" if "f" in pat else "C"
            break

    if threshold is None:
        return None

    if unit == "F":
        threshold = (threshold - 32) * 5 / 9

    return {"city": city, "threshold": threshold, "direction": direction}


def _get_weather_at(city, date_str):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT actual_high, actual_low, forecast_high, forecast_low
        FROM weather_data WHERE city=? AND date=?
    ''', (city, date_str))
    row  = c.fetchone()
    conn.close()
    return dict(row) if row else None


def score_weather_market(market_id, eval_timestamp):
    market = _get_market(market_id)
    if not market or market.get("market_type") != "weather":
        return None

    question = market.get("question", "")
    parsed   = _parse_weather_question(question)
    if not parsed:
        return None

    city, threshold, direction = parsed["city"], parsed["threshold"], parsed["direction"]
    hours_left = _hours_to_resolution(market, eval_timestamp)

    if hours_left <= 0 or hours_left > 24:
        return None

    yes_price = _get_yes_price(market_id, eval_timestamp)
    if yes_price is None or yes_price < 0.001 or yes_price > 0.99:
        return None

    resolved_at = market.get("resolved_at") or 0
    date_str    = datetime.fromtimestamp(resolved_at, tz=timezone.utc).strftime('%Y-%m-%d')
    weather     = _get_weather_at(city, date_str)
    if not weather:
        return None

    temp = weather["actual_high"] or weather["forecast_high"]
    if temp is None:
        return None

    diff = temp - threshold

    if direction == "above":
        if diff >= 3:     prob = 0.97
        elif diff >= 1:   prob = 0.88
        elif diff >= 0:   prob = 0.72
        elif diff >= -1:  prob = 0.35
        elif diff >= -3:  prob = 0.12
        else:             prob = 0.03
    else:
        if diff <= -3:    prob = 0.97
        elif diff <= -1:  prob = 0.88
        elif diff <= 0:   prob = 0.72
        elif diff <= 1:   prob = 0.35
        elif diff <= 3:   prob = 0.12
        else:             prob = 0.03

    edge = prob - yes_price

    if abs(diff) < 1.5 or edge < 0.10:
        return None

    ev = prob * (1.0 / yes_price) - 1.0

    return {
        "market_id":    market_id,
        "question":     question,
        "model":        "M2_weather",
        "yes_price":    round(yes_price, 4),
        "factor_prob":  round(prob, 4),
        "edge":         round(edge, 4),
        "ev":           round(ev, 4),
        "signal_score": round(ev * min(1.0, abs(diff) / 5.0), 4),
        "hours_left":   round(hours_left, 1),
        "factor_data":  json.dumps({
            "city": city, "threshold_c": round(threshold, 1),
            "actual_high_c": round(temp, 1), "diff_c": round(diff, 1),
            "direction": direction,
        })
    }


# ── M3: SPORTS ────────────────────────────────────────────────────────────────

def _parse_sports_teams(question):
    q        = question.strip()
    patterns = [
        r'will (?:the )?(.+?) (?:beat|defeat|win against) (?:the )?(.+?)[\?$]',
        r'^(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\-].*)?$',
        r'(.+?) -\d+\.?\d* \(?(.+?)\)?[\?$]',
    ]
    for pat in patterns:
        m = re.search(pat, q, re.IGNORECASE)
        if m:
            t1 = m.group(1).strip()
            t2 = m.group(2).strip().rstrip('?').strip()
            if 2 < len(t1) < 40 and 2 < len(t2) < 40:
                return (t1, t2)
    return None


def _get_vegas_line(team1, team2, date_str):
    conn = get_conn()
    c    = conn.cursor()
    c.execute('SELECT home_team, away_team, vegas_home_prob, vegas_away_prob FROM sports_lines WHERE game_date=?', (date_str,))
    rows = c.fetchall()
    conn.close()

    t1l = team1.lower()
    t2l = team2.lower()

    for row in rows:
        ht = (row[0] or "").lower()
        at = (row[1] or "").lower()
        if any(t in ht or ht in t for t in [t1l, t2l]) and \
           any(t in at or at in t for t in [t1l, t2l]):
            return {"home_team": row[0], "away_team": row[1],
                    "vegas_home_prob": row[2], "vegas_away_prob": row[3]}
    return None


def score_sports_market(market_id, eval_timestamp):
    market = _get_market(market_id)
    if not market or market.get("market_type") != "sports":
        return None

    question   = market.get("question", "")
    hours_left = _hours_to_resolution(market, eval_timestamp)

    if hours_left <= 0 or hours_left > 12:
        return None

    yes_price = _get_yes_price(market_id, eval_timestamp)
    if yes_price is None or yes_price < 0.05 or yes_price > 0.95:
        return None

    teams = _parse_sports_teams(question)
    if not teams:
        return None

    resolved_at = market.get("resolved_at") or 0
    date_str    = datetime.fromtimestamp(resolved_at, tz=timezone.utc).strftime('%Y-%m-%d')
    vegas       = _get_vegas_line(teams[0], teams[1], date_str)
    if not vegas:
        return None

    vegas_prob = vegas["vegas_home_prob"]
    edge       = vegas_prob - yes_price

    if abs(edge) < 0.08:
        return None

    ev = vegas_prob * (1.0 / yes_price) - 1.0

    return {
        "market_id":    market_id,
        "question":     question,
        "model":        "M3_sports",
        "yes_price":    round(yes_price, 4),
        "factor_prob":  round(vegas_prob, 4),
        "edge":         round(edge, 4),
        "ev":           round(ev, 4),
        "signal_score": round(abs(edge) * ev, 4),
        "hours_left":   round(hours_left, 1),
        "factor_data":  json.dumps({
            "team1": teams[0], "team2": teams[1],
            "vegas_prob": round(vegas_prob, 4),
            "poly_price": round(yes_price, 4),
            "gap": round(edge, 4),
        })
    }


# ── SCANNER ───────────────────────────────────────────────────────────────────

def scan_all_models(eval_timestamp, models=None):
    if models is None:
        models = ["M1_crypto", "M2_weather", "M3_sports"]

    conn = get_conn()
    c    = conn.cursor()
    c.execute('''
        SELECT id, market_type FROM markets
        WHERE created_at <= ? AND resolved_at > ? AND outcome IS NOT NULL
    ''', (eval_timestamp, eval_timestamp))
    open_markets = c.fetchall()
    conn.close()

    results  = {m: [] for m in models}
    scorers  = {
        "M1_crypto":  score_crypto_market,
        "M2_weather": score_weather_market,
        "M3_sports":  score_sports_market,
    }
    model_map = {
        "crypto_daily": "M1_crypto",
        "weather":      "M2_weather",
        "sports":       "M3_sports",
    }

    for row in open_markets:
        market_id, market_type = row[0], row[1]
        target = model_map.get(market_type)
        if not target or target not in models:
            continue
        sig = scorers[target](market_id, eval_timestamp)
        if sig:
            results[target].append(sig)

    for m in results:
        results[m].sort(key=lambda x: x["signal_score"], reverse=True)

    return results
