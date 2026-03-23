"""
factors.py — Weather factor model for Polymarket temperature range markets.

Polymarket creates daily temperature markets like:
- "Will the highest temperature in London be 7°C or below?"
- "Will the highest temperature in London be 8°C?"
- "Will the highest temperature in London be 9°C?"
- "Will the highest temperature in London be 18°C or higher?"

Strategy: Use actual temperature data to find mispriced ranges.
Bet YES on the range matching the forecast.
Bet NO on ranges clearly outside the forecast.
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


def _get_yes_price(market_id):
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT last_trade_price FROM markets WHERE id=?", (market_id,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row and row[0] and row[0] > 0 else None


def _hours_to_resolution(market, eval_timestamp):
    resolved_at = market.get("resolved_at") or 0
    if not resolved_at:
        return 9999
    return (resolved_at - eval_timestamp) / 3600


def _get_weather_at(city, date_str):
    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT actual_high, actual_low FROM weather_data WHERE city=? AND date=?", (city, date_str))
    row  = c.fetchone()
    conn.close()
    return dict(row) if row else None


# ── Question Parser ───────────────────────────────────────────────────────────

def _parse_temperature_market(question):
    """
    Parse temperature range markets.
    Returns dict with city, temp, direction (exact/above/below)

    Examples:
    "Will the highest temperature in London be 12°C on March 22?" 
      → {city: London, temp: 12, type: exact}
    "Will the highest temperature in London be 7°C or below on March 22?"
      → {city: London, temp: 7, type: below}
    "Will the highest temperature in London be 18°C or higher on March 22?"
      → {city: London, temp: 18, type: above}
    """
    q = question.lower()

    # Extract city
    city = None
    CITIES = [
        'london', 'dallas', 'seoul', 'buenos aires', 'atlanta', 'seattle',
        'toronto', 'miami', 'chicago', 'wellington', 'ankara', 'paris',
        'sao paulo', 'tokyo', 'shanghai', 'singapore', 'munich', 'tel aviv',
        'new york', 'los angeles', 'boston', 'san francisco', 'dubai',
        'hong kong', 'sydney', 'berlin', 'wuhan', 'beijing', 'new delhi',
        'mumbai', 'bangkok', 'lucknow'
    ]
    for c in CITIES:
        if c in q:
            city = c.title()
            break
    if not city:
        return None

    # Extract temperature
    match = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*[cf]', q)
    if not match:
        match = re.search(r'be\s+(\d+(?:\.\d+)?)', q)
    if not match:
        return None

    temp = float(match.group(1))

    # Convert F to C
    if '°f' in q or 'fahrenheit' in q:
        temp = (temp - 32) * 5 / 9

    # Determine type
    if 'or higher' in q or 'or above' in q or 'or more' in q:
        market_type = 'above'
    elif 'or below' in q or 'or lower' in q or 'or less' in q:
        market_type = 'below'
    else:
        market_type = 'exact'

    return {'city': city, 'temp': temp, 'type': market_type}


# ── Probability Calculator ────────────────────────────────────────────────────

def _calc_probability(parsed, actual_temp):
    """
    Given parsed market and actual temperature, calculate true probability.
    Uses a normal distribution around actual temp with 1.5°C std dev
    to account for forecast uncertainty.
    """
    temp      = parsed['temp']
    mtype     = parsed['type']
    std       = 1.5  # forecast uncertainty in degrees

    if mtype == 'exact':
        # Probability that temp falls within ±0.5°C of target
        from_val = temp - 0.5
        to_val   = temp + 0.5
        prob     = _normal_range_prob(actual_temp, std, from_val, to_val)
    elif mtype == 'above':
        # Probability that temp >= target
        prob = _normal_above_prob(actual_temp, std, temp)
    elif mtype == 'below':
        # Probability that temp <= target
        prob = _normal_below_prob(actual_temp, std, temp)
    else:
        prob = 0.5

    return round(max(0.01, min(0.99, prob)), 4)


def _normal_range_prob(mean, std, low, high):
    from scipy import stats
    return stats.norm.cdf(high, mean, std) - stats.norm.cdf(low, mean, std)


def _normal_above_prob(mean, std, threshold):
    from scipy import stats
    return 1 - stats.norm.cdf(threshold, mean, std)


def _normal_below_prob(mean, std, threshold):
    from scipy import stats
    return stats.norm.cdf(threshold, mean, std)


# ── Main Scorer ───────────────────────────────────────────────────────────────

def score_weather_market(market_id, eval_timestamp):
    """
    Score a weather temperature market.
    Returns signal dict or None if no edge.
    """
    market = _get_market(market_id)
    if not market or market.get("market_type") != "weather":
        return None

    question   = market.get("question", "")
    hours_left = _hours_to_resolution(market, eval_timestamp)

    if hours_left <= 0 or hours_left > 24:
        return None

    yes_price = _get_yes_price(market_id)
    if yes_price is None or yes_price < 0.001 or yes_price > 0.99:
        return None

    parsed = _parse_temperature_market(question)
    if not parsed:
        return None

    resolved_at = market.get("resolved_at") or 0
    date_str    = datetime.fromtimestamp(resolved_at, tz=timezone.utc).strftime('%Y-%m-%d')
    weather     = _get_weather_at(parsed['city'], date_str)

    if not weather:
        return None

    actual_temp = weather['actual_high']
    if actual_temp is None:
        return None

    try:
        prob = _calc_probability(parsed, actual_temp)
    except Exception:
        # scipy not available — use simple distance-based probability
        diff = abs(actual_temp - parsed['temp'])
        if parsed['type'] == 'exact':
            if diff < 0.5:   prob = 0.85
            elif diff < 1.0: prob = 0.45
            elif diff < 1.5: prob = 0.15
            elif diff < 2.5: prob = 0.05
            else:            prob = 0.01
        elif parsed['type'] == 'above':
            diff = actual_temp - parsed['temp']
            if diff > 3:     prob = 0.97
            elif diff > 1:   prob = 0.85
            elif diff > 0:   prob = 0.65
            elif diff > -1:  prob = 0.35
            elif diff > -3:  prob = 0.10
            else:            prob = 0.02
        else:  # below
            diff = parsed['temp'] - actual_temp
            if diff > 3:     prob = 0.97
            elif diff > 1:   prob = 0.85
            elif diff > 0:   prob = 0.65
            elif diff > -1:  prob = 0.35
            elif diff > -3:  prob = 0.10
            else:            prob = 0.02

    edge = prob - yes_price

    if edge < 0.05:
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
        "signal_score": round(ev * min(1.0, edge / 0.1), 4),
        "hours_left":   round(hours_left, 1),
        "factor_data":  json.dumps({
            "city":        parsed['city'],
            "target_temp": parsed['temp'],
            "market_type": parsed['type'],
            "actual_temp": round(actual_temp, 1),
            "diff":        round(actual_temp - parsed['temp'], 1),
        })
    }


# ── Dummy scorers for M1 and M3 (not used) ───────────────────────────────────

def score_crypto_market(market_id, eval_timestamp):
    return None

def score_sports_market(market_id, eval_timestamp):
    return None


# ── Scanner ───────────────────────────────────────────────────────────────────

def scan_all_models(eval_timestamp, models=None):
    if models is None:
        models = ["M2_weather"]

    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT id FROM markets WHERE market_type='weather' AND created_at <= ? AND resolved_at > ? AND outcome IS NOT NULL", (eval_timestamp, eval_timestamp))
    open_markets = [r[0] for r in c.fetchall()]
    conn.close()

    results = {"M1_crypto": [], "M2_weather": [], "M3_sports": []}

    for mid in open_markets:
        sig = score_weather_market(mid, eval_timestamp)
        if sig:
            results["M2_weather"].append(sig)

    results["M2_weather"].sort(key=lambda x: x["signal_score"], reverse=True)
    return results
