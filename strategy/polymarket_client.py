"""
polymarket_client.py - Real money trading on Polymarket CLOB API.

Uses py-clob-client (official Polymarket Python library) for auth and trading.
MetaMask wallets require signature_type=1 and funder address.

Railway environment variables required:
  POLYMARKET_PRIVATE_KEY = MetaMask private key (with 0x prefix)
  POLYMARKET_WALLET      = MetaMask wallet address (0x...)
  TRADING_MODE           = "real" or "paper"
  BET_SIZE_REAL          = bet size in dollars (default: 1.0)
"""

import os
import json
import time
import requests

CLOB_BASE    = "https://clob.polymarket.com"
GAMMA_BASE   = "https://gamma-api.polymarket.com"
CHAIN_ID     = 137  # Polygon mainnet

PRIVATE_KEY  = os.getenv("POLYMARKET_PRIVATE_KEY", "")
WALLET       = os.getenv("POLYMARKET_WALLET", "")
TRADING_MODE = os.getenv("TRADING_MODE", "paper")
BET_SIZE     = float(os.getenv("BET_SIZE_REAL", "1.0"))


def is_real_mode():
    return TRADING_MODE == "real" and bool(PRIVATE_KEY) and bool(WALLET)


def get_client():
    """Get authenticated py-clob-client instance."""
    from py_clob_client.client import ClobClient

    # Remove 0x prefix — py-clob-client expects key without it
    key = PRIVATE_KEY
    if key.startswith("0x") or key.startswith("0X"):
        key = key[2:]

    client = ClobClient(
        CLOB_BASE,
        key=key,
        chain_id=CHAIN_ID,
        signature_type=1,   # MetaMask / browser wallet
        funder=WALLET,      # Address holding the funds
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_token_id(market_id):
    """Get CLOB token ID for a market's YES outcome."""
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=15)
        if r.status_code == 200:
            data   = r.json()
            tokens = data.get("clobTokenIds")
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            return tokens[0] if tokens else None
    except Exception as e:
        print(f"  [TOKEN ERR] {e}")
    return None


def place_real_order(market_id, question, city, yes_price):
    """
    Place a real money market order on Polymarket.
    Returns dict with success status and details.
    """
    if not is_real_mode():
        return {"success": False, "error": "Not in real mode"}

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        client   = get_client()
        token_id = get_token_id(market_id)

        if not token_id:
            return {"success": False, "error": f"No token ID for market {market_id}"}

        # Place market order — buys at best available price
        mo     = MarketOrderArgs(
            token_id   = token_id,
            amount     = BET_SIZE,   # dollar amount to spend
            side       = BUY,
            order_type = OrderType.FOK,  # Fill or Kill — all or nothing
        )
        signed = client.create_market_order(mo)
        resp   = client.post_order(signed, OrderType.FOK)

        print(f"  [REAL ORDER] {city} | {question[:40]} | ${BET_SIZE} | {resp}")

        if resp and resp.get("success"):
            return {
                "success":   True,
                "order_id":  resp.get("orderID", ""),
                "market_id": market_id,
                "city":      city,
                "question":  question,
                "bet_size":  BET_SIZE,
                "mode":      "REAL",
                "response":  resp,
            }
        else:
            return {"success": False, "error": str(resp)}

    except ImportError:
        return {"success": False, "error": "py-clob-client not installed — check requirements.txt"}
    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "trace": traceback.format_exc()}


def test_connection():
    """Test credentials without placing a real order."""
    if not PRIVATE_KEY or not WALLET:
        return {"status": "error", "message": "POLYMARKET_PRIVATE_KEY or POLYMARKET_WALLET not set"}

    if not is_real_mode():
        return {"status": "paper", "message": "TRADING_MODE is not 'real' — paper trading only"}

    try:
        client = get_client()
        # Simple test — get server time (no auth needed but confirms client init worked)
        ok = client.get_ok()
        return {
            "status":   "connected",
            "message":  "Credentials verified — ready to trade",
            "wallet":   WALLET[:10] + "...",
            "mode":     "REAL",
            "bet_size": BET_SIZE,
            "server":   ok,
        }
    except ImportError:
        return {"status": "error", "message": "py-clob-client not installed — check requirements.txt"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    print("Testing Polymarket connection...")
    result = test_connection()
    print(json.dumps(result, indent=2))
