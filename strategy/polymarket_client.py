"""
polymarket_client.py - Real money trading on Polymarket CLOB API.

Uses py-clob-client with signature_type=0 for MetaMask/EOA wallets.

Railway environment variables:
  POLYMARKET_PRIVATE_KEY = MetaMask private key (with or without 0x)
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
CHAIN_ID     = 137

PRIVATE_KEY  = os.getenv("POLYMARKET_PRIVATE_KEY", "")
WALLET       = os.getenv("POLYMARKET_WALLET", "")
TRADING_MODE = os.getenv("TRADING_MODE", "paper")
BET_SIZE     = float(os.getenv("BET_SIZE_REAL", "1.0"))


def is_real_mode():
    return TRADING_MODE == "real" and bool(PRIVATE_KEY) and bool(WALLET)


def _clean_key(key):
    """Remove 0x prefix — py-clob-client expects raw hex key."""
    if key.startswith("0x") or key.startswith("0X"):
        return key[2:]
    return key


def get_client():
    """Get authenticated ClobClient for MetaMask/EOA wallet."""
    from py_clob_client.client import ClobClient
    client = ClobClient(
        CLOB_BASE,
        key=_clean_key(PRIVATE_KEY),
        chain_id=CHAIN_ID,
        signature_type=0,  # EOA/MetaMask — direct private key, no proxy
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_token_id(market_id):
    """Get CLOB YES token ID for a market."""
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=15)
        if r.status_code == 200:
            tokens = r.json().get("clobTokenIds")
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            return tokens[0] if tokens else None
    except Exception as e:
        print(f"  [TOKEN ERR] {e}")
    return None


def place_real_order(market_id, question, city, yes_price):
    """
    Place a real GTC limit order on Polymarket.
    Returns dict with success/error.
    """
    if not is_real_mode():
        return {"success": False, "error": "Not in real mode"}

    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        token_id = get_token_id(market_id)
        if not token_id:
            return {"success": False, "error": f"No token ID for market {market_id}"}

        client = get_client()

        # Price must be a clean decimal with max 4 decimal places
        price = round(float(yes_price), 4)
        if price <= 0 or price >= 1:
            return {"success": False, "error": f"Invalid price: {price}"}

        # Size = number of shares = dollar amount / price per share
        # Minimum order size on Polymarket is 5 shares
        size = round(BET_SIZE / price)
        if size < 5:
            size = 5

        order_args = OrderArgs(
            price    = price,
            size     = float(size),
            side     = BUY,
            token_id = token_id,
        )
        signed = client.create_order(order_args)
        resp   = client.post_order(signed, OrderType.GTC)

        print(f"  [ORDER] {city} | {question[:40]} | price={price} size={size} | {resp}")

        if resp and resp.get("success"):
            return {
                "success":  True,
                "order_id": resp.get("orderID", ""),
                "market_id": market_id,
                "city":     city,
                "question": question,
                "price":    price,
                "size":     size,
                "bet_size": BET_SIZE,
                "mode":     "REAL",
            }
        else:
            return {"success": False, "error": str(resp)}

    except ImportError:
        return {"success": False, "error": "py-clob-client not installed"}
    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "trace": traceback.format_exc()[:500]}


def test_connection():
    """Test credentials without placing any order."""
    if not PRIVATE_KEY or not WALLET:
        return {"status": "error", "message": "Missing POLYMARKET_PRIVATE_KEY or POLYMARKET_WALLET"}
    if not is_real_mode():
        return {"status": "paper", "message": "TRADING_MODE is not 'real'"}
    try:
        client = get_client()
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
        return {"status": "error", "message": "py-clob-client not installed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    print("Testing Polymarket connection...")
    print(json.dumps(test_connection(), indent=2))
