"""
polymarket_client.py - Real money trading on Polymarket CLOB API.

Uses private key signing to place real orders without MetaMask popup.
Supports both paper and real money modes via environment variable.

Set in Railway environment variables:
  POLYMARKET_PRIVATE_KEY = your MetaMask private key (0x...)
  POLYMARKET_WALLET      = your wallet address (0x...)
  TRADING_MODE           = "real" or "paper" (default: paper)
  BET_SIZE_REAL          = bet size in dollars (default: 1.0)
"""

import os
import json
import time
import requests
import hashlib
import hmac
from datetime import datetime

CLOB_BASE    = "https://clob.polymarket.com"
GAMMA_BASE   = "https://gamma-api.polymarket.com"

# Read from Railway environment variables
PRIVATE_KEY  = os.getenv("POLYMARKET_PRIVATE_KEY", "")
WALLET       = os.getenv("POLYMARKET_WALLET", "")
TRADING_MODE = os.getenv("TRADING_MODE", "paper")
BET_SIZE     = float(os.getenv("BET_SIZE_REAL", "1.0"))

def is_real_mode():
    return TRADING_MODE == "real" and PRIVATE_KEY and WALLET

def safe_get(url, params=None, headers=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30)
        except Exception as e:
            print(f"  [GET ERR] {e}")
        time.sleep(2 ** i)
    return None

def safe_post(url, data=None, headers=None, retries=3):
    for i in range(retries):
        try:
            r = requests.post(url, json=data, headers=headers, timeout=20)
            if r.status_code in (200, 201):
                return r.json()
            print(f"  [POST {r.status_code}] {r.text[:200]}")
        except Exception as e:
            print(f"  [POST ERR] {e}")
        time.sleep(2 ** i)
    return None

def get_api_key():
    """
    Derive API key from private key using Polymarket's auth endpoint.
    This is a one-time call that returns credentials for placing orders.
    """
    if not PRIVATE_KEY or not WALLET:
        return None, None, None

    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct

        account = Account.from_key(PRIVATE_KEY)

        # Polymarket uses a specific message for API key derivation
        nonce    = int(time.time())
        message  = f"This message attests that I control the given wallet\nnonce: {nonce}"
        msg      = encode_defunct(text=message)
        signed   = account.sign_message(msg)
        sig      = signed.signature.hex()

        # Get API credentials
        payload = {
            "address":   WALLET,
            "signature": sig,
            "nonce":     nonce,
        }
        resp = safe_post(f"{CLOB_BASE}/auth/derive-api-key", data=payload)
        if resp:
            return resp.get("apiKey"), resp.get("secret"), resp.get("passphrase")
    except Exception as e:
        print(f"  [AUTH ERR] {e}")

    return None, None, None

def get_token_id(market_id):
    """Get CLOB token ID for a market."""
    data = safe_get(f"{GAMMA_BASE}/markets/{market_id}")
    if not data:
        return None
    tokens = data.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            return None
    return tokens[0] if tokens else None

def get_current_price(token_id):
    """Get current YES price for a token."""
    data = safe_get(f"{CLOB_BASE}/price", params={"token_id": token_id, "side": "buy"})
    if data and "price" in data:
        return float(data["price"])
    return None

def place_real_order(market_id, question, city, yes_price):
    """
    Place a real money order on Polymarket.
    Returns dict with success status and details.
    """
    if not is_real_mode():
        return {"success": False, "error": "Not in real mode — check TRADING_MODE env var"}

    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct

        # Get token ID
        token_id = get_token_id(market_id)
        if not token_id:
            return {"success": False, "error": "Could not get token ID"}

        # Get current price
        current_price = get_current_price(token_id)
        if not current_price:
            current_price = yes_price

        # Calculate shares
        shares = round(BET_SIZE / current_price, 2)

        # Get API credentials
        api_key, secret, passphrase = get_api_key()
        if not api_key:
            return {"success": False, "error": "Could not get API credentials"}

        # Build order
        account   = Account.from_key(PRIVATE_KEY)
        nonce     = int(time.time() * 1000)
        order     = {
            "tokenID":   token_id,
            "side":      "BUY",
            "price":     str(current_price),
            "size":      str(shares),
            "orderType": "GTC",   # Good Till Cancelled
            "nonce":     nonce,
            "maker":     WALLET,
        }

        # Sign the order
        order_str = json.dumps(order, separators=(",", ":"), sort_keys=True)
        msg       = encode_defunct(text=order_str)
        signed    = account.sign_message(msg)
        order["signature"] = signed.signature.hex()

        # Submit order
        headers = {
            "POLY-API-KEY":    api_key,
            "POLY-PASSPHRASE": passphrase,
            "Content-Type":    "application/json",
        }
        result = safe_post(f"{CLOB_BASE}/order", data=order, headers=headers)

        if result and result.get("orderID"):
            print(f"  [REAL ORDER] {city} {question[:40]} | ${BET_SIZE} | order={result['orderID']}")
            return {
                "success":    True,
                "order_id":   result["orderID"],
                "market_id":  market_id,
                "city":       city,
                "question":   question,
                "price":      current_price,
                "shares":     shares,
                "bet_size":   BET_SIZE,
                "mode":       "REAL",
            }
        else:
            return {"success": False, "error": str(result)}

    except ImportError:
        return {"success": False, "error": "eth_account not installed — add to requirements.txt"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def test_connection():
    """Test that credentials work without placing a real order."""
    if not PRIVATE_KEY or not WALLET:
        return {"status": "error", "message": "No private key or wallet configured"}

    if not is_real_mode():
        return {"status": "paper", "message": "TRADING_MODE is not 'real' — paper trading only"}

    api_key, secret, passphrase = get_api_key()
    if api_key:
        return {
            "status":  "connected",
            "message": "Credentials verified — ready to trade",
            "wallet":  WALLET[:10] + "...",
            "mode":    "REAL",
            "bet_size": BET_SIZE,
        }
    else:
        return {"status": "error", "message": "Could not authenticate with private key"}


if __name__ == "__main__":
    print("Testing Polymarket connection...")
    result = test_connection()
    print(json.dumps(result, indent=2))
