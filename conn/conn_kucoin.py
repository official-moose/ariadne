#>> 🍁 A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 3                ✅ PRODUCTION READY
#>>
#>> KuCoin API Access
#>> mm/conn/conn_kucoin.py
#>>
#>> Live trading client for Ariadne
#>> Proxy connection required due to georestrictions by KuCoin
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250903.01

import os
import time
import json
import hmac
import base64
import hashlib
import requests
from dotenv import load_dotenv
from urllib.parse import urlencode

import logging
logger = logging.getLogger("Ariadne")

# ── Config ─────────────────────────────────────────────────────────────

load_dotenv("mm/data/secrets/.env")

API_KEY        = os.getenv("KUCOIN_API", "")
API_SECRET     = os.getenv("KUCOIN_SEC", "")
API_PASSPHRASE = os.getenv("KUCOIN_PASSPHRASE", "")
PROX_USR       = os.getenv("PROXY_USERNAME", "")
PROX_PWD       = os.getenv("PROXY_PASSWORD", "")
PROX_HOST      = os.getenv("PROXY_HOST", "")

BASE_URL       = "https://api.kucoin.com"
FUTURES_URL    = "https://api-futures.kucoin.com"

PROXY_CONFIG = {
   "http":  f"socks5://{PROX_USR}:{PROX_PWD}@{PROX_HOST}",
   "https": f"socks5h://{PROX_USR}:{PROX_PWD}@{PROX_HOST}"
}

FALLBACK = {
    "maker": 0.0010,
    "taker": 0.0010,
    "withdrawal": 0.25,  # placeholder; KuCoin needs /currencies per chain
}

# ── Signing ────────────────────────────────────────────────────────────

class KcSigner:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.encrypted_passphrase = self._sign_bytes(
            api_passphrase.encode("utf-8"), api_secret.encode("utf-8")
        )

    def _sign_bytes(self, plain: bytes, key: bytes) -> str:
        hm = hmac.new(key, plain, hashlib.sha256)
        return base64.b64encode(hm.digest()).decode()

    def headers(self, payload_string: str) -> dict:
        ts = str(int(time.time() * 1000))
        sig = self._sign_bytes((ts + payload_string).encode("utf-8"), self.api_secret.encode("utf-8"))
        return {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": sig,
            "KC-API-TIMESTAMP": ts,
            "KC-API-PASSPHRASE": self.encrypted_passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json",
        }

_signer = KcSigner(API_KEY, API_SECRET, API_PASSPHRASE)

# ── Core request ───────────────────────────────────────────────────────

def kucoin_request(method: str, path: str, params: dict = None, body: dict = None, timeout: int = 10):
    """
    Signs and sends a KuCoin REST request.
    Returns: (json_dict, latency_ms)
    """
    method = method.upper()
    session = requests.Session()
    session.proxies = PROXY_CONFIG

    # Build URL + query
    url = BASE_URL + path
    raw_url = path
    query = ""
    if method in ("GET", "DELETE") and params:
        query = urlencode(params)
        url += f"?{query}"
        raw_url += f"?{query}"

    # Body
    body_str = ""
    data_to_send = None
    if method in ("POST", "PUT") and body:
        body_str = json.dumps(body, separators=(",", ":"))
        data_to_send = body_str

    # KuCoin sign string is: method + endpoint(+query) + body (timestamp prefixed in header builder)
    payload = method + raw_url + body_str

    # Prepare
    req = requests.Request(method, url, data=data_to_send)
    prepped = req.prepare()

    # Headers (GET/DELETE should not send Content-Type)
    hdrs = _signer.headers(payload)
    if method in ("GET", "DELETE"):
        hdrs.pop("Content-Type", None)
    prepped.headers.update(hdrs)

    # Send
    resp = session.send(prepped, timeout=timeout)
    # Return JSON regardless of HTTP code; KuCoin embeds code in JSON
    return resp.json(), int(resp.elapsed.total_seconds() * 1000)

# Convenience alias used by your tests
def kucoin_auth(method: str, path: str, params: dict = None, body: dict = None, timeout: int = 10):
    return kucoin_request(method, path, params, body, timeout)

# ── Client ─────────────────────────────────────────────────────────────

class KucoinClient:
    def __init__(self):
        self._symbols = {}  # "BTC-USDT" -> "BTC-USDT"
        data, _ = kucoin_auth("GET", "/api/v1/symbols")
        for itm in data.get("data", []):
            pair_norm = f"{itm['baseCurrency']}-{itm['quoteCurrency']}".upper()
            self._symbols[pair_norm] = itm["symbol"]

    def list_products(self):
        return list(self._symbols.keys())

    def _pair(self, symbol: str) -> str:
        return self._symbols.get(symbol.upper(), symbol.upper())

    # ── Public Market Data ────────────────────────────────────────────

    def get_all_tickers(self):
        """Get all ticker data in one batch API call"""
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/allTickers")
            return d.get('data', {}).get('ticker', [])
        except Exception as e:
            logger.error(f"Error fetching batch tickers: {e}")
            return []

    def best_bid_price(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/orderbook/level1", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("bestBid", 0.0))
        except Exception:
            return 0.0

    def best_ask_price(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/orderbook/level1", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("bestAsk", 0.0))
        except Exception:
            return 0.0

    def last_trade_price(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/orderbook/level1", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("price", 0.0))
        except Exception:
            return 0.0

    def vol_24h(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/stats", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("vol", 0.0))
        except Exception:
            return 0.0

    def high_24h(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/stats", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("high", 0.0))
        except Exception:
            return 0.0

    def low_24h(self, symbol: str) -> float:
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/stats", {"symbol": self._pair(symbol)})
            return float((d.get("data") or {}).get("low", 0.0))
        except Exception:
            return 0.0

    def historical_ohlcv(self, symbol: str, timeframe: str, limit: int):
        """
        Returns KuCoin raw candles: [[time, open, close, high, low, volume, turnover], ...]
        """
        try:
            tf_map = {
                "1m":"1min","3m":"3min","5m":"5min","15m":"15min","30m":"30min",
                "1h":"1hour","2h":"2hour","4h":"4hour","6h":"6hour","8h":"8hour","12h":"12hour",
                "1d":"1day","1w":"1week"
            }
            t = tf_map.get(timeframe, timeframe)

            # derive step seconds
            step = 3600
            if t.endswith("min"):
                step = int(t[:-3]) * 60
            elif t.endswith("hour"):
                step = int(t[:-4]) * 3600
            elif t.endswith("day"):
                step = int(t[:-3]) * 86400
            elif t.endswith("week"):
                step = int(t[:-4]) * 7 * 86400

            now = int(time.time())
            start = now - step * max(1, int(limit))
            params = {"symbol": self._pair(symbol), "type": t, "startAt": start, "endAt": now}
            d, _ = kucoin_auth("GET", "/api/v1/market/candles", params)
            return d.get("data", [])
        except Exception:
            return []

    def order_book(self, symbol: str, depth: int = 10):
        try:
            # level2_20 returns up to 20 each side; we slice to requested depth
            d, _ = kucoin_auth("GET", "/api/v1/market/orderbook/level2_20", {"symbol": self._pair(symbol)})
            data = d.get("data") or {}
            bids = [(float(b[0]), float(b[1])) for b in (data.get("bids") or [])[:depth]]
            asks = [(float(a[0]), float(a[1])) for a in (data.get("asks") or [])[:depth]]
            return {"bids": bids, "asks": asks}
        except Exception:
            return {"bids": [], "asks": []}

    def last_trade(self, symbol: str):
        try:
            d, _ = kucoin_auth("GET", "/api/v1/market/histories", {"symbol": self._pair(symbol)})
            arr = d.get("data") or []
            if not arr:
                return (0, 0.0)
            trade = arr[0]
            ts = int(trade.get("time", 0)) // 1000  # ms -> s
            price = float(trade.get("price", 0.0))
            return (ts, price)
        except Exception:
            return (0, 0.0)

    def intraday_volume(self, symbol: str, interval: str = "ONE_MINUTE", limit: int = 5):
        try:
            interval_map = {
                "ONE_MINUTE": "1min",
                "FIVE_MINUTE": "5min",
                "FIFTEEN_MINUTE": "15min",
                "ONE_HOUR": "1hour",
            }
            k_interval = interval_map.get(interval.upper(), "1min")
            d, _ = kucoin_auth("GET", "/api/v1/market/candles", {
                "symbol": self._pair(symbol),
                "type": k_interval
            })
            candles = d.get("data", [])
            # [time, open, close, high, low, volume, turnover]
            vol_data = [(str(int(c[0]) // 1000), float(c[5])) for c in candles[:limit]]
            return list(reversed(vol_data))
        except Exception:
            return []

    # ── Private / Fees / Limits ───────────────────────────────────────

    def maker_fee(self, symbol: str = None):
        """
        Fetch maker fee from KuCoin. Returns {"source":"api|fallback","value":float}.
        """
        try:
            params = {"symbols": self._pair(symbol)} if symbol else None
            d, _ = kucoin_auth("GET", "/api/v1/trade-fees", params)
            if d.get("code") == "200000" and d.get("data"):
                row = d["data"][0] or {}
                val = float(row.get("makerFeeRate"))
                # If API returns 0 or missing, treat as error → fallback
                if val > 0:
                    return {"source": "api", "value": val}
            # explicit soft-error → fallback
            return {"source": "fallback", "value": FALLBACK["maker"]}
        except Exception:
            return {"source": "fallback", "value": FALLBACK["maker"]}


    def taker_fee(self, symbol: str = None):
        """
        Fetch taker fee from KuCoin. Returns {"source":"api|fallback","value":float}.
        """
        try:
            params = {"symbols": self._pair(symbol)} if symbol else None
            d, _ = kucoin_auth("GET", "/api/v1/trade-fees", params)
            if d.get("code") == "200000" and d.get("data"):
                row = d["data"][0] or {}
                val = float(row.get("takerFeeRate"))
                if val > 0:
                    return {"source": "api", "value": val}
            return {"source": "fallback", "value": FALLBACK["taker"]}
        except Exception:
            return {"source": "fallback", "value": FALLBACK["taker"]}


    def withdrawal_fee(self, symbol: str, network: str = None):
        """
        Always fetch XRP withdraw fee (your policy). Returns fee in XRP units.
        Uses KuCoin currencies endpoint and picks the XRP/XRPL chain row.
        """
        try:
            # v3 endpoint returns one object with 'chains' list
            d, _ = kucoin_auth("GET", "/api/v3/currencies/XRP")
            data = d.get("data") or {}

            # 'chains' might be list under either data['chains'] or data (older)
            chains = data.get("chains")
            if chains is None and isinstance(data, list):
                chains = data
            if not chains:
                return {"source": "fallback", "value": FALLBACK["withdrawal"], "network": "XRP"}

            fee_val = None
            for ch in chains:
                name = (ch.get("chain") or ch.get("name") or "").upper()
                if name in ("XRP", "XRPL", "RIPPLE"):
                    # Key names vary by doc/version: withdrawFee / withdrawMinFee / withdrawalMinFee
                    raw = ch.get("withdrawFee", ch.get("withdrawMinFee", ch.get("withdrawalMinFee")))
                    if raw is not None:
                        fee_val = float(raw)
                        break

            # If we didn't find an explicit XRP row, take the smallest withdraw fee among chains
            if fee_val is None:
                fees = []
                for ch in chains:
                    raw = ch.get("withdrawFee", ch.get("withdrawMinFee", ch.get("withdrawalMinFee")))
                    if raw is not None:
                        try: fees.append(float(raw))
                        except: pass
                if fees:
                    fee_val = min(fees)
                    return {"source": "api", "value": fee_val, "network": "XRP"}
                return {"source": "fallback", "value": FALLBACK["withdrawal"], "network": "XRP"}

            return {"source": "api", "value": fee_val, "network": "XRP"}

        except Exception:
            return {"source": "fallback", "value": FALLBACK["withdrawal"], "network": "XRP"}

    def min_trade_size(self, symbol: str):
        try:
            d, _ = kucoin_auth("GET", "/api/v1/symbols")
            ex = self._pair(symbol).upper()
            for itm in d.get("data", []):
                if itm.get("symbol", "").upper() == ex:
                    v = float(itm.get("baseMinSize", 0.0))
                    return {"source": "api", "value": v}
            return {"source": "fallback", "value": 0.0}
        except Exception:
            return {"source": "fallback", "value": 0.0}
        
    def get_recent_trades(self, symbol: str, limit: int = 100):
        """
        Fetches recent trades for a symbol.
        Args:
            symbol: The trading pair (e.g., 'BTC-USDT')
            limit: Number of trades to fetch (max 100)
        Returns:
            list: A list of recent trades, newest first.
        """
        try:
            params = {"symbol": self._pair(symbol)}
            d, _ = kucoin_auth("GET", "/api/v1/market/histories", params)
            return d.get('data', [])[:limit]
        except Exception as e:
            logger.error(f"Error fetching recent trades for {symbol}: {e}")
            return []

    # ── Wallet Functions  ─────────────────────────────────────────────
    def get_account_balances(self, account_type: str = "trade"):
        try:
            # Use the private API endpoint for accounts
            params = {"type": account_type}
            d, _ = kucoin_auth("GET", "/api/v1/accounts", params)
            
            balances = {}
            if d.get("code") == "200000":
                for account in d.get("data", []):
                    currency = (account.get("currency") or "").upper()
                    # Only count available balance, not held
                    available = float(account.get("available", 0.0))
                    if available > 0:
                        balances[currency] = balances.get(currency, 0.0) + available
            return balances
        except Exception as e:
            # Log the error and return an empty dict to avoid crashing the bot
            logger.error(f"Error fetching balances: {e}")
            return {}
        
    def get_account_balances_detailed(self, account_type: str = "trade"):
        """
        Returns { 'USDT': {'available': x, 'hold': y}, 'BTC': {...}, ... }
        Uses KuCoin accounts endpoint.
        """
        try:
            params = {"type": account_type}
            d, _ = kucoin_auth("GET", "/api/v1/accounts", params)
            balances = {}
            if d.get("code") == "200000":
                for acc in d.get("data", []):
                    currency = (acc.get("currency") or "").upper()
                    if not currency:
                        continue
                    available = float(acc.get("available", 0.0) or 0.0)
                    hold = float(acc.get("hold", acc.get("holds", 0.0) or 0.0) or 0.0)
                    cur = balances.get(currency, {"available": 0.0, "hold": 0.0})
                    cur["available"] += max(0.0, available)
                    cur["hold"] += max(0.0, hold)
                    balances[currency] = cur
            return balances
        except Exception as e:
            logger.error(f"Error fetching detailed balances: {e}")
            return {}
        
    def get_positions(self, account_type: str = "trade"):
        """
        Returns all assets currently held (positions) for the given account type.
        A position is defined as any asset with available > 0 or hold > 0.
        
        Args:
            account_type (str): KuCoin account type, e.g. 'trade', 'main', 'margin'
        
        Returns:
            dict: { 'USDT': {'available': x, 'hold': y, 'total': z}, ... }
        """
        try:
            balances = self.get_account_balances_detailed(account_type=account_type)
            positions = {}
            for currency, info in balances.items():
                available = float(info.get("available", 0.0))
                hold = float(info.get("hold", 0.0))
                total = available + hold
                if total > 0:
                    positions[currency] = {
                        "available": available,
                        "hold": hold,
                        "total": total
                    }
            return positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return {}
    
    def get_orders(self, symbol: str = None, status: str = "active"):
        """Get orders from exchange."""
        endpoint = "/api/v1/orders"
        params = {"status": status}
        if symbol:
            params["symbol"] = symbol
        return self._get(endpoint, params)
    
    def get_open_sells(self):
        params = {
            "status": "active",
            "side": "sell"
        }
        d, _ = kucoin_auth("GET", "/api/v1/orders", params=params)
        return d
    
    def get_open_buys(self):
        params = {
            "status": "active",
            "side": "buy"
        }
        d, _ = kucoin_auth("GET", "/api/v1/orders", params=params)
        return d

    # ── Orders ────────────────────────────────────────────────────────

    def create_limit_order(self, symbol: str, side: str, price: float, size: float,
                           post_only: bool = False, tif: str = "GTC",
                           hidden: bool = False, iceberg: bool = False) -> str:
        """
        Places a limit order; returns orderId on success, raises on error.
        """
        path = "/api/v1/orders"
        body = {
            "clientOid": str(int(time.time() * 1000)),
            "symbol": self._pair(symbol),
            "side": side.lower(),   # "buy"/"sell"
            "type": "limit",
            "price": str(round(float(price), 8)),
            "size": str(round(float(size), 8)),
            "timeInForce": tif,
            "postOnly": bool(post_only),
            "hidden": bool(hidden),
            "iceberg": bool(iceberg),
        }
        data, _ = kucoin_auth("POST", path, None, body)
        if data.get("code") == "200000":
            od = data.get("data") or {}
            if "orderId" in od:
                return str(od["orderId"])
        raise ValueError(f"Unexpected KuCoin response: {data}")

    def get_order(self, order_id: str):
        d, _ = kucoin_auth("GET", f"/api/v1/orders/{order_id}")
        return d

    def cancel_order(self, order_id: str):
        d, _ = kucoin_auth("DELETE", f"/api/v1/orders/{order_id}")
        return d
    
    def create_market_order(self, symbol: str, side: str, size: float) -> str:
        """
        Places a market order.
        Args:
            symbol: The trading pair (e.g., 'BTC-USDT')
            side: 'buy' or 'sell'
            size: The amount of the base currency to buy/sell
        Returns:
            str: The order ID from KuCoin
        Raises:
            ValueError: If the order placement fails.
        """
        path = "/api/v1/orders"
        body = {
            "clientOid": str(int(time.time() * 1000)),
            "symbol": self._pair(symbol),
            "side": side.lower(),
            "type": "market",
            "size": str(round(float(size), 8)),
        }
        data, _ = kucoin_auth("POST", path, None, body)
        if data.get("code") == "200000":
            od = data.get("data") or {}
            if "orderId" in od:
                return str(od["orderId"])
        raise ValueError(f"Failed to place market order: {data}")

    # ── Futures (funding rate helper) ─────────────────────────────────

    def funding_rate(self, symbol: str) -> float:
        """
        Maps spot symbol like BTC-USDT to futures perpetual (e.g., XBTUSDTM or BTCUSDTM-PERP),
        then fetches current funding rate. Best-effort; returns 0.0 if not found.
        """
        try:
            # Fetch active contracts via same proxy
            r = requests.get(f"{FUTURES_URL}/api/v1/contracts/active", timeout=10, proxies=PROXY_CONFIG)
            r.raise_for_status()
            contracts = r.json().get("data", [])

            base = symbol.split("-")[0].upper()

            fut_symbol = None
            for c in contracts:
                s = (c.get("symbol") or "").upper()
                # heuristics: endswith PERP; name formats differ
                if s.startswith(base) and "PERP" in s:
                    fut_symbol = c.get("symbol")
                    break

            if not fut_symbol:
                return 0.0

            r2 = requests.get(f"{FUTURES_URL}/api/v1/funding-rate/{fut_symbol}", timeout=10, proxies=PROXY_CONFIG)
            r2.raise_for_status()
            data = r2.json().get("data") or {}
            return float(data.get("fundingRate", 0.0))
        except Exception:
            return 0.0


# ── Public helpers ─────────────────────────────────────────────────────

def public_products():
    d, _ = kucoin_auth("GET", "/api/v1/symbols")
    return [item["symbol"] for item in d.get("data", [])]

def test_proxy_connection():
    try:
        test_url = "https://api.infoip.io/"
        resp = requests.get(test_url, proxies=PROXY_CONFIG, timeout=10)
        ip_info = resp.json()
        country = ip_info.get("country", "unknown")
        logger.info("Proxy test successful. Connection from: %s", country)
        return True
    except Exception as e:
        # Include traceback so you can see DNS/proxy errors in logs
        logger.critical("Proxy test failed: %s", e, exc_info=True)
        return False