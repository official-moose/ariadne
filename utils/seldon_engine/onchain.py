#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> On-Chain Signals (The Seldon Engine)
#>> mm/utils/seldon_engine/onchain.py
#>>
#>> On-chain signals
#>> Returns a dict with subscores and final onchain_score
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

import requests
import time
import logging

logger = logging.getLogger("Quorra")

# ── Configurable weights ─────────────────────────────
ONCHAIN_ENDPOINTS = {
    "exchange_flows": "https://api.coinglass.com/api/pro/v1/futures/open_interest_chart?symbol={symbol}",
    "whale_alerts": "https://api.whale-alert.io/v1/transactions?api_key=YOUR_KEY&min_value=1000000&currency={symbol}",
    "gas_tracker": "https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey=YOUR_KEY",
    "dex_liquidity": "https://api.geckoterminal.com/api/v2/networks/eth/pools/{pool_id}",
    "dormant_wallets": "https://api.glassnode.com/v1/metrics/addresses/supply_last_active_more_1y_percent?api_key=YOUR_KEY&asset={symbol}"
}


# ── Core Scoring Functions ──────────────────────────

def score_exchange_flows(symbol: str) -> int:
    try:
        url = ONCHAIN_ENDPOINTS["exchange_flows"].format(symbol=symbol.upper())
        r = requests.get(url)
        data = r.json()
        # Simulate a score: large inflows are bad (0–40), large outflows good (60–100)
        flow_delta = data.get("data", {}).get("netInflow", 0)
        if flow_delta > 0:
            return max(0, 40 - int(flow_delta / 10))
        else:
            return min(100, 60 + abs(int(flow_delta / 10)))
    except Exception as e:
        logger.warning(f"exchange_flows error: {e}")
        return 50

def score_whale_activity(symbol: str) -> int:
    try:
        url = ONCHAIN_ENDPOINTS["whale_alerts"].format(symbol=symbol.upper())
        r = requests.get(url)
        data = r.json()
        count = len(data.get("transactions", []))
        if count > 10:
            return 20  # lots of whale action
        elif count > 5:
            return 40
        else:
            return 70
    except Exception as e:
        logger.warning(f"whale_activity error: {e}")
        return 50

def score_gas_mempool() -> int:
    try:
        r = requests.get(ONCHAIN_ENDPOINTS["gas_tracker"])
        data = r.json().get("result", {})
        fast = int(data.get("FastGasPrice", 50))
        # Lower gas = more stable, higher = congestion = bearish
        if fast > 80:
            return 30
        elif fast > 60:
            return 50
        else:
            return 75
    except Exception as e:
        logger.warning(f"gas_tracker error: {e}")
        return 50

def score_dormant_wallets(symbol: str) -> int:
    try:
        url = ONCHAIN_ENDPOINTS["dormant_wallets"].format(symbol=symbol.upper())
        r = requests.get(url)
        pct = float(r.json()["data"][-1][1])
        # If too many old wallets are waking up — bearish
        if pct < 30:
            return 75
        elif pct < 40:
            return 50
        else:
            return 25
    except Exception as e:
        logger.warning(f"dormant_wallets error: {e}")
        return 50


# Placeholder for DEX liquidity — depends on platform integration

def score_dex_liquidity(pool_id: str = "") -> int:
    try:
        if not pool_id:
            return 50  # neutral if no context
        url = ONCHAIN_ENDPOINTS["dex_liquidity"].format(pool_id=pool_id)
        r = requests.get(url)
        tvl = float(r.json()["data"]["attributes"]["reserve_in_usd"])
        if tvl > 10_000_000:
            return 80
        elif tvl > 5_000_000:
            return 60
        else:
            return 40
    except Exception as e:
        logger.warning(f"dex_liquidity error: {e}")
        return 50


def get_onchain_score(symbol: str, pool_id: str = "") -> int:
    scores = {
        "exchange_flows": score_exchange_flows(symbol),
        "whale_activity": score_whale_activity(symbol),
        "gas_mempool": score_gas_mempool(),
        "dormant_wallets": score_dormant_wallets(symbol),
        "dex_liquidity": score_dex_liquidity(pool_id),
    }
    total = round(sum(scores.values()) / len(scores))
    scores["onchain_score"] = total
    return scores
