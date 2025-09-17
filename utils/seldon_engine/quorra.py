#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250904.01
#===================================================================
# last update: 2025 | Sept. 4                   Production ready ❌
#===================================================================
# Quorra (The Seldon Engine)
# mm/utils/seldon_engine/quorra.pyy
#
# This is an intuitive engine, not an oracle. It does not predict 
# price. It assists in gauging relative risk/reward across 
# multiple technical, market, and soon, on-chain dimensions. 
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import logging
import json
from mm.utils.seldon_engine import signals
from mm.utils.seldon_engine.onchain import get_onchain_score
from mm.utils.seldon_engine.seldon_config import LOGGING, SCORING_PATH, WEIGHTS
from mm.utils.helpers import inara

# 🔸 Application Imports ===========================================

logger = logging.getLogger("Quorra") 

class SigmaOps:
    def __init__(self):
        self.weights = WEIGHTS
        self.mode = inara.get_mode()
        self.client = inara.get_trading_client()

    def intuit(self, base: str, quote: str) -> float:
        symbol = f"{base.upper()}-{quote.upper()}"
        mode = get_mode()

        # 🔹 Get on-chain score and unpack breakdown ===============
        
        onchain_result = get_onchain_score(symbol)
        onchain_total = onchain_result.get("onchain_score", 0)

        # 🔹 Gather signal results (dicts with subscores + final) ==
        
        signals_data = {
            "trend": signals.trend_strength(symbol),
            "volatility": signals.volatility_status(symbol),
            "patterns": signals.pattern_alignment(symbol),
            "momentum": signals.momentum_confidence(symbol),
            "orderbook": signals.orderbook_balance(symbol),
            "volume_spike": signals.volume_surge(symbol),
            "candlestick": signals.candle_alerts(symbol),
        }

        # 🔹 Attach onchain as dict ================================
        signals_data["onchain"] = {
            **onchain_result,
            "final": onchain_total
        }

        # 🔹 Compute weighted score using .get("final") values =====
        weighted_score = 0
        for key, val in signals_data.items():
            score = val.get("final", 0)
            weighted_score += score * self.weights.get(key, 1)

        score = max(0, min(100, round(weighted_score)))

        if LOGGING:
            self._log_debug(symbol, signals_data, score)

        return score

    def _log_debug(self, symbol: str, breakdowns: dict, total_score: int):
        data = {
            "symbol": symbol,
            "mode": get_mode(),
            "score": total_score,
            "details": breakdowns
        }
        try:
            with open(SCORING_PATH, "a") as f:
                f.write(json.dumps(data) + "\n")
        except Exception as e:
            logger.warning(f"Quorra logging error: {e}")

