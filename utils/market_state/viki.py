#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> Viki
#>> mm/utils/market_state/viki.py
#>>
#>> Classifies the current market regime for a given 
#>> symbol using exchange data (candles, order book) 
#>> and derives a label + confidence.
#>> Designed to be called by Quorra (risk scoring) 
#>> and Hari (simulation realism).
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────

# Build|20250904.01

from __future__ import annotations
import math
import statistics
import logging
import json
from typing import Dict, List, Tuple

from mm.conn.conn_kucoin import KucoinClient
from mm.utils.market_state.viki_config import (
    LOGGING,
    LOG_PATH,
    RSI_LEN,
    BOLL_LEN,
    ATR_LEN,
    LOOKBACK_SHORT,
    LOOKBACK_LONG,
    PANIC_THRESH,
    CAPITULATION_THRESH,
    EUPHORIA_THRESH,
    STALE_THRESH,
    WOLF_THRESH,
    CORRECTION_DROP_PCT,
)
from mm.utils.helpers.inara import get_mode

logger = logging.getLogger("MarketState")

# KuCoin candle format: [time, open, close, high, low, volume, turnover]
OPEN=1; CLOSE=2; HIGH=3; LOW=4; VOL=5


class MarketState:
    """
    Determines market regime for a symbol from recent data.

    Methods
    -------
    classify(symbol: str) -> Dict
        Returns { 'symbol', 'mode', 'state', 'confidence', 'evidence': {...} }
    """

    def __init__(self):
        self.client = KucoinClient()

    # ── Public API ───────────────────────────────────────────────

    def classify(self, symbol: str) -> Dict:
        """Compute market state and confidence for a symbol.
        Returns a dict suitable for logging and programmatic use.
        """
        sym = symbol.upper()

        # Data pulls
        c15 = self._get_candles(sym, "15m", max(LOOKBACK_LONG, 100))  # 15m granularity
        c1h = self._get_candles(sym, "1h",  max(LOOKBACK_LONG, 100))  # hourly context
        c1d = self._get_candles(sym, "1d",  60)                       # multi-day context
        book = self.client.order_book(sym, depth=10)

        # Feature extraction
        features = {}
        features.update(self._price_features(c15, c1h, c1d))
        features.update(self._vol_features(c15))
        features.update(self._band_features(c15))
        features.update(self._orderbook_features(book))

        # State scoring
        scores = {
            "panic":        self._score_panic(features),
            "capitulation": self._score_capitulation(features),
            "euphoria":     self._score_euphoria(features),
            "stale":        self._score_stale(features),
            "wolf":         self._score_wolf(features),
            "correction":   self._score_correction(features),
            "normal":       0.0,  # filled below as 100 - max others if low signal
        }
        max_label, max_score = max(scores.items(), key=lambda kv: kv[1])
        if max_score < 25:
            # If nothing is strong, treat as normal; confidence = 100 - sum of variances
            scores["normal"] = 60.0
            max_label, max_score = "normal", 60.0

        out = {
            "symbol": sym,
            "mode": get_mode(),
            "state": max_label,
            "confidence": round(float(max_score), 1),
            "evidence": {**features, "state_scores": scores},
        }

        if LOGGING:
            self._log(out)
        return out

    # ── Data helpers ─────────────────────────────────────────────

    def _get_candles(self, symbol: str, timeframe: str, limit: int) -> List[List[float]]:
        arr = self.client.historical_ohlcv(symbol, timeframe, limit)
        # KuCoin returns newest first; reverse to chronological
        return list(reversed(arr))

    # ── Feature engineering ──────────────────────────────────────

    def _price_features(self, c15: List[List[float]], c1h: List[List[float]], c1d: List[List[float]]) -> Dict:
        f: Dict[str, float] = {}

        def closes(c):
            return [float(x[CLOSE]) for x in c]

        def highs(c):
            return [float(x[HIGH]) for x in c]

        def lows(c):
            return [float(x[LOW]) for x in c]

        cl15 = closes(c15)
        cl1h = closes(c1h)
        cl1d = closes(c1d)

        # Returns
        def pct(a,b):
            return 0.0 if b==0 else (a/b - 1.0) * 100.0

        f["ret_15m"] = pct(cl15[-1], cl15[max(0, -LOOKBACK_SHORT)])
        f["ret_1h"]  = pct(cl1h[-1],  cl1h[max(0, -LOOKBACK_SHORT)])
        f["ret_4h"]  = pct(cl1h[-1],  cl1h[max(0, -min(4, len(cl1h)-1))])

        # Drawdown from recent highs
        recent_high_1d = max(highs(c1d)) if c1d else cl1h[-1]
        f["drop_from_1d_high_pct"] = pct(cl1h[-1], recent_high_1d)  # negative if below high

        # RSI 14 on 15m
        f["rsi_15m"] = self._rsi(cl15, RSI_LEN)

        # Reversal frequency (sign flips) last N
        flips = 0
        for i in range(1, min(LOOKBACK_SHORT, len(cl15))):
            if (cl15[i] - cl15[i-1]) * (cl15[i-1] - cl15[i-2] if i-2>=0 else 0) < 0:
                flips += 1
        f["sign_flips_15m"] = float(flips)

        # True range proxy over 15m
        tr = []
        prev_close = None
        for x in c15:
            high, low, close = float(x[HIGH]), float(x[LOW]), float(x[CLOSE])
            if prev_close is None:
                tr.append(high - low)
            else:
                tr.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            prev_close = close
        f["atr_15m"] = statistics.fmean(tr[-ATR_LEN:]) if len(tr) >= ATR_LEN else statistics.fmean(tr) if tr else 0.0

        # Net change tightness (for wolf/stale discrimination)
        f["net_change_15m"] = cl15[-1] - cl15[-LOOKBACK_SHORT] if len(cl15) > LOOKBACK_SHORT else cl15[-1] - cl15[0]

        return f

    def _vol_features(self, c15: List[List[float]]) -> Dict:
        f: Dict[str, float] = {}
        vols = [float(x[VOL]) for x in c15]
        if not vols:
            f.update({"vol_z": 0.0, "vol_surge_ratio": 0.0})
            return f
        m = statistics.fmean(vols)
        s = statistics.pstdev(vols) or 1.0
        f["vol_z"] = (vols[-1] - m) / s
        base = statistics.fmean(vols[-max(5, min(20, len(vols)//3)):]) or 1.0
        f["vol_surge_ratio"] = vols[-1] / base
        return f

    def _band_features(self, c15: List[List[float]]) -> Dict:
        f: Dict[str, float] = {}
        closes = [float(x[CLOSE]) for x in c15]
        if len(closes) < BOLL_LEN:
            f.update({"bb_pos": 0.0, "bb_width_pct": 0.0})
            return f
        ma = statistics.fmean(closes[-BOLL_LEN:])
        std = statistics.pstdev(closes[-BOLL_LEN:]) or 1e-9
        upper = ma + 2*std
        lower = ma - 2*std
        last = closes[-1]
        # Position inside bands: <0 below lower, >1 above upper
        bb_pos = (last - lower) / (upper - lower)
        f["bb_pos"] = bb_pos
        f["bb_width_pct"] = (upper - lower) / last * 100.0
        return f

    def _orderbook_features(self, book: Dict) -> Dict:
        f: Dict[str, float] = {}
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        bid_px = float(bids[0][0]) if bids else 0.0
        ask_px = float(asks[0][0]) if asks else 0.0
        mid = (bid_px + ask_px) / 2 if bid_px and ask_px else 0.0
        spread = (ask_px - bid_px) / mid * 100.0 if mid else 0.0
        bid_vol = sum(v for _, v in bids)
        ask_vol = sum(v for _, v in asks)
        imb = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-9)
        f["spread_pct"] = spread
        f["book_imbalance"] = imb
        return f

    # ── Indicator calcs ──────────────────────────────────────────

    def _rsi(self, closes: List[float] | List[List[float]], length: int = 14) -> float:
        if closes and isinstance(closes[0], list):
            closes = [float(x[CLOSE]) for x in closes]  # type: ignore
        if len(closes) <= length:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i-1]
            if delta >= 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-delta)
        avg_gain = statistics.fmean(gains[-length:]) or 1e-9
        avg_loss = statistics.fmean(losses[-length:]) or 1e-9
        rs = avg_gain / avg_loss if avg_loss else float('inf')
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    # ── State scorers ────────────────────────────────────────────

    def _score_panic(self, f: Dict[str, float]) -> float:
        score = 0.0
        # Strong negative returns + volume spike + widening spread
        if f.get("ret_15m", 0.0) <= PANIC_THRESH["ret_15m"]:
            score += min(40.0, abs(f["ret_15m"]) * 4)
        if f.get("vol_z", 0.0) >= PANIC_THRESH["vol_z_min"]:
            score += min(30.0, (f["vol_z"] - PANIC_THRESH["vol_z_min"]) * 8)
        if f.get("spread_pct", 0.0) >= PANIC_THRESH["spread_pct_min"]:
            score += min(20.0, (f["spread_pct"] - PANIC_THRESH["spread_pct_min"]) * 2)
        if f.get("book_imbalance", 0.0) < -0.2:
            score += 10.0
        return max(0.0, min(100.0, score))

    def _score_capitulation(self, f: Dict[str, float]) -> float:
        score = 0.0
        if f.get("ret_1h", 0.0) <= CAPITULATION_THRESH["ret_1h"]:
            score += min(40.0, abs(f["ret_1h"]) * 3)
        if f.get("drop_from_1d_high_pct", 0.0) <= -CORRECTION_DROP_PCT:
            score += 20.0
        if f.get("vol_z", 0.0) >= CAPITULATION_THRESH["vol_z_min"]:
            score += min(25.0, (f["vol_z"] - CAPITULATION_THRESH["vol_z_min"]) * 6)
        if f.get("rsi_15m", 50.0) <= CAPITULATION_THRESH["rsi_max"]:
            score += 15.0
        if f.get("bb_pos", 0.5) < 0.0:  # below lower band decisively
            score += 10.0
        return max(0.0, min(100.0, score))

    def _score_euphoria(self, f: Dict[str, float]) -> float:
        score = 0.0
        if f.get("ret_1h", 0.0) >= EUPHORIA_THRESH["ret_1h"]:
            score += min(40.0, f["ret_1h"] * 3)
        if f.get("vol_z", 0.0) >= EUPHORIA_THRESH["vol_z_min"]:
            score += min(25.0, (f["vol_z"] - EUPHORIA_THRESH["vol_z_min"]) * 6)
        if f.get("rsi_15m", 50.0) >= EUPHORIA_THRESH["rsi_min"]:
            score += 15.0
        if f.get("bb_pos", 0.5) > 1.0:  # above upper band
            score += 10.0
        return max(0.0, min(100.0, score))

    def _score_stale(self, f: Dict[str, float]) -> float:
        score = 0.0
        # Low volatility, narrow band width, low volume, tight spread
        if abs(f.get("ret_15m", 0.0)) < STALE_THRESH["ret_abs_max"]:
            score += 25.0
        if f.get("bb_width_pct", 100.0) < STALE_THRESH["bb_width_max"]:
            score += 25.0
        if f.get("vol_surge_ratio", 1.0) < STALE_THRESH["vol_ratio_max"]:
            score += 25.0
        if f.get("spread_pct", 100.0) < STALE_THRESH["spread_pct_max"]:
            score += 25.0
        return max(0.0, min(100.0, score))

    def _score_wolf(self, f: Dict[str, float]) -> float:
        score = 0.0
        # High intraday volatility (ATR or band width), many sign flips, small net change
        if f.get("bb_width_pct", 0.0) >= WOLF_THRESH["bb_width_min"]:
            score += min(35.0, (f["bb_width_pct"] - WOLF_THRESH["bb_width_min"]) * 1.0)
        if f.get("sign_flips_15m", 0.0) >= WOLF_THRESH["flips_min"]:
            score += min(35.0, (f["sign_flips_15m"] - WOLF_THRESH["flips_min"]) * 4)
        if abs(f.get("net_change_15m", 0.0)) <= WOLF_THRESH["net_change_abs_max"]:
            score += 30.0
        return max(0.0, min(100.0, score))

    def _score_correction(self, f: Dict[str, float]) -> float:
        score = 0.0
        # 10%+ off recent highs and negative return context
        if f.get("drop_from_1d_high_pct", 0.0) <= -CORRECTION_DROP_PCT:
            score += 60.0
        if f.get("ret_4h", 0.0) < 0:
            score += min(40.0, abs(f["ret_4h"]))
        return max(0.0, min(100.0, score))

    # ── Logging ─────────────────────────────────────────────────

    def _log(self, payload: Dict) -> None:
        try:
            with open(LOG_PATH, "a") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception as e:
            logger.warning(f"MarketState logging error: {e}")
