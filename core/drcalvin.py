#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# Helen
# mm/helen.py
#
# Prefilters for USDT pairs matching volume requirements.
# Level I scoring.  
# Approving manager for proposals.
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import time
from tqdm import tqdm
from datetime import datetime 

import logging
logger = logging.getLogger("Ariadne")

# 🔸 Application Imports ===========================================

from mm.utils.helpers import inara
from mm.config.marcus import (
    QUOTE_CURRENCY,
    MIN_24H_VOLUME,
    MAX_24H_VOLUME,
    MIN_COIN_AGE,
    SPREAD_TIGHTNESS,
    ORDER_BOOK_DEPTH,
    SLIPPAGE_RESISTANCE,
    VOLATILITY_PROFILE,
    VOLUME_CONSISTENCY,
    PRICE_STABILITY,
    FEE_EFFICIENCY,
    EXECUTION_SPEED,
    MARKET_IMPACT,
    OPPORTUNITY_MOD,
    MIN_LIQUIDITY_SCORE
)

class Level_I:
    def __init__(self, config):
        self.cfg = config
        self.mode = inara.get_mode()
        self.client = inara.get_trading_client()
        
# 🔸 Prefilter =====================================================

    def pre_filter_pairs(self, client, all_pairs: List[str]) -> List[str]:
        try:
            all_tickers = client.get_all_tickers()
            if not all_tickers:
                return all_pairs
            
            ticker_dict = {item['symbol']: item for item in all_tickers}
            filtered_pairs = []

            for pair in all_pairs:
                if not pair.endswith(f"-{QUOTE_CURRENCY}"):
                    continue

                kucoin_symbol = client._pair(pair)
                ticker = ticker_dict.get(kucoin_symbol)

                if not ticker:
                    continue

                vol_value = float(ticker.get('volValue', 0))
                if vol_value < MIN_24H_VOLUME or vol_value > MAX_24H_VOLUME:
                    continue

                filtered_pairs.append(pair)

            mature_pairs = []
            maturity_window = int(time.time()) - (MIN_COIN_AGE * 86400)

            for pair in filtered_pairs:
                kucoin_symbol = client._pair(pair)
                candles = client.historical_ohlcv(kucoin_symbol, "1day", 8)

                if candles:
                    for candle in candles:
                        candle_timestamp = int(candle[0])
                        if maturity_window - 86400 <= candle_timestamp <= maturity_window:
                            mature_pairs.append(pair)
                            break

            return mature_pairs

        except Exception as e:
            logger.error(f"Error in prefiltering: {e}")
            return all_pairs
        
# 🔸 Scoring =======================================================

    def score_pair(self, symbol: str, client, historical_data=None) -> Dict[str, Any]:
        scores = {}
        category_breakdown = {}

        try:
            # ── Sub scores (all return 0..100) ───────────────────────────
            # LIQUIDITY (max 40 pts globally)
            spread_pct   = self._calculate_spread_score(symbol, client)            # 0..100
            depth_pct    = self._calculate_depth_score(symbol, client)             # 0..100
            slippage_pct = self._calculate_slippage_score(symbol, client)          # 0..100

            # MARKET (max 30 pts globally)
            volatility_pct = self._calculate_volatility_score(symbol, client, historical_data)  # 0..100
            volume_pct     = self._calculate_volume_consistency(symbol, client)                 # 0..100
            price_pct      = self._calculate_price_stability(symbol, client)                   # 0..100

            # TRADING (max 30 pts globally)
            fee_pct      = self._calculate_fee_efficiency(symbol, client)        # 0..100
            exec_pct     = self._calculate_execution_speed(symbol, client)       # 0..100
            impact_pct   = self._calculate_market_impact(symbol, client)         # 0..100

            # ── Convert % → global points via weights (no double-weighting) ─
            # Contribution (pts) = sub_weight * sub_pct
            spread_pts   = SPREAD_TIGHTNESS    * spread_pct
            depth_pts    = ORDER_BOOK_DEPTH    * depth_pct
            slippage_pts = SLIPPAGE_RESISTANCE * slippage_pct

            volatility_pts = VOLATILITY_PROFILE * volatility_pct
            volume_pts     = VOLUME_CONSISTENCY * volume_pct
            price_pts      = PRICE_STABILITY    * price_pct

            fee_pts    = FEE_EFFICIENCY  * fee_pct
            exec_pts   = EXECUTION_SPEED * exec_pct
            impact_pts = MARKET_IMPACT   * impact_pct

            # Category totals in points (Liquidity max 40, etc.)
            liquidity_points = spread_pts + depth_pts + slippage_pts
            market_points    = volatility_pts + volume_pts + price_pts
            trading_points   = fee_pts + exec_pts + impact_pts

            # Base score (0..100)
            base_score = liquidity_points + market_points + trading_points

            # ── Opportunity bonus (0..5 points) ──────────────────────────
            # imbalance_score is 0..100; OPPORTUNITY_MOD is 0.05 ⇒ +0..5 pts
            imbalance_score = self._calculate_order_book_imbalance(symbol, client)  # 0..100
            opportunity_boost = max(0.0, min(imbalance_score * OPPORTUNITY_MOD, 5.0))

            # Final can be 0..105 by design
            total_score = max(0.0, min(base_score + opportunity_boost, 105.0))

            # For convenience, keep simple “category scores” as points
            scores['liquidity_quality'] = liquidity_points
            scores['market_stability']  = market_points
            scores['trading_quality']   = trading_points

            # Breakdown: raw sub % and contribution pts
            category_breakdown['liquidity'] = {
                'total_points': liquidity_points,
                'subs': {
                    'spread':   {'pct': spread_pct,   'pts': spread_pts},
                    'depth':    {'pct': depth_pct,    'pts': depth_pts},
                    'slippage': {'pct': slippage_pct, 'pts': slippage_pts},
                }
            }
            category_breakdown['market'] = {
                'total_points': market_points,
                'subs': {
                    'volatility': {'pct': volatility_pct, 'pts': volatility_pts},
                    'volume':     {'pct': volume_pct,     'pts': volume_pts},
                    'price':      {'pct': price_pct,      'pts': price_pts},
                }
            }
            category_breakdown['trading'] = {
                'total_points': trading_points,
                'subs': {
                    'fee':     {'pct': fee_pct,    'pts': fee_pts},
                    'execution': {'pct': exec_pct, 'pts': exec_pts},
                    'impact': {'pct': impact_pct,  'pts': impact_pts},
                }
            }

            """
            # ── DEBUG ─────────────────────────────────────────────────────
            logger.debug(f"\n{symbol}")
            logger.debug(
                "LIQUIDITY | "
                f"{liquidity_points:5.2f} "
                f"[ Spread: {spread_pct:5.1f}% → {spread_pts:5.2f} ] "
                f"[ Depth: {depth_pct:5.1f}% → {depth_pts:5.2f} ] "
                f"[ Slippage: {slippage_pct:5.1f}% → {slippage_pts:5.2f} ]"
            )
            logger.debug(
                "MARKET    | "
                f"{market_points:5.2f} "
                f"[ Volatility: {volatility_pct:5.1f}% → {volatility_pts:5.2f} ] "
                f"[ Volume: {volume_pct:5.1f}% → {volume_pts:5.2f} ] "
                f"[ Price: {price_pct:5.1f}% → {price_pts:5.2f} ]"
            )
            logger.debug(
                "TRADING   | "
                f"{trading_points:5.2f} "
                f"[ Fee: {fee_pct:5.1f}% → {fee_pts:5.2f} ] "
                f"[ Execution: {exec_pct:5.1f}% → {exec_pts:5.2f} ] "
                f"[ Impact: {impact_pct:5.1f}% → {impact_pts:5.2f} ]"
            )
            logger.debug(f"MODIFIER  | imbalance={imbalance_score:.1f}% → +{opportunity_boost:.2f} pts")
            logger.debug(f"TOTAL     | base={base_score:.2f}  final={total_score:.2f}")
            """
            
            return {
                'total_score': total_score,                   # 0..105
                'base_score': base_score,                     # 0..100
                'opportunity_boost': opportunity_boost,       # 0..5
                'category_scores': scores,                    # per-category pts
                'category_breakdown': category_breakdown,     # detailed
                'grade': self._score_to_grade(total_score),
                'passed': total_score >= MIN_LIQUIDITY_SCORE,
            }

        except Exception as e:
            logger.error(f"❌ Error scoring {symbol}: {e}")
            return {
                'total_score': 0,
                'base_score': 0,
                'opportunity_boost': 0,
                'category_scores': {},
                'category_breakdown': {},
                'grade': 'F',
                'passed': False
            }


# 🔸 Calculations ==================================================
    
    # 🔹 Liquidity Calculations ====================================
    
    def _calculate_spread_score(self, symbol: str, client) -> float:
        try:
            orderbook = client.order_book(symbol, depth=2)
            if not orderbook['bids'] or not orderbook['asks']:
                return 0

            best_bid, best_ask = orderbook['bids'][0][0], orderbook['asks'][0][0]
            mid_price = (best_bid + best_ask) / 2
            spread_pct = (best_ask - best_bid) / mid_price

            # Tight spread = high score (0.1% spread = 100, 1% spread = 0)
            ideal_spread = 0.001  # 0.1%
            max_acceptable = 0.01  # 1%

            if spread_pct <= ideal_spread:
                return 100
            elif spread_pct >= max_acceptable:
                return 0
            else:
                return 100 * (1 - (spread_pct - ideal_spread) / (max_acceptable - ideal_spread))

        except Exception:
            return 0

    def _calculate_depth_score(self, symbol: str, client) -> float:
        try:
            orderbook = client.order_book(symbol, depth=10)
            if not orderbook['bids'] or not orderbook['asks']:
                return 0

            # Calculate total depth in USD for top 10 levels
            mid_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
            bid_depth = sum(amount for price, amount in orderbook['bids'][:10])
            ask_depth = sum(amount for price, amount in orderbook['asks'][:10])
            total_depth = (bid_depth + ask_depth) * mid_price

            # Score based on depth (100 = $100k+, 0 = $0)
            min_depth = 0
            max_depth = 100000  # $100k
            return min(100, 100 * (total_depth - min_depth) / (max_depth - min_depth))

        except Exception:
            return 0

    def _calculate_slippage_score(self, symbol: str, client) -> float:
        try:
            orderbook = client.order_book(symbol, depth=20)
            if not orderbook['bids'] or not orderbook['asks']:
                return 0

            # Calculate slippage for a $10k order
            order_size = 10000
            mid_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2

            # Simulate buy order slippage
            filled = 0
            avg_buy_price = 0
            for price, amount in orderbook['asks']:
                fill_amount = min(amount, (order_size - filled) / price)
                avg_buy_price += fill_amount * price
                filled += fill_amount
                if filled * price >= order_size:
                    break

            buy_slippage = (avg_buy_price / filled - mid_price) / mid_price if filled > 0 else 0

            # Score based on slippage (0% = 100, 2% = 0)
            max_slippage = 0.02  # 2%
            return max(0, 100 * (1 - buy_slippage / max_slippage))

        except Exception:
            return 0
        
    # 🔹 Market Calculations =======================================
    
    def _calculate_volatility_score(self, symbol: str, client, historical_data=None) -> float:
        try:
            if not historical_data:
                historical_data = client.historical_ohlcv(symbol, "1h", 24)

            if len(historical_data) < 12:  # Need at least 12 hours of data
                return 50  # Neutral score

            closing_prices = [float(candle[2]) for candle in historical_data if float(candle[2]) > 0]
            if len(closing_prices) < 2:
                return 50

            returns = np.diff(closing_prices) / closing_prices[:-1]
            volatility = float(np.std(returns))

            # Score: 0% volatility = 100, 5% volatility = 0
            ideal_volatility = 0.00
            max_volatility = 0.05  # 5%

            if volatility <= ideal_volatility:
                return 100
            elif volatility >= max_volatility:
                return 0
            else:
                return 100 * (1 - (volatility - ideal_volatility) / (max_volatility - ideal_volatility))

        except Exception:
            return 50  # Neutral on error

    def _calculate_volume_consistency(self, symbol: str, client) -> float:
        try:
            # Get recent volume data (last 24 hours)
            candles = client.historical_ohlcv(symbol, "1h", 24)
            if len(candles) < 12:
                return 50

            volumes = [float(candle[5]) for candle in candles]  # Volume is index 5
            if not any(volumes):
                return 0

            # Calculate coefficient of variation (lower = more consistent)
            mean_volume = np.mean(volumes)
            std_volume = np.std(volumes)
            cv = std_volume / mean_volume if mean_volume > 0 else 1.0

            # Score: 0 CV = 100 (perfect consistency), 1 CV = 0 (high variation)
            return max(0, 100 * (1 - min(cv, 1.0)))

        except Exception:
            return 50

    def _calculate_price_stability(self, symbol: str, client) -> float:
        try:
            # Check for recent large price movements
            trades = client.get_recent_trades(symbol, limit=100)
            if len(trades) < 20:
                return 50

            prices = [float(trade['price']) for trade in trades]
            max_price = max(prices)
            min_price = min(prices)
            price_range = (max_price - min_price) / min_price

            # Score based on price range in recent trades
            # 0% range = 100, 10% range = 0
            max_acceptable_range = 0.10  # 10%
            return max(0, 100 * (1 - min(price_range / max_acceptable_range, 1.0)))

        except Exception:
            return 50
    
    # 🔹 Trading Calculations ======================================
    
    def _calculate_fee_efficiency(self, symbol: str, client) -> float:
        try:
            fee_info = client.maker_fee(symbol)
            maker_fee = fee_info.get('value', 0.001)  # Default 0.1%

            # Score: 0% fee = 100, 0.2% fee = 0
            ideal_fee = 0.00
            max_fee = 0.002  # 0.2%

            if maker_fee <= ideal_fee:
                return 100
            elif maker_fee >= max_fee:
                return 0
            else:
                return 100 * (1 - (maker_fee - ideal_fee) / (max_fee - ideal_fee))

        except Exception:
            return 50

    def _calculate_execution_speed(self, symbol: str, client) -> float:
        try:
            trades = client.get_recent_trades(symbol, limit=50)
            if len(trades) < 10:
                return 30  # Low activity = poor execution

            # More trades = better liquidity = faster execution
            trade_count = len(trades)
            return min(100, trade_count * 2)  # 50 trades = 100 score

        except Exception:
            return 50

    def _calculate_market_impact(self, symbol: str, client) -> float:
        try:
            orderbook = client.order_book(symbol, depth=5)
            if not orderbook['bids'] or not orderbook['asks']:
                return 0

            # Estimate impact of a $1000 order
            order_size = 1000
            mid_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2

            # Calculate price impact on ask side
            filled = 0
            total_cost = 0
            for price, amount in orderbook['asks']:
                fill_amount = min(amount, (order_size - filled) / price)
                total_cost += fill_amount * price
                filled += fill_amount
                if filled * price >= order_size:
                    break

            avg_price = total_cost / filled if filled > 0 else mid_price
            impact = (avg_price - mid_price) / mid_price

            # Score: 0% impact = 100, 1% impact = 0
            return max(0, 100 * (1 - min(impact / 0.01, 1.0)))

        except Exception:
            return 50
    
    # 🔹 Opportunity Mod Calculations ==============================

    def _calculate_order_book_imbalance(self, symbol: str, client) -> float:
        try:
            orderbook = client.order_book(symbol, depth=10)
            if not orderbook['bids'] or not orderbook['asks']:
                return 0

            bid_depth = sum(amount for price, amount in orderbook['bids'][:10])
            ask_depth = sum(amount for price, amount in orderbook['asks'][:10])

            ratio = max(bid_depth, ask_depth) / min(bid_depth, ask_depth)

            if 1.0 <= ratio <= 1.5:
                return 50     # Balanced
            elif 1.5 < ratio <= 3.0:
                return 75     # Moderate imbalance = opportunity
            elif 3.0 < ratio <= 5.0:
                return 25     # Concerning imbalance
            else:
                return 0      # Extreme imbalance = avoid

        except Exception:
            return 0

# 🔸 Scoring ==================================================

    def _score_to_grade(self, score: float) -> str:
        if score >= 90: return 'A+'
        elif score >= 85: return 'A'
        elif score >= 80: return 'A-'
        elif score >= 75: return 'B+'
        elif score >= 70: return 'B'
        elif score >= 65: return 'B-'
        elif score >= 60: return 'C+'
        elif score >= 50: return 'C'
        elif score >= 40: return 'D'
        else: return 'F'
