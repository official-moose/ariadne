#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> Simulated Market Place
#>> mm/utils/soc/hari.py
#>>
#>> Checks simulated orders and mimicks the market process. 
#>> Applies "realisms" to keep me humble.
#>> Records trade, releases sim funds. 
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250905.01

import os
import sys
import time
import signal 
import random
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
from logging.handlers import RotatingFileHandler

from mm.utils.helpers.wintermute import get_db_connection, release_db_connection
from mm.utils.helpers.inara import get_mode

# ── Constants ─────────────────────────────────────────────────────────
CHECK_INTERVAL_SECONDS = 5
HEARTBEAT_CYCLES = 12  # Every 60 seconds
REALISM_CHANCE = 0.15  # 15% chance
REALISM_COOLDOWN_HOURS = 12
PID_FILE = os.getenv("SOC_PID_FILE", "/root/Echelon/valentrix/mm/utils/soc/soc.pid")
LOG_FILE = os.getenv("SOC_LOG_FILE", "/root/Echelon/valentrix/mm/utils/soc/soc.log")
MAKER_REST_THRESHOLD_SECONDS = 5  # age on book to count as maker

# Market condition thresholds
PANIC_VOLATILITY_THRESHOLD = 0.05  # 5% in 15 mins
STRESS_VOLATILITY_THRESHOLD = 0.02  # 2% in 15 mins
MOMENTUM_WINDOW_SECONDS = 5         # lookback for short-term drift
MOMENTUM_TOUCHED_THRESH = 0.001     # 0.10% move considered “moving away”
MOMENTUM_SKIP_BONUS = 0.07          # +7% skip chance when moving away

# ── Logging Setup (rotating) ─────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Ensure log directory exists
try:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
except Exception:
    pass

rot = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
rot.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S'))
stdout = logging.StreamHandler(sys.stdout)
stdout.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S'))

# avoid duplicate handlers if reloaded
if not logger.handlers:
    logger.addHandler(rot)
    logger.addHandler(stdout)

# ── Global shutdown flag ──────────────────────────────────────────────
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ── Database Connection ───────────────────────────────────────────────
def get_db_connection():
    """Get connection to ariadne database"""
    return psycopg2.connect(
        dbname="ariadne",
        user="postgres",
        host="localhost"
    )

# ── PID Management ────────────────────────────────────────────────────
def write_pid_file():
    """Write PID file for monit"""
    try:
        os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
    except Exception:
        pass
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    logger.info(f"[INIT] PID {os.getpid()} written to {PID_FILE}")

# ── Market Analysis ───────────────────────────────────────────────────
def calculate_volatility(conn, symbol: str, minutes: int = 10) -> float:
    """Calculate price volatility over specified minutes"""
    try:
        cur = conn.cursor()
        cutoff_time = utc_now_ts() - (minutes * 60)
        
        cur.execute("""
            SELECT MIN(last), MAX(last), AVG(last)
            FROM tickstick
            WHERE symbol = %s AND timestamp > %s
        """, (symbol, cutoff_time))
        
        result = cur.fetchone()
        cur.close()
        
        if result and result[0] and result[1] and result[2]:
            min_price, max_price, avg_price = float(result[0]), float(result[1]), float(result[2])
            if avg_price > 0:
                return (max_price - min_price) / avg_price
        
        return 0.0
    except Exception as e:
        logger.error(f"[ERROR] Failed to calculate volatility for {symbol}: {e}")
        return 0.0

def get_market_condition(conn, symbol: str) -> str:
    """Determine market condition based on volatility"""
    volatility = calculate_volatility(conn, symbol, minutes=15)
    
		    if volatility > PANIC_VOLATILITY_THRESHOLD:
        return "panic"
    elif volatility > STRESS_VOLATILITY_THRESHOLD:
        return "stress"
    else:
        return "normal"

# -- Fix UTC Seconds --------------------------------------------------
def utc_now_ts() -> int:
    """UTC 'now' as integer seconds."""
    return int(datetime.utcnow().timestamp())

# ── Order Book Analysis ───────────────────────────────────────────────
def get_order_book_state(conn, symbol: str) -> Dict:
    """Get current order book state for realism calculations, normalized:
       - both bid/ask > 0
       - ask >= bid (if inverted by <= 2 ticks, clamp to 'touch')
       - sizes non-negative
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT buy, sell, best_bid_size, best_ask_size, vol
            FROM tickstick
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        bid_raw  = float(row[0]) if row[0] is not None else 0.0
        ask_raw  = float(row[1]) if row[1] is not None else 0.0
        bid_sz   = float(row[2]) if row[2] is not None else 0.0
        ask_sz   = float(row[3]) if row[3] is not None else 0.0
        volume   = float(row[4]) if row[4] is not None else 0.0

        # Basic positivity checks
        if bid_raw <= 0.0 or ask_raw <= 0.0:
            return None

        # Enforce ask >= bid; if slightly inverted (<= 2 ticks), clamp to touch
        tick = get_price_increment(conn, symbol)
        if ask_raw < bid_raw:
            diff = bid_raw - ask_raw
            if tick and diff <= (2.0 * tick):
                # Treat as touch (micro inversion noise): clamp ask to bid
                ask_raw = bid_raw
            else:
                # Inverted beyond tolerance: discard snapshot
                logger.warning(f"[BOOK] Inverted book for {symbol}: bid={bid_raw} > ask={ask_raw} (diff={diff}); skipping snapshot")
                return None

        # Non-negative sizes
        bid_sz = max(bid_sz, 0.0)
        ask_sz = max(ask_sz, 0.0)

        return {
            'bid': bid_raw,
            'ask': ask_raw,
            'bid_size': bid_sz,
            'ask_size': ask_sz,
            'volume': volume
        }

    except Exception as e:
        logger.error(f"[ERROR] Failed to get/normalize order book for {symbol}: {e}")
        return None

# ── Realism Functions ─────────────────────────────────────────────────
def check_realism_history(conn, symbol: str) -> bool:
    """Check if symbol had realism applied in last 12 hours"""
    try:
        cur = conn.cursor()
        cutoff_time = datetime.utcnow() - timedelta(hours=REALISM_COOLDOWN_HOURS)
        
        cur.execute("""
            SELECT COUNT(*) FROM realism_history
            WHERE symbol = %s AND applied_at > %s
        """, (symbol, cutoff_time))
        
        count = cur.fetchone()[0]
        cur.close()
        return count > 0
    except Exception as e:
        logger.error(f"[ERROR] Failed to check realism history: {e}")
        return False

def record_realism(conn, order_id: str, symbol: str, realism_type: str, details: Dict):
    """Record realism application in history"""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO realism_history (order_id, symbol, realism_type, applied_at, details)
            VALUES (%s, %s, %s, NOW(), %s::jsonb)
            ON CONFLICT (order_id) DO NOTHING
        """, (order_id, symbol, realism_type, psycopg2.extras.Json(details)))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"[ERROR] Failed to record realism: {e}")
        conn.rollback()

def select_realism_type(market_condition: str) -> str:
    """Select realism type with weights based on market condition"""
    if market_condition == "panic":
        # In panic: more skips and delays
        types = ["slippage", "partial", "delay", "skip_touch"]
        weights = [0.2, 0.2, 0.3, 0.3]
    elif market_condition == "stress":
        # In stress: balanced with slight preference for delays
        types = ["slippage", "partial", "delay", "skip_touch"]
        weights = [0.3, 0.2, 0.3, 0.2]
    else:
        # Normal: focus on slippage
        types = ["slippage", "partial", "delay", "skip_touch"]
        weights = [0.4, 0.2, 0.2, 0.2]
    
    return random.choices(types, weights=weights, k=1)[0]

def apply_slippage(order: Dict, book_state: Dict, market_condition: str, quote_min_size: float) -> Dict:
    """Apply slippage realism to order"""
    # Determine slippage multiplier based on market
    if market_condition == "panic":
        multiplier = random.randint(10, 25)
    elif market_condition == "stress":
        multiplier = random.randint(5, 15)
    else:
        multiplier = random.randint(1, 5)
    
    # Compute slippage as NOTIONAL (USDT), then convert to per-unit price slip — using Decimal
    slippage_notional = Decimal(str(quote_min_size)) * Decimal(str(multiplier))

    # Remaining quantity to be filled (respect existing partials)
    remaining_qty = max(order['size'] - order.get('filled_size', 0), 0)
    if remaining_qty <= 0:
        order['skip'] = True
        order['realism_applied'] = 'slippage'
        order['slippage_amount'] = Decimal("0")
        logger.warning(f"[REALISM] Slippage: no remaining qty for order {order.get('id')} ({order['symbol']}); skipping.")
        return order

    d_remaining = Decimal(str(remaining_qty))
    price_slip_dec = slippage_notional / d_remaining  # Decimal per-unit move

    # 2% chance of favorable slippage (helps the trade)
    favorable = (random.random() < 0.02)

    # Apply slippage by side and favorability (Decimal → float for tick rounding)
    base_price = Decimal(str(order['price']))
    if order['side'] == 'buy':
        new_price_dec = (base_price - price_slip_dec) if favorable else (base_price + price_slip_dec)
    else:  # sell
        new_price_dec = (base_price + price_slip_dec) if favorable else (base_price - price_slip_dec)

    # Tick-round the final fill price
    tick = get_price_increment(conn, order['symbol'])
    order['fill_price'] = round_price_to_tick(float(new_price_dec), tick)

    # Sanity check — ensure minimum NOTIONAL at intended fill qty
    intended_fill_qty = order.get('partial_fill_qty', remaining_qty)
    if (Decimal(str(order['fill_price'])) * Decimal(str(intended_fill_qty))) < Decimal(str(quote_min_size)):
        order['skip'] = True

    order['realism_applied'] = 'slippage'
    # Positive means trader-unfavorable notional drag; negative means favorable
    order['slippage_amount'] = (-slippage_notional) if favorable else slippage_notional
    order['favorable_slippage'] = favorable

    logger.info(
        f"[REALISM] Slippage applied to {order['symbol']} "
        f"(notional={slippage_notional}, favorable={favorable})"
    )
    return order

def apply_partial_fill(order: Dict) -> Dict:
    """Apply partial fill realism"""
    # Compute partials on REMAINING qty (respect prior fills)
    already_filled = order.get('filled_size', 0) or 0
    remaining = max(order['size'] - already_filled, 0)

    if remaining <= 0:
        # Nothing to do; mark and return
        order['partial_fill_qty'] = 0
        order['remaining_qty'] = 0
        order['cancel_remainder'] = False
        order['realism_applied'] = 'partial'
        order['fill_percentage'] = 0.0
        logger.warning(f"[REALISM] Partial: no remaining qty for order {order.get('id')} ({order['symbol']}); skipping.")
        return order

    # Random fill between 40–90% of the remaining amount
    fill_percentage = random.uniform(0.4, 0.9)
    partial_qty = remaining * fill_percentage

    # Cap to remaining (numerical safety)
    partial_qty = min(partial_qty, remaining)

    # Round both quantities to base increment
    base_step = get_base_increment(conn, order['symbol'])
    partial_qty = round_qty_to_increment(partial_qty, base_step, side=order.get('side'))
    remaining_after = max(remaining - partial_qty, 0.0)
    remaining_after = round_qty_to_increment(remaining_after, base_step, side=order.get('side'))

    order['partial_fill_qty'] = partial_qty
    order['remaining_qty'] = remaining_after

    # 70% chance remainder STAYS on book, 30% CANCELLED
    order['cancel_remainder'] = (random.random() < 0.3)

    order['realism_applied'] = 'partial'
    order['fill_percentage'] = fill_percentage

    logger.info(
        f"[REALISM] Partial fill {fill_percentage:.1%} for {order['symbol']} "
        f"(filled_now={partial_qty}, remaining_after={order['remaining_qty']})"
    )
    return order

def apply_delay(order: Dict) -> Dict:
    """Apply fill delay realism"""
    delay_seconds = random.uniform(0.2, 2.5)
    order['fill_after'] = time.time() + delay_seconds
    order['realism_applied'] = 'delay'
    order['delay_seconds'] = delay_seconds
    
    logger.info(f"[REALISM] Applied {delay_seconds:.1f}s delay to {order['symbol']} order")
    return order

def apply_skip_touch(order: Dict, book_state: Dict, momentum: float = 0.0, record: bool = True) -> Dict:
    """Apply skip-on-touch realism with pressure, imbalance, and short-term momentum."""
    skip_chance = 0.10  # base 10%

    # Order-pressure vs top-of-book size
    if order['side'] == 'buy':
        top_qty = book_state.get('ask_size', 0)
    else:
        top_qty = book_state.get('bid_size', 0)

    if top_qty > 0:
        order_pressure = (order['size'] / top_qty)
        if order_pressure > 0.25:
            skip_chance += 0.05  # +5% if large vs top level

    # Book imbalance (-1..+1)
    total_vol = book_state.get('bid_size', 0) + book_state.get('ask_size', 0)
    if total_vol > 0:
        imbalance = (book_state.get('bid_size', 0) - book_state.get('ask_size', 0)) / total_vol
        if (imbalance < -0.3 and order['side'] == 'sell') or (imbalance > 0.3 and order['side'] == 'buy'):
            skip_chance += 0.05  # +5% when book bias works against your side

    # Short-term momentum away from the order price
    if (order['side'] == 'buy' and momentum > MOMENTUM_TOUCHED_THRESH) or \
       (order['side'] == 'sell' and momentum < -MOMENTUM_TOUCHED_THRESH):
        skip_chance += MOMENTUM_SKIP_BONUS  # e.g., +7%

    # Cap to a reasonable max
    if skip_chance > 0.50:
        skip_chance = 0.50

    # Roll for skip
    if random.random() < skip_chance:
        order['skip'] = True
        if record:
            order['realism_applied'] = 'skip_touch'
            order['skip_chance'] = skip_chance

    return order

# ── Order Processing ──────────────────────────────────────────────────
def should_fill_order(order: Dict, ticker_data: Dict) -> bool:
    """Fill predicate with safety guards:
       - require bid/ask present and > 0
       - require ask >= bid (otherwise ignore snapshot)
       - standard cross logic
    """
    if not ticker_data:
        return False

    bid = ticker_data.get('bid', 0.0) or 0.0
    ask = ticker_data.get('ask', 0.0) or 0.0

    # Basic validity
    if bid <= 0.0 or ask <= 0.0:
        return False

    # Reject inverted book (should be handled upstream, but double-guard here)
    if bid > ask:
        return False

    # Fill conditions
    if order['side'] == 'buy':
        # Buy fills when market ask <= our bid
        return ask <= order['price']
    else:
        # Sell fills when market bid >= our ask
        return bid >= order['price']

def get_quote_min_size(conn, symbol: str) -> float:
    """Get quote minimum size for a symbol"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT quote_min_size FROM symbol_info
            WHERE symbol = %s
        """, (symbol,))
        
        result = cur.fetchone()
        cur.close()
        
        if result:
            return float(result[0])
        else:
            # Default fallback
            return 0.00001
    except:
        return 0.00001
    
def get_price_increment(conn, symbol: str) -> float:
    """Get price increment (tick size) for a symbol"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_increment 
            FROM symbol_info
            WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    # Fallback to a small tick if unknown (will be refined in Step #5 rounding)
    return 1e-8


def prices_touch(p1: float, p2: float, tick: float) -> bool:
    """
    Consider prices 'touching' if they are within half a tick.
    Avoids fragile float equality.
    """
    if tick <= 0:
        # Defensive fallback; treat exact equality as touch
        return abs(p1 - p2) == 0.0
    return abs(p1 - p2) <= (tick * 0.5)

def get_short_term_momentum(conn, symbol: str, seconds: int = MOMENTUM_WINDOW_SECONDS) -> float:
    """
    Return short-term mid-price return over ~`seconds`:
      (mid_now - mid_prev) / mid_prev
    Uses latest mid and the last mid at or before the cutoff.
    If data missing or invalid, returns 0.0.
    """
    try:
        cutoff = utc_now_ts() - int(seconds)

        # prev mid at/just before cutoff
        cur = conn.cursor()
        cur.execute("""
            SELECT buy, sell
            FROM tickstick
            WHERE symbol = %s AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol, cutoff))
        prev = cur.fetchone()

        # latest mid now
        cur.execute("""
            SELECT buy, sell
            FROM tickstick
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        curr = cur.fetchone()
        cur.close()

        if not prev or not curr or not prev[0] or not prev[1] or not curr[0] or not curr[1]:
            return 0.0

        mid_prev = (float(prev[0]) + float(prev[1])) / 2.0
        mid_now  = (float(curr[0]) + float(curr[1])) / 2.0
        if mid_prev <= 0:
            return 0.0

        return (mid_now - mid_prev) / mid_prev
    except Exception:
        return 0.0

def has_realism_for_order(conn, order_id: str) -> bool:
    """Return True if this order already had any realism applied."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM realism_history WHERE order_id = %s LIMIT 1",
            (order_id,)
        )
        found = cur.fetchone() is not None
        cur.close()
        return found
    except Exception:
        # Defensive: if in doubt, do not double-apply realism
        return True
    
def get_base_increment(conn, symbol: str) -> float:
    """Get minimum increment for base asset quantity (size step)."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT base_increment
            FROM symbol_info
            WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return 1e-8  # defensive fallback


def get_quote_increment(conn, symbol: str) -> float:
    """Get minimum increment for quote (USDT) amounts, if needed for validations."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT quote_increment
            FROM symbol_info
            WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return 1e-8  # defensive fallback


# ---- Rounding helpers (Decimal-based for correctness; return float) ----
from decimal import Decimal, getcontext, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP
getcontext().prec = 28  # high precision for crypto math


def _to_dec(x) -> Decimal:
    # Accept float/str/Decimal safely
    return x if isinstance(x, Decimal) else Decimal(str(x))


def round_to_increment(value: float, increment: float, mode: str = "NEAREST") -> float:
    """
    Round value to a multiple of increment.
    mode: "NEAREST" (half-up), "FLOOR", "CEIL"
    """
    v = _to_dec(value)
    inc = _to_dec(increment) if increment else _to_dec(0)
    if inc <= 0:
        # No increment info; return as-is
        return float(v)

    q = v / inc
    if mode == "FLOOR":
        q = q.to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "CEIL":
        q = q.to_integral_value(rounding=ROUND_CEILING)
    else:  # NEAREST half-up
        q = q.to_integral_value(rounding=ROUND_HALF_UP)

    return float(q * inc)


def round_price_to_tick(price: float, tick: float) -> float:
    return round_to_increment(price, tick, mode="NEAREST")


def round_qty_to_increment(qty: float, step: float, side: str = None) -> float:
    """
    Round qty to base increment. By default, NEAREST.
    If you prefer conservative rounding by side, you can use:
      - buys: FLOOR (avoid exceeding funds)
      - sells: FLOOR (avoid selling more than held)
    For now we use NEAREST to keep behavior consistent, can revisit later.
    """
    return round_to_increment(qty, step, mode="NEAREST")

def _clear_cap_if_empty(asset: str) -> None:
    """
    If total remaining position for `asset` is zero (or dust), clear the cap in inventory_limits.
    Mode-aware: checks positions (live) or sim_positions (simulation).
    """
    conn = None
    try:
        try:
            mode = get_mode()
        except Exception:
            mode = "simulation"

        table = "positions" if mode == "live" else "sim_positions"
        dust = Decimal("0.00000001")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT COALESCE(SUM(quantity),0) FROM {table} WHERE asset = %s", (asset.upper(),))
        remaining = Decimal(str(cur.fetchone()[0] or 0))

        if remaining <= dust:
            cur.execute("""
                UPDATE inventory_limits
                   SET capped = FALSE,
                       reason = NULL,
                       capped_at = NULL,
                       updated_by = 'Hari',
                       updated_at = NOW()
                 WHERE asset = %s
            """, (asset.upper(),))
            conn.commit()
        cur.close()
    except Exception as e:
        logger.warning(f"[CAP] Clear check failed for {asset}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def get_fee_rates_from_tick(conn, symbol: str) -> tuple[float, float]:
    """
    Returns (maker_rate_dec, taker_rate_dec) from latest tickstick row.
    Accepts either decimals (0.0010) or bps (10) and coerces to decimals.
    Optional coeff columns are applied if present.
    """
    MAKER_KEYS = ("maker_fee", "maker_fee_bps", "maker_bps", "maker_rate")
    TAKER_KEYS = ("taker_fee", "taker_fee_bps", "taker_bps", "taker_rate")
    MAKER_COEFF_KEYS = ("maker_coeff", "maker_coef", "maker_coefficient")
    TAKER_COEFF_KEYS = ("taker_coeff", "taker_coef", "taker_coefficient")

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT *
            FROM tickstick
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone() or {}
        cur.close()

        def pick(row, keys, default=None):
            for k in keys:
                if k in row and row[k] is not None:
                    return float(row[k])
            return default

        maker_raw = pick(row, MAKER_KEYS, 0.0010)
        taker_raw = pick(row, TAKER_KEYS, 0.0010)
        m_coeff = pick(row, MAKER_COEFF_KEYS, 1.0)
        t_coeff = pick(row, TAKER_COEFF_KEYS, 1.0)

        maker = (maker_raw / 10000.0) if maker_raw > 1 else maker_raw
        taker = (taker_raw / 10000.0) if taker_raw > 1 else taker_raw

        maker *= m_coeff
        taker *= t_coeff

        maker = max(0.0, min(maker, 0.01))
        taker = max(0.0, min(taker, 0.01))
        return (maker, taker)
    except Exception:
        return (0.0010, 0.0010)

def process_fill(conn, order: Dict, fill_price: float, fill_qty: float, fee_amount: float = 0.0, role: str = None):
    """Atomic fill with row lock on sim_orders to prevent double-counting."""
    try:
        cur = conn.cursor()

        # 1) Lock the order row and fetch status/filled/size
        cur.execute("""
            SELECT status, COALESCE(filled_size, 0), size, side, symbol
            FROM sim_orders
            WHERE id = %s
            FOR UPDATE
        """, (order['id'],))
        row = cur.fetchone()
        if not row:
            cur.close()
            return
        status_db, filled_db, size_db, side_db, symbol_db = row

        # If already closed, bail
        if status_db not in ('open', 'partial'):
            cur.close()
            return

        # 2) Decimal-safe scalars
        d_price = Decimal(str(fill_price))
        d_qty   = Decimal(str(fill_qty))
        d_fee   = Decimal(str(fee_amount)) if fee_amount is not None else Decimal("0")
        d_zero  = Decimal("0")

        # 3) Clamp to remaining
        remaining = Decimal(str(size_db)) - Decimal(str(filled_db))
        if remaining <= d_zero:
            cur.close()
            return
        d_qty = min(d_qty, remaining)  # cannot overfill

        # 4) Insert trade
        cur.execute("""
            INSERT INTO sim_trades (timestamp, symbol, side, price, size, sim_order_id, fee)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s)
        """, (symbol_db, side_db, d_price, d_qty, order['id'], d_fee))

        # 5) Update order status/filled
        new_filled = Decimal(str(filled_db)) + d_qty
        if new_filled + d_zero < Decimal(str(size_db)):
            # still partial
            cur.execute("""
                UPDATE sim_orders
                SET filled_size = %s,
                    status = 'partial',
                    updated_at = NOW()
                WHERE id = %s
            """, (new_filled, order['id']))
        else:
            # full
            cur.execute("""
                UPDATE sim_orders
                SET filled_size = size,
                    status = 'filled',
                    filled_at = NOW(),
                    updated_at = NOW(),
                    deleted = TRUE,
                    deleted_at = NOW()
                WHERE id = %s
            """, (order['id'],))

        # 6) Balances
        base_asset = symbol_db.split('-')[0]
        notional = d_price * d_qty

        if side_db == 'buy':
            usdt_deduct = notional + d_fee
            # deduct USDT (available & hold)
            cur.execute("""
                UPDATE sim_balances
                SET available = available - %s,
                    hold = hold - %s
                WHERE asset = 'USDT'
            """, (usdt_deduct, usdt_deduct))
            # credit base asset
            cur.execute("""
                INSERT INTO sim_balances (asset, available, hold)
                VALUES (%s, %s, 0)
                ON CONFLICT (asset)
                DO UPDATE SET available = sim_balances.available + %s
            """, (base_asset, d_qty, d_qty))
        else:
            usdt_credit = notional - d_fee
            # debit base asset (available & hold)
            cur.execute("""
                UPDATE sim_balances
                SET available = available - %s,
                    hold = hold - %s
                WHERE asset = %s
            """, (d_qty, d_qty, base_asset))
            # credit USDT (available)
            cur.execute("""
                UPDATE sim_balances
                SET available = available + %s
                WHERE asset = 'USDT'
            """, (usdt_credit,))

        conn.commit()
        cur.close()

        logger.info(f"[FILL] Processed {side_db} {d_qty} {symbol_db} @ {d_price}")
        logger.info(f"[FEE] {symbol_db} {side_db} role={role} fee={d_fee} notional={notional}")
        
        # If this was a SELL, check whether to clear asset cap
        if side_db == 'sell':
            _clear_cap_if_empty(base_asset)

    except Exception as e:
        logger.error(f"[ERROR] Failed to process fill atomically: {e}")
        conn.rollback()

# ── Main Order Checker ────────────────────────────────────────────────
class SimOrderChecker:
    def __init__(self):
        self.cycle_count = 0
        self.delayed_orders = {}  # Track orders with delay realism
        
    def check_orders(self):
        """Main order checking logic"""
        conn = get_db_connection()
        
        try:
            # Get all open orders
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT id, symbol, side, price, size, 
                       COALESCE(filled_size, 0) as filled_size,
                       created_at
                FROM sim_orders
                WHERE status IN ('open', 'partial')
                  AND COALESCE(deleted, FALSE) = FALSE
                ORDER BY created_at
            """)
            
            orders = cur.fetchall()
            cur.close()
            
            # Process each order
            for order in orders:
                # Check delayed orders
                if order['id'] in self.delayed_orders:
                    plan = self.delayed_orders[order['id']]
                    if time.time() < plan['fill_after']:
                        continue  # Still delayed
                    else:
                        # Mark for post-delay re-validation; delete AFTER we recheck market
                        order['_delay_expired'] = True
                        order['_delay_plan'] = plan
                
                # Get current market data
                book_state = get_order_book_state(conn, order['symbol'])
                if not book_state:
                    continue
                
                # Check if order should fill
                if should_fill_order(order, book_state):
                    # If a delay just expired, re-validate execution conditions at the moment of fill
                    if order.get('_delay_expired'):
			# Ensure the order is still open/partial at delay expiry
			_cur = conn.cursor()
			_cur.execute("SELECT status FROM sim_orders WHERE id = %s", (order['id'],))
			_row = _cur.fetchone()
			_cur.close()
                            if not _row or _row[0] not in ('open', 'partial'):
			    try:
 				del self.delayed_orders[order['id']]
			    except KeyError:
                                pass
 			    continue
                        # Optional: if the touch is razor-thin, allow a skip check without re-tagging realism
                        tick = get_price_increment(conn, order['symbol'])
                        do_touch_check = (
                            (order['side'] == 'buy'  and prices_touch(order['price'], book_state['ask'], tick)) or
                            (order['side'] == 'sell' and prices_touch(order['price'], book_state['bid'], tick))
                        )
                        if do_touch_check:
                            mom = get_short_term_momentum(conn, order['symbol'], seconds=MOMENTUM_WINDOW_SECONDS)
                            _before = order.get('skip', False)
                            order = apply_skip_touch(order, book_state, momentum=mom, record=False)
                            # If we decided to skip due to touch after delay, honor it and clear the delay plan
                            if order.get('skip', False) and not _before:
                                # Delay is consumed; remove from queue and move on
                                try:
                                    del self.delayed_orders[order['id']]
                                except KeyError:
                                    pass
                                continue  # skip this order this cycle
                        # Delay is consumed regardless; remove the plan
                        try:
                            del self.delayed_orders[order['id']]
                        except KeyError:
                            pass

                    # Determine if realism should apply (per-order, not per-symbol)
                    apply_realism = False
                    realism_type = None

                    if not has_realism_for_order(conn, order['id']) and (random.random() < REALISM_CHANCE):
                        apply_realism = True
                        market_condition = get_market_condition(conn, order['symbol'])
                        realism_type = select_realism_type(market_condition)
                    
                    # Apply realism if selected
                    if apply_realism:
                        quote_min_size = get_quote_min_size(conn, order['symbol'])
                        
                        if realism_type == 'slippage':
                            order = apply_slippage(order, book_state, market_condition, quote_min_size)
                        elif realism_type == 'partial':
                            order = apply_partial_fill(order)
                        elif realism_type == 'delay':
                            order = apply_delay(order)
                            # Store a lightweight plan (timestamp only) to avoid stale order dicts
                            self.delayed_orders[order['id']] = {
                                'fill_after': order['fill_after']
                            }
                            continue  # Don't process now
                        elif realism_type == 'skip_touch':
                            # Only apply if price "touches" within half a tick
                            tick = get_price_increment(conn, order['symbol'])
                            if (order['side'] == 'buy' and prices_touch(order['price'], book_state['ask'], tick)) or \
                               (order['side'] == 'sell' and prices_touch(order['price'], book_state['bid'], tick)):
                                mom = get_short_term_momentum(conn, order['symbol'], seconds=MOMENTUM_WINDOW_SECONDS)
                                order = apply_skip_touch(order, book_state, momentum=mom)
                        
                        # Record realism application
                        if 'realism_applied' in order:
                            details = {
                                'market_condition': market_condition,
                                'book_state': book_state,
                                'realism_details': {k: v for k, v in order.items() 
                                                  if k in ['slippage_amount', 'fill_percentage', 
                                                          'delay_seconds', 'skip_chance']}
                            }
                            record_realism(conn, order['id'], order['symbol'], 
                                         order['realism_applied'], details)
                    
                    # Process fill if not skipped
                    if not order.get('skip', False):
                        fill_price = order.get('fill_price', order['price'])
                        fill_qty = order.get('partial_fill_qty', order['size'] - order['filled_size'])

                        # Final safety rounding before committing:
                        tick = get_price_increment(conn, order['symbol'])
                        base_step = get_base_increment(conn, order['symbol'])

                        fill_price = round_price_to_tick(fill_price, tick)
                        fill_qty = round_qty_to_increment(fill_qty, base_step, side=order.get('side'))
                        
                        # Min-notional guard: reject dust fills below exchange minimum
                        quote_min = get_quote_min_size(conn, order['symbol'])
                        if (fill_price * fill_qty) < quote_min:
                            logger.info(f"[GUARD] Notional {fill_price * fill_qty:.12f} < min {quote_min:.12f} for {order['symbol']} — skipping fill this cycle")
                            # Do not fill; keep order status unchanged so it can fill later when qty/notional is sufficient
                            continue

                        # Classify maker/taker by age on book
                        role = 'taker'
                        try:
                            if order.get('created_at'):
                                age = (datetime.utcnow() - order['created_at']).total_seconds()
                                role = 'maker' if age >= MAKER_REST_THRESHOLD_SECONDS else 'taker'
                        except Exception:
                            role = 'taker'

                        # Fee from latest tickstick row (Decimal-safe)
                        maker_rate, taker_rate = get_fee_rates_from_tick(conn, order['symbol'])
                        fee_rate_dec = Decimal(str(maker_rate if role == 'maker' else taker_rate))

                        trade_value_dec = Decimal(str(fill_price)) * Decimal(str(fill_qty))
                        fee_amount_dec = trade_value_dec * fee_rate_dec

                        process_fill(conn, order, fill_price, fill_qty, fee_amount=fee_amount_dec, role=role)
                        
                        # Handle partial fill remainder
                        if order.get('cancel_remainder', False):
                            remaining_qty = float(order.get('remaining_qty', 0.0))
                            asset = order['symbol'].split('-')[0]
                            try:
                                cur = conn.cursor()

                                if remaining_qty > 0:
                                    if order['side'] == 'buy':
                                        # Release USDT hold equal to remaining notional at limit price
                                        remaining_notional = float(order['price']) * remaining_qty
                                        cur.execute("""
                                            UPDATE sim_balances
                                            SET available = available + %s,
                                                hold = GREATEST(hold - %s, 0)
                                            WHERE asset = 'USDT'
                                        """, (remaining_notional, remaining_notional))
                                    else:
                                        # Release BASE asset hold equal to remaining units
                                        cur.execute("""
                                            UPDATE sim_balances
                                            SET available = available + %s,
                                                hold = GREATEST(hold - %s, 0)
                                            WHERE asset = %s
                                        """, (remaining_qty, remaining_qty, asset))

                                # Cancel & soft-delete the order (per #6)
                                cur.execute("""
                                    UPDATE sim_orders
                                    SET status = 'cancelled',
                                        updated_at = NOW(),
                                        deleted = TRUE,
                                        deleted_at = NOW()
                                    WHERE id = %s
                                """, (order['id'],))

                                conn.commit()
                                cur.close()
                            except Exception as e:
                                logger.error(f"[ERROR] Failed to release holds on cancel for order {order['id']}: {e}")

                        elif order.get('realism_applied') == 'partial' and float(order.get('remaining_qty', 0.0)) > 0:
                            # Keep order OPEN: ensure holds reflect the remaining portion
                            remaining_qty = float(order['remaining_qty'])
                            asset = order['symbol'].split('-')[0]
                            try:
                                cur = conn.cursor()
                                if order['side'] == 'buy':
                                    # Ensure at least remaining_notional is held in USDT
                                    target_notional = float(order['price']) * remaining_qty
                                    cur.execute("""
                                        UPDATE sim_balances
                                        SET hold = GREATEST(hold, %s)
                                        WHERE asset = 'USDT'
                                    """, (target_notional,))
                                else:
                                    # Ensure at least remaining_qty units are held in BASE asset
                                    cur.execute("""
                                        UPDATE sim_balances
                                        SET hold = GREATEST(hold, %s)
                                        WHERE asset = %s
                                    """, (remaining_qty, asset))

                                conn.commit()
                                cur.close()
                            except Exception as e:
                                logger.error(f"[WARN] Could not normalize holds for open remainder on order {order['id']}: {e}")
                
        except Exception as e:
            logger.error(f"[ERROR] Order checking failed: {e}")
        finally:
            conn.close()
    
    def update_heartbeat(self):
        """Update heartbeat in database"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO heartbeats (process_name, last_heartbeat, status, pid, cycle_count)
                VALUES ('soc', NOW(), 'ok', %s, %s)
                ON CONFLICT (process_name)
                DO UPDATE SET 
                    last_heartbeat = NOW(),
                    status = 'ok',
                    pid = %s,
                    cycle_count = %s
            """, (os.getpid(), self.cycle_count, os.getpid(), self.cycle_count))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"[HEARTBEAT] Updated (cycle {self.cycle_count})")
        except Exception as e:
            logger.error(f"[ERROR] Failed to update heartbeat: {e}")
    
    def run_continuous(self):
        """Run continuously with graceful shutdown"""

        # Safety: refuse to run when GOLIVE is enabled
        GOLIVE = None

        # Try multiple import paths in order of preference
        try:
            from mm.config.marcus import GOLIVE
        except ImportError:
            try:
                from ariadne_config import GOLIVE
            except ImportError:
                try:
                    from config import GOLIVE
                except ImportError:
                    pass

        # Also check environment variable as fallback
        if GOLIVE is None:
            GOLIVE = os.getenv('GOLIVE', 'false').lower() in ('true', '1', 'yes')

        # If still None, assume simulation mode (safe default)
        if GOLIVE is None:
            logger.warning("[WARN] Could not determine GOLIVE status, assuming simulation mode")
            GOLIVE = False

        if GOLIVE:
            logger.error("[ABORT] GOLIVE=True — SOC (simulation) must not run in live mode")
            return

        logger.info("[INIT] SOC starting up in simulation mode...")
        write_pid_file()
        
        # Ensure realism_history table exists
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realism_history (
                order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                realism_type TEXT NOT NULL,
                applied_at TIMESTAMP NOT NULL,
                details JSONB
            );
            CREATE INDEX IF NOT EXISTS idx_realism_symbol_time 
            ON realism_history(symbol, applied_at DESC);
        """)
        conn.commit()
        cur.close()
        conn.close()
        
        while not shutdown_requested:
            try:
                self.check_orders()
                self.cycle_count += 1
                
                # Update heartbeat every 12 cycles
                if self.cycle_count % HEARTBEAT_CYCLES == 0:
                    self.update_heartbeat()
                
                time.sleep(CHECK_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"[ERROR] Main loop error: {e}")
                time.sleep(CHECK_INTERVAL_SECONDS)
        
        logger.info("[SHUTDOWN] SOC shutting down gracefully")

# ── Main Entry Point ──────────────────────────────────────────────────
if __name__ == "__main__":
    checker = SimOrderChecker()
    checker.run_continuous()
