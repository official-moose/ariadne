#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# Simulation Client
# mm/conn/sim_kucoin.py
#
# Simulation client that mimics KucoinClient interface. 
# Uses live market data + simulated wallet/orders 
# Virtual 'paper trading'  
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import logging
import time
import threading
from typing import Dict, Optional, Tuple, List
from decimal import Decimal, ROUND_DOWN
import psycopg2
import psycopg2.extras

# Import the live client for market data
from mm.conn.conn_kucoin import KucoinClient

logger = logging.getLogger("ariadne.sim")

# ---- DB Connection -----------------------------------------------------------
def get_db_connection():
    """
    Create a PostgreSQL connection to 'ariadne' as postgres@localhost.
    """
    return psycopg2.connect(dbname="ariadne", user="postgres", host="localhost")

# ---- Helpers ----------------------------------------------------------------
D2 = Decimal("0.01")
D8 = Decimal("0.00000001")
ZERO = Decimal("0")

def _q2(x: Decimal) -> Decimal:
    """Quantize to 2 dp (money, human)."""
    return x.quantize(D2, rounding=ROUND_DOWN)

def _q8(x: Decimal) -> Decimal:
    """Quantize to 8 dp (crypto sizes/prices)."""
    return x.quantize(D8, rounding=ROUND_DOWN)

def _split_symbol(symbol: str) -> Tuple[str, str]:
    base, quote = symbol.upper().split("-", 1)
    return base, quote

# ---- Sim Client --------------------------------------------------------------
class SimClient:
    """
    Simulator backed by PostgreSQL tables:
      - tickstick (partitioned): market snapshot source for pricing/fees
      - sim_balances(asset, available, hold)
      - sim_orders(id, symbol, side, price, size, filled_size, status, created_at, updated_at, filled_at, deleted, deleted_at)
      - sim_trades(timestamp, symbol, side, price, size, sim_order_id, fee)
    """
    
    def __init__(self, db_path: str = None, db_lock=None):
        """Initialize with live client for market data and optional db lock for thread safety."""
        # db_path kept for compatibility but ignored (we use PostgreSQL)
        self.db_path = db_path or SIMULATION_DB_PATH
        self.db_lock = db_lock if db_lock else threading.RLock()  # Default to RLock if none provided
        self.live_client = KucoinClient()
        self._symbols = self.live_client._symbols

    # ── Public Market Data (use live data) ────────────────────────────
    
    def get_all_tickers(self):
        """Delegate to live client for real market data."""
        return self.live_client.get_all_tickers()

    def best_bid_price(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.best_bid_price(symbol)

    def best_ask_price(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.best_ask_price(symbol)

    def last_trade_price(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.last_trade_price(symbol)

    def vol_24h(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.vol_24h(symbol)

    def high_24h(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.high_24h(symbol)

    def low_24h(self, symbol: str) -> float:
        """Use live market data."""
        return self.live_client.low_24h(symbol)

    def historical_ohlcv(self, symbol: str, timeframe: str, limit: int):
        """Use live market data."""
        return self.live_client.historical_ohlcv(symbol, timeframe, limit)

    def order_book(self, symbol: str, depth: int = 10):
        """Use live market data."""
        return self.live_client.order_book(symbol, depth)

    def get_recent_trades(self, symbol: str, limit: int = 100):
        """Use live market data."""
        return self.live_client.get_recent_trades(symbol, limit)

    def list_products(self):
        """Use live market data."""
        return self.live_client.list_products()

    def _pair(self, symbol: str) -> str:
        """Use live client's pair method."""
        return self.live_client._pair(symbol)

    # ── Fees (simple/simulated) ───────────────────────────────────────

    def maker_fee(self, symbol: str = None):
        """Return simulated maker fee."""
        return {"source": "simulation", "value": 0.0010}

    def taker_fee(self, symbol: str = None):
        """Return simulated taker fee."""
        return {"source": "simulation", "value": 0.0010}

    def withdrawal_fee(self, symbol: str, network: str = None):
        """Return simulated withdrawal fee."""
        return {"source": "simulation", "value": 0.25, "network": "XRP"}

    def min_trade_size(self, symbol: str):
        """Get minimum trade size from live client."""
        live_result = self.live_client.min_trade_size(symbol)
        return {"source": "simulation", "value": live_result["value"]}

    # ── Internal fee helper (for PostgreSQL-based fees if needed) ────────
    
    def _get_fees_from_db(self, conn, symbol: str) -> Tuple[float, float]:
        """
        Return (maker, taker) as fractions (e.g., 0.001 = 0.1%).
        Falls back to default 0.0010 if not in tickstick.
        """
        MAKER_KEYS = ("maker_fee", "maker_fee_bps", "maker_bps", "maker_rate", "makerFeeRate")
        TAKER_KEYS = ("taker_fee", "taker_fee_bps", "taker_bps", "taker_rate", "takerFeeRate")
        MAKER_COEFF_KEYS = ("maker_coeff", "maker_coef", "maker_coefficient", "makerCoefficient")
        TAKER_COEFF_KEYS = ("taker_coeff", "taker_coef", "taker_coefficient", "takerCoefficient")

        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT *
                FROM tickstick
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone() or {}
            cur.close()

            def pick(rr: Dict, keys, default=None):
                for k in keys:
                    if k in rr and rr[k] is not None:
                        try:
                            return float(rr[k])
                        except Exception:
                            continue
                return default

            maker_raw = pick(row, MAKER_KEYS, 0.0010)
            taker_raw = pick(row, TAKER_KEYS, 0.0010)
            m_coeff = pick(row, MAKER_COEFF_KEYS, 1.0)
            t_coeff = pick(row, TAKER_COEFF_KEYS, 1.0)

            # Support bps if raw > 1 (e.g., 10 = 10 bps = 0.0010)
            maker = (maker_raw / 10000.0) if maker_raw > 1 else maker_raw
            taker = (taker_raw / 10000.0) if taker_raw > 1 else taker_raw

            maker *= (m_coeff or 1.0)
            taker *= (t_coeff or 1.0)

            # Clamp to sane range [0, 1%]
            maker = max(0.0, min(maker, 0.01))
            taker = max(0.0, min(taker, 0.01))
            return (maker, taker)
        except Exception:
            return (0.0010, 0.0010)

    # ── Wallet Functions (simulation only) ─────────────────────────────

    def get_account_balances(self, account_type: str = "trade") -> Dict[str, float]:
        """Available balances only (compat with live)."""
        with self.db_lock:
            return self._get_balances()
    
    def _get_balances(self) -> Dict[str, float]:
        """Internal method to get balances (available only)."""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT asset, available FROM sim_balances")
                balances = {row[0].upper(): float(row[1] or 0.0) for row in cur.fetchall()}
                cur.close()
                return balances
        except Exception as e:
            logger.error(f"Error reading balances: {e}")
            return {}

    def get_account_balances_detailed(self, account_type: str = "trade") -> Dict[str, Dict[str, float]]:
        """Available + hold, mirroring live client detailed call."""
        with self.db_lock:
            return self._get_balances_detailed()
    
    def _get_balances_detailed(self) -> Dict[str, Dict[str, float]]:
        """Internal method to get detailed balances."""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT asset, available, hold FROM sim_balances")
                result = {}
                for asset, available, hold in cur.fetchall():
                    result[asset.upper()] = {
                        "available": float(available or 0.0),
                        "hold": float(hold or 0.0)
                    }
                cur.close()
                return result
        except Exception as e:
            logger.error(f"Error fetching detailed balances: {e}")
            return {}

    def _update_balance(self, currency: str, delta_available: float = 0.0, delta_hold: float = 0.0):
        """Adjust available/hold for a currency."""
        with self.db_lock:
            self._update_balance_internal(currency, delta_available, delta_hold)
    
    def _update_balance_internal(self, currency: str, delta_available: float, delta_hold: float):
        """Internal method to update balance."""
        currency = currency.upper()
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                # Ensure row exists
                cur.execute(
                    "INSERT INTO sim_balances (asset, available, hold) VALUES (%s, 0, 0) ON CONFLICT (asset) DO NOTHING",
                    (currency,)
                )
                
                # Update balances
                cur.execute(
                    """
                    UPDATE sim_balances 
                    SET available = GREATEST(0, available + %s),
                        hold = GREATEST(0, hold + %s)
                    WHERE asset = %s
                    """,
                    (delta_available, delta_hold, currency)
                )
                conn.commit()
                cur.close()
        except Exception as e:
            logger.error(f"Error updating balance for {currency}: {e}")
            raise

    def _get_balance_row(self, currency: str) -> Tuple[float, float]:
        """Get balance row with lock support."""
        with self.db_lock:
            return self._get_balance_row_internal(currency)
    
    def _get_balance_row_internal(self, currency: str) -> Tuple[float, float]:
        """Internal method to get balance row."""
        currency = currency.upper()
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT available, hold FROM sim_balances WHERE asset = %s", (currency,))
                row = cur.fetchone()
                cur.close()
                return (float(row[0]), float(row[1])) if row else (0.0, 0.0)
        except Exception:
            return (0.0, 0.0)

    # ── Orders (simulation only) ─────────────────────────────────────

    def create_limit_order(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        post_only: bool = False,
        tif: str = "GTC",
        hidden: bool = False,
        iceberg: bool = False,
    ) -> str:
        """Place a simulated limit order. Applies/creates holds atomically."""
        with self.db_lock:
            return self._create_limit_order_internal(symbol, side, price, size, post_only, tif, hidden, iceberg)
    
    def _create_limit_order_internal(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        post_only: bool,
        tif: str,
        hidden: bool,
        iceberg: bool
    ) -> str:
        """Internal method to create limit order."""
        side = side.lower()
        if side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")

        order_id = f"SIM_{side.upper()}_{int(time.time() * 1000)}"
        base, quote = _split_symbol(symbol)

        d_price = _q8(Decimal(str(price)))
        d_size = _q8(Decimal(str(size)))

        try:
            with get_db_connection() as conn:
                cur = conn.cursor()

                # Ensure balance rows exist
                cur.execute("INSERT INTO sim_balances (asset, available, hold) VALUES (%s, 0, 0) ON CONFLICT (asset) DO NOTHING", (quote,))
                cur.execute("INSERT INTO sim_balances (asset, available, hold) VALUES (%s, 0, 0) ON CONFLICT (asset) DO NOTHING", (base,))

                if side == "buy":
                    # Hold quote equal to notional
                    notional = float(_q8(d_price * d_size))
                    
                    # Check available balance
                    cur.execute("SELECT available FROM sim_balances WHERE asset = %s", (quote,))
                    row = cur.fetchone()
                    available = float(row[0]) if row else 0.0
                    
                    if available < notional:
                        raise ValueError(
                            f"Insufficient {quote} available to place BUY hold: need {notional:.8f}, have {available:.8f}"
                        )
                    
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET available = available - %s,
                            hold = hold + %s
                        WHERE asset = %s
                        """,
                        (notional, notional, quote)
                    )
                else:
                    # Hold base units equal to size
                    size_float = float(d_size)
                    
                    # Check available balance
                    cur.execute("SELECT available FROM sim_balances WHERE asset = %s", (base,))
                    row = cur.fetchone()
                    available = float(row[0]) if row else 0.0
                    
                    if available < size_float:
                        raise ValueError(
                            f"Insufficient {base} available to place SELL hold: need {size_float:.8f}, have {available:.8f}"
                        )
                    
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET available = available - %s,
                            hold = hold + %s
                        WHERE asset = %s
                        """,
                        (size_float, size_float, base)
                    )

                # Insert order row
                cur.execute(
                    """
                    INSERT INTO sim_orders (id, symbol, side, price, size, filled_size, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, 'open', NOW(), NOW())
                    """,
                    (order_id, symbol, side, float(d_price), float(d_size), 0.0)
                )

                conn.commit()
                cur.close()
                logger.info(f"[ORDER] Created {side} {d_size} {symbol} @ {d_price} (id {order_id})")
                return order_id
        except Exception as e:
            logger.error(f"[ERROR] create_limit_order failed: {e}")
            raise

    def create_market_order(self, symbol: str, side: str, size: float) -> str:
        """Simulate market order placement using current best price. Abort if price is 0."""
        price = self.best_ask_price(symbol) if side.lower() == "buy" else self.best_bid_price(symbol)
        price = float(price or 0.0)
        if price <= 0.0:
            raise ValueError(f"Cannot place market {side} for {symbol}: no valid price (got {price}).")
        return self.create_limit_order(symbol, side, price, size)

    def cancel_order(self, order_id: str) -> Dict:
        """Cancel an open/partial order and release remaining holds."""
        with self.db_lock:
            return self._cancel_order_internal(order_id)
    
    def _cancel_order_internal(self, order_id: str) -> Dict:
        """Internal method to cancel order."""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                
                # Lock the order row
                cur.execute(
                    """
                    SELECT symbol, side, price, size, COALESCE(filled_size,0)
                    FROM sim_orders
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (order_id,)
                )
                r = cur.fetchone()
                
                if not r:
                    cur.close()
                    return {"code": "400000", "msg": "Order not found"}

                symbol, side, price, size, filled = r
                base, quote = _split_symbol(symbol)
                d_price = Decimal(str(price))
                d_size = Decimal(str(size))
                d_filled = Decimal(str(filled))
                remaining_qty = max(ZERO, d_size - d_filled)

                if side == "buy":
                    remaining_notional = float(_q8(d_price * remaining_qty))
                    # Release quote hold: hold -> available
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET available = available + %s,
                            hold = GREATEST(hold - %s, 0)
                        WHERE asset = %s
                        """,
                        (remaining_notional, remaining_notional, quote)
                    )
                else:
                    # Release base hold: hold -> available
                    remaining_float = float(remaining_qty)
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET available = available + %s,
                            hold = GREATEST(hold - %s, 0)
                        WHERE asset = %s
                        """,
                        (remaining_float, remaining_float, base)
                    )

                # Soft delete + status
                cur.execute(
                    """
                    UPDATE sim_orders
                    SET status = 'cancelled',
                        updated_at = NOW(),
                        deleted = TRUE,
                        deleted_at = NOW()
                    WHERE id = %s
                    """,
                    (order_id,)
                )

                conn.commit()
                cur.close()
                logger.info(f"[CANCEL] Order {order_id} cancelled; holds released.")
                return {"code": "200000", "data": {"cancelledOrderId": order_id}}
        except Exception as e:
            logger.error(f"[ERROR] cancel_order failed: {e}")
            return {"code": "400000", "msg": "Cancel failed"}

    def get_order(self, order_id: str) -> Dict:
        """Get simulated order details."""
        with self.db_lock:
            return self._get_order_internal(order_id)
    
    def _get_order_internal(self, order_id: str) -> Dict:
        """Internal method to get order."""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT id, symbol, side, price, size, filled_size, status, created_at, updated_at, filled_at, deleted, deleted_at
                    FROM sim_orders
                    WHERE id = %s
                    """,
                    (order_id,)
                )
                row = cur.fetchone()
                cur.close()
                
                if not row:
                    return {"code": "400000", "msg": "Order not found"}
                
                data = {
                    "id": row["id"],
                    "symbol": row["symbol"],
                    "side": row["side"],
                    "price": float(row["price"]),
                    "size": float(row["size"]),
                    "filledSize": float(row["filled_size"] or 0.0),
                    "status": row["status"],
                }
                return {"code": "200000", "data": data}
        except Exception as e:
            logger.error(f"Error getting order {order_id}: {e}")
            return {"code": "400000", "msg": "Order not found"}

    # ── Fills (keep the enhanced PostgreSQL version) ─────────────────
    
    def fill_order(self, order_id: str, fill_price: float, fill_qty: float, fee_amount: float = 0.0, role: Optional[str] = None) -> None:
        """
        Execute an order fill atomically:
          - Insert sim_trades
          - Update sim_orders (partial/filled, timestamps, soft-delete on full)
          - Update sim_balances (deduct/add with fee)
        """
        with self.db_lock:
            self._fill_order_internal(order_id, fill_price, fill_qty, fee_amount, role)
    
    def _fill_order_internal(self, order_id: str, fill_price: float, fill_qty: float, fee_amount: float, role: Optional[str]) -> None:
        """Internal method to fill order."""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()

                # 1) Lock and fetch the order state
                cur.execute(
                    """
                    SELECT status, COALESCE(filled_size,0), size, side, symbol, price
                    FROM sim_orders
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (order_id,)
                )
                row = cur.fetchone()
                if not row:
                    cur.close()
                    return
                
                status_db, filled_db, size_db, side_db, symbol_db, price_db = row

                if status_db not in ("open", "partial"):
                    cur.close()
                    return

                d_price = _q8(Decimal(str(fill_price)))
                d_qty = _q8(Decimal(str(fill_qty)))
                d_fee = _q8(Decimal(str(fee_amount))) if fee_amount is not None else ZERO

                # Clamp to remaining
                remaining = _q8(Decimal(str(size_db)) - Decimal(str(filled_db)))
                if remaining <= ZERO:
                    cur.close()
                    return
                d_qty = min(d_qty, remaining)

                # 2) Insert trade
                cur.execute(
                    """
                    INSERT INTO sim_trades (timestamp, symbol, side, price, size, sim_order_id, fee)
                    VALUES (NOW(), %s, %s, %s, %s, %s, %s)
                    """,
                    (symbol_db, side_db, float(d_price), float(d_qty), order_id, float(d_fee))
                )

                # 3) Update order
                new_filled = _q8(Decimal(str(filled_db)) + d_qty)
                if new_filled < Decimal(str(size_db)):
                    cur.execute(
                        """
                        UPDATE sim_orders
                        SET filled_size = %s,
                            status = 'partial',
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (float(new_filled), order_id)
                    )
                else:
                    cur.execute(
                        """
                        UPDATE sim_orders
                        SET filled_size = size,
                            status = 'filled',
                            filled_at = NOW(),
                            updated_at = NOW(),
                            deleted = TRUE,
                            deleted_at = NOW()
                        WHERE id = %s
                        """,
                        (order_id,)
                    )

                # 4) Balances
                base_asset, quote_asset = symbol_db.split("-")
                notional = _q8(d_price * d_qty)
                
                if side_db == "buy":
                    # BUY: reduce quote hold by filled notional+fee, add base available
                    total_quote = float(_q8(notional + d_fee))
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET hold = GREATEST(0, hold - %s)
                        WHERE asset = %s
                        """,
                        (total_quote, quote_asset)
                    )
                    cur.execute(
                        """
                        INSERT INTO sim_balances (asset, available, hold)
                        VALUES (%s, %s, 0)
                        ON CONFLICT (asset) DO UPDATE
                        SET available = sim_balances.available + EXCLUDED.available
                        """,
                        (base_asset, float(d_qty))
                    )
                else:
                    # SELL: reduce base hold by filled qty, add quote available (less fee)
                    quote_credit = float(_q8(notional - d_fee))
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET hold = GREATEST(0, hold - %s)
                        WHERE asset = %s
                        """,
                        (float(d_qty), base_asset)
                    )
                    cur.execute(
                        """
                        UPDATE sim_balances
                        SET available = available + %s
                        WHERE asset = %s
                        """,
                        (quote_credit, quote_asset)
                    )

                conn.commit()
                cur.close()
                logger.info(f"[FILL] {side_db.upper()} {d_qty} {symbol_db} @ {d_price} (fee {d_fee})")
        except Exception as e:
            logger.error(f"[ERROR] fill_order failed: {e}")
            raise