#===================================================================
# ?? A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ?
#===================================================================
# Malcolm - Purchasing Manager
# mm/core/malcolm.py
#
# Creates and manages buy orders
# Identifies entry opportunities from Dr. Calvin
# Manages order lifecycle from proposal to execution
#
# [520] [741] [8]
#===================================================================
# ?? THE COMMANDER            ? PERSISTANT RUNTIME  ? MONIT MANAGED
#===================================================================

# ============= SECTION 1: IMPORTS & CONFIGURATION =================

import os
import sys
import json
import time
import signal
import select
import logging
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple, Any 
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# Add project root to path
sys.path.append('/root/Echelon/valentrix')

# Local imports
from mm.config import marcus
from mm.utils.helpers.inara import get_mode, get_trading_client
from mm.utils.tqt.andi import get_andi
from mm.utils.helpers.wintermute import (
    write_pid_file,
    cleanup_pid_file,
    update_heartbeat,
    parse_symbol
)
from mm.core.drcalvin import ValueOps  # For getting best pairs
from mm.core.julius import Julius  # For checking available funds

# Configuration
TARGET_SPREAD_PCT = Decimal(str(getattr(marcus, 'TARGET_SPREAD_PCT', 0.003)))  # 0.3% default
MAX_EXPOSURE_PER_PAIR = Decimal(str(getattr(marcus, 'MAX_EXPOSURE_PER_PAIR', 0.10)))  # 10% max per pair
MAX_ORDERS_PER_PAIR = getattr(marcus, 'MAX_ORDERS_PER_PAIR', 1)
MAX_ACTIVE_PAIRS = getattr(marcus, 'MAX_ACTIVE_PAIRS', 5)
MIN_TRADE_SIZE = getattr(marcus, 'MIN_TRADE_SIZE', 10.0)  # Minimum trade in USDT
PID_FILE = "mm/config/pid/malcolm.pid"
HEARTBEAT_INTERVAL = 60  # seconds

# Channels from Lamar
CHAN_APPROVED = "proposals_approved_malcolm"
CHAN_DENIED = "proposals_denied_malcolm"
CHAN_EXPIRED = "proposals_expired_malcolm"

# Decimal helpers
D2 = Decimal("0.01")
D8 = Decimal("0.00000001")

def q2(x: Decimal) -> Decimal:
    """Quantize to 2 decimal places"""
    return x.quantize(D2, rounding=ROUND_DOWN)

def q8(x: Decimal) -> Decimal:
    """Quantize to 8 decimal places"""
    return x.quantize(D8, rounding=ROUND_DOWN)

# Logger
logger = logging.getLogger("malcolm")
logger.setLevel(logging.INFO)

# ============= SECTION 2: MALCOLM CLASS DEFINITION ================

class Malcolm:
    """
    Purchasing Manager - Authority for buy orders
    
    Responsibilities:
    - Get best trading pairs from Dr. Calvin
    - Create buy proposals when funds available
    - Place orders on approval
    - Link orders to holds
    - Respect position limits
    """
    
    def __init__(self):
        """Initialize Malcolm with database connections and trading client"""
        
        logger.info("Malcolm initializing...")
        
        # Get mode and trading client from Inara
        self.mode = get_mode()
        self.client = get_trading_client()
        logger.info(f"Malcolm mode: {self.mode}")
        
        # Initialize dependencies
        self.andi = get_andi()
        self.julius = Julius()  # For checking funds
        self.drcalvin = ValueOps()  # For getting best pairs
        
        # Database connections
        self._setup_db_connections()
        
        # State tracking
        self.running = False
        self.shutdown_requested = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        self.pending_orders = {}  # prop_id -> order details
        self.active_pairs = set()  # Track which pairs we're trading
        
        # Signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Malcolm initialized successfully")
    
    def _setup_db_connections(self):
        """Setup PostgreSQL connections for LISTEN and operations"""
        
        # LISTEN connection (autocommit ON for notifications)
        self.listen_conn = psycopg2.connect(
            dbname="ariadne",
            user="postgres",
            host="localhost"
        )
        self.listen_conn.set_session(autocommit=True)
        self.listen_cur = self.listen_conn.cursor()
        
        # Subscribe to channels
        self.listen_cur.execute(f"LISTEN {CHAN_APPROVED};")
        self.listen_cur.execute(f"LISTEN {CHAN_DENIED};")
        self.listen_cur.execute(f"LISTEN {CHAN_EXPIRED};")
        logger.info(f"Listening on: {CHAN_APPROVED}, {CHAN_DENIED}, {CHAN_EXPIRED}")
        
        # Operations connection (for queries/updates)
        self.ops_conn = psycopg2.connect(
            dbname="ariadne",
            user="postgres",
            host="localhost"
        )
        self.ops_cur = self.ops_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ============= SECTION 3: NOTIFICATION LISTENERS ==================
    
    def process_notifications(self):
        """Check for and process PostgreSQL notifications"""
        
        # Non-blocking check for notifications
        if select.select([self.listen_conn], [], [], 0) == ([], [], []):
            return
        
        self.listen_conn.poll()
        
        while self.listen_conn.notifies:
            notify = self.listen_conn.notifies.pop(0)
            channel = notify.channel
            payload = notify.payload
            
            logger.info(f"Received notification on {channel}: {payload}")
            
            try:
                data = json.loads(payload) if payload else {}
                
                if channel == CHAN_APPROVED:
                    self.handle_approval(data)
                elif channel == CHAN_DENIED:
                    self.handle_denial(data)
                elif channel == CHAN_EXPIRED:
                    self.handle_expiry(data)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")
    
    def handle_approval(self, data: Dict[str, Any]):
        """
        Handle proposal approval notification
        Place order immediately and link to hold
        """
        
        prop_id = data.get('prop_id')
        hold_id = data.get('hold_id')  # Only present in simulation
        
        if not prop_id:
            logger.error("Approval missing prop_id")
            return
        
        # Fetch proposal details
        proposal = self.fetch_proposal(prop_id)
        if not proposal:
            logger.error(f"Approved proposal {prop_id} not found")
            return
        
        # Place the order
        try:
            symbol = proposal['symbol']
            size = float(proposal['size_intent'])
            price = float(proposal['price_intent'])
            
            # Check mode and place order
            if self.mode in ['simulation', 'live']:
                order = self.client.create_limit_order(
                    symbol=symbol,
                    side='buy',
                    size=size,
                    price=price
                )
                
                order_id = order.get('orderId')
                logger.info(f"Placed buy order {order_id} for {symbol}: {size} @ {price}")
                
                # Record order via Andi
                self.andi.queue_order({
                    'order_id': order_id,
                    'symbol': symbol,
                    'side': 'buy',
                    'price': price,
                    'size': size,
                    'status': 'open',
                    'proposal_id': prop_id,
                    'origin': 'malcolm'
                })
                
                # Link hold to order (simulation only)
                if hold_id and self.mode == 'simulation':
                    self.link_hold_to_order(hold_id, order_id)
                
                # Track this pair as active
                self.active_pairs.add(symbol)
                    
            elif self.mode == 'shadow':
                # Shadow mode - log but don't place
                logger.info(f"[SHADOW] Would place buy: {symbol} {size} @ {price}")
                
            else:
                logger.warning(f"Cannot place orders in {self.mode} mode")
                
        except Exception as e:
            logger.error(f"Error placing order for proposal {prop_id}: {e}")
    
    def handle_denial(self, data: Dict[str, Any]):
        """Handle proposal denial notification"""
        
        prop_id = data.get('prop_id')
        reason = data.get('reason', 'Unknown')
        
        logger.warning(f"Proposal {prop_id} denied: {reason}")
        
        # Clean up any pending data
        self.pending_orders.pop(prop_id, None)
    
    def handle_expiry(self, data: Dict[str, Any]):
        """Handle proposal expiry notification"""
        
        prop_id = data.get('prop_id')
        
        logger.warning(f"Proposal {prop_id} expired")
        
        # Clean up
        self.pending_orders.pop(prop_id, None)

    # ============= SECTION 4: PROPOSAL GENERATION =====================
    
    def generate_buy_proposals(self):
        """
        Generate buy proposals for best trading pairs
        Only if funds available
        """
        
        # First check if we have available USDT
        available_usdt = self.julius.get_available_balance('USDT')
        
        if available_usdt < MIN_TRADE_SIZE:
            logger.debug(f"Insufficient funds: ${available_usdt:.2f} < ${MIN_TRADE_SIZE}")
            return
        
        logger.info(f"Generating buy proposals with ${available_usdt:.2f} available")
        
        # Get best pairs from Dr. Calvin
        best_pairs = self.drcalvin.get_best_pairs()
        
        if not best_pairs:
            logger.debug("No pairs from Dr. Calvin")
            return
        
        # Update active pairs tracking
        self.update_active_pairs()
        
        # Count how many proposals we can create
        proposals_created = 0
        max_proposals = min(
            MAX_ACTIVE_PAIRS - len(self.active_pairs),
            int(available_usdt / MIN_TRADE_SIZE)
        )
        
        if max_proposals <= 0:
            logger.debug(f"At maximum active pairs ({len(self.active_pairs)}/{MAX_ACTIVE_PAIRS})")
            return
        
        # Create proposals for top pairs
        for pair_data in best_pairs:
            if proposals_created >= max_proposals:
                break
            
            symbol = pair_data['symbol']
            score = pair_data.get('score', 0)
            
            # Skip if already trading this pair
            if symbol in self.active_pairs:
                continue
            
            # Skip if we already have pending order for this symbol
            if self.has_pending_order(symbol):
                continue
            
            # Calculate buy price and size
            price = self.calculate_buy_price(symbol)
            if not price:
                continue
            
            # Calculate position size (respecting max exposure)
            size = self.calculate_position_size(symbol, price, available_usdt)
            if not size or size * price < MIN_TRADE_SIZE:
                continue
            
            # Create proposal
            self.create_buy_proposal(symbol, size, price, score)
            proposals_created += 1
            
            # Reduce available for next calculation
            available_usdt -= (size * price)
        
        if proposals_created > 0:
            logger.info(f"Created {proposals_created} buy proposals")
    
    def calculate_buy_price(self, symbol: str) -> Optional[Decimal]:
        """
        Calculate buy price using market price and spread
        Buy below market to capture spread
        """
        
        market_price = self.get_market_price(symbol)
        if not market_price:
            return None
        
        # Buy at market price minus spread
        buy_price = market_price * (Decimal('1') - TARGET_SPREAD_PCT)
        
        # Round to 8 decimal places
        buy_price = q8(buy_price)
        
        logger.debug(f"{symbol}: market={market_price:.8f}, buy={buy_price:.8f}")
        
        return buy_price
    
    def calculate_position_size(self, symbol: str, price: Decimal, available: Decimal) -> Optional[Decimal]:
        """
        Calculate position size respecting exposure limits
        Max 10% of total capital per pair
        """
        
        # Get total equity for exposure calculation
        total_equity = self.julius.equity_usdt()
        
        if total_equity <= 0:
            return None
        
        # Maximum we can spend on this position
        max_position_value = total_equity * MAX_EXPOSURE_PER_PAIR
        
        # Also limited by available funds
        max_affordable = available
        
        # Use the smaller limit
        max_value = min(max_position_value, max_affordable)
        
        # Calculate size
        size = max_value / price
        
        # Round to 8 decimal places
        size = q8(size)
        
        logger.debug(f"{symbol}: equity=${total_equity:.2f}, max_exposure=${max_position_value:.2f}, "
                    f"available=${available:.2f}, size={size:.8f}")
        
        return size
    
    def get_market_price(self, symbol: str) -> Optional[Decimal]:
        """Get current market price for symbol"""
        
        try:
            # Try database first (tickstick)
            self.ops_cur.execute("""
                SELECT last FROM tickstick
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            
            result = self.ops_cur.fetchone()
            if result:
                return Decimal(str(result['last']))
            
            # Fallback to client
            ticker = self.client.get_ticker(symbol)
            if ticker and 'last' in ticker:
                return Decimal(str(ticker['last']))
                
        except Exception as e:
            logger.error(f"Error getting market price for {symbol}: {e}")
        
        return None
    
    def create_buy_proposal(self, symbol: str, size: Decimal, price: Decimal, score: float):
        """Create a buy proposal in the database"""
        
        try:
            # Estimate fees (0.1% of notional)
            notional = size * price
            fees_est = notional * Decimal('0.001')
            
            # Insert proposal
            self.ops_cur.execute("""
                INSERT INTO proposals 
                (symbol, side, price_intent, size_intent, fees_est, creator, created_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                RETURNING prop_id
            """, (
                symbol,
                'buy',
                str(price),
                str(size),
                str(fees_est),
                'malcolm',
                'pending'
            ))
            
            result = self.ops_cur.fetchone()
            prop_id = result['prop_id']
            
            self.ops_conn.commit()
            
            logger.info(f"Created buy proposal {prop_id}: {symbol} {size:.8f} @ {price:.8f} "
                       f"(score={score:.2f}, value=${notional:.2f})")
            
            # Track pending order
            self.pending_orders[prop_id] = {
                'symbol': symbol,
                'size': size,
                'price': price
            }
            
        except Exception as e:
            logger.error(f"Error creating proposal for {symbol}: {e}")
            self.ops_conn.rollback()

    # ============= SECTION 5: ORDER MANAGEMENT ========================
    
    def fetch_proposal(self, prop_id: str) -> Optional[Dict[str, Any]]:
        """Fetch proposal details from database"""
        
        try:
            self.ops_cur.execute("""
                SELECT * FROM proposals
                WHERE prop_id = %s AND deleted = FALSE
            """, (prop_id,))
            
            return self.ops_cur.fetchone()
            
        except Exception as e:
            logger.error(f"Error fetching proposal {prop_id}: {e}")
            return None
    
    def link_hold_to_order(self, hold_id: str, order_id: str):
        """Link hold to order (simulation only)"""
        
        try:
            # Julius handles the hold linking
            self.julius.link_hold_to_order(hold_id, order_id)
            
            logger.info(f"Linked hold {hold_id} to order {order_id}")
            
        except Exception as e:
            # Alert but don't fail - order is already placed
            logger.warning(f"Could not link hold {hold_id} to order {order_id}: {e}")
    
    def has_pending_order(self, symbol: str) -> bool:
        """Check if we have a pending order for this symbol"""
        
        for order in self.pending_orders.values():
            if order['symbol'] == symbol:
                return True
        return False
    
    def update_active_pairs(self):
        """Update list of pairs we're actively trading"""
        
        try:
            # Get all open orders
            if self.mode == 'simulation':
                self.ops_cur.execute("""
                    SELECT DISTINCT symbol FROM sim_orders
                    WHERE status = 'open' AND side = 'buy'
                      AND deleted = FALSE
                """)
            else:
                self.ops_cur.execute("""
                    SELECT DISTINCT symbol FROM orders
                    WHERE status = 'open' AND side = 'buy'
                      AND deleted = FALSE
                """)
            
            pairs = self.ops_cur.fetchall()
            
            self.active_pairs = {row['symbol'] for row in pairs}
            
            logger.debug(f"Active pairs: {self.active_pairs}")
            
        except Exception as e:
            logger.error(f"Error updating active pairs: {e}")

    # ============= SECTION 6: HOUSEKEEPING ============================
    
    def cleanup_old_proposals(self):
        """Clean up old pending proposals that never got processed"""
        
        try:
            # Mark proposals older than 5 minutes as expired
            self.ops_cur.execute("""
                UPDATE proposals
                SET status = 'expired',
                    decision_notes = 'Expired by Malcolm cleanup',
                    decision_stamp = NOW()
                WHERE creator = 'malcolm'
                  AND status = 'pending'
                  AND created_at < NOW() - INTERVAL '5 minutes'
                RETURNING prop_id
            """)
            
            expired = self.ops_cur.fetchall()
            
            if expired:
                self.ops_conn.commit()
                logger.info(f"Cleaned up {len(expired)} expired proposals")
                
                # Clean pending orders
                for row in expired:
                    self.pending_orders.pop(row['prop_id'], None)
            else:
                self.ops_conn.rollback()
                
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")
            self.ops_conn.rollback()

    # ============= SECTION 7: LIFECYCLE & MONITORING ==================
    
    def run_forever(self):
        """Main execution loop"""
        
        write_pid_file(PID_FILE)
        self.running = True
        
        logger.info("Malcolm main loop starting...")
        
        try:
            while self.running and not self.shutdown_requested:
                
                # Process notifications
                self.process_notifications()
                
                # Generate buy proposals every cycle
                # Skip if mode doesn't allow trading
                if self.mode in ['simulation', 'live', 'shadow']:
                    self.generate_buy_proposals()
                
                # Cleanup old proposals (every 10 cycles)
                if self.cycle_count % 10 == 0:
                    self.cleanup_old_proposals()
                
                # Heartbeat (every 60 seconds)
                if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
                    update_heartbeat('malcolm')
                    logger.debug(f"Heartbeat {self.cycle_count}, active pairs: {len(self.active_pairs)}")
                    self.last_heartbeat = time.time()
                
                self.cycle_count += 1
                
                # Sleep before next cycle
                time.sleep(1)  # 1 second between cycles
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown"""
        
        logger.info("Malcolm shutting down...")
        self.running = False
        
        # Close database connections
        try:
            self.listen_cur.close()
            self.listen_conn.close()
            self.ops_cur.close()
            self.ops_conn.close()
        except:
            pass
        
        cleanup_pid_file(PID_FILE)
        logger.info("Malcolm shutdown complete")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_requested = True

# ============= MAIN ENTRY POINT ====================================

if __name__ == "__main__":
    try:
        malcolm = Malcolm()
        malcolm.run_forever()
    except Exception as e:
        logger.error(f"Failed to start Malcolm: {e}")
        cleanup_pid_file(PID_FILE)