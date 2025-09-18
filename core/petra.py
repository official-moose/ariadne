#===================================================================
# 🌹 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ✅
#===================================================================
# Petra - Sales Manager
# mm/core/petra.py
#
# Creates and manages sell orders
# Ensures profitable exits from positions
# Manages order lifecycle from proposal to execution
#
# [520] [741] [8]
#===================================================================
# 🜜 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
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

# Configuration
TARGET_SPREAD_PCT = Decimal(str(getattr(marcus, 'TARGET_SPREAD_PCT', 0.003)))  # 0.3% default
MIN_PROFIT_PCT = Decimal(str(getattr(marcus, 'MIN_PROFIT_PCT', 0.002)))  # 0.2% minimum profit
MIN_TRADE_SIZE = getattr(marcus, 'MIN_TRADE_SIZE', 10.0)  # Minimum trade in USDT
PID_FILE = "mm/config/pid/petra.pid"
HEARTBEAT_INTERVAL = 60  # seconds

# Channels from Lamar
CHAN_APPROVED = "proposals_approved_petra"
CHAN_DENIED = "proposals_denied_petra" 
CHAN_EXPIRED = "proposals_expired_petra"

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
logger = logging.getLogger("petra")
logger.setLevel(logging.INFO)

# ============= SECTION 2: PETRA CLASS DEFINITION ==================

class Petra:
    """
    Sales Manager - Authority for sell orders
    
    Responsibilities:
    - Create sell proposals for all inventory
    - Ensure profitable pricing (never sell at loss)
    - Place orders on approval
    - Link orders to asset holds
    - High-frequency profit taking
    """
    
    def __init__(self):
        """Initialize Petra with database connections and trading client"""
        
        logger.info("Petra initializing...")
        
        # Get mode and trading client from Inara
        self.mode = get_mode()
        self.client = get_trading_client()
        logger.info(f"Petra mode: {self.mode}")
        
        # Initialize Andi for order operations
        self.andi = get_andi()
        
        # Database connections
        self._setup_db_connections()
        
        # State tracking
        self.running = False
        self.shutdown_requested = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        self.pending_orders = {}  # prop_id -> order details
        
        # Signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Petra initialized successfully")
    
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
                    side='sell',
                    size=size,
                    price=price
                )
                
                order_id = order.get('orderId')
                logger.info(f"Placed sell order {order_id} for {symbol}: {size} @ {price}")
                
                # Record order via Andi
                self.andi.queue_order({
                    'order_id': order_id,
                    'symbol': symbol,
                    'side': 'sell',
                    'price': price,
                    'size': size,
                    'status': 'open',
                    'proposal_id': prop_id,
                    'origin': 'petra'
                })
                
                # Link hold to order (simulation only)
                if hold_id and self.mode == 'simulation':
                    self.link_hold_to_order(hold_id, order_id)
                    
            elif self.mode == 'shadow':
                # Shadow mode - log but don't place
                logger.info(f"[SHADOW] Would place sell: {symbol} {size} @ {price}")
                
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
        
        # Could resubmit if conditions still favorable
        # For now just clean up
        self.pending_orders.pop(prop_id, None)

    # ============= SECTION 4: PROPOSAL GENERATION =====================
    
    def generate_sell_proposals(self):
        """
        Generate sell proposals for all inventory positions
        Ensures profitable pricing and proper spread
        """
        
        # Get current positions
        positions = self.get_positions()
        
        if not positions:
            logger.debug("No positions to sell")
            return
        
        logger.info(f"Generating proposals for {len(positions)} positions")
        
        for symbol, position_data in positions.items():
            try:
                # Skip if position too small
                if position_data['value_usdt'] < MIN_TRADE_SIZE:
                    logger.debug(f"Position {symbol} too small: ${position_data['value_usdt']:.2f}")
                    continue
                
                # Calculate sell price
                sell_price = self.calculate_sell_price(symbol, position_data)
                
                if not sell_price:
                    logger.debug(f"Cannot determine profitable price for {symbol}")
                    continue
                
                # Create proposal
                self.create_sell_proposal(
                    symbol=symbol,
                    size=position_data['available'],
                    price=sell_price,
                    cost_basis=position_data['cost_basis']
                )
                
            except Exception as e:
                logger.error(f"Error generating proposal for {symbol}: {e}")
    
    def get_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all current positions with cost basis
        Returns: {symbol: {available, cost_basis, entry_price, value_usdt}}
        """
        
        positions = {}
        
        if self.mode == 'simulation':
            # Query sim_positions
            self.ops_cur.execute("""
                SELECT symbol, 
                       COALESCE(qty, 0) as total,
                       COALESCE(cost_basis, 0) as cost_basis,
                       COALESCE(entry_price, 0) as entry_price
                FROM sim_positions
                WHERE qty > 0
            """)
            
            for row in self.ops_cur.fetchall():
                symbol = row['symbol']
                total = Decimal(str(row['total']))
                
                # Check for active holds
                base, _ = parse_symbol(symbol)
                self.ops_cur.execute("""
                    SELECT COALESCE(SUM(qty_remaining), 0) as held
                    FROM asset_holds
                    WHERE asset = %s 
                      AND status = 'active'
                      AND deleted = FALSE
                """, (base,))
                
                held_result = self.ops_cur.fetchone()
                held = Decimal(str(held_result['held'])) if held_result else Decimal('0')
                
                available = total - held
                
                if available > 0:
                    positions[symbol] = {
                        'available': available,
                        'cost_basis': Decimal(str(row['cost_basis'])),
                        'entry_price': Decimal(str(row['entry_price'])),
                        'value_usdt': available * Decimal(str(row['entry_price']))
                    }
                    
        else:  # live mode
            # Get positions from exchange
            exchange_positions = self.client.get_positions()
            
            # Get cost basis from positions table
            for asset, info in exchange_positions.items():
                if asset == 'USDT':
                    continue
                    
                symbol = f"{asset}-USDT"
                available = Decimal(str(info['available']))
                
                if available > 0:
                    # Query positions table for cost basis
                    self.ops_cur.execute("""
                        SELECT cost_basis, entry_price
                        FROM positions
                        WHERE symbol = %s
                          AND deleted = FALSE
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (symbol,))
                    
                    cost_data = self.ops_cur.fetchone()
                    
                    if cost_data:
                        positions[symbol] = {
                            'available': available,
                            'cost_basis': Decimal(str(cost_data['cost_basis'])),
                            'entry_price': Decimal(str(cost_data['entry_price'])),
                            'value_usdt': available * Decimal(str(cost_data['entry_price']))
                        }
        
        return positions
    
    def calculate_sell_price(self, symbol: str, position_data: Dict) -> Optional[Decimal]:
        """
        Calculate profitable sell price
        Ensures we never sell below cost basis + fees
        """
        
        cost_basis = position_data['cost_basis']
        entry_price = position_data['entry_price']
        
        # Get current market price
        market_price = self.get_market_price(symbol)
        if not market_price:
            return None
        
        # Calculate minimum profitable price (cost + fees + min profit)
        # Assuming 0.1% fee on both buy and sell
        fee_rate = Decimal('0.001')
        min_price = entry_price * (Decimal('1') + fee_rate * 2 + MIN_PROFIT_PCT)
        
        # Calculate target price with spread
        target_price = market_price * (Decimal('1') + TARGET_SPREAD_PCT)
        
        # Use higher of minimum profitable or target
        sell_price = max(min_price, target_price)
        
        # Round to 8 decimal places
        sell_price = q8(sell_price)
        
        logger.debug(f"{symbol}: entry={entry_price:.8f}, market={market_price:.8f}, "
                    f"min={min_price:.8f}, target={target_price:.8f}, sell={sell_price:.8f}")
        
        return sell_price
    
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
    
    def create_sell_proposal(self, symbol: str, size: Decimal, price: Decimal, cost_basis: Decimal):
        """Create a sell proposal in the database"""
        
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
                'sell',
                str(price),
                str(size),
                str(fees_est),
                'petra',
                'pending'
            ))
            
            result = self.ops_cur.fetchone()
            prop_id = result['prop_id']
            
            self.ops_conn.commit()
            
            logger.info(f"Created sell proposal {prop_id}: {symbol} {size:.8f} @ {price:.8f} "
                       f"(cost_basis={cost_basis:.8f}, profit={(price/cost_basis - 1)*100:.2f}%)")
            
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
        """Link asset hold to order (simulation only)"""
        
        try:
            # Import Helen's method for linking
            # This updates the hold with the order_id
            from mm.core.helen import Helen
            helen = Helen()
            helen.link_asset_hold_to_order(hold_id, order_id)
            
            logger.info(f"Linked hold {hold_id} to order {order_id}")
            
        except Exception as e:
            # Alert but don't fail - order is already placed
            logger.warning(f"Could not link hold {hold_id} to order {order_id}: {e}")

    # ============= SECTION 6: HOUSEKEEPING ============================
    
    def cleanup_old_proposals(self):
        """Clean up old pending proposals that never got processed"""
        
        try:
            # Mark proposals older than 5 minutes as expired
            self.ops_cur.execute("""
                UPDATE proposals
                SET status = 'expired',
                    decision_notes = 'Expired by Petra cleanup',
                    decision_stamp = NOW()
                WHERE creator = 'petra'
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
        
        logger.info("Petra main loop starting...")
        
        try:
            while self.running and not self.shutdown_requested:
                
                # Process notifications
                self.process_notifications()
                
                # Generate sell proposals every cycle
                # Skip if mode doesn't allow trading
                if self.mode in ['simulation', 'live', 'shadow']:
                    self.generate_sell_proposals()
                
                # Cleanup old proposals (every 10 cycles)
                if self.cycle_count % 10 == 0:
                    self.cleanup_old_proposals()
                
                # Heartbeat (every 60 seconds)
                if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
                    update_heartbeat('petra')
                    logger.debug(f"Heartbeat {self.cycle_count}, tracking {len(self.pending_orders)} pending")
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
        
        logger.info("Petra shutting down...")
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
        logger.info("Petra shutdown complete")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_requested = True

# ============= MAIN ENTRY POINT ====================================

if __name__ == "__main__":
    try:
        petra = Petra()
        petra.run_forever()
    except Exception as e:
        logger.error(f"Failed to start Petra: {e}")
        cleanup_pid_file(PID_FILE)