#===================================================================
# 🌸 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ✅
#===================================================================
# Helen - Inventory Manager
# mm/core/helen.py
#
# Manages inventory positions and asset holds
# Vets proposals for inventory availability
# Final approval for SELL proposals
#
# [520] [741] [8]
#===================================================================
# 💫 PERSISTENT RUNTIME      ➰ MONIT MANAGED      ✔ MODE AWARE
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
from typing import Dict, Optional, Tuple, Any
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
MAX_ASSET_PCT = getattr(marcus, 'MAX_ASSET_PCT', Decimal('0.20'))  # 20% max per asset
CAP_MARGIN = getattr(marcus, 'CAP_MARGIN', Decimal('0.01'))  # 1% margin
PROPOSAL_TIMEOUT_SEC = 30
PID_FILE = "mm/config/pid/helen.pid"

# Channels
CHAN_INV_VET = "inventory_vet_req"
CHAN_FIN_VET = "final_vet_req"
CHAN_HOLD_CREATED = "order_hold_created"

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
logger = logging.getLogger("helen")
logger.setLevel(logging.INFO)

# ============= SECTION 2: HELEN CLASS DEFINITION ==================

class Helen:
    """
    Inventory Manager - Authority for asset positions
    
    Responsibilities:
    - Vet proposals for inventory requirements
    - Manage asset holds for orders
    - Final approval for SELL proposals
    - Enforce concentration limits
    - Mode-aware position management
    """
    
    def __init__(self):
        """Initialize Helen with database connections and trading client"""
        
        logger.info("Helen initializing...")
        
        # Get mode and trading client from Inara
        self.mode = get_mode()
        self.client = get_trading_client()
        logger.info(f"Helen mode: {self.mode}")
        
        # Initialize Andi for asset operations
        self.andi = get_andi()
        
        # Database connections
        self._setup_db_connections()
        
        # State tracking
        self.running = False
        self.shutdown_requested = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        
        # Signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Helen initialized successfully")
    
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
        self.listen_cur.execute(f"LISTEN {CHAN_INV_VET};")
        self.listen_cur.execute(f"LISTEN {CHAN_FIN_VET};")
        logger.info(f"Listening on channels: {CHAN_INV_VET}, {CHAN_FIN_VET}")
        
        # Operations connection (for queries/updates)
        self.ops_conn = psycopg2.connect(
            dbname="ariadne",
            user="postgres",
            host="localhost"
        )
        self.ops_cur = self.ops_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ============= SECTION 3: PROPOSAL LISTENERS ======================
    
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
                prop_id = data.get('prop_id')
                
                if not prop_id:
                    logger.error(f"No prop_id in notification payload")
                    continue
                
                if channel == CHAN_INV_VET:
                    self.handle_inventory_vet(prop_id)
                elif channel == CHAN_FIN_VET:
                    self.handle_final_vet(prop_id)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")

    # ============= SECTION 4: PROPOSAL VETTING ========================
    
    def handle_inventory_vet(self, prop_id: str):
        """
        Handle inventory vetting request
        For BUY: Check concentration limits
        For SELL: Check available inventory
        """
        
        try:
            # Fetch proposal
            self.ops_cur.execute("""
                SELECT * FROM proposals 
                WHERE prop_id = %s AND deleted = FALSE
            """, (prop_id,))
            
            proposal = self.ops_cur.fetchone()
            if not proposal:
                logger.error(f"Proposal {prop_id} not found")
                return
            
            side = proposal['side']
            symbol = proposal['symbol']
            size = Decimal(str(proposal['size_intent']))
            price = Decimal(str(proposal['price_intent']))
            
            approved = False
            reason = ""
            
            if side == 'buy':
                # Check if purchase would breach concentration limits
                approved, reason = self.check_concentration_limit(symbol, size, price)
                
            elif side == 'sell':
                # Check if we have enough inventory
                approved, reason = self.check_inventory_available(symbol, size)
            
            # Update proposal
            self.ops_cur.execute("""
                UPDATE proposals 
                SET invt_vet = %s,
                    invt_stamp = NOW(),
                    decision_notes = CASE 
                        WHEN %s != '' THEN 
                            COALESCE(decision_notes || '; ', '') || %s
                        ELSE decision_notes
                    END
                WHERE prop_id = %s
            """, (
                'approved' if approved else 'denied',
                reason, reason,
                prop_id
            ))
            self.ops_conn.commit()
            
            logger.info(f"Inventory vet complete for {prop_id}: {'approved' if approved else 'denied'}")
            
        except Exception as e:
            logger.error(f"Error in handle_inventory_vet: {e}")
            self.ops_conn.rollback()
    
    def handle_final_vet(self, prop_id: str):
        """
        Handle final approval request (SELL orders only)
        Helen does final approval for sells, Julius for buys
        """
        
        try:
            # Fetch proposal with all vetting status
            self.ops_cur.execute("""
                SELECT * FROM proposals 
                WHERE prop_id = %s AND deleted = FALSE
            """, (prop_id,))
            
            proposal = self.ops_cur.fetchone()
            if not proposal:
                logger.error(f"Proposal {prop_id} not found")
                return
            
            # Only handle SELL final approval
            if proposal['side'] != 'sell':
                logger.debug(f"Skipping final vet for BUY proposal {prop_id}")
                return
            
            # Check age (step 18 of workflow!)
            created_at = proposal['created_at']
            age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
            
            if age_seconds > PROPOSAL_TIMEOUT_SEC:
                # Expired
                self.ops_cur.execute("""
                    UPDATE proposals 
                    SET status = 'expired',
                        decision_stamp = NOW(),
                        decision_notes = %s
                    WHERE prop_id = %s
                """, (
                    f"Proposal timed out: {age_seconds:.1f} seconds",
                    prop_id
                ))
                self.ops_conn.commit()
                logger.warning(f"Proposal {prop_id} expired: {age_seconds:.1f}s")
                return
            
            # Check all vets are approved (step 17)
            bank_vet = proposal['bank_vet']
            invt_vet = proposal['invt_vet']
            risk_vet = proposal['risk_vet']
            
            all_approved = (bank_vet == 'approved' and 
                           invt_vet == 'approved' and 
                           risk_vet == 'approved')
            
            if all_approved:
                # Create asset hold if in simulation mode (step 20)
                hold_id = None
                if self.mode == 'simulation':
                    hold_id = self._create_asset_hold(proposal)
                
                # Mark approved (step 19)
                self.ops_cur.execute("""
                    UPDATE proposals 
                    SET status = 'approved',
                        decision_stamp = NOW()
                    WHERE prop_id = %s
                """, (prop_id,))
                self.ops_conn.commit()
                
                # Notify hold created (if applicable)
                if hold_id:
                    self._notify_hold_created(prop_id, hold_id)
                
                logger.info(f"Proposal {prop_id} APPROVED (hold: {hold_id})")
                
            else:
                # Denied - compile reasons (step 17)
                reasons = []
                if bank_vet != 'approved':
                    reasons.append("Not bank approved")
                if invt_vet != 'approved':
                    reasons.append("Not inventory approved")
                if risk_vet != 'approved':
                    reasons.append("Not risk approved")
                
                reason_text = "; ".join(reasons)
                
                self.ops_cur.execute("""
                    UPDATE proposals 
                    SET status = 'denied',
                        decision_stamp = NOW(),
                        decision_notes = %s
                    WHERE prop_id = %s
                """, (reason_text, prop_id))
                self.ops_conn.commit()
                
                logger.warning(f"Proposal {prop_id} DENIED: {reason_text}")
                
        except Exception as e:
            logger.error(f"Error in handle_final_vet: {e}")
            self.ops_conn.rollback()

    # ============= SECTION 5: CORE INVENTORY MANAGEMENT ===============
    
    def check_concentration_limit(self, symbol: str, size: Decimal, price: Decimal) -> Tuple[bool, str]:
        """
        Check if BUY would breach concentration limits
        Returns (approved, reason)
        """
        
        base, quote = parse_symbol(symbol)
        
        # Calculate what portfolio would look like after purchase
        purchase_value = size * price
        
        # Get current portfolio value
        total_portfolio_value = self.get_total_portfolio_value()
        
        # Get current asset value
        current_asset_value = self.get_asset_value(base)
        
        # Calculate new concentration
        new_asset_value = current_asset_value + purchase_value
        new_portfolio_value = total_portfolio_value + purchase_value
        
        if new_portfolio_value == Decimal('0'):
            return True, ""  # Can't breach limits if no portfolio
        
        concentration = new_asset_value / new_portfolio_value
        
        # Check against limit + margin
        limit = MAX_ASSET_PCT + CAP_MARGIN
        
        if concentration > limit:
            reason = f"Would breach concentration limit: {concentration:.1%} > {limit:.1%}"
            logger.warning(f"BUY denied for {symbol}: {reason}")
            return False, reason
        
        return True, ""
    
    def check_inventory_available(self, symbol: str, size: Decimal) -> Tuple[bool, str]:
        """
        Check if we have enough inventory for SELL
        Returns (approved, reason)
        """
        
        base, quote = parse_symbol(symbol)
        
        # Get available inventory (total minus holds)
        available = self.get_available_inventory(base)
        
        if available >= size:
            return True, ""
        else:
            reason = f"Insufficient inventory: need {size}, have {available}"
            logger.warning(f"SELL denied for {symbol}: {reason}")
            return False, reason
    
    def get_available_inventory(self, asset: str) -> Decimal:
        """
        Get available inventory for an asset
        Available = total position - active holds
        """
        
        if self.mode == 'simulation':
            # Query sim_positions
            self.ops_cur.execute("""
                SELECT COALESCE(qty, 0) as total
                FROM sim_positions 
                WHERE symbol = %s
            """, (f"{asset}-USDT",))
            
            result = self.ops_cur.fetchone()
            total = Decimal(str(result['total'])) if result else Decimal('0')
            
            # Subtract active holds
            self.ops_cur.execute("""
                SELECT COALESCE(SUM(qty_remaining), 0) as held
                FROM asset_holds
                WHERE asset = %s 
                  AND status = 'active'
                  AND deleted = FALSE
            """, (asset,))
            
            result = self.ops_cur.fetchone()
            held = Decimal(str(result['held'])) if result else Decimal('0')
            
            return total - held
            
        else:  # live mode
            # Get positions from exchange
            positions = self.client.get_positions()
            
            if asset in positions:
                available = Decimal(str(positions[asset]['available']))
                return available
            
            return Decimal('0')
    
    def get_asset_value(self, asset: str) -> Decimal:
        """Get current value of asset holdings in USDT"""
        
        if asset == 'USDT':
            # USDT value is just the amount
            if self.mode == 'simulation':
                self.ops_cur.execute("""
                    SELECT COALESCE(available, 0) as amount
                    FROM sim_balances
                    WHERE asset = 'USDT'
                """)
                result = self.ops_cur.fetchone()
                return Decimal(str(result['amount'])) if result else Decimal('0')
            else:
                positions = self.client.get_positions()
                if 'USDT' in positions:
                    return Decimal(str(positions['USDT']['total']))
                return Decimal('0')
        
        # For other assets, get quantity and multiply by last price
        quantity = self.get_available_inventory(asset) 
        
        # Get last trade price
        symbol = f"{asset}-USDT"
        last_price = self.get_last_price(symbol)
        
        return quantity * last_price
    
    def get_total_portfolio_value(self) -> Decimal:
        """Get total portfolio value in USDT"""
        
        total = Decimal('0')
        
        if self.mode == 'simulation':
            # Get all positions
            self.ops_cur.execute("""
                SELECT symbol, qty FROM sim_positions
                WHERE qty > 0
            """)
            
            positions = self.ops_cur.fetchall()
            
            for pos in positions:
                symbol = pos['symbol']
                qty = Decimal(str(pos['qty']))
                
                # Get last price
                last_price = self.get_last_price(symbol)
                total += qty * last_price
            
            # Add USDT balance
            self.ops_cur.execute("""
                SELECT COALESCE(available, 0) + COALESCE(hold, 0) as total
                FROM sim_balances
                WHERE asset = 'USDT'
            """)
            
            result = self.ops_cur.fetchone()
            if result:
                total += Decimal(str(result['total']))
                
        else:  # live mode
            positions = self.client.get_positions()
            
            for asset, info in positions.items():
                if asset == 'USDT':
                    total += Decimal(str(info['total']))
                else:
                    qty = Decimal(str(info['total']))
                    symbol = f"{asset}-USDT"
                    last_price = self.get_last_price(symbol)
                    total += qty * last_price
        
        return total
    
    def get_last_price(self, symbol: str) -> Decimal:
        """Get last trade price for a symbol"""
        
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
            logger.error(f"Error getting last price for {symbol}: {e}")
        
        return Decimal('0')
    
    def _create_asset_hold(self, proposal: Dict) -> str:
        """
        Create asset hold for SELL order via Andi
        Returns hold_id
        """
        
        hold_id = f"HLD-{uuid.uuid4().hex[:8]}"
        
        symbol = proposal['symbol']
        base, quote = parse_symbol(symbol)
        size = Decimal(str(proposal['size_intent']))
        
        payload = {
            'hold_id': hold_id,
            'symbol': symbol,
            'side': 'sell',
            'asset': base,
            'qty': str(size),
            'origin': 'helen',
            'reason': f"Sell hold for proposal {proposal['prop_id']}"
        }
        
        self.andi.queue_asset('assets.create', payload, 'helen')
        logger.info(f"Created asset hold {hold_id} for {size} {base}")
        
        return hold_id
    
    def link_asset_hold_to_order(self, hold_id: str, order_id: str):
        """Link an asset hold to an order after order placement"""
        
        payload = {
            'hold_id': hold_id,
            'order_id': order_id
        }
        
        self.andi.queue_asset('assets.link', payload, 'helen')
        logger.info(f"Linked asset hold {hold_id} to order {order_id}")
    
    def release_asset_hold(self, hold_id: str, qty: Decimal, reason: str = "Order cancelled"):
        """Release an asset hold (partial or full)"""
        
        payload = {
            'hold_id': hold_id,
            'qty': str(qty),
            'reason': reason
        }
        
        self.andi.queue_asset('assets.release', payload, 'helen')
        logger.info(f"Released {qty} from asset hold {hold_id}")
    
    def settle_asset_hold(self, hold_id: str, order_id: str, asset: str, qty: Decimal):
        """Settle an asset hold after order fill"""
        
        payload = {
            'hold_id': hold_id,
            'order_id': order_id,
            'asset': asset,
            'qty': str(qty)
        }
        
        self.andi.queue_asset('assets.settle', payload, 'helen')
        logger.info(f"Settled asset hold {hold_id} for order {order_id}")
    
    def _notify_hold_created(self, prop_id: str, hold_id: str):
        """Notify Lamar that hold was created"""
        
        payload = json.dumps({
            'prop_id': prop_id,
            'hold_id': hold_id,
            'created_by': 'helen'
        })
        
        self.ops_cur.execute(f"NOTIFY {CHAN_HOLD_CREATED}, %s", (payload,))
        self.ops_conn.commit()
        logger.info(f"Notified hold creation: {hold_id} for {prop_id}")

    # ============= SECTION 6: HOUSEKEEPING ============================
    
    def sweep_stale_asset_holds(self):
        """
        Find and release orphaned asset holds
        Runs periodically to maintain inventory integrity
        """
        
        try:
            # Only in simulation mode
            if self.mode != 'simulation':
                return
            
            # Find stale holds (no order_id and older than 10 minutes)
            self.ops_cur.execute("""
                SELECT hold_id, asset, qty_remaining
                FROM asset_holds
                WHERE status = 'active'
                  AND order_id IS NULL
                  AND created_at < NOW() - INTERVAL '10 minutes'
                  AND deleted = FALSE
            """)
            
            stale_holds = self.ops_cur.fetchall()
            
            if not stale_holds:
                return
            
            logger.info(f"Found {len(stale_holds)} stale asset holds to sweep")
            
            for hold in stale_holds:
                hold_id = hold['hold_id']
                asset = hold['asset']
                qty = hold['qty_remaining']
                
                # Release via Andi
                payload = {
                    'hold_id': hold_id,
                    'asset': asset,
                    'qty': str(qty),
                    'reason': "Stale hold sweep after 10 minutes"
                }
                
                self.andi.queue_asset('assets.release', payload, 'helen')
                logger.info(f"Swept stale asset hold {hold_id}: {qty} {asset}")
                
        except Exception as e:
            logger.error(f"Error in sweep_stale_asset_holds: {e}")

    # ============= SECTION 7: LIFECYCLE & MONITORING ==================
    
    def run_forever(self):
        """Main execution loop"""
        
        write_pid_file(PID_FILE)
        self.running = True
        
        logger.info("Helen main loop starting...")
        
        try:
            while self.running and not self.shutdown_requested:
                
                # Process notifications
                self.process_notifications()
                
                # Periodic sweep (every 5 minutes)
                if self.cycle_count % 300 == 0:
                    self.sweep_stale_asset_holds()
                
                # Heartbeat (every 60 seconds)
                if time.time() - self.last_heartbeat > 60:
                    update_heartbeat('helen')
                    self.cycle_count += 1
                    logger.debug(f"Heartbeat {self.cycle_count}")
                    self.last_heartbeat = time.time()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown"""
        
        logger.info("Helen shutting down...")
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
        logger.info("Helen shutdown complete")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_requested = True

# ============= MAIN ENTRY POINT ====================================

if __name__ == "__main__":
    try:
        helen = Helen()
        helen.run_forever()
    except Exception as e:
        logger.error(f"Failed to start Helen: {e}")
        cleanup_pid_file(PID_FILE)