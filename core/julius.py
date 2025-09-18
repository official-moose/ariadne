#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ✅
#===================================================================
# Julius - Banker
# mm/core/julius.py
#
# The single point of truth for cash/wallet state inside 
# the system.
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

#🔶 SECTION 1: IMPORTS & CONFIGURATION =============================

import os
import sys
import json
import time
import signal
import logging
import uuid
import select
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# 🔹 Add project root to path ======================================

sys.path.append('/root/Echelon/valentrix')

# 🔹 Local imports
from mm.config import marcus
from mm.utils.helpers.inara import get_mode, get_trading_client
from mm.utils.tqt.andi import get_andi
from mm.utils.helpers.wintermute import (
    write_pid_file, 
    cleanup_pid_file,
    update_heartbeat
)

# 🔹 Configuration =================================================

FEE_RESERVE_USDT = Decimal("20.00")
HOLD_SWEEP_AGE_SEC = 600  # 10 minutes for stale holds
PROPOSAL_TIMEOUT_SEC = 30  # 30 seconds for proposal expiry
BALANCES_JSON_PATH = os.getenv("BALANCES_JSON_PATH", "mm/data/source/balances.json")
PID_FILE = "mm/config/pid/julius.pid"

# 🔹 Channels ======================================================

CHAN_BANK_VET = "bank_vet_req"
CHAN_FIN_VET = "final_vet_req" 
CHAN_HOLD_CREATED = "order_hold_created"

# 🔹 Decimal helpers ===============================================

D2 = Decimal("0.01")
D8 = Decimal("0.00000001")

def q2(x: Decimal) -> Decimal:
    """Quantize to 2 decimal places (USDT)"""
    return x.quantize(D2, rounding=ROUND_DOWN)

def q8(x: Decimal) -> Decimal:
    """Quantize to 8 decimal places (crypto)"""
    return x.quantize(D8, rounding=ROUND_DOWN)

# 🔹 Advanced Logger ===============================================

from mm.utils.helpers.wintermute import init_logging

logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Julius"  
)

# === End === 

# 🔶 SECTION 2: JULIUS CLASS DEFINITION ============================

class Julius:
    """
    Banking Manager - Single authority for wallet operations
    
    Responsibilities:
    - Vet proposals for banking requirements
    - Create/manage USDT holds for orders
    - Final approval for BUY proposals  
    - Profitability check for SELL proposals
    - Mode-aware balance management
    """
    
    def __init__(self):
        """Initialize Julius with database connections and trading client"""
        
        logger.info("Julius initializing...")
        
        # 🔹 Get mode and trading client from Inara ================
        
        self.mode = get_mode()
        self.client = get_trading_client()
        logger.info(f"Julius mode: {self.mode}")
        
        # 🔹 Initialize Andi for hold operations ===================
        
        self.andi = get_andi()
        
        # 🔹 Database connections ==================================
        
        self._setup_db_connections()
        
        # 🔹 State tracking ========================================
        
        self.running = False
        self.shutdown_requested = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        self.last_sweep = time.time()
        
        # 🔹 Signal handlers =======================================
        
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Julius initialized successfully")
    
    def _setup_db_connections(self):
        """Setup PostgreSQL connections for LISTEN and operations"""
        
        # 🔹 LISTEN connection (autocommit ON for notifications) ===
        
        self.listen_conn = psycopg2.connect(
            dbname="ariadne", 
            user="postgres", 
            host="localhost"
        )
        self.listen_conn.set_session(autocommit=True)
        self.listen_cur = self.listen_conn.cursor()
        
        # 🔹 Subscribe to channels =================================
        
        self.listen_cur.execute(f"LISTEN {CHAN_BANK_VET};")
        self.listen_cur.execute(f"LISTEN {CHAN_FIN_VET};")
        logger.info(f"Listening on channels: {CHAN_BANK_VET}, {CHAN_FIN_VET}")
        
        # 🔹 Operations connection (for queries/updates) ===========
        
        self.ops_conn = psycopg2.connect(
            dbname="ariadne",
            user="postgres", 
            host="localhost"
        )
        self.ops_cur = self.ops_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 🔶 SECTION 3: PROPOSAL LISTENERS =================================
    
    def process_notifications(self):
        """Check for and process PostgreSQL notifications"""
        
        # 🔹 Non-blocking check for notifications ==================
        
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
                    
                if channel == CHAN_BANK_VET:
                    self.handle_bank_vet(prop_id)
                elif channel == CHAN_FIN_VET:
                    self.handle_final_vet(prop_id)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")

# 🔶 SECTION 4: PROPOSAL VETTING ===================================
    
    def handle_bank_vet(self, prop_id: str):
        """
        Handle initial banking approval request
        For BUY: Check available USDT
        For SELL: Check profitability
        """
        
        try:
            # 🔹 Fetch proposal ====================================
            
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
            price = Decimal(str(proposal['price_intent']))
            size = Decimal(str(proposal['size_intent']))
            fees_est = Decimal(str(proposal.get('fees_est', '0')))
            
            approved = False
            reason = ""
            
            if side == 'buy':
                # Check available USDT for purchase
                required_usdt = (price * size) + fees_est
                available_usdt = self.get_available_balance('USDT')
                
                if available_usdt >= required_usdt:
                    approved = True
                    logger.info(f"BUY approved: {required_usdt} USDT available")
                else:
                    reason = f"Insufficient USDT: need {required_usdt}, have {available_usdt}"
                    logger.warning(f"BUY denied: {reason}")
                    
            elif side == 'sell':
                # Check profitability (TODO: implement cost basis lookup)
                # For now, approve if sale price > fees
                min_profitable = fees_est * 2  # Must cover both buy and sell fees
                sale_value = price * size
                
                if sale_value > min_profitable:
                    approved = True
                    logger.info(f"SELL approved: profitable at {sale_value} USDT")
                else:
                    reason = f"Not profitable: {sale_value} USDT < {min_profitable} min"
                    logger.warning(f"SELL denied: {reason}")
            
            # 🔹 Update proposal ===================================
            
            self.ops_cur.execute("""
                UPDATE proposals 
                SET bank_vet = %s,
                    bank_stamp = NOW(),
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
            
            logger.info(f"Bank vet complete for {prop_id}: {'approved' if approved else 'denied'}")
            
        except Exception as e:
            logger.error(f"Error in handle_bank_vet: {e}")
            self.ops_conn.rollback()
    
    def handle_final_vet(self, prop_id: str):
        """
        Handle final approval request (BUY orders only)
        Julius does final approval for buys, Helen for sells
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
            
            # Only handle BUY final approval
            if proposal['side'] != 'buy':
                logger.debug(f"Skipping final vet for SELL proposal {prop_id}")
                return
                
            # Check age
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
            
            # Check all vets are approved
            bank_vet = proposal['bank_vet']
            invt_vet = proposal['invt_vet'] 
            risk_vet = proposal['risk_vet']
            
            all_approved = (bank_vet == 'approved' and 
                           invt_vet == 'approved' and 
                           risk_vet == 'approved')
            
            if all_approved:
                # Create hold if in simulation mode
                hold_id = None
                if self.mode == 'simulation':
                    hold_id = self._create_buy_hold(proposal)
                
                # Mark approved
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
                # Denied - compile reasons
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

# 🔶 SECTION 5: CORE BANKING =======================================
    
    def get_available_balance(self, asset: str) -> Decimal:
        """
        Get available balance for an asset
        Mode-aware: reads from JSON in sim, API in live
        """
        
        asset = asset.upper()
        
        if self.mode == 'simulation':
            # Read from sim_balances table
            try:
                self.ops_cur.execute("""
                    SELECT available FROM sim_balances 
                    WHERE asset = %s
                """, (asset,))
                
                result = self.ops_cur.fetchone()
                if result:
                    return Decimal(str(result['available']))
                return Decimal('0')
                
            except Exception as e:
                logger.error(f"Error reading sim balance: {e}")
                return Decimal('0')
                
        else:  # live mode
            # Read from KuCoin API via client
            try:
                balances = self.client.get_accounts()
                for bal in balances:
                    if bal['currency'] == asset and bal['type'] == 'trade':
                        return Decimal(str(bal['available']))
                return Decimal('0')
                
            except Exception as e:
                logger.error(f"Error reading live balance: {e}")
                return Decimal('0')
    
    def get_all_balances(self) -> Dict[str, Dict[str, Decimal]]:
        """
        Get all balances
        Returns: {asset: {available, hold, total}}
        """
        
        balances = {}
        
        if self.mode == 'simulation':
            try:
                self.ops_cur.execute("""
                    SELECT asset, available, hold, 
                           (available + hold) as total
                    FROM sim_balances
                """)
                
                for row in self.ops_cur.fetchall():
                    balances[row['asset']] = {
                        'available': Decimal(str(row['available'])),
                        'hold': Decimal(str(row['hold'])),
                        'total': Decimal(str(row['total']))
                    }
                    
            except Exception as e:
                logger.error(f"Error reading sim balances: {e}")
                
        else:  # live mode
            try:
                accounts = self.client.get_accounts()
                for acc in accounts:
                    if acc['type'] == 'trade':
                        asset = acc['currency']
                        balances[asset] = {
                            'available': Decimal(str(acc['available'])),
                            'hold': Decimal(str(acc['hold'])),
                            'total': Decimal(str(acc['balance']))
                        }
                        
            except Exception as e:
                logger.error(f"Error reading live balances: {e}")
        
        return balances
    
    def _create_buy_hold(self, proposal: Dict) -> str:
        """
        Create hold for BUY order via Andi
        Returns hold_id
        """
        
        hold_id = f"HLD-{uuid.uuid4().hex[:8]}"
        
        price = Decimal(str(proposal['price_intent']))
        size = Decimal(str(proposal['size_intent']))
        fees = Decimal(str(proposal.get('fees_est', '0')))
        amount = (price * size) + fees
        
        payload = {
            'hold_id': hold_id,
            'symbol': proposal['symbol'],
            'side': 'buy',
            'asset': 'USDT',
            'amount': str(amount),
            'origin': 'julius',
            'reason': f"Buy hold for proposal {proposal['prop_id']}"
        }
        
        self.andi.queue_hold('holds.create', payload, 'julius')
        logger.info(f"Created hold {hold_id} for {amount} USDT")
        
        return hold_id
    
    def link_hold_to_order(self, hold_id: str, order_id: str):
        """Link a hold to an order after order placement"""
        
        payload = {
            'hold_id': hold_id,
            'order_id': order_id
        }
        
        self.andi.queue_hold('holds.link', payload, 'julius')
        logger.info(f"Linked hold {hold_id} to order {order_id}")
    
    def release_hold(self, hold_id: str, amount: Decimal, reason: str = "Order cancelled"):
        """Release a hold (partial or full)"""
        
        payload = {
            'hold_id': hold_id,
            'asset': 'USDT',
            'amount': str(amount),
            'reason': reason
        }
        
        self.andi.queue_hold('holds.release', payload, 'julius')
        logger.info(f"Released {amount} from hold {hold_id}")
    
    def settle_hold(self, order_id: str, symbol: str, side: str, 
                    fill_price: Decimal, fill_qty: Decimal, 
                    fee_paid: Decimal, pnl: Decimal = None):
        """Settle a hold after order fill"""
        
        base, quote = symbol.split('-')
        
        if side == 'buy':
            # Deduct USDT, add base asset
            asset_out = 'USDT'
            amount_out = (fill_price * fill_qty) + fee_paid
            asset_in = base
            amount_in = fill_qty
        else:
            # This shouldn't happen (Julius doesn't hold for sells)
            logger.error(f"Unexpected sell settlement for order {order_id}")
            return
        
        # Find hold by order_id
        self.ops_cur.execute("""
            SELECT hold_id FROM wallet_holds
            WHERE order_id = %s AND status = 'active'
        """, (order_id,))
        
        result = self.ops_cur.fetchone()
        if not result:
            logger.error(f"No active hold found for order {order_id}")
            return
            
        hold_id = result['hold_id']
        
        payload = {
            'hold_id': hold_id,
            'order_id': order_id,
            'asset_out': asset_out,
            'amount_out': str(amount_out),
            'asset_in': asset_in,
            'amount_in': str(amount_in),
            'fee_paid': str(fee_paid),
            'pnl': str(pnl) if pnl else None
        }
        
        self.andi.queue_hold('holds.settle', payload, 'julius')
        logger.info(f"Settled hold {hold_id} for order {order_id}")
    
    def _notify_hold_created(self, prop_id: str, hold_id: str):
        """Notify Lamar that hold was created"""
        
        payload = json.dumps({
            'prop_id': prop_id,
            'hold_id': hold_id,
            'created_by': 'julius'
        })
        
        self.ops_cur.execute(f"NOTIFY {CHAN_HOLD_CREATED}, %s", (payload,))
        self.ops_conn.commit()
        logger.info(f"Notified hold creation: {hold_id} for {prop_id}")

# 🔶 SECTION 6: HOUSEKEEPING =======================================
    
    def sweep_stale_holds(self):
        """
        Find and release orphaned holds
        Runs periodically to maintain balance integrity
        """
        
        try:
            # Only in simulation mode
            if self.mode != 'simulation':

                return
                
            # Find stale holds (no order_id and older than threshold)
            age_minutes = HOLD_SWEEP_AGE_SEC / 60
            
            self.ops_cur.execute("""
                SELECT hold_id, asset, amount_remaining
                FROM wallet_holds
                WHERE status = 'active'
                  AND order_id IS NULL
                  AND created_at < NOW() - INTERVAL '%s minutes'
                  AND deleted = FALSE
            """, (age_minutes,))
            
            stale_holds = self.ops_cur.fetchall()
            
            if not stale_holds:
                return
                
            logger.info(f"Found {len(stale_holds)} stale holds to sweep")
            
            for hold in stale_holds:
                hold_id = hold['hold_id']
                asset = hold['asset']
                amount = hold['amount_remaining']
                
                # Release via Andi
                payload = {
                    'hold_id': hold_id,
                    'asset': asset,
                    'amount': str(amount),
                    'reason': f"Stale hold sweep after {age_minutes} minutes"
                }
                
                self.andi.queue_hold('holds.release', payload, 'julius')
                logger.info(f"Swept stale hold {hold_id}: {amount} {asset}")
                
        except Exception as e:
            logger.error(f"Error in sweep_stale_holds: {e}")

# 🔶 SECTION 7: LIFECYCLE & MONITORING =============================
    
    def run_forever(self):
        """Main execution loop"""
        
        write_pid_file(PID_FILE)
        self.running = True
        
        logger.info("Julius main loop starting...")
        
        try:
            while self.running and not self.shutdown_requested:
                
                # Process notifications
                self.process_notifications()
                
                # Periodic sweep (every 5 minutes)
                if time.time() - self.last_sweep > 300:
                    self.sweep_stale_holds()
                    self.last_sweep = time.time()
                
                # Heartbeat (every 60 seconds)
                if time.time() - self.last_heartbeat > 60:
                    update_heartbeat('julius')
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
        
        logger.info("Julius shutting down...")
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
        logger.info("Julius shutdown complete")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_requested = True

# 🔶 MAIN ENTRY POINT ==============================================

if __name__ == "__main__":
    try:
        julius = Julius()
        julius.run_forever()
    except Exception as e:
        logger.error(f"Failed to start Julius: {e}")
        cleanup_pid_file(PID_FILE)