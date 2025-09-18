#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ✅
#===================================================================
# Lamar - Proposal Approvals Orchestrator
# mm/utils/seldon_engine/lamar.py
#
# Orchestrates proposal vetting sequence
# Routes notifications between managers
# Tracks state in JSON for speed
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

# ============= SECTION 1: IMPORTS & CONFIGURATION =================

import os
import sys
import json
import time
import signal
import select
import logging
from typing import Dict, Optional, Any
from pathlib import Path

import psycopg2
import psycopg2.extras

# Add project root to path
sys.path.append('/root/Echelon/valentrix')

# Local imports
from mm.utils.helpers.inara import get_mode
from mm.utils.helpers.wintermute import (
    write_pid_file,
    cleanup_pid_file,
    update_heartbeat
)

# Configuration
STATE_FILE = "mm/data/state/proposals_state.json"
PID_FILE = "mm/config/pid/lamar.pid"
HEARTBEAT_INTERVAL = 60  # seconds

# PostgreSQL trigger channels (incoming)
CHAN_NEW_PROPOSAL = "trg_proposal_insert"
CHAN_VET_UPDATE = "trg_proposal_vet_update"
CHAN_HOLD_CREATED = "order_hold_created"

# Manager notification channels (outgoing)
CHAN_BANK_VET = "bank_vet_req"
CHAN_INV_VET = "inventory_vet_req"
CHAN_RISK_VET = "risk_vet_req"
CHAN_FIN_VET = "final_vet_req"

# Originator notification channels (outgoing)
CHAN_BID_APP = "proposals_approved_malcolm"
CHAN_BID_DEN = "proposals_denied_malcolm"
CHAN_BID_EXP = "proposals_expired_malcolm"
CHAN_ASK_APP = "proposals_approved_petra"
CHAN_ASK_DEN = "proposals_denied_petra"
CHAN_ASK_EXP = "proposals_expired_petra"

# Logger
logger = logging.getLogger("lamar")
logger.setLevel(logging.INFO)

# ============= SECTION 2: LAMAR CLASS DEFINITION ==================

class Lamar:
    """
    Proposal Approvals Orchestrator
    
    Responsibilities:
    - Listen for new proposals and vet updates
    - Route to appropriate managers in correct sequence
    - Track state in JSON for speed
    - Notify originators of final decisions
    """
    
    def __init__(self):
        """Initialize Lamar with database connections and state tracking"""
        
        logger.info("Lamar initializing...")
        
        # Database connections
        self._setup_db_connections()
        
        # State tracking
        self.proposal_states = {}  # prop_id -> state dict
        self.pending_holds = {}    # prop_id -> creator mapping
        self._load_state()
        
        # Lifecycle
        self.running = False
        self.shutdown_requested = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        
        # Signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Lamar initialized successfully")
    
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
        self.listen_cur.execute(f"LISTEN {CHAN_NEW_PROPOSAL};")
        self.listen_cur.execute(f"LISTEN {CHAN_VET_UPDATE};")
        self.listen_cur.execute(f"LISTEN {CHAN_HOLD_CREATED};")
        logger.info(f"Listening on: {CHAN_NEW_PROPOSAL}, {CHAN_VET_UPDATE}, {CHAN_HOLD_CREATED}")
        
        # Operations connection (for queries/updates)
        self.ops_conn = psycopg2.connect(
            dbname="ariadne",
            user="postgres",
            host="localhost"
        )
        self.ops_cur = self.ops_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ============= SECTION 3: POSTGRESQL LISTENERS ====================
    
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
            
            logger.info(f"Notification on {channel}: {payload}")
            
            try:
                data = json.loads(payload) if payload else {}
                
                if channel == CHAN_NEW_PROPOSAL:
                    self.handle_new_proposal(data)
                elif channel == CHAN_VET_UPDATE:
                    self.handle_vet_update(data)
                elif channel == CHAN_HOLD_CREATED:
                    self.handle_hold_created(data)
                    
            except Exception as e:
                logger.error(f"Error processing notification: {e}")

    # ============= SECTION 4: PROPOSAL ROUTING LOGIC ==================
    
    def handle_new_proposal(self, data: Dict[str, Any]):
        """
        Handle new proposal notification
        Route to first approver based on side
        """
        
        prop_id = data.get('prop_id')
        if not prop_id:
            logger.error("New proposal missing prop_id")
            return
        
        # Fetch proposal details
        proposal = self.fetch_proposal(prop_id)
        if not proposal:
            logger.error(f"Proposal {prop_id} not found")
            return
        
        side = proposal['side']
        creator = proposal['creator']
        
        # Initialize state tracking
        self.proposal_states[prop_id] = {
            'side': side,
            'creator': creator,
            'bank_vet': None,
            'invt_vet': None,
            'risk_vet': None,
            'current_step': 'start',
            'timestamp': time.time()
        }
        self._save_state()
        
        # Route to first approver based on side
        if side == 'buy':
            # BUY: Start with Julius
            self.notify_manager(CHAN_BANK_VET, prop_id)
            self.proposal_states[prop_id]['current_step'] = 'bank_vet'
            logger.info(f"BUY proposal {prop_id} routed to Julius")
            
        elif side == 'sell':
            # SELL: Start with Helen
            self.notify_manager(CHAN_INV_VET, prop_id)
            self.proposal_states[prop_id]['current_step'] = 'invt_vet'
            logger.info(f"SELL proposal {prop_id} routed to Helen")
        
        # Log routing
        self.log_routing(prop_id, self.proposal_states[prop_id]['current_step'], 
                        f"Initial routing for {side} proposal")
    
    def handle_vet_update(self, data: Dict[str, Any]):
        """
        Handle vet update notification
        Route to next approver in sequence or finalize
        """
        
        prop_id = data.get('prop_id')
        if not prop_id:
            logger.error("Vet update missing prop_id")
            return
        
        # Check if we're tracking this proposal
        if prop_id not in self.proposal_states:
            logger.warning(f"Vet update for untracked proposal {prop_id}")
            # Try to recover state from DB
            proposal = self.fetch_proposal(prop_id)
            if proposal:
                self.proposal_states[prop_id] = {
                    'side': proposal['side'],
                    'creator': proposal['creator'],
                    'bank_vet': proposal.get('bank_vet'),
                    'invt_vet': proposal.get('invt_vet'),
                    'risk_vet': proposal.get('risk_vet'),
                    'current_step': 'unknown',
                    'timestamp': time.time()
                }
        
        state = self.proposal_states[prop_id]
        
        # Fetch current proposal status
        proposal = self.fetch_proposal(prop_id)
        if not proposal:
            logger.error(f"Proposal {prop_id} not found")
            return
        
        # Update our state with latest vets
        state['bank_vet'] = proposal.get('bank_vet')
        state['invt_vet'] = proposal.get('invt_vet')
        state['risk_vet'] = proposal.get('risk_vet')
        
        # Check for any denials
        if state['bank_vet'] == 'denied' or state['invt_vet'] == 'denied' or state['risk_vet'] == 'denied':
            self.handle_denial(prop_id, proposal)
            return
        
        # Check for expiry
        if proposal['status'] == 'expired':
            self.handle_expiry(prop_id, proposal)
            return
        
        # Route to next step based on side and current progress
        if state['side'] == 'buy':
            self.route_buy_proposal(prop_id, state)
        else:
            self.route_sell_proposal(prop_id, state)
        
        self._save_state()
    
    def route_buy_proposal(self, prop_id: str, state: Dict[str, Any]):
        """
        Route BUY proposal through sequence:
        Julius → Helen → Grayson → Julius(final)
        """
        
        if state['current_step'] in ['start', 'bank_vet'] and state['bank_vet'] == 'approved':
            # Julius done, send to Helen
            self.notify_manager(CHAN_INV_VET, prop_id)
            state['current_step'] = 'invt_vet'
            self.log_routing(prop_id, 'helen', 'BUY: bank approved, routing to inventory')
            
        elif state['current_step'] == 'invt_vet' and state['invt_vet'] == 'approved':
            # Helen done, send to Grayson
            self.notify_manager(CHAN_RISK_VET, prop_id)
            state['current_step'] = 'risk_vet'
            self.log_routing(prop_id, 'grayson', 'BUY: inventory approved, routing to risk')
            
        elif state['current_step'] == 'risk_vet' and state['risk_vet'] == 'approved':
            # Grayson done, send to Julius for final
            self.notify_manager(CHAN_FIN_VET, prop_id)
            state['current_step'] = 'final_vet'
            self.log_routing(prop_id, 'julius_final', 'BUY: risk approved, routing for final approval')
            
        elif state['current_step'] == 'final_vet':
            # Check if proposal is fully approved
            proposal = self.fetch_proposal(prop_id)
            if proposal['status'] == 'approved':
                # In simulation, wait for hold creation
                if get_mode() == 'simulation':
                    self.pending_holds[prop_id] = state['creator']
                    logger.info(f"BUY proposal {prop_id} approved, waiting for hold")
                else:
                    # In live mode, notify originator immediately
                    self.notify_originator_approved(prop_id, state['creator'])
    
    def route_sell_proposal(self, prop_id: str, state: Dict[str, Any]):
        """
        Route SELL proposal through sequence:
        Helen → Grayson → Julius → Helen(final)
        """
        
        if state['current_step'] in ['start', 'invt_vet'] and state['invt_vet'] == 'approved':
            # Helen done, send to Grayson
            self.notify_manager(CHAN_RISK_VET, prop_id)
            state['current_step'] = 'risk_vet'
            self.log_routing(prop_id, 'grayson', 'SELL: inventory approved, routing to risk')
            
        elif state['current_step'] == 'risk_vet' and state['risk_vet'] == 'approved':
            # Grayson done, send to Julius
            self.notify_manager(CHAN_BANK_VET, prop_id)
            state['current_step'] = 'bank_vet'
            self.log_routing(prop_id, 'julius', 'SELL: risk approved, routing to bank')
            
        elif state['current_step'] == 'bank_vet' and state['bank_vet'] == 'approved':
            # Julius done, send to Helen for final
            self.notify_manager(CHAN_FIN_VET, prop_id)
            state['current_step'] = 'final_vet'
            self.log_routing(prop_id, 'helen_final', 'SELL: bank approved, routing for final approval')
            
        elif state['current_step'] == 'final_vet':
            # Check if proposal is fully approved
            proposal = self.fetch_proposal(prop_id)
            if proposal['status'] == 'approved':
                # In simulation, wait for hold creation
                if get_mode() == 'simulation':
                    self.pending_holds[prop_id] = state['creator']
                    logger.info(f"SELL proposal {prop_id} approved, waiting for hold")
                else:
                    # In live mode, notify originator immediately
                    self.notify_originator_approved(prop_id, state['creator'])
    
    def handle_hold_created(self, data: Dict[str, Any]):
        """
        Handle hold creation notification
        Final step before notifying originator
        """
        
        prop_id = data.get('prop_id')
        hold_id = data.get('hold_id')
        
        if not prop_id or prop_id not in self.pending_holds:
            logger.warning(f"Unexpected hold notification for {prop_id}")
            return
        
        creator = self.pending_holds.pop(prop_id)
        logger.info(f"Hold {hold_id} created for proposal {prop_id}")
        
        # Notify originator that proposal is ready with hold
        self.notify_originator_approved(prop_id, creator, hold_id)
    
    def handle_denial(self, prop_id: str, proposal: Dict[str, Any]):
        """Handle denied proposal"""
        
        creator = proposal['creator']
        denied_by = []
        
        if proposal.get('bank_vet') == 'denied':
            denied_by.append('julius')
        if proposal.get('invt_vet') == 'denied':
            denied_by.append('helen')
        if proposal.get('risk_vet') == 'denied':
            denied_by.append('grayson')
        
        reason = f"Denied by: {', '.join(denied_by)}"
        
        # Notify originator of denial
        if creator == 'malcolm':
            channel = CHAN_BID_DEN
        else:
            channel = CHAN_ASK_DEN
        
        self.notify_channel(channel, {
            'prop_id': prop_id,
            'reason': reason,
            'type': 'denied'
        })
        
        # Clean up state
        self.proposal_states.pop(prop_id, None)
        self.pending_holds.pop(prop_id, None)
        self._save_state()
        
        logger.info(f"Proposal {prop_id} denied: {reason}")
    
    def handle_expiry(self, prop_id: str, proposal: Dict[str, Any]):
        """Handle expired proposal"""
        
        creator = proposal['creator']
        
        # Notify originator of expiry
        if creator == 'malcolm':
            channel = CHAN_BID_EXP
        else:
            channel = CHAN_ASK_EXP
        
        self.notify_channel(channel, {
            'prop_id': prop_id,
            'type': 'expired'
        })
        
        # Clean up state
        self.proposal_states.pop(prop_id, None)
        self.pending_holds.pop(prop_id, None)
        self._save_state()
        
        logger.info(f"Proposal {prop_id} expired")

    # ============= SECTION 5: STATE MANAGEMENT (JSON) =================
    
    def _load_state(self):
        """Load proposal states from JSON file"""
        
        try:
            state_path = Path(STATE_FILE)
            if state_path.exists():
                with open(state_path, 'r') as f:
                    data = json.load(f)
                    self.proposal_states = data.get('proposals', {})
                    self.pending_holds = data.get('pending_holds', {})
                    logger.info(f"Loaded {len(self.proposal_states)} proposals from state file")
            else:
                logger.info("No existing state file found")
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            self.proposal_states = {}
            self.pending_holds = {}
    
    def _save_state(self):
        """Save proposal states to JSON file"""
        
        try:
            state_path = Path(STATE_FILE)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'proposals': self.proposal_states,
                'pending_holds': self.pending_holds,
                'timestamp': time.time()
            }
            
            with open(state_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    # ============= SECTION 6: NOTIFICATION HANDLERS ===================
    
    def fetch_proposal(self, prop_id: str) -> Optional[Dict[str, Any]]:
        """Fetch proposal from database"""
        
        try:
            self.ops_cur.execute("""
                SELECT * FROM proposals 
                WHERE prop_id = %s AND deleted = FALSE
            """, (prop_id,))
            
            return self.ops_cur.fetchone()
            
        except Exception as e:
            logger.error(f"Error fetching proposal {prop_id}: {e}")
            return None
    
    def notify_manager(self, channel: str, prop_id: str):
        """Send notification to manager channel"""
        
        payload = json.dumps({'prop_id': prop_id})
        
        try:
            self.ops_cur.execute(f"NOTIFY {channel}, %s", (payload,))
            self.ops_conn.commit()
            logger.debug(f"Notified {channel} for proposal {prop_id}")
            
        except Exception as e:
            logger.error(f"Error notifying {channel}: {e}")
    
    def notify_channel(self, channel: str, data: Dict[str, Any]):
        """Send notification with data to channel"""
        
        payload = json.dumps(data)
        
        try:
            self.ops_cur.execute(f"NOTIFY {channel}, %s", (payload,))
            self.ops_conn.commit()
            logger.debug(f"Notified {channel}: {data}")
            
        except Exception as e:
            logger.error(f"Error notifying {channel}: {e}")
    
    def notify_originator_approved(self, prop_id: str, creator: str, hold_id: str = None):
        """Notify originator that proposal is approved and ready"""
        
        # Select channel based on creator
        if creator == 'malcolm':
            channel = CHAN_BID_APP
        else:
            channel = CHAN_ASK_APP
        
        data = {'prop_id': prop_id, 'type': 'approved'}
        if hold_id:
            data['hold_id'] = hold_id
        
        self.notify_channel(channel, data)
        
        # Clean up state
        self.proposal_states.pop(prop_id, None)
        self._save_state()
        
        logger.info(f"Proposal {prop_id} approved, notified {creator}")
    
    def log_routing(self, prop_id: str, routed_to: str, notes: str = None):
        """Log routing decision to database"""
        
        try:
            # Note: proposal_router_log uses integer proposal_id
            # Extract numeric ID from prop_id (e.g., "TMP00001M01" -> 1)
            proposal_num = int(''.join(filter(str.isdigit, prop_id.split('M')[0].split('P')[0])))
            
            self.ops_cur.execute("""
                INSERT INTO proposal_router_log (proposal_id, routed_to, notes)
                VALUES (%s, %s, %s)
            """, (proposal_num, routed_to, notes))
            self.ops_conn.commit()
            
        except Exception as e:
            logger.warning(f"Error logging route for {prop_id}: {e}")

    # ============= SECTION 7: LIFECYCLE & MONITORING ==================
    
    def run_forever(self):
        """Main execution loop"""
        
        write_pid_file(PID_FILE)
        self.running = True
        
        logger.info("Lamar orchestration loop starting...")
        
        try:
            while self.running and not self.shutdown_requested:
                
                # Process notifications
                self.process_notifications()
                
                # Clean up old proposals (older than 10 minutes)
                self.cleanup_old_proposals()
                
                # Heartbeat
                if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
                    update_heartbeat('lamar')
                    self.cycle_count += 1
                    logger.debug(f"Heartbeat {self.cycle_count}, tracking {len(self.proposal_states)} proposals")
                    self.last_heartbeat = time.time()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
        finally:
            self.shutdown()
    
    def cleanup_old_proposals(self):
        """Remove stale proposals from state tracking"""
        
        now = time.time()
        stale_threshold = 600  # 10 minutes
        
        to_remove = []
        for prop_id, state in self.proposal_states.items():
            if now - state['timestamp'] > stale_threshold:
                to_remove.append(prop_id)
        
        if to_remove:
            for prop_id in to_remove:
                self.proposal_states.pop(prop_id, None)
                self.pending_holds.pop(prop_id, None)
            self._save_state()
            logger.info(f"Cleaned up {len(to_remove)} stale proposals")
    
    def shutdown(self):
        """Clean shutdown"""
        
        logger.info("Lamar shutting down...")
        self.running = False
        
        # Save final state
        self._save_state()
        
        # Close database connections
        try:
            self.listen_cur.close()
            self.listen_conn.close()
            self.ops_cur.close()
            self.ops_conn.close()
        except:
            pass
        
        cleanup_pid_file(PID_FILE)
        logger.info("Lamar shutdown complete")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_requested = True

# ============= MAIN ENTRY POINT ====================================

if __name__ == "__main__":
    try:
        lamar = Lamar()
        lamar.run_forever()
    except Exception as e:
        logger.error(f"Failed to start Lamar: {e}")
        cleanup_pid_file(PID_FILE)