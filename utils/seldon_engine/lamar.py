#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250909.01
#===================================================================
# last update: 2025 | Sept. 9                   Production ready ❌
#===================================================================
# Lamar - Proposal Approvals Orchestrator
# mm/utils/seldon_engine/lamar.py
#
# Listens to Postgres and notifies managers.
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import json
import select
import logging
from psycopg2.extras import DictCursor
from mm.utils.helpers.wintermute import get_db_connection
from mm.utils.helpers.inara import get_mode

logger = logging.getLogger(__name__)

# Incoming channels
CHAN_VET = "proposals_vet_changed"
CHAN_HOLD_CREATED = "order_hold_created"

# Outgoing channels
CHAN_BANK_VET = "bank_vet_req"
CHAN_INV_VET = "inventory_vet_req"
CHAN_RISK_VET = "risk_vet_req"
CHAN_FIN_VET = "final_vet_req"
CHAN_BID_APP = "proposals_approved_malcolm"
CHAN_BID_DEN = "proposals_denied_malcolm"
CHAN_BID_EXP = "proposals_expired_malcolm"
CHAN_ASK_APP = "proposals_approved_petra"
CHAN_ASK_DEN = "proposals_denied_petra"
CHAN_ASK_EXP = "proposals_expired_petra"

class SigInt: 
    def __init__(self):
        self.listen_conn = None
        self.oper_conn = get_db_connection()
        self.pending_holds = {}  # prop_id -> creator mapping while waiting for hold

    def _open_listen(self):
        self.listen_conn = get_db_connection(autocommit=True)
        cur = self.listen_conn.cursor()
        cur.execute(f"LISTEN {CHAN_VET};")
        cur.execute(f"LISTEN {CHAN_EXPIRED};")
        cur.execute(f"LISTEN {CHAN_HOLD_CREATED};")
        cur.close()
        logger.info(f"[LISTEN] Channels: {CHAN_VET}, {CHAN_EXPIRED}, {CHAN_HOLD_CREATED}")

    def _notify(self, channel, payload):
        cur = self.oper_conn.cursor()
        cur.execute(f"NOTIFY {channel}, %s;", (json.dumps(payload),))
        self.oper_conn.commit()
        cur.close()

    def _log_routing(self, proposal_id, routed_to, notes=None):
        cur = self.oper_conn.cursor()
        cur.execute(
            """
            INSERT INTO proposal_router_log (proposal_id, routed_to, notes)
            VALUES (%s, %s, %s)
            ON CONFLICT (proposal_id, routed_to) DO NOTHING;
            """,
            (proposal_id, routed_to, notes),
        )
        self.oper_conn.commit()
        cur.close()

    def _fetch_proposal(self, prop_id):
        cur = self.oper_conn.cursor(cursor_factory=DictCursor)
        cur.execute("SELECT * FROM proposals WHERE prop_id = %s;", (prop_id,))
        row = cur.fetchone()
        cur.close()
        return dict(row) if row else None

    def _handle_vet_update(self, prop_id):
        """Handle when any vet field changes"""
        proposal = self._fetch_proposal(prop_id)
        if not proposal:
            logger.warning(f"Proposal {prop_id} not found.")
            return

        creator = proposal["creator"]
        
        # Check for denials
        for vet_type, manager in [("risk_vet", "grayson"), ("invt_vet", "helen"), ("bank_vet", "julius")]:
            if proposal.get(vet_type) == "denied":
                channel = CHAN_DENIED_M if creator == "Malcolm" else CHAN_DENIED_P
                self._notify(channel, {"proposal_id": prop_id, "denied_by": manager})
                self._log_routing(prop_id, "originator", notes=f"denied by {manager}")
                return

        # Check if all approved
        if all(proposal.get(field) == "approved" for field in ["risk_vet", "invt_vet", "bank_vet"]):
            mode = get_mode()
            
            if mode == "simulation":
                # In simulation, request hold creation from appropriate manager
                if creator == "Malcolm":
                    self._notify(CHAN_CREATE_HOLD_J, {"proposal_id": prop_id})
                    self.pending_holds[prop_id] = creator
                    self._log_routing(prop_id, "julius", notes="requesting hold creation")
                elif creator == "Petra":
                    self._notify(CHAN_CREATE_HOLD_H, {"proposal_id": prop_id})
                    self.pending_holds[prop_id] = creator
                    self._log_routing(prop_id, "helen", notes="requesting hold creation")
            else:
                # In live mode, holds are created differently (handled by managers directly)
                # Just mark as ready immediately
                self._mark_ready(prop_id, creator)

    def _handle_hold_created(self, prop_id):
        """Handle when Julius/Helen confirm hold is created"""
        creator = self.pending_holds.pop(prop_id, None)
        if not creator:
            logger.warning(f"Unexpected hold created for {prop_id}")
            return
        
        # Now that hold is created, mark as ready
        self._mark_ready(prop_id, creator)

    def _mark_ready(self, prop_id, creator):
        """Send ready signal to originator"""
        # Update proposal status
        cur = self.oper_conn.cursor()
        cur.execute(
            "UPDATE proposals SET status = 'approved' WHERE prop_id = %s;",
            (prop_id,)
        )
        self.oper_conn.commit()
        cur.close()
        
        # Notify originator
        channel = CHAN_READY_M if creator == "Malcolm" else CHAN_READY_P
        self._notify(channel, {"proposal_id": prop_id})
        self._log_routing(prop_id, "originator", notes="fully approved and ready")

    def _handle_expiry(self, prop_id):
        """Handle expired proposals"""
        proposal = self._fetch_proposal(prop_id)
        if not proposal:
            return

        creator = proposal["creator"]
        channel = CHAN_DENIED_M if creator == "Malcolm" else CHAN_DENIED_P
        self._notify(channel, {"proposal_id": prop_id, "expired": True})
        self._log_routing(prop_id, "originator", notes="expired")
        
        # Clean up any pending hold request
        self.pending_holds.pop(prop_id, None)

    def run(self):
        self._open_listen()
        logger.info("[LAMAR] Routing loop started.")

        while True:
            if select.select([self.listen_conn], [], [], 5) == ([], [], []):
                continue

            self.listen_conn.poll()
            while self.listen_conn.notifies:
                notify = self.listen_conn.notifies.pop(0)
                try:
                    payload = json.loads(notify.payload)
                    prop_id = payload.get("proposal_id")
                    
                    if not prop_id:
                        continue

                    if notify.channel == CHAN_VET:
                        self._handle_vet_update(prop_id)
                    elif notify.channel == CHAN_EXPIRED:
                        self._handle_expiry(prop_id)
                    elif notify.channel == CHAN_HOLD_CREATED:
                        self._handle_hold_created(prop_id)
                        
                except Exception as e:
                    logger.error(f"Error processing notification: {e}")

if __name__ == "__main__":
    lamar = Lamar()
    lamar.run()