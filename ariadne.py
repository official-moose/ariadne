#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                  Production ready ✅
#===================================================================
# Ariadne Actual
# mm/ariadne.py
#
# Initialized: August 19, 2025
# Sim Testing: ---   
# Launched: ---
# KuCoin exchange market maker bot. 
# Initial capital of $2,500 CAD
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import os
import importlib
import smtplib
import ssl
import uuid
import logging
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from zoneinfo import ZoneInfo

# 🔸 third-party imports ===========================================

from dotenv import load_dotenv

# 🔸 local application imports =====================================

import mm.config.marcus as marcus
from mm.core.drcalvin import ValueOps
from mm.core.grayson import RiskOps
from mm.utils.seldon_engine.quorra import SigmaOps
from mm.core.petra import Petra
from mm.core.helen import Helen
from mm.core.malcolm import Malcolm
from mm.core.julius import Julius
from mm.core.verity import IntelOps
from mm.utils.seldon_engine.lamar import Lamar
from mm.core.alec import Alec
from mm.utils.nexus_6.rachael import Replicant
from mm.utils.helpers.wintermute import update_heartbeat
from mm.utils.tqdm.agnes import setup_logger

logger = setup_logger("ariadne", level=logging.INFO)

# 🔸 load env for this process =====================================

load_dotenv("mm/data/secrets/.env")

# 🔸 Drop-in Emailer (from wintermute) =============================

from mm.utils.helpers.wintermute import send_email

# 🔸 Drop-in Logger (from wintermute)  =============================

from mm.utils.helpers.wintermute import init_logging

logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Ariadne"
)

# 🔸 Ariadne Class =================================================

class Ariadne:

    def __init__(self, inara, logger):
        self.inara = inara
        self.logger = logger
        self.client = None
        self.mode = None
        self.cycle_count = 0

 # 🔸 STARTUP =======================================================

    def run(self):
        import time

        while True:
            self.logger.info("Starting cycle...")
            self.mode = self.inara.get_mode()
            self.client = self.inara.get_trading_client()
            self.logger.info(f"Mode: {self.mode}, Client: {self.client}")

    # 🔸 OPEN ORDERS RISK ASSESSMENT====================================
            logger.info("▪▪▪▪▪▪▪▪→ Complete.\n")
            logger.info("Stating the risk assessment phase.")
        
            current_orders = self.client.get_orders()
            self.logger.info("Get open orders -> Fetched.")
            
            if not current_orders:
                self.logger.info("No open orders, moving on to production cycle.")
            
            for order in current_orders.copy():
                print("Cycling through open orders.") 
                order_id = order["id"]
                
                grayson = RiskOps(order)
                if not grayson.compliant():
                    Alec.cancel_orders_for_pair(order)
                    self.logger.info(f"Order {order_id} canceled by Grayson.")
                    continue

                score = ValueOps.score_pair(order)
                if score < 75:
                    Alec.cancel_orders_for_pair(order)
                    self.logger.info(f"Order {order_id} canceled by Dr. Calvin (score {score}).")
                    continue

                risk_client = SigmaOps(order)
                score2 = risk_client.score_pair()
                if score2 >= 80:
                    continue
                elif 70 <= score2 < 80:
                    Replicant(order).process()
                    current_orders.remove(order)
                    continue
                else:
                    Alec.cancel_orders_for_pair(order)
                    self.logger.info(f"Order {order_id} canceled by Quorra (score {score2}).")
                    continue

    # 🔸 SELL CYCLE ====================================================
            logger.info("▪▪▪▪▪▪▪▪→ Complete.\n")
            logger.info("Loading Petra, beginning the sell cycle.")
        
            self.logger.info("Starting sell cycle...")
        
            petra = Petra()
            proposals = petra.prepare_sell_orders(Helen.get_positions())

            for proposal in proposals:
                response = Lamar.listen(proposal)
                if response == "expired":
                    score = SigmaOps(proposal).score_pair()
                    if score >= 95:
                        petra.resubmit(proposal)
                elif response == "denied":
                    score = SigmaOps(proposal).score_pair()
                    if score <= 75:
                        self.logger.warning(f"Proposal denied and below threshold: {proposal}")
                elif response == "approved":
                    self.client.place_order(proposal)

    # 🔸 BUY CYCLE =====================================================
            
            best_pairs = Helen.get_best_pairs()
            best_pairs = [p for p in best_pairs if RiskOps(p).compliant()]
            scored_pairs = [(p, SigmaOps(p).score_pair()) for p in best_pairs]

            malcolm = Malcolm(self.client)
            buy_proposals = malcolm.prepare_buy_orders(scored_pairs)

            for proposal in buy_proposals:
                response = Lamar.listen(proposal)
                if response == "expired":
                    score = SigmaOps(proposal).score_pair()
                    if score >= 95:
                        malcolm.resubmit(proposal)
                elif response == "denied":
                    score = SigmaOps(proposal).score_pair()
                    if score <= 75:
                        self.logger.warning(f"Proposal denied and below threshold: {proposal}")
                elif response == "approved":
                    self.client.place_order(proposal)
                    Database.record_order(proposal)

    # 🔸 HOUSEKEEPING CYCLE ============================================

            if self.mode == "simulation":
                Julius().sweep_stale_holds()
                Helen.sweep_stale_holds()

            IntelOps(self.client).scan()

            if self.cycle_count % 10 == 0:
                Database.save_state()

            if self.cycle_count % 6 == 0:  # ~ every 2 minutes if cycle ~20s
                update_heartbeat("ariadne", conn)

            self.cycle_count += 1
            time.sleep(20)
            
# ⚡ Entry Point ⚡ ==================================================
            
print("✔ File loaded")

if __name__ == "__main__":
    print("✔ Main block entered")

    try:
        from mm.utils.helpers import inara
        print("✔ Inara imported")

        bot = Ariadne(inara, logger)
        print("🧬 Ariadne instantiated")

        bot.run()
    except Exception as e:
        print("⛔ CRASH:", e)