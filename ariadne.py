#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
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

# 🔸 Drop-in Emailer ===============================================

def send_email(subject: str, status: str, title: str, message: str) -> str:

    importlib.reload(marcus)
    if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
        return "disabled"
    if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
        return "Simple Mail Transfer Protocol not established. No conn."

    host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
    port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
    recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

    USERCODE = "ARI"  # hardcode per file

    # ---- Edit Sender Info (per file) ----
    user = os.getenv(f"{USERCODE}_USR")
    pwd = os.getenv(f"{USERCODE}_PWD")
    sender_email = user
    sender_name = os.getenv(f"{USERCODE}_NAME")
    # -------------------------------------

    STATUS_COLORS = {
        "STATCON3": "#F1C232",
        "STATCON2": "#E69138",
        "STATCON1": "#CC0000",
        "SIGCON1":  "#FB6D8B",
        "OPSCON5":  "#F5F5F5",
        "OPSCON1":  "#990000",
    }
    status_text = str(status).upper()
    status_color = STATUS_COLORS.get(status_text, "#BE644C")

    msg = EmailMessage()
    domain = sender_email.split("@")[1] if "@" in sender_email else "hodlcorp.io"
    msg_id = f"<{uuid.uuid4()}@{domain}>"
    msg["Message-ID"] = msg_id
    msg["From"] = formataddr((sender_name, sender_email))
    msg["To"] = recipient
    msg["Subject"] = subject
    msg["X-Priority"] = "1"
    msg["X-MSMail-Priority"] = "High"
    msg["Importance"] = "High"

    now_tz = datetime.now(ZoneInfo("America/Toronto"))
    sent_str = now_tz.strftime("%Y-%m-%d %H:%M:%S America/Toronto")
    epoch_ms = int(now_tz.timestamp() * 1000)
    mid_clean = msg_id.strip("<>").split("@", 1)[0]

    html_body = f"""
<div style="font-family: monospace;">
  <table role="presentation" width="100%" height="20px" cellpadding="8px" cellspacing="0" border="0">
    <tbody><tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
      <td align="left" style="color:#EFEFEF;letter-spacing:12px;">INTCOMM</td>
      <td align="right" style="color:{status_color};letter-spacing:4px;">{status_text}</td>
    </tr>
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:14px;font-weight:600;color:#333;">
      <td colspan="2">{title}</td>
    </tr>
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:11px;font-weight:400;line-height:1.5;color:#333;">
      <td colspan="2">{message}</td>
    </tr>
    <tr width="100%" height="25px"><td colspan="2">&nbsp;</td></tr>
  </tbody></table>
  <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
    <tbody><tr style="background-color:#333;">
      <td colspan="2" style="color:#efefef;font-size:12px;font-weight:600;">DOCINT</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">SENT</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{sent_str}</td>
    </tr>
    <tr style="background-color:#F2F2F0;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">EPOCH</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{epoch_ms} (ms since 1970/01/01 0:00 UTC)</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">m.ID</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{mid_clean}</td>
    </tr>
  </tbody></table>
</div>
"""

    msg.add_alternative(html_body, subtype="html")
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as s:
        if user and pwd:
            s.login(user, pwd)
        s.send_message(msg)

    return msg_id

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
            
            self.logger.info("Starting sell cycle...")
        
            petra = Petra(self.client)
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