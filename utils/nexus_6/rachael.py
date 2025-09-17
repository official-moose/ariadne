#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250915.01
#===================================================================
# last update: 2025 | Sept. 15                  Production ready ❌
#===================================================================
# Rachael
# mm/utils/nexus-6/rachael.py
#
# Rachael: Nexus-6 logic unit for near-pass order recovery.
# Handles orders scoring between 70–80 by applying staged 
# adjustments (split, widen, reprice), tracking each attempt via 
# persistent memory. 
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

import os
import json
import logging
from datetime import datetime

# ── Logger Setup ──────────────────────────────────────────────────────
logger = logging.getLogger('rachael.replicant')
logger.setLevel(logging.INFO)

log_path = os.path.join(os.path.dirname(__file__), 'rachael.log')
handler = logging.FileHandler(log_path)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

MEMORY_PATH = os.path.join(os.path.dirname(__file__), 'memories')
os.makedirs(MEMORY_PATH, exist_ok=True)

# ── Rachael Class ──────────────────────────────────────────────────────

class Replicant:
    """
    Rachael: Nexus-6 logic unit for near-pass order recovery.
    Handles orders scoring between 70–80 by applying staged adjustments
    (split, widen, reprice), tracking each attempt via persistent memory.
    """

    def __init__(self, order: dict):
        self.order = order
        self.order_id = str(order.get('id'))
        self.memory_file = os.path.join(MEMORY_PATH, f"{self.order_id}.json")
        self.meta = self._recall()

    def _recall(self):
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"[🕊️ Rachael] Order {self.order_id}: memory load failed: {e}")
                return {"naomi_stage": 0}
        else:
            return {
                "naomi_stage": 0,
                "first_seen": datetime.utcnow().isoformat()
            }

    def _remember(self):
        try:
            with open(self.memory_file, 'w') as f:
                json.dump(self.meta, f, indent=4)
        except Exception as e:
            logger.error(f"[🕊️ Rachael] Order {self.order_id}: failed to write memory: {e}")

    def process(self):
        stage = self.meta.get("naomi_stage", 0)

        if stage == 0:
            self._apply_split()
        elif stage == 1:
            self._apply_widen()
        elif stage == 2:
            self._apply_reprice()
        else:
            logger.info(f"[🕊️ Rachael] Order {self.order_id}: exhausted all options.")
            return

        self.meta["naomi_stage"] = stage + 1
        self.meta["last_modified"] = datetime.utcnow().isoformat()
        self._remember()

    def _apply_split(self):
        logger.info(f"[🕊️ Rachael] Order {self.order_id}: attempting split.")

        original_size = self.order.get('size')
        if not original_size or original_size < 2:
            logger.info(f"[🕊️ Rachael] Order {self.order_id}: split skipped — size too small.")
            return

        split_size = original_size // 2
        logger.info(f"[🕊️ Rachael] Order {self.order_id}: split into 2 x {split_size} units.")

        self.meta["last_adjustment"] = {
            "type": "split",
            "new_size": split_size
        }

    def _apply_widen(self):
        logger.info(f"[🕊️ Rachael] Order {self.order_id}: widening spread.")

        current_spread = self.order.get('spread', 0.005)
        widened_spread = round(current_spread * 1.2, 6)
        logger.info(f"[🕊️ Rachael] Order {self.order_id}: spread widened from {current_spread:.4%} → {widened_spread:.4%}.")

        self.order['spread'] = widened_spread
        self.meta["last_adjustment"] = {
            "type": "widen",
            "old_spread": current_spread,
            "new_spread": widened_spread
        }

    def _apply_reprice(self):
        logger.info(f"[🕊️ Rachael] Order {self.order_id}: adjusting price level.")

        price = self.order.get('price')
        side = self.order.get('side')
        if not price or not side:
            logger.warning(f"[🕊️ Rachael] Order {self.order_id}: insufficient data to reprice.")
            return

        adjust_pct = 0.002  # 0.2%
        if side == 'buy':
            new_price = round(price * (1 - adjust_pct), 2)

        else:
            new_price = round(price * (1 + adjust_pct), 2)

        logger.info(f"[🕊️ Rachael] Order {self.order_id}: price adjusted from {price} → {new_price} ({side}).")

        self.order['price'] = new_price
        self.meta["last_adjustment"] = {
            "type": "reprice",
            "old_price": price,
            "new_price": new_price
        }

