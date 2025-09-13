#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> Ariadne Actual
#>> mm/ariadne.py
#>>
#>> Initialized: August 19, 2025
#>> Sim Testing: ---   
#>> Launched: ---
#>> KuCoin exchange market maker bot. 
#>> Initial capital of $2,500 CAD
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]      
#>>────────────────────────────────────────────────────────────────

# Build|20250905.01

# ── 9.1 Import modules and packages ───────────────────────────────────
import os
import sys
import time
import json
import signal
import logging
import psycopg2
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ── 9.2 Import settings and parameters ────────────────────────────────
from mm.config.marcus import (
    GOLIVE, INITIAL_CAPITAL, QUOTE_CURRENCY, 
    MAX_EXPOSURE_PER_PAIR, MAX_TOTAL_EXPOSURE,
    TARGET_SPREAD_PCT, MIN_ORDER_SIZE_USDT,
    MAX_ORDERS_PER_PAIR, MAX_PAIRS_TO_TRADE,
    LOOP_DELAY, SIM_STATE_FILE, LIVE_STATE_FILE,
    SHOW_PROGRESS
)

# ── 9.3 Import Core Components (Managers) ─────────────────────────────
from mm.core.grayson import Grayson
from mm.core.naomi import Naomi
from mm.core.helen import Helen
from mm.core.petra import Petra
from mm.core.malcolm import Malcolm
from mm.core.julius import Julius
from mm.core.christian import Christian
from mm.core.alec import Alec
from mm.core.drcalvin import DrCalvin
from mm.core.verity import Verity

# ── 9.4 Helpers ───────────────────────────────────────────────────────
from mm.utils.helpers.timezone import get_email_date
from mm.utils.helpers.wintermute import parse_symbol

# ── 9.5 Set the Trading Client ────────────────────────────────────────
if GOLIVE:
    from mm.conn.conn_kucoin import KucoinClient as TradingClient
    STATE_FILE = LIVE_STATE_FILE
    DB_PATH = "/root/Echelon/valentrix/mm/data/live/ariadne_live.db"
else:
    from mm.conn.sim_kucoin import SimClient as TradingClient
    STATE_FILE = SIM_STATE_FILE
    DB_PATH = "/root/Echelon/valentrix/mm/data/sims/ariadne_sim.db"

# ── 9.6 Initialize @dataclass ─────────────────────────────────────────
@dataclass
class BotState:
    """Maintains bot state between cycles"""
    total_equity: float = 0.0
    open_orders: Dict[str, List[str]] = None
    positions: Dict[str, float] = None
    start_time: Optional[float] = None
    cycle_count: int = 0
    last_save: float = 0.0
    
    def __post_init__(self):
        if self.open_orders is None:
            self.open_orders = {}
        if self.positions is None:
            self.positions = {}

# ── 9.7 Initialize performance monitoring ─────────────────────────────
# Handled by Verity manager

# ── 9.8 Initialize main Ariadne Class ─────────────────────────────────
class Ariadne:
    def __init__(self):
        # ── 9.8.1 Set up the logger ──────────────────────────────────
        self._setup_logging()
        print("Start-up: Logging initialized.")
        
        # ── 9.8.2 Check heartbeats ───────────────────────────────────
        self._check_background_processes()
        
        # ── 9.8.3 Add file handler ───────────────────────────────────
        file_handler = logging.FileHandler('mm/logs/ariadne.log')
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self.logger.addHandler(file_handler)
        print("Start-up: Starting ariadne.log entries.")
        
        # ── 9.8.4 Initialize TradingClient ───────────────────────────
        self.client = TradingClient()
        print("Start-up: TradingClient initialized")
        if GOLIVE:
            print("Ariadne is operating in a LIVE ENVIRONMENT.")
        else:
            print("Ariadne is currently simulating operations.")
        
        # ── 9.8.5 Initialize BotState ────────────────────────────────
        self.state = BotState()
        print("Start-up: BotState initialized.")
        
        # ── 9.8.6 Initialize USDT Guardrail ──────────────────────────
        # Built into managers that check quote currency
        print("Start-up: USDT Guardrail initialized.")
        
        # ── 9.8.7 Load State ─────────────────────────────────────────
        self._load_state()
        
        # Initialize managers
        self.risk_manager = Grayson()
        self.panic_manager = Naomi()
        self.inventory_manager = Helen(self.client)
        self.sales_manager = Petra(self.client)
        self.purchasing_manager = Malcolm(self.client)
        self.banker = Julius(self.client)
        self.accountant = Christian()
        self.termination_officer = Alec(self.client)
        self.talent_scout = DrCalvin()
        self.performance = Verity()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = False
        self.paused = False
        
        print("Start-up sequence complete.")
    
    def _setup_logging(self):
        """Configure logging"""
        self.logger = logging.getLogger('ariadne')
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(console_handler)
    
    def _check_background_processes(self):
        """Check heartbeats table for background process status"""
        try:
            conn = psycopg2.connect(dbname="ariadne", user="postgres", host="localhost")
            cur = conn.cursor()
            
            processes = ['ticker_sticker', 'soc', 'trans_cutie', 'partition_manager']
            for proc in processes:
                cur.execute("""
                    SELECT last_heartbeat, pid 
                    FROM heartbeats 
                    WHERE process_name = %s 
                    AND last_heartbeat > NOW() - INTERVAL '5 minutes'
                """, (proc,))
                
                result = cur.fetchone()
                if result:
                    if proc == 'ticker_sticker':
                        print("Background process [ Ticker Sticker ] is active.")
                    elif proc == 'soc':
                        print("Background process [ SOC ] is active.")
                    elif proc == 'trans_cutie':
                        print("Background process [ Trans Cutie ] is active.")
                    elif proc == 'partition_manager':
                        print("Background process [ Partition Manager ] is active.")
            
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.warning(f"Could not check heartbeats: {e}")
    
    def _load_state(self):
        """Load previous state from JSON file"""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    data = json.load(f)
                    self.state = BotState(**data)
                print("Start-up: Previous state loaded.")
            except Exception as e:
                self.logger.error(f"Failed to load state: {e}")
                print("Start-up: Starting new state file.")
        else:
            print("Start-up: Starting new state file.")
    
    def _save_state(self):
        """Save current state to JSON file"""
        try:
            state_dict = {
                'total_equity': self.state.total_equity,
                'open_orders': self.state.open_orders,
                'positions': self.state.positions,
                'start_time': self.state.start_time,
                'cycle_count': self.state.cycle_count,
                'last_save': time.time()
            }
            with open(STATE_FILE, 'w') as f:
                json.dump(state_dict, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    # ── 9.9 Required Methods ─────────────────────────────────────────
    
    def _cancel_phase(self):
        """9.10.1 Cancel Phase - from Termination Officer"""
        try:
            cancelled = self.termination_officer.cancel_stale_orders()
            for order_id, reason in cancelled:
                self.logger.info(f"{order_id} has been cancelled by the Termination Officer for {reason}.")
        except Exception as e:
            self.logger.error(f"Cancel phase failed: {e}")
    
    def _check_inventory(self):
        """9.10.2 Check inventory - from Inventory Manager"""
        try:
            self.state.positions = self.inventory_manager.get_positions()
            return self.state.positions
        except Exception as e:
            self.logger.error(f"Inventory check failed: {e}")
            return {}
    
    def _sell_phase(self):
        """9.10.3 Sell Phase - from Sales Manager"""
        if not self.state.positions:
            self.logger.info("Inventory empty, no assets to sell this cycle.")
            return
        
        self.logger.info("Checking for sales opportunities...")
        try:
            sales = self.sales_manager.execute_sales(self.state.positions)
            for sale in sales:
                self.logger.info(f"SELL order entered for {sale['quantity']} {sale['symbol']} @ {sale['ask']} by the Sales Manager.")
                # Update state
                if sale['symbol'] in self.state.open_orders:
                    self.state.open_orders[sale['symbol']].append(sale['order_id'])
                else:
                    self.state.open_orders[sale['symbol']] = [sale['order_id']]
        except Exception as e:
            self.logger.error(f"Sell phase failed: {e}")
    
    def _buy_phase(self):
        """9.10.4 Buy Phase - from Purchasing Manager"""
        self.logger.info("Looking for buying opportunities...")
        try:
            # Get available capital
            available_capital = self._get_available_capital()
            
            # Get best pairs
            market_data = self._get_market_data()
            best_pairs = self.talent_scout.get_best_pairs(market_data)
            
            # Filter by risk
            allowed_pairs = []
            for pair in best_pairs:
                if self.risk_manager.can_trade_pair(pair['symbol'], self.state.total_equity):
                    allowed_pairs.append(pair)
                if len(allowed_pairs) >= MAX_PAIRS_TO_TRADE:
                    break
            
            # Execute purchases
            purchases = self.purchasing_manager.execute_purchases(
                allowed_pairs, 
                available_capital,
                self.state.positions
            )
            
            for purchase in purchases:
                self.logger.info(f"BUY order entered for {purchase['quantity']} {purchase['symbol']} @ {purchase['bid']} by the Purchasing Manager.")
                # Update state
                if purchase['symbol'] in self.state.open_orders:
                    self.state.open_orders[purchase['symbol']].append(purchase['order_id'])
                else:
                    self.state.open_orders[purchase['symbol']] = [purchase['order_id']]
                    
        except Exception as e:
            self.logger.error(f"Buy phase failed: {e}")
    
    def _get_available_capital(self):
        """9.10.5 Get/Refresh available capital - from Julius"""
        try:
            balances = self.banker.get_balances()
            return balances.get(QUOTE_CURRENCY, {}).get('available', 0)
        except Exception as e:
            self.logger.error(f"Failed to get available capital: {e}")
            return 0
    
    def _get_market_data(self):
        """9.10.6 Fetch latest market data"""
        try:
            # In refactored version, this would query tickstick table
            # For now, using client's ticker data
            return self.client.get_ticker()
        except Exception as e:
            self.logger.error(f"Failed to get market data: {e}")
            return []
    
    def _panic_check(self):
        """9.10.7 Panic check"""
        try:
            panic_status = self.panic_manager.check_panic_conditions(
                self.state.positions,
                self.state.total_equity
            )
            if panic_status['panic_mode']:
                self.logger.critical(f"PANIC MODE TRIGGERED: {panic_status['reason']}")
                # Cancel all orders
                self.termination_officer.cancel_all_orders()
                # Optionally close all positions
                if panic_status['close_positions']:
                    self._emergency_close_positions()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Panic check failed: {e}")
            return False
    
    def _emergency_close_positions(self):
        """Emergency close all positions"""
        self.logger.critical("EMERGENCY: Closing all positions")
        try:
            for symbol, quantity in self.state.positions.items():
                if quantity > 0:
                    self.client.place_order(
                        symbol=symbol,
                        side='sell',
                        size=quantity,
                        order_type='market'
                    )
        except Exception as e:
            self.logger.error(f"Emergency close failed: {e}")
    
    def _update_equity(self):
        """9.10.8 Update equity tracking"""
        try:
            total = 0
            
            # Get all balances
            balances = self.banker.get_balances()
            
            # Add USDT value
            total += balances.get(QUOTE_CURRENCY, {}).get('total', 0)
            
            # Add position values
            market_data = self._get_market_data()
            for symbol, quantity in self.state.positions.items():
                if quantity > 0:
                    # Find current price
                    for ticker in market_data:
                        if ticker.get('symbol') == symbol:
                            price = float(ticker.get('last', 0))
                            total += quantity * price
                            break
            
            self.state.total_equity = total
            self.performance.record_metric('total_equity', total)
            
        except Exception as e:
            self.logger.error(f"Equity update failed: {e}")
    
    # ── 9.10 Start the main loop ─────────────────────────────────────
    def run(self):
        """Main trading loop"""
        self.running = True
        
        print("Trading activities commencing. Let's make some market.")
        print("[520] I am open to receiving wealth for the highest good, my children and their future.")
        print("[741] Any limiting beliefs about wealth are cleared from my energy now.")
        print("  [8] Abundance flows to me in limitless, expected, and unexpected ways.")
        
        if self.state.start_time is None:
            self.state.start_time = time.time()
        
        while self.running:
            try:
                loop_start = time.time()
                
                if self.paused:
                    time.sleep(1)
                    continue
                
                # ── 9.10.1 Cancel Phase ──────────────────────────────
                self._cancel_phase()
                
                # ── 9.10.2 Check inventory ───────────────────────────
                self._check_inventory()
                
                # ── 9.10.3 Sell Phase ────────────────────────────────
                self._sell_phase()
                
                # ── 9.10.4 Buy Phase ─────────────────────────────────
                self._buy_phase()
                
                # ── 9.10.7 Panic check ───────────────────────────────
                if self._panic_check():
                    self.logger.warning("Panic conditions detected, skipping to next cycle")
                    time.sleep(LOOP_DELAY)
                    continue
                
                # ── 9.10.8 Update equity ─────────────────────────────
                self._update_equity()
                
                # ── 9.10.9 Check risk limits ─────────────────────────
                risk_status = self.risk_manager.check_risk_limits(self.state.total_equity)
                if not risk_status['trading_allowed']:
                    self.logger.warning(f"Risk limits exceeded: {risk_status['reason']}")
                    self.paused = True
                
                # ── 9.10.10 Connection check ─────────────────────────
                # Built into client methods
                
                # ── 9.11 State Persistence ───────────────────────────
                self.state.cycle_count += 1
                if self.state.cycle_count % 10 == 0:
                    self._save_state()
                
                # Performance tracking
                loop_time = time.time() - loop_start
                self.performance.record_metric('loop_time', loop_time)
                
                # ── 9.12 Sleep and Repeat ────────────────────────────
                sleep_time = max(0, LOOP_DELAY - loop_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Main loop error: {e}")
                time.sleep(LOOP_DELAY)
        
        # Cleanup on exit
        self._save_state()
        self.logger.info("Ariadne shutdown complete")

# ── Main Entry Point ──────────────────────────────────────────────────
if __name__ == "__main__":
    bot = Ariadne()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print(f"Fatal error: {e}")