#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> SIMDASH - CLI Interface
#>> mm/utils/tools/dashboard.py
#>>
#>> Ariadne Dashboard - Terminal UI for Market Maker Bot
#>> Simulation mode monitoring 
#>> Press [ X ] to exit the dashboard.
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]      
#>>────────────────────────────────────────────────────────────────

# Build|20250905.02

import os
import sys
import sqlite3
import json
import time
from datetime import datetime, timedelta
from tabulate import tabulate
import pytz

# Database paths
SIM_DB_PATH = "/root/Echelon/valentrix/mm/data/sims/ariadne_sim.db"
TICK_DB_PATH = "/root/Echelon/valentrix/mm/data/live/tick_snapshots.db"
STATE_FILE_PATH = "/root/Echelon/valentrix/mm/data/state/sim_state.json"
LOG_FILE_PATH = "/root/Echelon/valentrix/mm/logs/ariadne.log"

# Color codes for terminal
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
WHITE = "\033[97m"
BOLD = "\033[1m"
DIM = "\033[2m"

TZ_TORONTO = pytz.timezone("America/Toronto")

class AriadneDashboard:
   def __init__(self):
       self.sim_db = SIM_DB_PATH
       self.tick_db = TICK_DB_PATH
       self.state_file = STATE_FILE_PATH
       self.log_file = LOG_FILE_PATH
       self.refresh_interval = 5
       self.countdown = self.refresh_interval
       
       # Load start time once and keep it
       state = self._load_state()
       self.start_time = state.get('start_time', time.time())
       
   def clear_screen(self):
       os.system('cls' if os.name == 'nt' else 'clear')
   
   def run(self):
       """Main dashboard loop"""
       while True:
           try:
               self.countdown = self.refresh_interval
               self.display()
               
               for i in range(self.refresh_interval):
                   time.sleep(1)
                   self.countdown -= 1
                   
           except KeyboardInterrupt:
               break
           except Exception as e:
               print(f"Dashboard error: {e}")
               time.sleep(5)
   
   def display(self):
       """Render the complete dashboard"""
       self.clear_screen()
       
       # Get all data
       state = self._load_state()
       balances = self._get_balances()
       orders = self._get_orders()
       fills = self._get_fills()
       metrics = self._calculate_metrics(state, fills)
       
       # Header
       print(f"\n{'='*120}")
       print(f"{'ARIADNE v4.4 - Market Maker Dashboard':^120}")
       print(f"{'='*120}\n")
       
       # Top row with proper runtime
       runtime = self._format_runtime()
       total_exposure = balances.get('hold', 0.0)
       
       print(f"RUNTIME: {runtime:<20}    Next refresh in {self.countdown}s    TOTAL EXPOSURE: ${total_exposure:,.2f} USDT")
       print(f"{'-'*120}\n")
       
       # Main sections with fixed column widths
       print(f"{'ACCOUNT SUMMARY':<24}{'ORDER SUMMARY':<24}{'P&L SUMMARY':<24}{'METRICS':<24}{'CURRENT PARAMS':<24}")
       print(f"{'-'*23} {'-'*23} {'-'*23} {'-'*23} {'-'*23}")
       
       # Row 1
       print(f"{'Available USDT':<15}${balances.get('available', 0):>7.2f} ", end="")
       print(f"{'Unfilled Orders':<15}{len([o for o in orders if not self._is_filled(o[0], fills)]):>8} ", end="")
       print(f"{'Gross Profit':<15}${metrics['gross_profit']:>7.2f} ", end="")
       print(f"{'Avg Order Time':<15}{metrics['avg_order_time']:>8} ", end="")
       print(f"{'Target Spread':<15}{'1.0%':>8}")
       
       # Row 2
       print(f"{'Held (unfilled)':<15}${balances.get('hold', 0):>7.2f} ", end="")
       print(f"{'Filled Orders':<15}{len(fills):>8} ", end="")
       print(f"{'Fees':<15}${metrics['total_fees']:>7.2f} ", end="")
       print(f"{'Avg Spread Capture':<15}{metrics['avg_spread']:>7.2f}% ", end="")
       print(f"{'Min Volume':<15}{'$5M':>8}")
       
       # Row 3
       print(f"{'Committed (filled)':<15}${balances.get('committed', 0):>7.2f} ", end="")
       print(f"{'Total Current Orders':<15}{len(orders):>8} ", end="")
       print(f"{'Net Profit':<15}${metrics['net_profit']:>7.2f} ", end="")
       print(f"{'Fill Rate':<15}{metrics['fill_rate']:>7.2f}% ", end="")
       print(f"{'Max Volume':<15}{'$200M':>8}")
       
       # Row 4
       print(f"{'Total USDT':<15}${balances.get('total', 0):>7.2f} ", end="")
       print(f"{'':<23} ", end="")
       print(f"{'Current P&L %':<15}{metrics['pnl_pct']:>7.2f}% ", end="")
       print(f"{'':<23} ", end="")
       print(f"{'Daily Target':<15}{'4.45%':>8}")
       
       # Orders section
       print(f"\n{'ORDERS'}")
       print(f"{'-'*120}")
       self._display_orders(orders, fills)
       
       # Console section - fixed height
       print(f"\n{'CONSOLE'}")
       print(f"{'-'*120}")
       self._display_console()
   
   def _display_orders(self, orders, fills):
       """Display unfilled and filled orders tables"""
       
       # UNFILLED
       print(f"\n{'UNFILLED'}")
       unfilled_data = []
       
       for order in orders[:20]:
           order_id, ts, symbol, side, price, size, sim_order_id, _ = order
           
           if not self._is_filled(order_id, fills):
               order_time = datetime.fromtimestamp(ts, TZ_TORONTO).strftime('%H:%M:%S')
               bid, ask = self._get_market_prices(symbol)
               market_price = ask if side.lower() == 'buy' else bid
               
               if market_price and market_price > 0:
                   diff_pct = ((market_price - price) / price * 100)
                   diff_str = f"{diff_pct:+.1f}%"
                   market_str = f"${market_price:.2f}"
               else:
                   diff_str = ""
                   market_str = ""
               
               side_colored = f"{RED}BUY{RESET}" if side.lower() == 'buy' else f"{GREEN}SELL{RESET}"
               
               unfilled_data.append([
                   order_time,
                   symbol,
                   side_colored,
                   f"@ {price:.8f}",
                   f"{size:.6f}",
                   f"${price * size:.2f}",
                   market_str,
                   diff_str
               ])
       
       if unfilled_data:
           headers = ["TIME", "SYMBOL", "SIDE", "ORDER B/A", "AMOUNT", "TOTAL", "MARKET", "DIFF%"]
           print(tabulate(unfilled_data, headers=headers, tablefmt="plain",
                         colalign=("left", "left", "left", "right", "right", "right", "right", "right")))
       else:
           print("(no unfilled orders)")
       
       # FILLED
       print(f"\n{'FILLED'}")
       
       if not fills:
           print("(no filled orders yet)")
       else:
           filled_data = []
           for fill in fills[:10]:
               fill_id, order_id, fill_ts, fill_price, fill_size = fill
               
               order_info = self._get_order_info(order_id)
               if order_info:
                   _, order_ts, symbol, side, order_price, _, _, _ = order_info
                   
                   fill_time = datetime.fromtimestamp(fill_ts, TZ_TORONTO).strftime('%H:%M:%S')
                   side_colored = f"{RED}BUY{RESET}" if side.lower() == 'buy' else f"{GREEN}SELL{RESET}"
                   
                   spread = 0.0
                   if side.lower() == 'buy':
                       spread = ((order_price - fill_price) / order_price * 100)
                   else:
                       spread = ((fill_price - order_price) / order_price * 100)
                   
                   filled_data.append([
                       fill_time,
                       symbol,
                       side_colored,
                       f"@ {order_price:.8f}",
                       f"{fill_size:.6f}",
                       f"${order_price * fill_size:.2f}",
                       f"${fill_price:.2f}",
                       f"{spread:+.1f}%"
                   ])
           
           headers = ["TIME", "SYMBOL", "SIDE", "ORDER B/A", "AMOUNT", "TOTAL", "FILL PRICE", "SPREAD%"]
           print(tabulate(filled_data, headers=headers, tablefmt="plain",
                         colalign=("left", "left", "left", "right", "right", "right", "right", "right")))
   
   def _display_console(self):
       """Display last 10 log lines - fixed height"""
       try:
           with open(self.log_file, 'r') as f:
               lines = f.readlines()
               # Always show exactly 10 lines (pad if needed)
               last_lines = lines[-10:] if len(lines) >= 10 else lines
               
               for line in last_lines:
                   if ' - ' in line:
                       parts = line.split(' - ', 2)
                       if len(parts) >= 3:
                           # Keep original timestamp from log
                           print(f"{parts[0]} {parts[2].strip()[:100]}")  # Limit line length
                   else:
                       print(line.strip()[:120])
               
               # Pad to 10 lines if needed
               for _ in range(10 - len(last_lines)):
                   print("")
       except:
           print("(log file not available)")
           for _ in range(9):
               print("")
   
   def _format_runtime(self):
       """Calculate runtime from stored start time"""
       elapsed = time.time() - self.start_time
       days = int(elapsed // 86400)
       hours = int((elapsed % 86400) // 3600)
       minutes = int((elapsed % 3600) // 60)
       return f"{days}d {hours}h {minutes}m"
   
   def _get_balances(self):
       """Get current balances from database"""
       try:
           conn = sqlite3.connect(self.sim_db, timeout=1)
           c = conn.cursor()
           
           c.execute("SELECT available, hold FROM simulation_balances WHERE currency = 'USDT'")
           row = c.fetchone()
           available = float(row[0]) if row else 0.0
           hold = float(row[1]) if row else 0.0
           
           # Calculate committed from non-USDT assets
           c.execute("SELECT currency, available FROM simulation_balances WHERE currency != 'USDT' AND available > 0")
           committed = 0.0
           for currency, amount in c.fetchall():
               bid, _ = self._get_market_prices(f"{currency}-USDT")
               if bid:
                   committed += amount * bid
           
           conn.close()
           return {
               'available': available,
               'hold': hold,
               'committed': committed,
               'total': available + hold + committed
           }
       except Exception as e:
           return {'available': 0, 'hold': 0, 'committed': 0, 'total': 0}
   
   def _get_orders(self):
       """Get all orders from database"""
       try:
           conn = sqlite3.connect(self.sim_db, timeout=1)
           c = conn.cursor()
           c.execute("SELECT * FROM orders ORDER BY timestamp DESC")
           orders = c.fetchall()
           conn.close()
           return orders
       except:
           return []
   
   def _get_order_info(self, order_id):
       """Get specific order info"""
       try:
           conn = sqlite3.connect(self.sim_db, timeout=1)
           c = conn.cursor()
           c.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
           order = c.fetchone()
           conn.close()
           return order
       except:
           return None
   
   def _get_fills(self):
       """Get all filled orders"""
       try:
           conn = sqlite3.connect(self.sim_db, timeout=1)
           c = conn.cursor()
           c.execute("SELECT * FROM simulated_trades ORDER BY fill_timestamp DESC")
           fills = c.fetchall()
           conn.close()
           return fills
       except:
           return []
   
   def _get_market_prices(self, symbol):
       """Get current bid/ask from tick database"""
       try:
           conn = sqlite3.connect(self.tick_db, timeout=1)
           c = conn.cursor()
           c.execute("SELECT buy, sell FROM tick_snapshots WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1", (symbol,))
           row = c.fetchone()
           conn.close()
           if row:
               return float(row[0]), float(row[1])
           return None, None
       except:
           return None, None
   
   def _is_filled(self, order_id, fills):
       """Check if order has been filled"""
       return any(f[1] == order_id for f in fills)
   
   def _calculate_metrics(self, state, fills):
       """Calculate performance metrics"""
       total_fees = len(fills) * 0.001 * 100
       gross_profit = 0.0
       
       for fill in fills:
           _, order_id, _, fill_price, fill_size = fill
           order_info = self._get_order_info(order_id)
           if order_info:
               _, _, _, side, order_price, _, _, _ = order_info
               if side.lower() == 'sell':
                   gross_profit += (fill_price - order_price) * fill_size
       
       return {
           'gross_profit': gross_profit,
           'total_fees': total_fees,
           'net_profit': gross_profit - total_fees,
           'pnl_pct': ((gross_profit - total_fees) / 2500.0 * 100) if gross_profit > 0 else 0,
           'avg_order_time': "7m 48s",
           'avg_spread': 0.0,
           'fill_rate': (len(fills) / max(len(self._get_orders()), 1) * 100)
       }
   
   def _load_state(self):
       """Load bot state from file"""
       try:
           with open(self.state_file, 'r') as f:
               return json.load(f)
       except:
           return {}

if __name__ == "__main__":
   dashboard = AriadneDashboard()
   dashboard.run()