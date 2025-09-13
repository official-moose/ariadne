# /root/Echelon/valentrix/mm/scripts/reset_simulation.py
# last update: August 21, 2025

"""
Simulation Reset Script for Ariadne
Clears all trading data while preserving database structure.
Safe alternative to deleting the entire database file.
"""

import sqlite3
import os
import json
from pathlib import Path
import sys
import tty
import termios

# Define the MM root and the target database
MM_ROOT = "/root/Echelon/valentrix/mm"
SIM_DB_PATH = os.path.join(MM_ROOT, "data/sims/ariadne_sim.db")
STATE_FILE_PATH = os.path.join(MM_ROOT, "data/state/sim_state.json")  # Updated to sim_state.json

def get_single_keypress(prompt: str = "") -> str:
    """Get a single keypress without requiring Enter"""
    print(prompt, end="", flush=True)
    
    # Save terminal settings
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    
    try:
        tty.setraw(sys.stdin.fileno())
        char = sys.stdin.read(1)
    finally:
        # Restore terminal settings
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    
    print()  # New line after input
    return char.lower()

def reset_simulation_data():
    """Clears all trading data while preserving database structure and adds 2500 USDT."""
    
    if not os.path.exists(SIM_DB_PATH):
        print(f"❌ Simulation database not found at: {SIM_DB_PATH}")
        print("   The database may not exist or the path is incorrect.")
        return False
    
    try:
        conn = sqlite3.connect(SIM_DB_PATH)
        c = conn.cursor()
        
        # Clear all tables in proper order to maintain referential integrity
        c.execute("DELETE FROM simulated_trades")
        c.execute("DELETE FROM orders")
        c.execute("DELETE FROM order_books")
        c.execute("DELETE FROM simulation_balances")
        
        # Insert 2500 USDT as initial capital
        c.execute("INSERT INTO simulation_balances (currency, available) VALUES ('USDT', 2500.0)")
        
        # Check if sqlite_sequence table exists before trying to reset it
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'")
        if c.fetchone():
            c.execute("DELETE FROM sqlite_sequence WHERE name IN ('orders', 'order_books', 'simulated_trades')")
        
        conn.commit()
        conn.close()
        
        print(f"✅ Simulation database reset successfully: {SIM_DB_PATH}")
        print("   - All orders cleared")
        print("   - All order book snapshots cleared") 
        print("   - All simulated trades cleared")
        print("   - 2500.0 USDT added to simulation_balances")
        print("   - Auto-increment counters reset (if applicable)")
        print("   - Database structure preserved")
        
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Error resetting simulation database: {e}")
        return False

def reset_state_file():
    """Reset the bot state file to initial conditions"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
        
        # Create fresh initial state
        initial_state = {
            "total_equity": 2500.0,
            "total_committed_value": 0.0,
            "open_orders": {},
            "last_heartbeat": 0,
            "blacklist": {},
            "start_time": 0,
            "cycle_count": 0,
            "performance_metrics": {}
        }
        
        with open(STATE_FILE_PATH, 'w') as f:
            json.dump(initial_state, f, indent=2)
            
        print(f"✅ State file reset to initial conditions: {STATE_FILE_PATH}")
        return True
        
    except Exception as e:
        print(f"❌ Error resetting state file: {e}")
        return False

def main():
    """Main function to run the simulation reset."""
    print("\n" + "="*50)
    print("        ARIADNE SIMULATION RESET")
    print("="*50)
    print("\nThis will clear ALL trading data from the simulation database.")
    print("All orders, trades, and order book history will be permanently deleted.")
    print("Bot state will be reset to initial conditions (2500 USDT, no inventory).")
    print("Database structure and schema will be preserved.")
    print("\n" + "="*50)
    
    # Get confirmation with single keypress
    response = get_single_keypress("Press Y to reset simulation or N to cancel: ")
    
    if response == 'y':
        print("\n🔄 Resetting simulation...")
        
        db_success = reset_simulation_data()
        state_success = reset_state_file()
        
        if db_success and state_success:
            print("\n✅ Complete reset successful! Ready for fresh start.")
            print("   - 2500.0 USDT available in simulation")
            print("   - All previous trading data cleared")
            print("   - State reset to initial conditions")
        else:
            print("\n❌ Reset partially failed. Check errors above.")
            
    else:
        print("\n❌ Reset cancelled. No changes were made.")
    
    print("\n" + "="*50)
    print("Reset operation complete")
    print("="*50)

if __name__ == "__main__":
    main()