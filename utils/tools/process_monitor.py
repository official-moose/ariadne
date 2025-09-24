#!/usr/bin/env python3
"""
Database monitoring dashboard for Ariadne
"""

import psycopg2
import subprocess
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Configuration
REFRESH_INTERVAL = 15  # seconds

# ANSI color codes
GREEN = '\033[106;30m'
DARK_GREEN = '\033[96m'
LIGHT_GRAY = '\033[90m'
RED = '\033[95m'
RESET = '\033[0m'
MONIT_GREEN = '\033[92m'

def get_monit_status():
    """Get monit summary with color coding"""
    try:
        result = subprocess.run(['sudo', 'monit', 'summary'], 
                              capture_output=True, text=True, timeout=5)
        lines = result.stdout.split('\n')
        filtered_lines = []
        for line in lines:
            if "Process" in line:
                if "OK" in line:
                    # Color OK status green
                    line = line.replace("OK", f"{MONIT_GREEN}OK{RESET}")
                filtered_lines.append(line)
        return "\n".join(filtered_lines)
    except Exception as e:
        return f"Could not get monit status: {e}"

def parse_partition_info(partition_name):
    """Parse partition name and return formatted info with color"""
    try:
        # Extract date/time from partition name: tickstick_YYYY_MM_DD_HH
        parts = partition_name.split('_')
        if len(parts) == 5:
            year, month, day, hour = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
            
            # Create datetime object (UTC)
            partition_dt = datetime(year, month, day, hour, tzinfo=ZoneInfo("UTC"))
            partition_end = partition_dt + timedelta(hours=1)
            
            # Convert to Toronto time for display
            tz = ZoneInfo("America/Toronto")
            partition_dt_local = partition_dt.astimezone(tz)
            partition_end_local = partition_end.astimezone(tz)
            
            # Format date - use the LOCAL date for display
            date_str = partition_dt_local.strftime("%b %d, %Y")
            
            # Format time range
            time_str = f"{partition_dt_local.strftime('%-I%p').lower()} → {partition_end_local.strftime('%-I%p').lower()}"
            
            # Determine status and color based on UTC comparison
            now_utc = datetime.now(ZoneInfo("UTC"))
            
            if partition_dt <= now_utc < partition_end:
                status = "Current"
                color = GREEN
            elif partition_end <= now_utc and (now_utc - partition_end).total_seconds() < 3600:
                status = "Previous"
                color = DARK_GREEN
            elif partition_end <= now_utc:
                status = "Expired"
                color = RED
            else:
                status = "Reserved"
                color = LIGHT_GRAY
            
            return date_str, time_str, status, color
    except Exception as e:
        return "Error", "Error", "Error", RESET
    
    return "Unknown", "Unknown", "Unknown", RESET

def get_sig_intel_column_count(record):
    """Count non-null columns in a record"""
    return sum(1 for col in record if col is not None)

def main():
    # Read cycle times from config file
    config_path = "mm/config/marcus.py"
    with open(config_path, "r") as f:
        config_content = f.read()
    
    cycle_times = {}
    for line in config_content.split("\n"):
        if line.startswith("HB_"):
            parts = line.split("=")
            if len(parts) == 2:
                key, value = parts
                key = key.strip()
                value = value.strip()
                if value.isdigit():
                    cycle_times[key] = int(value)

    while True:
        # Rest of the main function code...
        # Clear screen
        os.system('clear')
        
        tz_toronto = ZoneInfo("America/Toronto")
        now = datetime.now(tz_toronto)
        
        # Header with timestamp and countdown timer
        print(f"\n{'='*73}")
        print(f"ARIADNE DASHBOARD - {now.strftime('%Y-%m-%d %H:%M:%S %Z')} - Refresh in {REFRESH_INTERVAL}s")
        print(f"{'='*73}")
        
        try:
            # Connect to database
            conn = psycopg2.connect(database="ariadne", user="postgres", host="localhost")
            cur = conn.cursor()
            
            # 1. Monit Status
            print("\nMONIT STATUS")
            print("="*73)
            print(get_monit_status())
            
            # 2. Check heartbeats
            print("\nPROCESS HEARTBEATS")
            print("="*73)
            
            cur.execute("""
                SELECT 
                    process_name,
                    last_heartbeat,
                    EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::int as seconds_since
                FROM heartbeats
                ORDER BY process_name
            """)
            
            print(f"{'Process':<20} {'Last Stamp':<25} {'Time Since':<15} {'Cycle Time':<15}")
            print("-"*73)
            
            for row in cur.fetchall():
                process, last_hb, seconds = row
                if seconds < 60:
                    time_str = f"{seconds}s ago"
                elif seconds < 3600:
                    time_str = f"{seconds//60}m ago" 
                else:
                    time_str = f"{seconds//3600}h {(seconds%3600)//60}m ago"
                
                last_hb_str = last_hb.strftime("%Y-%m-%d %H:%M:%S") if last_hb else "Never"
                
                cycle_time = f"{cycle_times.get(process, '-')}s"
                print(f"{process:<20} {last_hb_str:<25} {time_str:<15} {cycle_time:<15}")

            
            # 3. Check tickstick partitions
            print("\n" + "="*73) 
            print("TICKSTICK PARTITIONS")
            print("="*73)
            
            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE tablename LIKE 'tickstick_%'
                ORDER BY tablename 
            """)
            
            partitions = cur.fetchall()
            
            if partitions:
                print(f"{'Partition Name':<28} {'Date':<15} {'Time Range':<15} {'Status':<10}") 
                print("-"*73)
                
                for (partition,) in partitions:
                    date_str, time_str, status, color = parse_partition_info(partition)
                    print(f"{color}{partition:<28} {date_str:<15} {time_str:<15} {status:<10}{RESET}")
            else:
                print("No partitions found")
            
            # 4. Recent ticker data 
            print("\n" + "="*73)
            print("5 MOST RECENT TICKER ENTRIES") 
            print("="*73)
            
            try:
                cur.execute("""
                    SELECT timestamp, symbol, last
                    FROM tickstick
                    ORDER BY timestamp DESC 
                    LIMIT 5
                """)
                
                rows = cur.fetchall()
                
                if rows:
                    print(f"{'Timestamp':<25} {'Symbol':<15} {'Last Price':<12}")
                    print("-"*73)
                    
                    for timestamp, symbol, last in rows:
                        dt = datetime.fromtimestamp(timestamp, tz=tz_toronto)
                        time_str = dt.strftime("%Y-%m-%d %H:%M:%S %Z") 
                        print(f"{time_str:<25} {symbol:<15} {last:<12.8f}")
                else:
                    print("No recent data found")
                    
            except psycopg2.Error as e:
                print(f"Error querying ticker data: {e}")
            
            # 5. Check signals_intel partitions
            print("\n" + "="*73)
            print("SIGNALS_INTEL PARTITIONS") 
            print("="*73)
            
            cur.execute("""
                SELECT tablename
                FROM pg_tables 
                WHERE tablename LIKE 'signals_intel_%'
                ORDER BY tablename
            """)
            
            partitions = cur.fetchall()
            
            if partitions:
                print(f"{'Partition Name':<28} {'Date':<15} {'Time Range':<15} {'Status':<10}")
                print("-"*73)
                
                for (partition,) in partitions:
                    date_str, time_str, status, color = parse_partition_info(partition)
                    print(f"{color}{partition:<28} {date_str:<15} {time_str:<15} {status:<10}{RESET}") 
            else:
                print("No partitions found")
            
            # 6. Recent signals_intel data
            print("\n" + "="*73)
            print("5 MOST RECENT SIGNALS_INTEL ENTRIES")
            print("="*73)
            
            try:
                cur.execute(""" 
                    SELECT timestamp, symbol, *
                    FROM signals_intel
                    ORDER BY timestamp DESC
                    LIMIT 5
                """)
                
                rows = cur.fetchall()
                
                if rows:
                    print(f"{'Timestamp':<25} {'Symbol':<15} {'Datapoints':<15}")
                    print("-"*73)
                    
                    for record in rows:
                        timestamp, symbol, *columns = record
                        dt = datetime.fromtimestamp(timestamp, tz=tz_toronto)
                        time_str = dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                        populated_cols = get_sig_intel_column_count(columns)
                        total_cols = len(columns)
                        print(f"{time_str:<25} {symbol:<15} {populated_cols}/{total_cols:<10}")
                else:
                    print("No recent data found")
                    
            except psycopg2.Error as e:
                print(f"Error querying signals_intel data: {e}")
            
            cur.close()
            conn.close()
            
        except psycopg2.Error as e:
            print(f"\nDatabase connection error: {e}")
        except KeyboardInterrupt:
            print("\n\nDashboard stopped by user")
            break
        
        # Refresh countdown 
        print(f"\n{'='*73}")
        print(f"Auto-refresh every {REFRESH_INTERVAL} seconds (Ctrl+C to exit)")
        print(f"{'='*73}")
        
        try:
            for i in range(REFRESH_INTERVAL, 0, -1):
                print(f"\rRefreshing in {i}s...", end="", flush=True)
                time.sleep(1)
            print() 
        except KeyboardInterrupt:
            print("\n\nDashboard stopped by user")
            break

if __name__ == "__main__":
    main()