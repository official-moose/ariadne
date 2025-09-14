#!/usr/bin/env python3
"""
Database monitoring dashboard for Ariadne
Auto-refreshes every 5 minutes
"""

import psycopg2
import subprocess
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def get_monit_status():
    """Get monit summary"""
    try:
        result = subprocess.run(['sudo', 'monit', 'summary'], 
                              capture_output=True, text=True, timeout=5)
        return result.stdout
    except Exception as e:
        return f"Could not get monit status: {e}"

def parse_partition_info(partition_name):
    """Parse partition name and return formatted info"""
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
            
            # Format date and time
            date_str = partition_dt_local.strftime("%b %d, %Y")
            
            # Format time range (handle midnight crossover)
            if partition_dt_local.date() == partition_end_local.date():
                time_str = f"{partition_dt_local.strftime('%-I%p').lower()} → {partition_end_local.strftime('%-I%p').lower()}"
            else:
                time_str = f"{partition_dt_local.strftime('%-I%p').lower()} → {partition_end_local.strftime('%-I%p').lower()}+1"
            
            # Determine status
            now_utc = datetime.now(ZoneInfo("UTC"))
            if partition_end < now_utc:
                status = "Expired"
                queue = "---"
            else:
                status = "Ok"
                # Calculate queue position
                hours_ahead = int((partition_dt - now_utc).total_seconds() / 3600) + 1
                if hours_ahead <= 0:
                    queue = "Active"
                else:
                    queue = str(hours_ahead)
            
            return date_str, time_str, queue, status
    except Exception as e:
        return "Error", "Error", "?", "Error"
    
    return "Unknown", "Unknown", "?", "Unknown"

def main():
    while True:
        # Clear screen
        os.system('clear')
        
        # Header with timestamp
        print(f"\n{'='*60}")
        print(f"ARIADNE DASHBOARD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        try:
            # Connect to database
            conn = psycopg2.connect(database="ariadne", user="postgres", host="localhost")
            cur = conn.cursor()
            
            # 1. Monit Status
            print("\nMONIT STATUS")
            print("="*60)
            print(get_monit_status())
            
            # 2. Check heartbeats
            print("\nPROCESS HEARTBEATS")
            print("="*60)
            
            cur.execute("""
                SELECT 
                    process_name,
                    last_heartbeat,
                    EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::int as seconds_since
                FROM heartbeats
                ORDER BY process_name
            """)
            
            print(f"{'Process':<20} {'Last Stamp':<25} {'Time Since':<15}")
            print("-"*60)
            
            for row in cur.fetchall():
                process, last_hb, seconds = row
                if seconds < 60:
                    time_str = f"{seconds}s ago"
                elif seconds < 3600:
                    time_str = f"{seconds//60}m ago"
                else:
                    time_str = f"{seconds//3600}h {(seconds%3600)//60}m ago"
                
                last_hb_str = last_hb.strftime("%Y-%m-%d %H:%M:%S") if last_hb else "Never"
                print(f"{process:<20} {last_hb_str:<25} {time_str:<15}")
            
            # 3. Check partitions with enhanced display
            print("\n" + "="*60)
            print("TICKSTICK PARTITIONS")
            print("="*60)
            
            cur.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE tablename LIKE 'tickstick_%' 
                ORDER BY tablename
            """)
            
            partitions = cur.fetchall()
            
            if partitions:
                print(f"{'Partition Name':<28} {'Date':<15} {'Time Range':<15} {'Queue':<7} {'Status':<10}")
                print("-"*85)
                
                for (partition,) in partitions:
                    date_str, time_str, queue, status = parse_partition_info(partition)
                    print(f"{partition:<28} {date_str:<15} {time_str:<15} {queue:<7} {status:<10}")
            else:
                print("No partitions found")
            
            # 4. Recent ticker data
            print("\n" + "="*60)
            print("10 MOST RECENT TICKER ENTRIES")
            print("="*60)
            
            try:
                cur.execute("""
                    SELECT timestamp, symbol, last
                    FROM tickstick
                    ORDER BY timestamp DESC
                    LIMIT 10
                """)
                
                rows = cur.fetchall()
                
                if rows:
                    print(f"{'Timestamp':<25} {'Symbol':<15} {'Last Price':<12}")
                    print("-"*60)
                    
                    tz = ZoneInfo("America/Toronto")
                    for timestamp, symbol, last in rows:
                        dt = datetime.fromtimestamp(timestamp, tz=tz)
                        time_str = dt.strftime("%Y-%m-%d %H:%M:%S EST")
                        print(f"{time_str:<25} {symbol:<15} {last:<12.8f}")
                else:
                    print("No recent data found")
                    
            except psycopg2.Error as e:
                print(f"Error querying ticker data: {e}")
            
            cur.close()
            conn.close()
            
        except psycopg2.Error as e:
            print(f"\nDatabase connection error: {e}")
        except KeyboardInterrupt:
            print("\n\nDashboard stopped by user")
            break
        
        # Refresh countdown
        print(f"\n{'='*60}")
        print("Auto-refresh in 5 minutes (Ctrl+C to exit)")
        print(f"{'='*60}")
        
        try:
            time.sleep(300)  # 5 minutes
        except KeyboardInterrupt:
            print("\n\nDashboard stopped by user")
            break

if __name__ == "__main__":
    main()