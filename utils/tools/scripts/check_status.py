#!/usr/bin/env python3 
"""
Database monitoring script for Ariadne
Checks heartbeats, partitions, and recent ticker data
"""

import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

def main():
    # Connect to database
    conn = psycopg2.connect(database="ariadne", user="postgres", host="localhost")
    cur = conn.cursor()
    
    # 1. Check heartbeats
    print("\n" + "="*60)
    print("PROCESS HEARTBEATS")
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
    
    # 2. Check partitions
    print("\n" + "="*60)
    print("TICKSTICK PARTITIONS")
    print("="*60)
    
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename LIKE 'tickstick_%' 
        ORDER BY tablename DESC
    """)
    
    partitions = cur.fetchall()
    for (partition,) in partitions:
        print(partition)
    
    if not partitions:
        print("No partitions found")
    
    # 3. Recent ticker data
    print("\n" + "="*60)
    print("10 MOST RECENT TICKER ENTRIES")
    print("="*60)
    
    # Try to query the parent table first
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

if __name__ == "__main__":
    main()