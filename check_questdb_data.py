#!/usr/bin/env python3
"""
Check QuestDB Data - Verify what's actually in the database
"""

import psycopg2
import sys
import pandas as pd
from datetime import datetime

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def check_questdb():
    """Check QuestDB connection and data"""
    
    print("=" * 60)
    print("🔍 CHECKING QUESTDB DATA")
    print("=" * 60)
    
    try:
        # Connect to QuestDB
        print("\n1️⃣ Connecting to QuestDB...")
        conn = psycopg2.connect(
            host='localhost',
            port=8812,  # PostgreSQL wire protocol port
            database='qdb',
            user='admin',
            password='quest'
        )
        print("✅ Connected to QuestDB successfully!")
        
        cursor = conn.cursor()
        
        # Check if table exists
        print("\n2️⃣ Checking tables...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        if not tables:
            print("❌ No tables found in QuestDB!")
            print("\n📝 To create table and load data, run:")
            print("   python questdb_manager.py")
            return
        
        print(f"✅ Found tables: {[t[0] for t in tables]}")
        
        # Check data in ohlcv1d table
        print("\n3️⃣ Checking data in ohlcv1d table...")
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM ohlcv1d")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total records: {total_records:,}")
        
        if total_records == 0:
            print("❌ Table exists but has NO DATA!")
            print("\n📝 To load data, run:")
            print("   python questdb_manager.py")
            return
        
        # Count by symbol
        print("\n4️⃣ Records by symbol:")
        cursor.execute("""
            SELECT symbol, COUNT(*) as count,
                   MIN(date) as first_date,
                   MAX(date) as last_date
            FROM ohlcv1d
            GROUP BY symbol
            ORDER BY symbol
        """)
        
        symbol_data = cursor.fetchall()
        for symbol, count, first_date, last_date in symbol_data:
            print(f"   {symbol}: {count:,} records ({first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')})")
        
        # Show latest prices
        print("\n5️⃣ Latest prices in database:")
        cursor.execute("""
            SELECT symbol, date, close, volume
            FROM ohlcv1d
            WHERE date = (SELECT MAX(date) FROM ohlcv1d WHERE symbol = ohlcv1d.symbol)
            ORDER BY symbol
        """)
        
        latest_prices = cursor.fetchall()
        for symbol, date, close, volume in latest_prices:
            print(f"   {symbol}: ${close:.2f} (Volume: {volume:,}) - {date.strftime('%Y-%m-%d')}")
        
        # Sample data
        print("\n6️⃣ Sample data (last 5 records for SPY):")
        cursor.execute("""
            SELECT date, open, high, low, close, volume
            FROM ohlcv1d
            WHERE symbol = 'SPY'
            ORDER BY date DESC
            LIMIT 5
        """)
        
        sample_data = cursor.fetchall()
        if sample_data:
            print("\n   Date       | Open    | High    | Low     | Close   | Volume")
            print("   " + "-" * 65)
            for row in sample_data:
                date, open_p, high, low, close, volume = row
                print(f"   {date.strftime('%Y-%m-%d')} | {open_p:7.2f} | {high:7.2f} | {low:7.2f} | {close:7.2f} | {volume:,}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ QUESTDB DATA VERIFIED!")
        print("=" * 60)
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Cannot connect to QuestDB: {e}")
        print("\n📝 To fix:")
        print("1. Start QuestDB: start_questdb.bat")
        print("2. Wait 10-15 seconds")
        print("3. Run this script again")
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def show_web_console_info():
    """Show how to access web console"""
    print("\n" + "=" * 60)
    print("🌐 QUESTDB WEB CONSOLE")
    print("=" * 60)
    print("\n📊 You can also check data via web browser:")
    print("1. Open browser: http://localhost:9000")
    print("2. Click 'Console' tab")
    print("3. Run SQL queries:")
    print("\n   -- See all data")
    print("   SELECT * FROM ohlcv1d LIMIT 100;")
    print("\n   -- Count records by symbol")
    print("   SELECT symbol, COUNT(*) FROM ohlcv1d GROUP BY symbol;")
    print("\n   -- Latest prices")
    print("   SELECT * FROM ohlcv1d WHERE symbol = 'SPY' ORDER BY date DESC LIMIT 10;")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    success = check_questdb()
    show_web_console_info()
    
    if not success:
        print("\n⚠️ If QuestDB has no data, run these commands:")
        print("1. python questdb_manager.py  # Load all CSV data")
        print("2. python check_questdb_data.py  # Check again")