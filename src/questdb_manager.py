#!/usr/bin/env python3
"""
QuestDB Manager - Check status, create tables, and insert data
"""

import psycopg2
import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class QuestDBManager:
    def __init__(self):
        self.host = 'localhost'
        self.port = 8812  # PostgreSQL wire protocol
        self.database = 'qdb'
        self.user = 'admin'
        self.password = 'quest'
        
    def check_connection(self):
        """Check if QuestDB is accessible"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("✅ QuestDB connection successful!")
            
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ QuestDB connection failed: {e}")
            print("\n📝 To fix this:")
            print("1. Run 'start_questdb.bat' as Administrator")
            print("2. Wait 10-15 seconds for QuestDB to start")
            print("3. Access web console at http://localhost:9000")
            return False
    
    def create_tables(self):
        """Create OHLCV table"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            cursor = conn.cursor()
            
            # Drop existing table
            try:
                cursor.execute("DROP TABLE IF EXISTS ohlcv1d")
                print("📊 Dropped existing ohlcv1d table")
            except:
                pass
            
            # Create new table
            create_sql = """
            CREATE TABLE ohlcv1d (
                symbol STRING,
                date TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH
            """
            
            cursor.execute(create_sql)
            conn.commit()
            
            print("✅ Created ohlcv1d table successfully!")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to create table: {e}")
            return False
    
    def insert_csv_data(self, csv_path):
        """Insert data from CSV file"""
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            print(f"📥 Read {len(df)} records from {csv_path}")
            
            # Get symbol from filename
            symbol = Path(csv_path).stem.split('_')[0]
            
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            cursor = conn.cursor()
            
            # Insert data
            inserted = 0
            for _, row in df.iterrows():
                try:
                    sql = """
                    INSERT INTO ohlcv1d (symbol, date, open, high, low, close, volume, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    values = (
                        symbol,
                        pd.to_datetime(row['date']),
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row['volume']),
                        pd.to_datetime(row['date'])
                    )
                    
                    cursor.execute(sql, values)
                    inserted += 1
                except Exception as e:
                    print(f"  ⚠️ Skip row: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✅ Inserted {inserted} records for {symbol}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to insert data: {e}")
            return False
    
    def query_data(self):
        """Query and display data"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            cursor = conn.cursor()
            
            # Get record count by symbol
            cursor.execute("""
                SELECT symbol, COUNT(*) as count,
                       MIN(date) as first_date,
                       MAX(date) as last_date
                FROM ohlcv1d
                GROUP BY symbol
                ORDER BY symbol
            """)
            
            results = cursor.fetchall()
            
            print("\n📊 Data in QuestDB:")
            print("-" * 60)
            for symbol, count, first_date, last_date in results:
                print(f"{symbol}: {count} records ({first_date} to {last_date})")
            
            # Get latest prices
            cursor.execute("""
                SELECT symbol, date, close
                FROM ohlcv1d
                WHERE date = (SELECT MAX(date) FROM ohlcv1d)
                ORDER BY symbol
            """)
            
            latest = cursor.fetchall()
            
            print("\n💵 Latest Prices:")
            print("-" * 60)
            for symbol, date, close in latest:
                print(f"{symbol}: ${close:.2f} ({date})")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return False
    
    def load_all_csv_files(self):
        """Load all CSV files from data/csv directory"""
        csv_dir = Path('./data/csv')
        
        if not csv_dir.exists():
            print("❌ No data/csv directory found")
            return False
        
        csv_files = list(csv_dir.glob('*_ohlcv_5y.csv'))
        
        if not csv_files:
            print("❌ No CSV files found")
            return False
        
        print(f"📁 Found {len(csv_files)} CSV files")
        
        for csv_file in csv_files:
            print(f"\n📥 Loading {csv_file.name}...")
            self.insert_csv_data(csv_file)
        
        return True

def main():
    manager = QuestDBManager()
    
    print("🔍 QuestDB Manager")
    print("=" * 60)
    
    # Check connection
    if not manager.check_connection():
        print("\n❌ QuestDB is not running!")
        print("Please run 'start_questdb.bat' first")
        return
    
    # Create tables
    print("\n📊 Setting up tables...")
    manager.create_tables()
    
    # Load data
    print("\n📥 Loading data...")
    manager.load_all_csv_files()
    
    # Query data
    print("\n📊 Querying data...")
    manager.query_data()
    
    print("\n✅ QuestDB setup complete!")
    print("\n📝 Access QuestDB:")
    print("   Web Console: http://localhost:9000")
    print("   SQL Query: SELECT * FROM ohlcv1d LIMIT 10;")

if __name__ == "__main__":
    main()