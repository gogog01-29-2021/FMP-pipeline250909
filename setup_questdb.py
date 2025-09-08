import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def create_questdb_table():
    """Create the ohlcv1d table in QuestDB"""
    
    questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
    questdb_port = os.getenv('QUESTDB_PORT', '9000')
    
    # SQL to create the ohlcv1d table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ohlcv1d (
        symbol SYMBOL,
        name STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume LONG,
        adj_close DOUBLE,
        ts TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL;
    """
    
    # QuestDB REST API endpoint
    url = f"http://{questdb_host}:{questdb_port}/exec"
    
    try:
        response = requests.get(url, params={'query': create_table_sql})
        response.raise_for_status()
        
        result = response.json()
        
        if 'error' in result:
            print(f"❌ Error creating table: {result['error']}")
            return False
        else:
            print("✅ Successfully created ohlcv1d table in QuestDB")
            print("📊 Table schema:")
            print("   - symbol: SYMBOL (indexed)")
            print("   - name: STRING")
            print("   - open, high, low, close, adj_close: DOUBLE")
            print("   - volume: LONG")
            print("   - ts: TIMESTAMP (partitioned by day)")
            return True
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to QuestDB at {questdb_host}:{questdb_port}")
        print("🔧 Make sure QuestDB is running:")
        print("   1. Download QuestDB from: https://questdb.io/get-questdb/")
        print("   2. Extract and run: questdb.exe start")
        print("   3. Access web console at: http://localhost:9000")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def check_questdb_connection():
    """Check if QuestDB is running and accessible"""
    questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
    questdb_port = os.getenv('QUESTDB_PORT', '9000')
    
    url = f"http://{questdb_host}:{questdb_port}/exec"
    
    try:
        response = requests.get(url, params={'query': 'SELECT 1;'})
        response.raise_for_status()
        print(f"✅ QuestDB is running at {questdb_host}:{questdb_port}")
        return True
        
    except requests.exceptions.ConnectionError:
        print(f"❌ QuestDB is not running at {questdb_host}:{questdb_port}")
        return False

def show_table_info():
    """Show information about the ohlcv1d table"""
    questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
    questdb_port = os.getenv('QUESTDB_PORT', '9000')
    
    url = f"http://{questdb_host}:{questdb_port}/exec"
    
    queries = [
        ("Table exists check", "SELECT table_name FROM tables() WHERE table_name = 'ohlcv1d';"),
        ("Row count", "SELECT COUNT(*) as total_rows FROM ohlcv1d;"),
        ("Date range", "SELECT MIN(ts) as min_date, MAX(ts) as max_date FROM ohlcv1d;"),
        ("Symbols", "SELECT DISTINCT symbol FROM ohlcv1d ORDER BY symbol;")
    ]
    
    print(f"\n📊 QuestDB Table Information")
    print(f"{'='*50}")
    
    for description, query in queries:
        try:
            response = requests.get(url, params={'query': query})
            response.raise_for_status()
            result = response.json()
            
            if 'error' in result:
                print(f"❌ {description}: {result['error']}")
            else:
                print(f"✅ {description}:")
                if 'dataset' in result and result['dataset']:
                    for row in result['dataset']:
                        print(f"   {row}")
                else:
                    print("   No results")
                    
        except Exception as e:
            print(f"❌ Error checking {description}: {e}")

def main():
    """Main setup function"""
    print("🔧 QuestDB Setup for ETF Data Project")
    print("=" * 50)
    
    # Check connection
    if not check_questdb_connection():
        print("\n🚨 Please start QuestDB first!")
        print("Download from: https://questdb.io/get-questdb/")
        return
    
    # Create table
    if create_questdb_table():
        print("\n📊 Table created successfully!")
    else:
        print("\n❌ Failed to create table")
        return
    
    # Show table info (initially empty)
    show_table_info()
    
    print(f"\n✅ QuestDB setup complete!")
    print(f"🌐 Access QuestDB web console: http://localhost:9000")
    print(f"🏃 Ready to run: python etf_fmp_processor.py")

if __name__ == "__main__":
    main()
