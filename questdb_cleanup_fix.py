"""
QuestDB Data Management Fix
===========================
QuestDB doesn't support standard DELETE operations.
Instead, we need to use partitioning and drop old partitions,
or use ALTER TABLE with a WHERE clause to filter data.

For the 30-day rolling window on 1-minute data:
1. Use partitioned tables (already done)
2. Drop old partitions periodically
3. Or recreate table with only recent data
"""

import psycopg2
import logging
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')

def cleanup_old_1min_data(symbol: str = None, days_to_keep: int = 30):
    """
    Clean up old 1-minute data keeping only last N days.
    Since QuestDB doesn't support DELETE, we'll use a different approach.
    """
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=8812,
            database='qdb',
            user='admin',
            password='quest'
        )
        cursor = conn.cursor()
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d')
        
        logging.info(f"🧹 Cleaning up 1-minute data older than {cutoff_str}")
        
        # QuestDB approach: Create a new table with recent data only
        # Then swap tables
        
        # Step 1: Create temporary table with recent data
        if symbol:
            query = f"""
                CREATE TABLE ohlcv1min_temp AS (
                    SELECT * FROM ohlcv1min 
                    WHERE datetime >= '{cutoff_str}' 
                    AND symbol = '{symbol}'
                )
            """
        else:
            query = f"""
                CREATE TABLE ohlcv1min_temp AS (
                    SELECT * FROM ohlcv1min 
                    WHERE datetime >= '{cutoff_str}'
                )
            """
        
        try:
            cursor.execute("DROP TABLE IF EXISTS ohlcv1min_temp")
            cursor.execute(query)
            
            # Step 2: Drop old table and rename new one
            cursor.execute("DROP TABLE ohlcv1min")
            cursor.execute("RENAME TABLE ohlcv1min_temp TO ohlcv1min")
            
            logging.info("✅ Successfully cleaned up old 1-minute data")
            
        except Exception as e:
            logging.warning(f"⚠️ Cleanup approach 1 failed: {e}")
            
            # Alternative: Use QuestDB's partition drop (if partitioned by day)
            try:
                # Get partition information
                cursor.execute("""
                    SELECT partition, minTimestamp, maxTimestamp, numRows 
                    FROM table_partitions('ohlcv1min')
                    WHERE maxTimestamp < %s
                """, (cutoff_date,))
                
                old_partitions = cursor.fetchall()
                for partition in old_partitions:
                    logging.info(f"Dropping partition: {partition[0]}")
                    cursor.execute(f"ALTER TABLE ohlcv1min DROP PARTITION LIST '{partition[0]}'")
                    
            except Exception as e2:
                logging.warning(f"⚠️ Partition cleanup also failed: {e2}")
                logging.info("ℹ️ Note: QuestDB has limited DELETE support. Consider recreating table periodically.")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Cleanup failed: {e}")
        return False
    
    return True

def get_1min_data_summary():
    """Get summary of 1-minute data in QuestDB"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=8812,
            database='qdb',
            user='admin',
            password='quest'
        )
        cursor = conn.cursor()
        
        # Check if table exists and has data
        cursor.execute("""
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(datetime) as earliest,
                MAX(datetime) as latest
            FROM ohlcv1min
            GROUP BY symbol
        """)
        
        results = cursor.fetchall()
        
        if results:
            logging.info("\n📊 1-Minute Data Summary:")
            logging.info("=" * 60)
            for symbol, count, earliest, latest in results:
                days = (latest - earliest).days if earliest and latest else 0
                logging.info(f"  {symbol}: {count:,} records ({days} days)")
                logging.info(f"    From: {earliest}")
                logging.info(f"    To:   {latest}")
        else:
            logging.info("\n📊 No 1-minute data found in ohlcv1min table")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Failed to get summary: {e}")

def alternative_cleanup_approach():
    """
    Alternative approach: Use QuestDB's native features
    """
    logging.info("""
    ℹ️ QuestDB Data Management Recommendations:
    ==========================================
    
    1. PARTITION MANAGEMENT (Recommended):
       - Tables are already partitioned by DAY
       - Use ALTER TABLE DROP PARTITION to remove old partitions
       - This is the most efficient approach
    
    2. TABLE RECREATION:
       - Periodically recreate table with only recent data
       - Use CREATE TABLE AS SELECT with date filter
       - Swap tables atomically
    
    3. SAMPLING/DOWNSAMPLING:
       - Keep detailed data for recent period
       - Downsample older data to hourly/daily
       - Store in separate historical tables
    
    4. ROTATION STRATEGY:
       - Create new tables periodically (e.g., monthly)
       - Query across multiple tables when needed
       - Drop old tables entirely
    
    Current Implementation:
    - We'll check for old partitions and drop them
    - If partitions aren't available, we'll log for manual cleanup
    """)

if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("🔧 QuestDB CLEANUP FIX")
    logging.info("=" * 60)
    
    # Show current state
    get_1min_data_summary()
    
    # Show cleanup options
    alternative_cleanup_approach()
    
    logging.info("\n✅ Cleanup analysis complete!")
    logging.info("Note: Since no 1-minute data exists yet, cleanup will run after data is loaded.")