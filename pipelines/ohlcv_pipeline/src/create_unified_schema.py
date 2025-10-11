#!/usr/bin/env python3
"""
Create Unified QuestDB Schema
Creates all necessary tables for the unified data warehouse
"""

import psycopg2
import sys
import io
from datetime import datetime

# Fix Windows encoding
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except (ValueError, AttributeError):
        pass  # Already wrapped or not available

class UnifiedSchemaCreator:
    def __init__(self):
        self.db_params = {
            'host': 'localhost',
            'port': 8812,
            'database': 'qdb',
            'user': 'admin',
            'password': 'quest'
        }

    def check_connection(self):
        """Check if QuestDB is accessible"""
        try:
            conn = psycopg2.connect(**self.db_params)
            print("✅ QuestDB connection successful!")
            conn.close()
            return True
        except Exception as e:
            print(f"❌ QuestDB connection failed: {e}")
            print("\n📝 To fix this:")
            print("1. Run 'start_questdb.bat' as Administrator")
            print("2. Wait 10-15 seconds for QuestDB to start")
            print("3. Access web console at http://localhost:9000")
            return False

    def create_unified_ohlcv_table(self):
        """Create main unified OHLCV table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            # Drop existing table if requested
            print("\n📊 Creating ohlcv_unified table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS ohlcv_unified (
                symbol STRING,
                asset_class STRING,
                index_membership STRING,
                timestamp TIMESTAMP,
                timeframe STRING,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                adj_close DOUBLE,
                vwap DOUBLE,
                returns DOUBLE,
                log_returns DOUBLE,
                data_source STRING,
                inserted_at TIMESTAMP
            ) timestamp(timestamp) PARTITION BY DAY;
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created ohlcv_unified table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create ohlcv_unified table: {e}")
            return False

    def create_symbol_metadata_table(self):
        """Create symbol metadata table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n📊 Creating symbol_metadata table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS symbol_metadata (
                symbol STRING,
                name STRING,
                sector STRING,
                industry STRING,
                asset_class STRING,
                index_membership STRING,
                market_cap DOUBLE,
                ipo_date TIMESTAMP,
                data_start_date TIMESTAMP,
                data_end_date TIMESTAMP,
                total_records_1min LONG,
                total_records_1hour LONG,
                total_records_1day LONG,
                last_updated TIMESTAMP,
                is_active BOOLEAN
            );
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created symbol_metadata table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create symbol_metadata table: {e}")
            return False

    def create_index_constituents_table(self):
        """Create index constituents tracking table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n📊 Creating index_constituents table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS index_constituents (
                index_name STRING,
                symbol STRING,
                date_added TIMESTAMP,
                date_removed TIMESTAMP,
                weight_percent DOUBLE,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY YEAR;
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created index_constituents table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create index_constituents table: {e}")
            return False

    def create_data_quality_table(self):
        """Create data quality metrics table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n📊 Creating data_quality table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS data_quality (
                symbol STRING,
                timeframe STRING,
                check_date TIMESTAMP,
                total_records LONG,
                missing_days LONG,
                data_gaps LONG,
                anomalies LONG,
                quality_score DOUBLE,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH;
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created data_quality table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create data_quality table: {e}")
            return False

    def create_correlation_table(self):
        """Create pre-computed correlation table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n📊 Creating pairwise_correlation table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS pairwise_correlation (
                symbol1 STRING,
                symbol2 STRING,
                date TIMESTAMP,
                correlation_30d DOUBLE,
                correlation_90d DOUBLE,
                correlation_252d DOUBLE,
                covariance DOUBLE,
                timestamp TIMESTAMP
            ) timestamp(timestamp) PARTITION BY MONTH;
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created pairwise_correlation table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create pairwise_correlation table: {e}")
            return False

    def create_rolling_stats_table(self):
        """Create pre-computed rolling statistics table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n📊 Creating rolling_statistics table...")

            create_sql = """
            CREATE TABLE IF NOT EXISTS rolling_statistics (
                symbol STRING,
                timestamp TIMESTAMP,
                window_days INT,
                mean_return DOUBLE,
                std_dev DOUBLE,
                sharpe_ratio DOUBLE,
                max_drawdown DOUBLE,
                var_95 DOUBLE,
                skewness DOUBLE,
                kurtosis DOUBLE
            ) timestamp(timestamp) PARTITION BY MONTH;
            """

            cursor.execute(create_sql)
            conn.commit()
            print("✅ Created rolling_statistics table")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Failed to create rolling_statistics table: {e}")
            return False

    def verify_tables(self):
        """Verify all tables were created"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            print("\n🔍 Verifying tables...")

            cursor.execute("SELECT table_name FROM tables;")
            tables = [row[0] for row in cursor.fetchall()]

            expected_tables = [
                'ohlcv_unified',
                'symbol_metadata',
                'index_constituents',
                'data_quality',
                'pairwise_correlation',
                'rolling_statistics'
            ]

            print("\n📋 Tables in database:")
            for table in expected_tables:
                if table in tables:
                    print(f"  ✅ {table}")
                else:
                    print(f"  ❌ {table} - MISSING")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"❌ Verification failed: {e}")
            return False

    def create_all_tables(self):
        """Create all tables in the schema"""
        print("=" * 80)
        print("🏗️  UNIFIED SCHEMA CREATOR")
        print("=" * 80)

        if not self.check_connection():
            return False

        # Create all tables
        self.create_unified_ohlcv_table()
        self.create_symbol_metadata_table()
        self.create_index_constituents_table()
        self.create_data_quality_table()
        self.create_correlation_table()
        self.create_rolling_stats_table()

        # Verify
        self.verify_tables()

        print("\n" + "=" * 80)
        print("✅ SCHEMA CREATION COMPLETE!")
        print("=" * 80)
        print("\n📝 Access QuestDB:")
        print("   Web Console: http://localhost:9000")
        print("   Sample Query: SELECT * FROM ohlcv_unified LIMIT 10;")
        print("\n📊 Tables created:")
        print("   - ohlcv_unified          (Main OHLCV data)")
        print("   - symbol_metadata        (Symbol information)")
        print("   - index_constituents     (Index membership)")
        print("   - data_quality           (Quality metrics)")
        print("   - pairwise_correlation   (Pre-computed correlations)")
        print("   - rolling_statistics     (Pre-computed stats)")

        return True

def main():
    creator = UnifiedSchemaCreator()
    creator.create_all_tables()

if __name__ == "__main__":
    main()
