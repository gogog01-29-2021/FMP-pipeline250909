#!/usr/bin/env python3
"""
Test Schema Collisions - Show what happens when different processors try to create tables
This simulates the real problems you'll face when QuestDB is running
"""

import psycopg2
import pandas as pd
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

class SchemaCollisionTester:
    """Test what happens with schema inconsistencies"""

    def __init__(self):
        self.db_params = {
            'host': 'localhost',
            'port': 8812,
            'database': 'qdb',
            'user': 'admin',
            'password': 'quest'
        }

    def check_questdb_connection(self):
        """Check if QuestDB is running"""
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.close()
            print("✅ QuestDB connection successful")
            return True
        except Exception as e:
            print(f"❌ QuestDB connection failed: {e}")
            print("\n🔧 To fix this:")
            print("1. Open Command Prompt as Administrator")
            print("2. Navigate to: questdb-9.0.3-rt-windows-x86-64\\bin")
            print("3. Run: questdb.exe start")
            print("4. Wait 10-15 seconds for startup")
            print("5. Verify at: http://localhost:9000")
            return False

    def simulate_questdb_manager_schema(self):
        """Simulate questdb_manager.py trying to create table"""
        schema_sql = """
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

        print("\n📊 QUESTDB_MANAGER.PY attempting to create table:")
        print("=" * 60)
        print(schema_sql)

        return {
            'processor': 'questdb_manager.py',
            'columns': ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'timestamp'],
            'column_count': 8,
            'partition': 'BY MONTH',
            'has_adjclose': False,
            'has_metadata': False,
            'symbol_type': 'STRING'
        }

    def simulate_questdb_client_schema(self):
        """Simulate questdb_client.py trying to create table"""
        schema_sql = """
        CREATE TABLE ohlcv1d (
            timestamp TIMESTAMP,
            symbol SYMBOL CAPACITY 128 CACHE,
            etf_name STRING,
            index_tracked SYMBOL CAPACITY 32 CACHE,
            expense_ratio DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume LONG
        ) timestamp(timestamp) PARTITION BY MONTH
        """

        print("\n📊 QUESTDB_CLIENT.PY attempting to create table:")
        print("=" * 60)
        print(schema_sql)

        return {
            'processor': 'questdb_client.py',
            'columns': ['timestamp', 'symbol', 'etf_name', 'index_tracked', 'expense_ratio',
                       'open', 'high', 'low', 'close', 'adj_close', 'volume'],
            'column_count': 11,
            'partition': 'BY MONTH',
            'has_adjclose': True,
            'has_metadata': True,
            'symbol_type': 'SYMBOL CAPACITY 128 CACHE'
        }

    def simulate_etf_fmp_processor_schema(self):
        """Simulate etf_fmp_questdb_processor.py trying to create table"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS ohlcv1d (
            symbol SYMBOL,
            date TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            adjClose DOUBLE,
            timestamp TIMESTAMP
        ) timestamp(timestamp) PARTITION BY MONTH
        """

        print("\n📊 ETF_FMP_QUESTDB_PROCESSOR.PY attempting to create table:")
        print("=" * 60)
        print(schema_sql)

        return {
            'processor': 'etf_fmp_questdb_processor.py',
            'columns': ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjClose', 'timestamp'],
            'column_count': 9,
            'partition': 'BY MONTH',
            'has_adjclose': True,
            'has_metadata': False,
            'symbol_type': 'SYMBOL'
        }

    def analyze_conflicts(self, schemas):
        """Analyze what conflicts would occur"""
        print("\n💥 SCHEMA CONFLICT ANALYSIS:")
        print("=" * 60)

        conflicts = []

        # Column count conflicts
        column_counts = [s['column_count'] for s in schemas]
        if len(set(column_counts)) > 1:
            conflicts.append({
                'type': 'COLUMN_COUNT_MISMATCH',
                'description': f"Different column counts: {column_counts}",
                'impact': 'INSERT statements will fail with column mismatch errors'
            })

        # adjClose naming conflicts
        adjclose_variants = []
        for s in schemas:
            if s['has_adjclose']:
                if 'adjClose' in s['columns']:
                    adjclose_variants.append('adjClose')
                elif 'adj_close' in s['columns']:
                    adjclose_variants.append('adj_close')

        if len(set(adjclose_variants)) > 1:
            conflicts.append({
                'type': 'ADJCLOSE_NAMING_CONFLICT',
                'description': f"Mixed naming: {set(adjclose_variants)}",
                'impact': 'Queries will fail trying to reference non-existent columns'
            })

        # Symbol type conflicts
        symbol_types = [s['symbol_type'] for s in schemas]
        if len(set(symbol_types)) > 1:
            conflicts.append({
                'type': 'SYMBOL_TYPE_CONFLICT',
                'description': f"Different symbol types: {set(symbol_types)}",
                'impact': 'Performance degradation and potential insert failures'
            })

        # Missing metadata conflicts
        has_metadata = [s['has_metadata'] for s in schemas]
        if True in has_metadata and False in has_metadata:
            conflicts.append({
                'type': 'METADATA_AVAILABILITY_CONFLICT',
                'description': "Some schemas have ETF metadata, others don't",
                'impact': 'Data loss when processors try to insert metadata into basic schemas'
            })

        return conflicts

    def simulate_data_insert_failures(self, conflicts):
        """Simulate what happens when you try to insert data with conflicts"""
        print("\n🔥 SIMULATED INSERT FAILURES:")
        print("=" * 60)

        # Sample data that would cause failures
        sample_data = {
            'symbol': 'SPY',
            'date': '2025-09-19',
            'open': 662.33,
            'high': 664.55,
            'low': 660.37,
            'close': 663.7,
            'volume': 97853200,
            'adjClose': 663.7,  # This field name varies!
            'etf_name': 'SPDR S&P 500 ETF Trust',  # Only some schemas have this
            'expense_ratio': 0.0945  # Only some schemas have this
        }

        print(f"📊 Attempting to insert sample data:")
        print(f"   {sample_data}")

        for conflict in conflicts:
            print(f"\n❌ {conflict['type']}:")
            print(f"   Problem: {conflict['description']}")
            print(f"   Result: {conflict['impact']}")

            if conflict['type'] == 'COLUMN_COUNT_MISMATCH':
                print(f"   SQL Error: INSERT INTO ohlcv1d (8 columns) VALUES (11 values)")
                print(f"   Error Code: column count mismatch")

            elif conflict['type'] == 'ADJCLOSE_NAMING_CONFLICT':
                print(f"   SQL Error: column 'adjClose' does not exist")
                print(f"   Alternative: column 'adj_close' does not exist")

            elif conflict['type'] == 'METADATA_AVAILABILITY_CONFLICT':
                print(f"   SQL Error: column 'etf_name' does not exist")
                print(f"   SQL Error: column 'expense_ratio' does not exist")

    def simulate_real_processor_runs(self):
        """Simulate what happens when multiple processors run"""
        print("\n⚡ SIMULATING MULTIPLE PROCESSOR EXECUTION:")
        print("=" * 60)

        # Processor execution order
        execution_order = [
            ('questdb_manager.py', 'User runs basic data loading'),
            ('etf_fmp_questdb_processor.py', 'Nightly update runs'),
            ('questdb_client.py', 'Enhanced analysis runs')
        ]

        for i, (processor, context) in enumerate(execution_order):
            print(f"\n{i+1}. {context}")
            print(f"   Processor: {processor}")

            if i == 0:
                print(f"   ✅ Creates table with basic schema (8 columns)")
                print(f"   ✅ Inserts data successfully")
            elif i == 1:
                print(f"   ❌ Tries to create table with adjClose field")
                print(f"   ❌ Table already exists with different schema")
                print(f"   ❌ INSERT fails: adjClose column doesn't exist")
                print(f"   💥 Process crashes or skips records")
            else:
                print(f"   ❌ Tries to create table with metadata fields")
                print(f"   ❌ Table already exists with different schema")
                print(f"   ❌ INSERT fails: etf_name, expense_ratio don't exist")
                print(f"   💥 Data loss: metadata discarded")

    def run_collision_test(self):
        """Run the complete collision test"""
        print("🧪 SCHEMA COLLISION TEST")
        print("Testing what happens with your current inconsistent schemas")
        print("=" * 60)

        # Check connection first
        if not self.check_questdb_connection():
            print("\n⚠️ QuestDB not running - showing simulation only")
            print("When QuestDB IS running, these exact problems will occur:")

        # Simulate different processors
        schemas = [
            self.simulate_questdb_manager_schema(),
            self.simulate_questdb_client_schema(),
            self.simulate_etf_fmp_processor_schema()
        ]

        # Analyze conflicts
        conflicts = self.analyze_conflicts(schemas)

        # Show insert failures
        self.simulate_data_insert_failures(conflicts)

        # Show real execution problems
        self.simulate_real_processor_runs()

        # Summary
        print(f"\n📋 COLLISION TEST SUMMARY:")
        print("=" * 60)
        print(f"✅ Schemas analyzed: {len(schemas)}")
        print(f"❌ Conflicts found: {len(conflicts)}")
        print(f"💥 Critical issues: {len([c for c in conflicts if 'MISMATCH' in c['type']])}")

        if conflicts:
            print(f"\n🚨 YOUR PIPELINE WILL FAIL with these schema inconsistencies!")
            print(f"🔧 SOLUTION: Implement unified schema before production")
        else:
            print(f"\n✅ No conflicts detected - schemas are compatible")

        return len(conflicts) == 0

def main():
    """Run schema collision testing"""
    tester = SchemaCollisionTester()
    success = tester.run_collision_test()

    if not success:
        print(f"\n💡 NEXT STEPS:")
        print(f"1. Design unified schema")
        print(f"2. Update all processors to use same schema")
        print(f"3. Test with QuestDB running")
        print(f"4. Run boss demo with working pipeline")

if __name__ == "__main__":
    main()