#!/usr/bin/env python3
"""
Create persistent analytics tables in QuestDB
Store calculation results for easy querying
"""

import psycopg2
from datetime import datetime

print("="*80)
print("CREATING ANALYTICS TABLES IN QUESTDB")
print("="*80)

# QuestDB connection
db_params = {
    'host': 'localhost',
    'port': 8812,
    'database': 'qdb',
    'user': 'admin',
    'password': 'quest'
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# ==================== TABLE 1: BETA RESULTS ====================

print("\n[1/4] Creating beta_results table...")

cursor.execute("""
CREATE TABLE IF NOT EXISTS beta_results (
    symbol STRING,
    beta DOUBLE,
    r_squared DOUBLE,
    correlation DOUBLE,
    observations INT,
    calculated_at TIMESTAMP
) timestamp(calculated_at) PARTITION BY MONTH;
""")
conn.commit()
print("OK - beta_results table created")

# ==================== TABLE 2: CORRELATION MATRIX ====================

print("\n[2/4] Creating correlation_matrix table...")

cursor.execute("""
CREATE TABLE IF NOT EXISTS correlation_matrix (
    symbol1 STRING,
    symbol2 STRING,
    correlation DOUBLE,
    observations INT,
    calculated_at TIMESTAMP
) timestamp(calculated_at) PARTITION BY MONTH;
""")
conn.commit()
print("OK - correlation_matrix table created")

# ==================== TABLE 3: VOLATILITY METRICS ====================

print("\n[3/4] Creating volatility_metrics table...")

cursor.execute("""
CREATE TABLE IF NOT EXISTS volatility_metrics (
    symbol STRING,
    annual_volatility_pct DOUBLE,
    annual_return_pct DOUBLE,
    sharpe_ratio DOUBLE,
    observations INT,
    calculated_at TIMESTAMP
) timestamp(calculated_at) PARTITION BY MONTH;
""")
conn.commit()
print("OK - volatility_metrics table created")

# ==================== TABLE 4: SIGNIFICANT EVENTS ====================

print("\n[4/4] Creating significant_events table...")

cursor.execute("""
CREATE TABLE IF NOT EXISTS significant_events (
    symbol STRING,
    event_date TIMESTAMP,
    return_pct DOUBLE,
    close_price DOUBLE,
    event_type STRING,
    detected_at TIMESTAMP
) timestamp(detected_at) PARTITION BY MONTH;
""")
conn.commit()
print("OK - significant_events table created")

# ==================== VERIFY ====================

print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print("\nAll tables in QuestDB:")
for table in tables:
    print(f"  - {table[0]}")

cursor.close()
conn.close()

print("\n" + "="*80)
print("ANALYTICS TABLES CREATED!")
print("="*80)
print("\nThese tables will store persistent analytics results:")
print("  1. beta_results - Beta calculations vs SPY")
print("  2. correlation_matrix - Correlation between symbols")
print("  3. volatility_metrics - Volatility and Sharpe ratios")
print("  4. significant_events - Major price movements")
print("\nRun 'python populate_analytics_tables.py' to fill them with data")
