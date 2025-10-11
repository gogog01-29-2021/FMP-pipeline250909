#!/usr/bin/env python3
"""
Load data into QuestDB and run SQL analytics
"""

import psycopg2
import pandas as pd
from pathlib import Path
import sys

print("="*80)
print("LOADING DATA INTO QUESTDB AND RUNNING ANALYTICS")
print("="*80)

# QuestDB connection
db_params = {
    'host': 'localhost',
    'port': 8812,
    'database': 'qdb',
    'user': 'admin',
    'password': 'quest'
}

# ==================== STEP 1: CREATE SCHEMA ====================

print("\n[1/5] Creating QuestDB schema...")

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Create main table
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
    print("OK - Schema created successfully")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"ERROR - Schema creation failed: {e}")
    print("\nMake sure QuestDB is running:")
    print("  cd questdb-9.0.3-rt-windows-x86-64/bin")
    print("  questdb.exe start")
    sys.exit(1)

# ==================== STEP 2: LOAD DATA ====================

print("\n[2/5] Loading CSV data into QuestDB...")

csv_files = list(Path('data/csv/etf').glob('*.csv'))
print(f"Found {len(csv_files)} CSV files")

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

total_inserted = 0

for csv_file in csv_files:
    symbol = csv_file.stem.split('_')[0]
    timeframe = csv_file.stem.split('_')[1]

    print(f"\n  Loading {symbol} ({timeframe})...")

    df = pd.read_csv(csv_file)

    # Add metadata if missing
    if 'asset_class' not in df.columns:
        df['asset_class'] = 'etf'
    if 'index_membership' not in df.columns:
        df['index_membership'] = 'SP500' if symbol in ['SPY', 'QQQ'] else 'N/A'
    if 'timeframe' not in df.columns:
        df['timeframe'] = timeframe
    if 'data_source' not in df.columns:
        df['data_source'] = 'FMP'
    if 'inserted_at' not in df.columns:
        df['inserted_at'] = pd.Timestamp.now()

    inserted = 0
    for _, row in df.iterrows():
        try:
            insert_sql = """
            INSERT INTO ohlcv_unified (
                symbol, asset_class, index_membership, timestamp, timeframe,
                open, high, low, close, volume,
                adj_close, vwap, returns, log_returns,
                data_source, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_sql, (
                symbol,
                row.get('asset_class', 'etf'),
                row.get('index_membership', 'N/A'),
                pd.to_datetime(row['date']),
                row.get('timeframe', timeframe),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                float(row.get('adj_close', row['close'])),
                float(row.get('vwap', (row['high'] + row['low'] + row['close'])/3)),
                float(row.get('returns', 0)),
                float(row.get('log_returns', 0)),
                row.get('data_source', 'FMP'),
                pd.Timestamp.now()
            ))
            inserted += 1

            if inserted % 100 == 0:
                conn.commit()

        except Exception as e:
            print(f"    Skip row: {e}")
            continue

    conn.commit()
    total_inserted += inserted
    print(f"    OK - Inserted {inserted} records")

cursor.close()
conn.close()

print(f"\nOK - Total records inserted: {total_inserted}")

# ==================== STEP 3: VERIFY DATA ====================

print("\n[3/5] Verifying data in QuestDB...")

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM ohlcv_unified")
count = cursor.fetchone()[0]
print(f"  Total records in database: {count}")

cursor.execute("""
    SELECT symbol, timeframe, COUNT(*) as count
    FROM ohlcv_unified
    GROUP BY symbol, timeframe
    ORDER BY symbol, timeframe
""")

print("\n  Records by symbol and timeframe:")
for symbol, timeframe, cnt in cursor.fetchall():
    print(f"    {symbol:6s} ({timeframe:5s}): {cnt:4d} records")

# ==================== STEP 4: RUN ANALYTICS ====================

print("\n[4/5] Running SQL Analytics...")

# Beta Analysis
print("\n  [A] Beta Calculation (vs SPY)...")
beta_sql = """
WITH spy_returns AS (
    SELECT
        timestamp::date as date,
        returns as spy_return
    FROM ohlcv_unified
    WHERE symbol = 'SPY' AND timeframe = '1day' AND returns IS NOT NULL
),
stock_spy_joined AS (
    SELECT
        s.symbol,
        s.returns as stock_return,
        spy.spy_return
    FROM ohlcv_unified s
    JOIN spy_returns spy ON s.timestamp::date = spy.date
    WHERE s.timeframe = '1day'
        AND s.symbol != 'SPY'
        AND s.returns IS NOT NULL
)
SELECT
    symbol,
    round(covar_samp(stock_return, spy_return) / var_samp(spy_return), 3) as beta,
    round(corr(stock_return, spy_return) * corr(stock_return, spy_return), 3) as r_squared,
    round(corr(stock_return, spy_return), 3) as correlation,
    count(*) as observations
FROM stock_spy_joined
GROUP BY symbol
ORDER BY beta DESC
"""

cursor.execute(beta_sql)
print("\n  Beta Results:")
print(f"  {'Symbol':8s} {'Beta':>8s} {'R²':>8s} {'Corr':>8s} {'Obs':>6s}")
print("  " + "-"*45)
for row in cursor.fetchall():
    print(f"  {row[0]:8s} {row[1]:8.3f} {row[2]:8.3f} {row[3]:8.3f} {row[4]:6d}")

# Correlation Analysis
print("\n  [B] Correlation Analysis...")
corr_sql = """
WITH daily_returns AS (
    SELECT
        symbol,
        timestamp::date as date,
        returns
    FROM ohlcv_unified
    WHERE timeframe = '1day' AND returns IS NOT NULL
),
corr_pairs AS (
    SELECT
        a.symbol as symbol1,
        b.symbol as symbol2,
        round(corr(a.returns, b.returns), 3) as correlation,
        count(*) as observations
    FROM daily_returns a
    JOIN daily_returns b ON a.date = b.date AND a.symbol < b.symbol
    GROUP BY a.symbol, b.symbol
)
SELECT * FROM corr_pairs WHERE observations >= 10
ORDER BY abs(correlation) DESC
"""

cursor.execute(corr_sql)
print("\n  High Correlations:")
print(f"  {'Pair':20s} {'Correlation':>12s} {'Obs':>6s}")
print("  " + "-"*45)
for row in cursor.fetchall()[:10]:
    print(f"  {row[0]:6s}-{row[1]:6s}     {row[2]:12.3f} {row[3]:6d}")

# Volatility Analysis
print("\n  [C] Volatility Analysis...")
vol_sql = """
SELECT
    symbol,
    round(stddev_samp(returns) * sqrt(252) * 100, 2) as annual_vol_pct,
    round(avg(returns) * 252 * 100, 2) as annual_return_pct,
    round((avg(returns) / stddev_samp(returns)) * sqrt(252), 2) as sharpe_ratio,
    count(*) as observations
FROM ohlcv_unified
WHERE timeframe = '1day' AND returns IS NOT NULL
GROUP BY symbol
ORDER BY annual_vol_pct DESC
"""

cursor.execute(vol_sql)
print("\n  Volatility Metrics:")
print(f"  {'Symbol':8s} {'Vol%':>8s} {'Ret%':>8s} {'Sharpe':>8s} {'Obs':>6s}")
print("  " + "-"*50)
for row in cursor.fetchall():
    print(f"  {row[0]:8s} {row[1]:8.2f} {row[2]:8.2f} {row[3]:8.2f} {row[4]:6d}")

# Event Study
print("\n  [D] Significant Events (|return| > 3%)...")
event_sql = """
SELECT
    symbol,
    timestamp::date as event_date,
    round(returns * 100, 2) as return_pct,
    close
FROM ohlcv_unified
WHERE timeframe = '1day'
    AND abs(returns) > 0.03
ORDER BY abs(returns) DESC
LIMIT 10
"""

cursor.execute(event_sql)
print("\n  Significant Price Moves:")
print(f"  {'Symbol':8s} {'Date':12s} {'Return%':>10s} {'Close':>10s}")
print("  " + "-"*50)
for row in cursor.fetchall():
    print(f"  {row[0]:8s} {str(row[1]):12s} {row[2]:10.2f} {row[3]:10.2f}")

cursor.close()
conn.close()

# ==================== STEP 5: SUMMARY ====================

print("\n" + "="*80)
print("ANALYTICS COMPLETE!")
print("="*80)

print("\nOK - Data loaded into QuestDB: ohlcv_unified table")
print("OK - Analytics completed:")
print("  - Beta calculation (vs SPY)")
print("  - Correlation matrix")
print("  - Volatility analysis")
print("  - Significant events")

print("\nAccess QuestDB Web Console: http://localhost:9000")
print("\nRun custom queries:")
print("  SELECT * FROM ohlcv_unified WHERE symbol = 'SPY';")
print("  SELECT symbol, AVG(close) FROM ohlcv_unified GROUP BY symbol;")

print("\n" + "="*80)
