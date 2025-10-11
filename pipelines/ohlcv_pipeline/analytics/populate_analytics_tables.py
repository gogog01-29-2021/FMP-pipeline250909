#!/usr/bin/env python3
"""
Populate analytics tables with calculation results
This saves analytics results to persistent tables in QuestDB
"""

import psycopg2
from datetime import datetime

print("="*80)
print("POPULATING ANALYTICS TABLES")
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

calculated_at = datetime.now()

# ==================== POPULATE BETA RESULTS ====================

print("\n[1/4] Populating beta_results table...")

beta_sql = """
INSERT INTO beta_results
SELECT
    symbol,
    round(covar_samp(stock_return, spy_return) / var_samp(spy_return), 3) as beta,
    round(corr(stock_return, spy_return) * corr(stock_return, spy_return), 3) as r_squared,
    round(corr(stock_return, spy_return), 3) as correlation,
    count(*) as observations,
    now() as calculated_at
FROM (
    SELECT
        s.symbol,
        s.returns as stock_return,
        spy.spy_return
    FROM ohlcv_unified s
    JOIN (
        SELECT
            timestamp::date as date,
            returns as spy_return
        FROM ohlcv_unified
        WHERE symbol = 'SPY' AND timeframe = '1day' AND returns IS NOT NULL
    ) spy ON s.timestamp::date = spy.date
    WHERE s.timeframe = '1day'
        AND s.symbol != 'SPY'
        AND s.returns IS NOT NULL
) stock_spy_joined
GROUP BY symbol
ORDER BY beta DESC
"""

cursor.execute(beta_sql)
conn.commit()
print(f"OK - Inserted {cursor.rowcount} beta calculations")

# ==================== POPULATE CORRELATION MATRIX ====================

print("\n[2/4] Populating correlation_matrix table...")

corr_sql = """
INSERT INTO correlation_matrix
SELECT
    a.symbol as symbol1,
    b.symbol as symbol2,
    round(corr(a.returns, b.returns), 3) as correlation,
    count(*) as observations,
    now() as calculated_at
FROM (
    SELECT
        symbol,
        timestamp::date as date,
        returns
    FROM ohlcv_unified
    WHERE timeframe = '1day' AND returns IS NOT NULL
) a
JOIN (
    SELECT
        symbol,
        timestamp::date as date,
        returns
    FROM ohlcv_unified
    WHERE timeframe = '1day' AND returns IS NOT NULL
) b ON a.date = b.date AND a.symbol < b.symbol
GROUP BY a.symbol, b.symbol
"""

cursor.execute(corr_sql)
conn.commit()
print(f"OK - Inserted {cursor.rowcount} correlation pairs")

# ==================== POPULATE VOLATILITY METRICS ====================

print("\n[3/4] Populating volatility_metrics table...")

vol_sql = """
INSERT INTO volatility_metrics
SELECT
    symbol,
    round(stddev_samp(returns) * sqrt(252) * 100, 2) as annual_vol_pct,
    round(avg(returns) * 252 * 100, 2) as annual_return_pct,
    round((avg(returns) / stddev_samp(returns)) * sqrt(252), 2) as sharpe_ratio,
    count(*) as observations,
    now() as calculated_at
FROM ohlcv_unified
WHERE timeframe = '1day' AND returns IS NOT NULL
GROUP BY symbol
ORDER BY annual_vol_pct DESC
"""

cursor.execute(vol_sql)
conn.commit()
print(f"OK - Inserted {cursor.rowcount} volatility metrics")

# ==================== POPULATE SIGNIFICANT EVENTS ====================

print("\n[4/4] Populating significant_events table...")

event_sql = """
INSERT INTO significant_events
SELECT
    symbol,
    timestamp::date as event_date,
    round(returns * 100, 2) as return_pct,
    close as close_price,
    CASE
        WHEN returns > 0.03 THEN 'SPIKE_UP'
        WHEN returns < -0.03 THEN 'SPIKE_DOWN'
        ELSE 'NORMAL'
    END as event_type,
    now() as detected_at
FROM ohlcv_unified
WHERE timeframe = '1day'
    AND abs(returns) > 0.03
ORDER BY abs(returns) DESC
"""

cursor.execute(event_sql)
conn.commit()
print(f"OK - Inserted {cursor.rowcount} significant events")

# ==================== VERIFY RESULTS ====================

print("\n" + "="*80)
print("VERIFICATION - QUERYING ANALYTICS TABLES")
print("="*80)

# Beta results
print("\n[A] Beta Results:")
cursor.execute("SELECT symbol, beta, r_squared, correlation FROM beta_results ORDER BY beta DESC")
print(f"  {'Symbol':<8} {'Beta':>8} {'R²':>8} {'Corr':>8}")
print("  " + "-"*40)
for row in cursor.fetchall():
    print(f"  {row[0]:<8} {row[1]:8.3f} {row[2]:8.3f} {row[3]:8.3f}")

# Correlation matrix
print("\n[B] Correlation Matrix:")
cursor.execute("SELECT symbol1, symbol2, correlation FROM correlation_matrix ORDER BY abs(correlation) DESC LIMIT 5")
print(f"  {'Pair':<15} {'Correlation':>12}")
print("  " + "-"*30)
for row in cursor.fetchall():
    print(f"  {row[0]}-{row[1]:<10} {row[2]:12.3f}")

# Volatility metrics
print("\n[C] Volatility Metrics:")
cursor.execute("SELECT symbol, annual_volatility_pct, annual_return_pct, sharpe_ratio FROM volatility_metrics ORDER BY annual_volatility_pct DESC")
print(f"  {'Symbol':<8} {'Vol%':>8} {'Ret%':>8} {'Sharpe':>8}")
print("  " + "-"*40)
for row in cursor.fetchall():
    print(f"  {row[0]:<8} {row[1]:8.2f} {row[2]:8.2f} {row[3]:8.2f}")

# Significant events
print("\n[D] Top 5 Significant Events:")
cursor.execute("SELECT symbol, event_date, return_pct, event_type FROM significant_events ORDER BY abs(return_pct) DESC LIMIT 5")
print(f"  {'Symbol':<8} {'Date':<12} {'Return%':>10} {'Type':<12}")
print("  " + "-"*50)
for row in cursor.fetchall():
    print(f"  {row[0]:<8} {str(row[1]):<12} {row[2]:10.2f} {row[3]:<12}")

cursor.close()
conn.close()

print("\n" + "="*80)
print("ANALYTICS TABLES POPULATED!")
print("="*80)
print("\nYou can now query these tables anytime:")
print("  SELECT * FROM beta_results;")
print("  SELECT * FROM correlation_matrix;")
print("  SELECT * FROM volatility_metrics;")
print("  SELECT * FROM significant_events;")
print("\nAccess via web console: http://localhost:9000")
