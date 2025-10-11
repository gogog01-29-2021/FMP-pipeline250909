#!/usr/bin/env python3
"""
Comprehensive Financial Analytics
- Beta Calculation
- Regression Analysis
- Correlation Analysis
- Pair Trading Opportunities
- Event Studies
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("="*80)
print("COMPREHENSIVE FINANCIAL ANALYTICS")
print("="*80)

# ==================== LOAD DATA ====================

print("\n[1/6] Loading data from CSV files...")

data_dir = Path('data/csv/etf')
csv_files = list(data_dir.glob('*_1day_*.csv'))

print(f"Found {len(csv_files)} daily data files")

# Load all daily data
all_data = {}
for csv_file in csv_files:
    symbol = csv_file.stem.split('_')[0]
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['returns'] = df['close'].pct_change()
    all_data[symbol] = df
    print(f"  Loaded {symbol}: {len(df)} records")

if not all_data:
    print("ERROR: No data files found!")
    exit(1)

# ==================== BETA CALCULATION ====================

print("\n[2/6] Calculating Beta (vs SPY)...")
print("-"*80)

if 'SPY' not in all_data:
    print("WARNING: SPY not found, cannot calculate beta")
    betas = {}
else:
    spy_returns = all_data['SPY'][['date', 'returns']].rename(columns={'returns': 'spy_returns'})

    betas = {}
    for symbol, df in all_data.items():
        if symbol == 'SPY':
            betas[symbol] = 1.0
            continue

        # Merge with SPY
        merged = df[['date', 'returns']].merge(spy_returns, on='date', how='inner')
        merged = merged.dropna()

        if len(merged) < 20:
            continue

        # Calculate beta: Cov(stock, market) / Var(market)
        covariance = merged['returns'].cov(merged['spy_returns'])
        variance = merged['spy_returns'].var()
        beta = covariance / variance if variance != 0 else 0

        # Calculate R-squared
        correlation = merged['returns'].corr(merged['spy_returns'])
        r_squared = correlation ** 2

        betas[symbol] = {
            'beta': beta,
            'r_squared': r_squared,
            'correlation': correlation,
            'observations': len(merged)
        }

        print(f"{symbol:6s}: Beta = {beta:6.3f}, R² = {r_squared:6.3f}, Corr = {correlation:6.3f}")

# ==================== REGRESSION ANALYSIS ====================

print("\n[3/6] Running Regression Analysis...")
print("-"*80)

regression_results = {}

if 'SPY' in all_data:
    spy_returns = all_data['SPY'][['date', 'returns']].rename(columns={'returns': 'market_return'})

    for symbol, df in all_data.items():
        if symbol == 'SPY':
            continue

        # Prepare data
        merged = df[['date', 'returns', 'close']].merge(spy_returns, on='date', how='inner')
        merged = merged.dropna()

        if len(merged) < 30:
            continue

        # Simple Linear Regression: stock_return = alpha + beta * market_return
        X = merged['market_return'].values
        y = merged['returns'].values

        # Calculate using numpy
        n = len(X)
        x_mean = np.mean(X)
        y_mean = np.mean(y)

        # Beta (slope)
        numerator = np.sum((X - x_mean) * (y - y_mean))
        denominator = np.sum((X - x_mean) ** 2)
        beta = numerator / denominator if denominator != 0 else 0

        # Alpha (intercept)
        alpha = y_mean - beta * x_mean

        # R-squared
        y_pred = alpha + beta * X
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y_mean) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        # Standard Error
        residuals = y - y_pred
        rmse = np.sqrt(np.mean(residuals ** 2))

        regression_results[symbol] = {
            'alpha': alpha,
            'beta': beta,
            'r_squared': r_squared,
            'rmse': rmse,
            'observations': n
        }

        # Annualize alpha (assume 252 trading days)
        annual_alpha = alpha * 252 * 100  # Convert to percentage

        print(f"{symbol:6s}: α = {annual_alpha:7.2f}% (annual), β = {beta:6.3f}, R² = {r_squared:6.3f}")

# ==================== CORRELATION ANALYSIS ====================

print("\n[4/6] Correlation Analysis...")
print("-"*80)

# Create returns matrix
symbols = list(all_data.keys())
returns_matrix = pd.DataFrame()

for symbol in symbols:
    df = all_data[symbol]
    returns_matrix[symbol] = df.set_index('date')['returns']

# Calculate correlation matrix
corr_matrix = returns_matrix.corr()

print("\nCorrelation Matrix:")
print(corr_matrix.round(3))

# Find high correlations
print("\nHighly Correlated Pairs (correlation > 0.8):")
high_corr_pairs = []
for i in range(len(symbols)):
    for j in range(i+1, len(symbols)):
        corr = corr_matrix.iloc[i, j]
        if abs(corr) > 0.8:
            high_corr_pairs.append((symbols[i], symbols[j], corr))
            print(f"  {symbols[i]:6s} - {symbols[j]:6s}: {corr:6.3f}")

# ==================== PAIR TRADING OPPORTUNITIES ====================

print("\n[5/6] Pair Trading Analysis...")
print("-"*80)

pair_trading_opportunities = []

for symbol1 in symbols:
    for symbol2 in symbols:
        if symbol1 >= symbol2:
            continue

        # Get aligned data
        df1 = all_data[symbol1][['date', 'close']].rename(columns={'close': 'close1'})
        df2 = all_data[symbol2][['date', 'close']].rename(columns={'close': 'close2'})

        merged = df1.merge(df2, on='date', how='inner')

        if len(merged) < 30:
            continue

        # Calculate correlation
        correlation = merged['close1'].corr(merged['close2'])

        # Calculate spread
        merged['ratio'] = merged['close1'] / merged['close2']
        spread_mean = merged['ratio'].mean()
        spread_std = merged['ratio'].std()
        current_ratio = merged['ratio'].iloc[-1]

        # Z-score of current spread
        z_score = (current_ratio - spread_mean) / spread_std if spread_std != 0 else 0

        # Cointegration test (simple version - check if spread is mean reverting)
        # Calculate half-life of mean reversion
        spread_diff = merged['ratio'].diff().dropna()
        spread_lag = merged['ratio'].shift(1).dropna()

        if len(spread_diff) > 10:
            # AR(1) model
            merged_temp = pd.DataFrame({
                'diff': spread_diff,
                'lag': spread_lag[1:]
            }).dropna()

            if len(merged_temp) > 5:
                # Simple regression
                x = merged_temp['lag'].values
                y = merged_temp['diff'].values

                # Slope of AR(1)
                lambda_param = np.corrcoef(x, y)[0, 1] * (np.std(y) / np.std(x)) if np.std(x) != 0 else 0

                # Half-life
                if lambda_param < 0:
                    half_life = -np.log(2) / lambda_param
                else:
                    half_life = float('inf')
            else:
                half_life = float('inf')
        else:
            half_life = float('inf')

        if correlation > 0.7 and abs(z_score) > 1.5 and half_life < 30:
            pair_trading_opportunities.append({
                'pair': f"{symbol1}-{symbol2}",
                'correlation': correlation,
                'z_score': z_score,
                'half_life': half_life,
                'signal': 'Long' if z_score < -1.5 else 'Short'
            })

if pair_trading_opportunities:
    print("\nPair Trading Opportunities (|z-score| > 1.5):")
    for opp in sorted(pair_trading_opportunities, key=lambda x: abs(x['z_score']), reverse=True):
        print(f"  {opp['pair']:12s}: Z-score = {opp['z_score']:6.2f}, Corr = {opp['correlation']:5.3f}, "
              f"Half-life = {opp['half_life']:5.1f} days, Signal = {opp['signal']}")
else:
    print("\nNo pair trading opportunities found with current criteria")

# ==================== EVENT STUDY ====================

print("\n[6/6] Event Study Analysis...")
print("-"*80)

# Example: Find significant price movements (events)
print("\nSignificant Daily Moves (|return| > 3%):")

significant_events = []

for symbol, df in all_data.items():
    df_copy = df.copy()
    df_copy['abs_return'] = df_copy['returns'].abs()

    # Find days with >3% moves
    significant = df_copy[df_copy['abs_return'] > 0.03].copy()

    if len(significant) > 0:
        print(f"\n{symbol}:")
        for _, row in significant.iterrows():
            ret_pct = row['returns'] * 100
            direction = "UP" if row['returns'] > 0 else "DOWN"
            print(f"  {row['date'].strftime('%Y-%m-%d')}: {ret_pct:+6.2f}% {direction}")

            significant_events.append({
                'symbol': symbol,
                'date': row['date'],
                'return': row['returns'],
                'close': row['close'],
                'direction': direction
            })

# Calculate average returns around events
if significant_events:
    print("\nEvent Study: Average returns before/after significant moves")
    print("(analyzing ±3 days around events)")

    event_analysis = []

    for event in significant_events[:5]:  # Analyze first 5 events
        symbol = event['symbol']
        event_date = event['date']

        df = all_data[symbol]
        event_idx = df[df['date'] == event_date].index[0]

        # Get returns before and after (if available)
        before_returns = []
        after_returns = []

        for i in range(1, 4):  # 3 days
            if event_idx - i >= 0:
                before_returns.append(df.iloc[event_idx - i]['returns'])
            if event_idx + i < len(df):
                after_returns.append(df.iloc[event_idx + i]['returns'])

        if before_returns and after_returns:
            event_analysis.append({
                'symbol': symbol,
                'date': event_date,
                'event_return': event['return'],
                'avg_before': np.mean(before_returns),
                'avg_after': np.mean(after_returns)
            })

    if event_analysis:
        print("\nResults:")
        for analysis in event_analysis:
            print(f"  {analysis['symbol']} on {analysis['date'].strftime('%Y-%m-%d')}:")
            print(f"    Event: {analysis['event_return']*100:+6.2f}%")
            print(f"    Avg 3 days before: {analysis['avg_before']*100:+6.2f}%")
            print(f"    Avg 3 days after:  {analysis['avg_after']*100:+6.2f}%")

# ==================== SUMMARY REPORT ====================

print("\n" + "="*80)
print("ANALYTICS SUMMARY")
print("="*80)

print(f"\nSymbols Analyzed: {len(all_data)}")
print(f"Date Range: {min(df['date'].min() for df in all_data.values()).strftime('%Y-%m-%d')} to "
      f"{max(df['date'].max() for df in all_data.values()).strftime('%Y-%m-%d')}")

if betas:
    print(f"\nBeta Analysis: {len(betas)} symbols")
    avg_beta = np.mean([v['beta'] if isinstance(v, dict) else v for v in betas.values()])
    print(f"  Average Beta: {avg_beta:.3f}")

if regression_results:
    print(f"\nRegression Results: {len(regression_results)} symbols")
    positive_alpha = sum(1 for r in regression_results.values() if r['alpha'] > 0)
    print(f"  Symbols with positive alpha: {positive_alpha}/{len(regression_results)}")

print(f"\nCorrelation Analysis: {len(symbols)} × {len(symbols)} matrix")
print(f"  High correlation pairs found: {len(high_corr_pairs)}")

print(f"\nPair Trading Opportunities: {len(pair_trading_opportunities)}")

print(f"\nSignificant Events Identified: {len(significant_events)}")

# Save results
print("\n" + "="*80)
print("Saving results to CSV files...")

# Save beta results
if betas:
    beta_df = pd.DataFrame([
        {
            'symbol': symbol,
            'beta': v['beta'] if isinstance(v, dict) else v,
            'r_squared': v.get('r_squared', np.nan) if isinstance(v, dict) else np.nan,
            'correlation': v.get('correlation', np.nan) if isinstance(v, dict) else np.nan
        }
        for symbol, v in betas.items()
    ])
    beta_df.to_csv('analytics_beta_results.csv', index=False)
    print("  Saved: analytics_beta_results.csv")

# Save correlation matrix
corr_matrix.to_csv('analytics_correlation_matrix.csv')
print("  Saved: analytics_correlation_matrix.csv")

# Save regression results
if regression_results:
    reg_df = pd.DataFrame(regression_results).T
    reg_df.to_csv('analytics_regression_results.csv')
    print("  Saved: analytics_regression_results.csv")

# Save pair trading opportunities
if pair_trading_opportunities:
    pair_df = pd.DataFrame(pair_trading_opportunities)
    pair_df.to_csv('analytics_pair_trading.csv', index=False)
    print("  Saved: analytics_pair_trading.csv")

# Save significant events
if significant_events:
    events_df = pd.DataFrame(significant_events)
    events_df.to_csv('analytics_significant_events.csv', index=False)
    print("  Saved: analytics_significant_events.csv")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print("\nOutput files created in current directory:")
print("  - analytics_beta_results.csv")
print("  - analytics_correlation_matrix.csv")
print("  - analytics_regression_results.csv")
print("  - analytics_pair_trading.csv")
print("  - analytics_significant_events.csv")
