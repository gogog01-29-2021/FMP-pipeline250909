import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os

class ETFAnalyzer:
    """
    Analyze ETF data for volatility, performance, and expense ratios
    """
    
    def __init__(self, data_dir='./data'):
        self.data_dir = data_dir
        self.parquet_dir = os.path.join(data_dir, 'parquet')
        
    def load_combined_data(self):
        """
        Load the most recent combined ETF data
        """
        import glob
        parquet_files = glob.glob(os.path.join(self.parquet_dir, "all_etfs_5y_daily_*.parquet"))
        
        if not parquet_files:
            print("No combined ETF data found. Please run the main pipeline first.")
            return None
        
        # Get the most recent file
        latest_file = max(parquet_files, key=os.path.getctime)
        print(f"Loading data from: {latest_file}")
        
        df = pd.read_parquet(latest_file)
        df['date'] = pd.to_datetime(df['date'])
        return df
    
    def analyze_volatility_and_fees(self, df):
        """
        Analyze ETFs based on volatility and expense ratios
        """
        if df is None:
            return None
        
        # Calculate metrics by symbol
        etf_metrics = df.groupby(['symbol', 'index_tracked', 'expense_ratio']).agg({
            'daily_return': ['std', 'mean'],
            'volume': 'mean',
            'close': ['first', 'last']
        }).round(4)
        
        # Flatten column names
        etf_metrics.columns = ['volatility_daily', 'avg_daily_return', 'avg_volume', 'first_price', 'last_price']
        etf_metrics = etf_metrics.reset_index()
        
        # Calculate annualized metrics
        etf_metrics['volatility_annual'] = etf_metrics['volatility_daily'] * np.sqrt(252)
        etf_metrics['annual_return'] = etf_metrics['avg_daily_return'] * 252
        etf_metrics['total_return'] = ((etf_metrics['last_price'] / etf_metrics['first_price']) - 1) * 100
        
        # Sort by criteria: high volatility, low fees
        etf_metrics['score'] = etf_metrics['volatility_annual'] / (etf_metrics['expense_ratio'] + 0.01)  # Avoid division by zero
        etf_metrics = etf_metrics.sort_values('score', ascending=False)
        
        return etf_metrics
    
    def generate_report(self, df):
        """
        Generate comprehensive ETF analysis report
        """
        if df is None:
            return
        
        print("ETF Analysis Report")
        print("="*80)
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Data Period: {df['date'].min().date()} to {df['date'].max().date()}")
        print(f"Total Records: {len(df):,}")
        print(f"ETFs Analyzed: {df['symbol'].nunique()}")
        
        # Get metrics
        metrics = self.analyze_volatility_and_fees(df)
        
        print("\n📊 ETF RANKING (by Volatility/Expense Ratio Score)")
        print("="*80)
        print(f"{'Rank':<4} {'Symbol':<6} {'Index':<15} {'Vol%':<8} {'Fee%':<6} {'Score':<8} {'Tot Ret%':<10}")
        print("-"*80)
        
        for idx, row in metrics.head(10).iterrows():
            print(f"{idx+1:<4} {row['symbol']:<6} {row['index_tracked']:<15} "
                  f"{row['volatility_annual']*100:<8.2f} {row['expense_ratio']:<6.2f} "
                  f"{row['score']:<8.1f} {row['total_return']:<10.1f}")
        
        print("\n📈 BEST ETFs BY INDEX (Lowest Expense Ratio)")
        print("="*80)
        
        for index in df['index_tracked'].unique():
            index_data = metrics[metrics['index_tracked'] == index].sort_values('expense_ratio')
            if not index_data.empty:
                best = index_data.iloc[0]
                print(f"{index:<15}: {best['symbol']} (Fee: {best['expense_ratio']:.2f}%, "
                      f"Vol: {best['volatility_annual']*100:.1f}%, Return: {best['total_return']:.1f}%)")
        
        print("\n⚡ HIGHEST VOLATILITY ETFs (for trading opportunities)")
        print("="*80)
        high_vol = metrics.nlargest(5, 'volatility_annual')
        for _, row in high_vol.iterrows():
            print(f"{row['symbol']:<6}: {row['volatility_annual']*100:.1f}% annual volatility "
                  f"(Fee: {row['expense_ratio']:.2f}%, Return: {row['total_return']:.1f}%)")
        
        return metrics
    
    def create_visualization(self, df):
        """
        Create visualizations for ETF analysis
        """
        if df is None:
            return
        
        metrics = self.analyze_volatility_and_fees(df)
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('ETF Analysis Dashboard', fontsize=16)
        
        # 1. Volatility vs Expense Ratio scatter plot
        ax1 = axes[0, 0]
        scatter = ax1.scatter(metrics['expense_ratio'], metrics['volatility_annual']*100, 
                            c=metrics['total_return'], s=60, alpha=0.7, cmap='RdYlGn')
        ax1.set_xlabel('Expense Ratio (%)')
        ax1.set_ylabel('Annual Volatility (%)')
        ax1.set_title('Volatility vs Expense Ratio')
        plt.colorbar(scatter, ax=ax1, label='Total Return (%)')
        
        # Add labels for top ETFs
        for _, row in metrics.head(5).iterrows():
            ax1.annotate(row['symbol'], 
                        (row['expense_ratio'], row['volatility_annual']*100),
                        xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        # 2. Performance by Index
        ax2 = axes[0, 1]
        index_performance = metrics.groupby('index_tracked')['total_return'].mean().sort_values(ascending=False)
        bars = ax2.bar(range(len(index_performance)), index_performance.values)
        ax2.set_xticks(range(len(index_performance)))
        ax2.set_xticklabels(index_performance.index, rotation=45, ha='right')
        ax2.set_ylabel('Average Total Return (%)')
        ax2.set_title('Performance by Index')
        
        # Color bars based on performance
        for i, bar in enumerate(bars):
            if index_performance.values[i] > 0:
                bar.set_color('green')
            else:
                bar.set_color('red')
        
        # 3. Volatility distribution by Index
        ax3 = axes[1, 0]
        index_vol_data = []
        index_labels = []
        for index in metrics['index_tracked'].unique():
            index_data = metrics[metrics['index_tracked'] == index]['volatility_annual'] * 100
            if len(index_data) > 0:
                index_vol_data.append(index_data.values)
                index_labels.append(index)
        
        ax3.boxplot(index_vol_data, labels=index_labels)
        ax3.set_ylabel('Annual Volatility (%)')
        ax3.set_title('Volatility Distribution by Index')
        ax3.tick_params(axis='x', rotation=45)
        
        # 4. Top ETFs by Score
        ax4 = axes[1, 1]
        top_etfs = metrics.head(8)
        bars = ax4.barh(range(len(top_etfs)), top_etfs['score'])
        ax4.set_yticks(range(len(top_etfs)))
        ax4.set_yticklabels(top_etfs['symbol'])
        ax4.set_xlabel('Volatility/Fee Score')
        ax4.set_title('Top ETFs by Score')
        
        plt.tight_layout()
        
        # Save plot
        plot_path = os.path.join(self.data_dir, 'etf_analysis_dashboard.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"Dashboard saved to: {plot_path}")
        plt.show()

def main():
    """
    Run ETF analysis
    """
    analyzer = ETFAnalyzer()
    df = analyzer.load_combined_data()
    
    if df is not None:
        metrics = analyzer.generate_report(df)
        
        # Create visualization
        try:
            analyzer.create_visualization(df)
        except ImportError:
            print("matplotlib not available for visualization")
        except Exception as e:
            print(f"Error creating visualization: {e}")

if __name__ == "__main__":
    main()
