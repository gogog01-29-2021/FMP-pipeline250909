print("Starting simple ETF test...")

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Create data directory
data_dir = Path('./data')
csv_dir = Path('./data/csv')
data_dir.mkdir(exist_ok=True)
csv_dir.mkdir(exist_ok=True)

print(f"Data directory: {data_dir}")
print(f"CSV directory: {csv_dir}")

# Generate simple test data
dates = pd.date_range('2020-01-01', '2025-01-01', freq='D')
prices = 100 + np.random.randn(len(dates)).cumsum() * 0.5

df = pd.DataFrame({
    'date': dates,
    'symbol': 'SPY',
    'price': prices
})

# Save CSV file
csv_file = csv_dir / f'SPY_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
df.to_csv(csv_file, index=False)

print(f"Created test file: {csv_file}")
print(f"File exists: {csv_file.exists()}")
print(f"File size: {csv_file.stat().st_size if csv_file.exists() else 'N/A'}")
print("Test completed!")
