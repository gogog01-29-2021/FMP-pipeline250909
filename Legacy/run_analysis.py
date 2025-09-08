#!/usr/bin/env python3
"""
ETF Data Analysis Runner
Comprehensive script to run the entire ETF data analysis pipeline
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

def check_requirements():
    """Check if all required packages are installed"""
    print("🔍 Checking Python packages...")
    
    required_packages = [
        'pandas', 'requests', 'pyarrow', 'questdb', 
        'python-dotenv', 'numpy', 'matplotlib', 'seaborn'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n🚨 Missing packages: {', '.join(missing_packages)}")
        print("🔧 Installing missing packages...")
        
        try:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install'] + missing_packages
            )
            print("✅ Packages installed successfully!")
        except subprocess.CalledProcessError:
            print("❌ Failed to install packages. Please install manually.")
            return False
    
    return True

def check_env_file():
    """Check if .env file exists and has required variables"""
    env_file = Path('.env')
    
    if not env_file.exists():
        print("❌ .env file not found!")
        print("🔧 Creating .env file from template...")
        
        # Copy from .env.example
        example_file = Path('.env.example')
        if example_file.exists():
            with open(example_file, 'r') as src:
                content = src.read()
            
            with open(env_file, 'w') as dst:
                dst.write(content)
            
            print("✅ .env file created from template")
            print("🚨 IMPORTANT: Please edit .env file and add your FMP_API_KEY")
            return False
        else:
            print("❌ No .env.example file found!")
            return False
    
    # Load and check environment variables
    load_dotenv()
    
    fmp_key = os.getenv('FMP_API_KEY')
    if not fmp_key or fmp_key == 'your_fmp_api_key_here':
        print("❌ FMP_API_KEY not set in .env file!")
        print("🔧 Please get your API key from https://financialmodelingprep.com/")
        print("📝 Then update the FMP_API_KEY in your .env file")
        return False
    
    print("✅ Environment configuration looks good!")
    return True

def check_questdb():
    """Check if QuestDB is running"""
    import requests
    
    questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
    questdb_port = os.getenv('QUESTDB_PORT', '9000')
    
    try:
        response = requests.get(f"http://{questdb_host}:{questdb_port}/exec", 
                              params={'query': 'SELECT 1;'}, timeout=5)
        response.raise_for_status()
        print(f"✅ QuestDB is running at {questdb_host}:{questdb_port}")
        return True
        
    except requests.exceptions.RequestException:
        print(f"❌ QuestDB not running at {questdb_host}:{questdb_port}")
        print("🔧 Please start QuestDB:")
        print("   1. Download from: https://questdb.io/get-questdb/")
        print("   2. Extract and run: questdb.exe")
        print("   3. Access at: http://localhost:9000")
        return False

def setup_directories():
    """Create necessary directories"""
    directories = ['data', 'data/parquet', 'data/csv']
    
    for dir_path in directories:
        Path(dir_path).mkdir(exist_ok=True)
    
    print("✅ Data directories created")

def run_questdb_setup():
    """Run QuestDB table setup"""
    print("\n🔧 Setting up QuestDB tables...")
    
    try:
        import setup_questdb
        setup_questdb.main()
        return True
    except Exception as e:
        print(f"❌ QuestDB setup failed: {e}")
        return False

def run_etf_processor():
    """Run the main ETF data processor"""
    print("\n🚀 Starting ETF data processing...")
    
    try:
        import etf_fmp_processor
        etf_fmp_processor.main()
        return True
    except Exception as e:
        print(f"❌ ETF processing failed: {e}")
        return False

def show_results():
    """Show summary of results"""
    print(f"\n📊 PROCESSING COMPLETE!")
    print(f"{'='*60}")
    
    # Check data files
    data_dir = Path('data')
    parquet_dir = data_dir / 'parquet'
    csv_dir = data_dir / 'csv'
    
    if parquet_dir.exists():
        parquet_files = list(parquet_dir.glob('*.parquet'))
        print(f"📁 Parquet files: {len(parquet_files)}")
        for file in parquet_files:
            print(f"   - {file.name}")
    
    if csv_dir.exists():
        csv_files = list(csv_dir.glob('*.csv'))
        print(f"📁 CSV files: {len(csv_files)}")
        for file in csv_files:
            print(f"   - {file.name}")
    
    # Check analysis file
    analysis_file = data_dir / 'etf_analysis_results.csv'
    if analysis_file.exists():
        print(f"📊 Analysis results: {analysis_file}")
    
    print(f"\n🌐 QuestDB Web Console: http://localhost:9000")
    print(f"🗄️  Table name: ohlcv1d")
    print(f"📈 Query example: SELECT * FROM ohlcv1d WHERE symbol = 'SPY' LIMIT 10;")

def main():
    """Main execution pipeline"""
    print("🎯 ETF Data Analysis Pipeline")
    print("=" * 50)
    
    # Step 1: Check Python packages
    if not check_requirements():
        print("❌ Requirements check failed")
        return
    
    # Step 2: Check environment configuration
    if not check_env_file():
        print("❌ Environment check failed")
        return
    
    # Step 3: Setup directories
    setup_directories()
    
    # Step 4: Check QuestDB
    if not check_questdb():
        print("❌ QuestDB check failed")
        return
    
    # Step 5: Setup QuestDB tables
    if not run_questdb_setup():
        print("❌ QuestDB setup failed")
        return
    
    # Step 6: Run ETF data processor
    if not run_etf_processor():
        print("❌ ETF processing failed")
        return
    
    # Step 7: Show results
    show_results()
    
    print(f"\n🎉 All done! ETF data analysis completed successfully!")

if __name__ == "__main__":
    main()
