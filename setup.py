#!/usr/bin/env python3
"""
Setup and Installation Script for ETF Data Project
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is 3.8+"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ required. Current version: {version.major}.{version.minor}")
        return False
    print(f"✅ Python version {version.major}.{version.minor} is compatible")
    return True

def create_directories():
    """Create necessary directories"""
    dirs = ['data', 'data/parquet', 'data/csv', 'logs']
    for dir_name in dirs:
        Path(dir_name).mkdir(parents=True, exist_ok=True)
        print(f"📁 Created directory: {dir_name}")

def setup_environment():
    """Setup Python environment and install dependencies"""
    print("🐍 Setting up Python environment...")
    
    # Upgrade pip
    if not run_command("python -m pip install --upgrade pip", "Upgrading pip"):
        return False
    
    # Install requirements
    if not run_command("pip install -r requirements.txt", "Installing Python packages"):
        return False
    
    return True

def setup_env_file():
    """Setup .env file from template"""
    if not os.path.exists('.env'):
        if os.path.exists('.env.example'):
            shutil.copy('.env.example', '.env')
            print("📝 Created .env file from template")
            print("⚠️  Please edit .env file with your FMP API key!")
        else:
            print("❌ .env.example file not found")
            return False
    else:
        print("✅ .env file already exists")
    return True

def check_questdb():
    """Check if QuestDB is available"""
    print("🔍 Checking QuestDB availability...")
    
    # Check if QuestDB directory exists
    if os.path.exists('questdb'):
        print("✅ QuestDB directory found")
        return True
    else:
        print("⚠️  QuestDB not found locally")
        print("📖 To install QuestDB:")
        print("   1. Run: powershell -ExecutionPolicy Bypass -File install_questdb.ps1")
        print("   2. Or download from: https://questdb.io/get-questdb/")
        print("   3. Or use Docker: docker run -p 9000:9000 -p 8812:8812 questdb/questdb")
        return False

def display_next_steps():
    """Display next steps for the user"""
    print("\n" + "="*60)
    print("🎉 ETF Data Project Setup Complete!")
    print("="*60)
    print("\n📋 NEXT STEPS:")
    print("1. 🔑 Edit .env file with your FMP API key:")
    print("   - Get free API key from: https://financialmodelingprep.com/developer/docs")
    print("   - Edit FMP_API_KEY in .env file")
    
    print("\n2. 🗄️  Start QuestDB:")
    print("   - Option A: Run ./questdb/start_questdb.bat (if installed locally)")
    print("   - Option B: Docker: docker run -p 9000:9000 -p 8812:8812 questdb/questdb")
    print("   - Option C: Cloud: Use QuestDB Cloud")
    
    print("\n3. 🚀 Run the pipeline:")
    print("   python main.py")
    
    print("\n4. 📊 Analyze results:")
    print("   python etf_analyzer.py")
    
    print("\n🌐 QuestDB URLs (when running):")
    print("   - Web Console: http://localhost:9000")
    print("   - PostgreSQL: localhost:8812")
    
    print("\n📁 Data will be saved to:")
    print("   - Parquet files: ./data/parquet/")
    print("   - CSV files: ./data/csv/")
    print("   - QuestDB table: ohlcv1d")

def main():
    """Main setup function"""
    print("ETF Data Project - Setup Script")
    print("="*40)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Create directories
    create_directories()
    
    # Setup environment
    if not setup_environment():
        print("❌ Failed to setup Python environment")
        sys.exit(1)
    
    # Setup .env file
    if not setup_env_file():
        print("❌ Failed to setup .env file")
        sys.exit(1)
    
    # Check QuestDB
    check_questdb()
    
    # Display next steps
    display_next_steps()

if __name__ == "__main__":
    main()
