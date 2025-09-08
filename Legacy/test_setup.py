#!/usr/bin/env python3
"""
Test script to verify ETF data processing setup
"""

import sys
import os
from pathlib import Path

def test_imports():
    """Test if all required packages can be imported"""
    print("🧪 Testing package imports...")
    
    required_packages = {
        'pandas': 'pandas',
        'requests': 'requests', 
        'pyarrow': 'pyarrow',
        'questdb': 'questdb',
        'numpy': 'numpy',
        'dotenv': 'python-dotenv'
    }
    
    failed_imports = []
    
    for module, package in required_packages.items():
        try:
            __import__(module)
            print(f"✅ {package}")
        except ImportError as e:
            print(f"❌ {package}: {e}")
            failed_imports.append(package)
    
    return len(failed_imports) == 0

def test_env_config():
    """Test environment configuration"""
    print("\n🧪 Testing environment configuration...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        # Check .env file exists
        if not Path('.env').exists():
            print("❌ .env file not found")
            return False
        
        print("✅ .env file exists")
        
        # Check FMP API key
        fmp_key = os.getenv('FMP_API_KEY')
        if not fmp_key or fmp_key == 'your_fmp_api_key_here':
            print("❌ FMP_API_KEY not configured")
            return False
        
        print("✅ FMP_API_KEY configured")
        
        # Check directories
        data_dir = Path(os.getenv('DATA_DIR', './data'))
        if data_dir.exists():
            print(f"✅ Data directory exists: {data_dir}")
        else:
            print(f"⚠️ Data directory will be created: {data_dir}")
        
        return True
        
    except ImportError:
        print("❌ python-dotenv not available")
        return False

def test_questdb_connection():
    """Test QuestDB connection"""
    print("\n🧪 Testing QuestDB connection...")
    
    try:
        import requests
        
        questdb_host = os.getenv('QUESTDB_HOST', 'localhost')
        questdb_port = os.getenv('QUESTDB_PORT', '9000')
        
        response = requests.get(
            f"http://{questdb_host}:{questdb_port}/exec",
            params={'query': 'SELECT 1;'},
            timeout=5
        )
        response.raise_for_status()
        
        print(f"✅ QuestDB is accessible at {questdb_host}:{questdb_port}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ QuestDB not accessible: {e}")
        print("💡 Make sure QuestDB is running:")
        print("   Download from: https://questdb.io/get-questdb/")
        return False

def test_fmp_api():
    """Test FMP API access"""
    print("\n🧪 Testing FMP API access...")
    
    try:
        import requests
        from dotenv import load_dotenv
        
        load_dotenv()
        fmp_key = os.getenv('FMP_API_KEY')
        
        if not fmp_key or fmp_key == 'your_fmp_api_key_here':
            print("❌ FMP API key not configured")
            return False
        
        # Test API with a simple request
        url = "https://financialmodelingprep.com/api/v3/profile/SPY"
        params = {'apikey': fmp_key}
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data and len(data) > 0:
            print(f"✅ FMP API working - Got data for SPY")
            return True
        else:
            print("❌ FMP API returned empty data")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ FMP API request failed: {e}")
        return False
    except Exception as e:
        print(f"❌ FMP API test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🔬 ETF Data Processing - System Test")
    print("=" * 50)
    
    tests = [
        ("Package Imports", test_imports),
        ("Environment Config", test_env_config), 
        ("QuestDB Connection", test_questdb_connection),
        ("FMP API Access", test_fmp_api)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        result = test_func()
        results.append((test_name, result))
    
    # Summary
    print(f"\n📊 TEST SUMMARY")
    print("=" * 30)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nResults: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! Ready to run ETF analysis.")
        print("Run: python etf_fmp_processor.py")
    else:
        print("\n⚠️ Some tests failed. Please fix issues before running analysis.")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
