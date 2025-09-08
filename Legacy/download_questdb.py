#!/usr/bin/env python3
"""
Quick QuestDB Download Helper and Startup Guide
"""
import requests
import os
from pathlib import Path

def download_questdb():
    """Download QuestDB binary for Windows"""
    print("🚀 QuestDB Download Helper")
    print("=" * 40)
    
    # QuestDB download URL (latest stable version)
    questdb_version = "8.1.4"
    download_url = f"https://github.com/questdb/questdb/releases/download/{questdb_version}/questdb-{questdb_version}-rt-windows-amd64.zip"
    
    print(f"📥 Download QuestDB binary from:")
    print(f"   {download_url}")
    print(f"")
    print(f"📁 Recommended: Save to Downloads folder")
    print(f"📂 Extract to: C:\\questdb or Desktop\\questdb")
    print(f"")
    print(f"🏃 After extraction:")
    print(f"   1. Navigate to extracted folder")
    print(f"   2. Double-click 'questdb.exe' OR")
    print(f"   3. Run in Command Prompt: questdb.exe")
    print(f"")
    print(f"🌐 QuestDB will start at: http://localhost:9000")
    print(f"")
    
    return download_url

def check_questdb_status():
    """Check if QuestDB is running"""
    try:
        response = requests.get("http://localhost:9000/exec", 
                              params={'query': 'SELECT 1;'}, 
                              timeout=3)
        response.raise_for_status()
        print("✅ QuestDB is running at http://localhost:9000")
        return True
    except requests.exceptions.RequestException:
        print("❌ QuestDB is not running")
        return False

def setup_instructions():
    """Show setup instructions"""
    print("\n🔧 SETUP STEPS:")
    print("1. ✅ Download QuestDB binary (not source code)")
    print("2. 📂 Extract to a folder")  
    print("3. 🏃 Run questdb.exe")
    print("4. 🌐 Open http://localhost:9000 in browser")
    print("5. 🚀 Run: python etf_fmp_processor.py")
    
    print("\n💡 ALTERNATIVE - Use Docker:")
    print("   docker run -p 9000:9000 -p 9009:9009 questdb/questdb")

def main():
    print("🔍 Checking QuestDB status...")
    
    if check_questdb_status():
        print("\n🎉 QuestDB is already running! You're ready to go!")
        print("🚀 Run: python etf_fmp_processor.py")
    else:
        print("\n⚠️ QuestDB is not running yet")
        download_url = download_questdb()
        setup_instructions()
        
        # Open download URL in browser
        try:
            import webbrowser
            print(f"\n🌐 Opening download page in browser...")
            webbrowser.open(download_url)
        except:
            pass

if __name__ == "__main__":
    main()
