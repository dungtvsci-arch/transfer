#!/usr/bin/env python3
"""
Test script to check if Flask server is working properly
"""

import requests
import json
import sys

def test_server():
    base_url = "http://127.0.0.1:5000"
    
    print("🧪 Testing ETL Server...")
    print("=" * 50)
    
    # Test 1: Basic connectivity
    try:
        response = requests.get(f"{base_url}/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health check: PASSED")
            health_data = response.json()
            print(f"   Status: {health_data.get('status')}")
            print(f"   Active processes: {health_data.get('active_processes')}")
        else:
            print(f"❌ Health check: FAILED (Status: {response.status_code})")
    except requests.exceptions.ConnectionError:
        print("❌ Health check: FAILED (Cannot connect to server)")
        print("   Make sure Flask server is running on port 5000")
        return False
    except Exception as e:
        print(f"❌ Health check: FAILED (Error: {e})")
        return False
    
    # Test 2: Debug information
    try:
        response = requests.get(f"{base_url}/debug", timeout=5)
        if response.status_code == 200:
            print("✅ Debug info: PASSED")
            debug_data = response.json()
            print(f"   Current directory: {debug_data.get('current_directory')}")
            files = debug_data.get('files_exist', {})
            for file, exists in files.items():
                status = "✅" if exists else "❌"
                print(f"   {status} {file}: {'Found' if exists else 'NOT FOUND'}")
        else:
            print(f"❌ Debug info: FAILED (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ Debug info: FAILED (Error: {e})")
    
    # Test 3: Main page
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("✅ Main page: PASSED")
            content = response.text
            if "ETL Dashboard" in content or "web_etl" in content:
                print("   ✅ HTML content looks correct")
            else:
                print("   ⚠️  HTML content might be incomplete")
        else:
            print(f"❌ Main page: FAILED (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ Main page: FAILED (Error: {e})")
    
    print("=" * 50)
    print("🎯 Test completed!")
    print()
    print("📋 Next steps:")
    print("   1. If all tests passed, open: http://127.0.0.1:5000")
    print("   2. If tests failed, check server logs")
    print("   3. Make sure all files are in the same directory")
    
    return True

if __name__ == "__main__":
    test_server()
