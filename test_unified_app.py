#!/usr/bin/env python3
"""
Test script for the unified Cyclone Impact Toolkit
Tests API initialization and basic functionality
"""

import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.api.historical_api import HistoricalApi
from backend.api.nowcast_api import NowcastApi
from backend.repositories.historical_repository import HistoricalRepository
from backend.repositories.nowcast_repository import NowcastRepository


def test_apis():
    """Test API initialization and basic functionality."""
    print("=== Testing Unified Cyclone Impact Toolkit APIs ===\n")

    # Test API initialization
    print("1. Testing API initialization...")
    try:
        nowcast_api = NowcastApi()
        historical_api = HistoricalApi()
        print("✓ APIs initialized successfully")
    except Exception as e:
        print(f"✗ API initialization failed: {e}")
        return False

    # Test repository initialization
    print("\n2. Testing repository initialization...")
    try:
        nowcast_repo = NowcastRepository()
        historical_repo = HistoricalRepository()
        print("✓ Repositories initialized successfully")
    except Exception as e:
        print(f"✗ Repository initialization failed: {e}")
        return False

    # Test nowcast functionality
    print("\n3. Testing nowcast functionality...")
    try:
        typhoon_list = nowcast_api.get_typhoon_list()
        print(f"✓ Nowcast typhoon list: {len(typhoon_list)} typhoons")

        fishing_grounds = nowcast_api.get_fishing_grounds()
        print(f"✓ Nowcast fishing grounds: {len(fishing_grounds)} grounds")

        dashboard_data = nowcast_api.get_dashboard_data()
        print(f"✓ Nowcast dashboard data keys: {list(dashboard_data.keys())}")

    except Exception as e:
        print(f"✗ Nowcast functionality test failed: {e}")

    # Test historical functionality
    print("\n4. Testing historical functionality...")
    try:
        typhoon_list = historical_api.get_typhoon_list()
        print(f"✓ Historical typhoon list: {len(typhoon_list)} typhoons")

        years = historical_api.get_available_years()
        print(f"✓ Historical available years: {years}")

        dashboard_data = historical_api.get_dashboard_data()
        print(f"✓ Historical dashboard data keys: {list(dashboard_data.keys())}")

    except Exception as e:
        print(f"✗ Historical functionality test failed: {e}")

    # Test cleanup
    print("\n5. Testing cleanup...")
    try:
        nowcast_api.close()
        historical_api.close()
        nowcast_repo.close()
        historical_repo.close()
        print("✓ Cleanup completed successfully")
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")

    print("\n=== Test completed ===")
    return True


if __name__ == "__main__":
    test_apis()
