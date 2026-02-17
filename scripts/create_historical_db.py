#!/usr/bin/env python3
"""
Script to create the historical typhoon database from CSV data
"""

import os
import sys

# Add parent directory to path to import etl module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.etl_historical_data import create_historical_database

if __name__ == "__main__":
    # Define paths for all CSV files
    csv_paths = [
        "frontend/static/historical/data/phl_boatdiff2_2023.csv",
        "frontend/static/historical/data/phl_boatdiff2_2024.csv",
        "frontend/static/historical/data/phl_boatdiff2_2025.csv",
    ]
    track_files_dir = "frontend/static/historical/data/"
    db_path = "database/historical.json"

    # Check if CSV files exist
    missing_files = [path for path in csv_paths if not os.path.exists(path)]
    if missing_files:
        print(f"Error: CSV files not found: {missing_files}")
        sys.exit(1)

    print("Creating historical typhoon database...")
    print(f"CSV sources: {csv_paths}")
    print(f"Track files directory: {track_files_dir}")
    print(f"Database output: {db_path}")
    print("-" * 50)

    try:
        create_historical_database(csv_paths, track_files_dir, db_path)
        print("\nHistorical database created successfully!")
        print(f"Database location: {os.path.abspath(db_path)}")
    except Exception as e:
        print(f"\nError creating database: {e}")
        sys.exit(1)
