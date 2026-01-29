#!/usr/bin/env python3
"""Fix nowcast database format for TinyDB."""

import json


def fix_database():
    """Convert nowcast database to proper TinyDB format."""
    db_path = "database/nowcast.json"

    # Read current database
    with open(db_path) as f:
        data = json.load(f)

    # Extract typhoon records
    if "typhoons" in data and isinstance(data["typhoons"], dict):
        typhoons = list(data["typhoons"].values())

        # Write new format (TinyDB expects the table data directly)
        with open(db_path, "w") as f:
            json.dump({"typhoons": typhoons}, f, indent=2)

        print(f"Converted database with {len(typhoons)} typhoons")
    else:
        print("Database already in correct format or invalid structure")


if __name__ == "__main__":
    fix_database()
