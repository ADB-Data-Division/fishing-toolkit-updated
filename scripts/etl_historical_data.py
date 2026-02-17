"""
ETL Function for Historical Typhoon Data
Transforms CSV data into TinyDB database aligned with nowcast structure
"""

import os
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from tinydb import TinyDB


def transform_csv_to_dashboard_format(
    csv_row: pd.Series, baseline_values: dict[str, float] | None = None
) -> dict[str, Any]:
    """Transform CSV row to dashboard format."""
    name = csv_row["Typhoon"]
    avg_speed = csv_row["Ave. Stm Speed (knot)"]
    date_range = format_date_range(csv_row["Date Range"])

    # Extract fishing ground data - handle both G0-G4 and G0-G5 formats
    grounds_data = {}
    distances = []

    # Check how many grounds we have (G0-G4 or G0-G5)
    max_grounds = 5  # Default to 5
    for i in range(6):  # Check up to G5
        if f"G{i} Distance (km)" not in csv_row:
            max_grounds = i
            break

    for i in range(max_grounds):  # G0-G4 or G0-G5
        distance = csv_row[f"G{i} Distance (km)"]
        boat_diff = csv_row[f"G{i} (Boat Diff%)"]
        distances.append(float(distance))

        # Use actual baseline from CSV if available, otherwise estimate
        ground_key = f"ground{i}"
        if baseline_values and ground_key in baseline_values:
            baseline = baseline_values[ground_key]
        else:
            baseline = estimate_baseline(boat_diff, distance)

        grounds_data[f"ground{i}"] = {"baseline": baseline, "difference": float(boat_diff), "distance": float(distance)}

    # Find minimum distance and closest ground
    min_distance = min(distances)
    closest_ground_idx = distances.index(min_distance)
    closest_ground = f"Ground {closest_ground_idx}"

    # Estimate missing values
    max_speed = estimate_max_speed(avg_speed)
    max_wind = estimate_max_wind(avg_speed)
    average_boats = calculate_average_boats(grounds_data)

    # Extract year from date range
    year = extract_year_from_date_range(date_range)

    return {
        "name": name,
        "year": year,
        "dates": date_range,
        "avgSpeed": f"{float(avg_speed):.1f}",
        "maxSpeed": f"{float(max_speed):.1f}",
        "maxWind": f"{float(max_wind):.1f}",
        "closestGround": closest_ground,
        "minDistance": f"{float(min_distance):.1f}",
        "boatData": grounds_data,
        "averageBoats": round(average_boats, 1),
    }


def extract_year_from_date_range(date_range: str) -> int:
    """Extract year from date range string."""
    import re

    try:
        # Handle "2024-07-19 to 2024-07-23" format
        if " to " in date_range:
            start_date_str = date_range.split(" to ")[0]
            return int(start_date_str.split("-")[0])
        # Handle "2024-July-19 to 2024-July-23" format
        elif "-" in date_range:
            return int(date_range.split("-")[0])
    except (ValueError, IndexError):
        pass

    # Fallback: try to extract year from any 4-digit number
    year_match = re.search(r"\b(20\d{2})\b", date_range)
    if year_match:
        return int(year_match.group(1))

    # Default fallback
    return 2024


def format_date_range(date_range: str) -> str:
    """Format date range to match dashboard format."""
    # Convert "2024-07-19 to 2024-07-23" to "2024-July-19 to 2024-July-23"
    try:
        parts = date_range.split(" to ")
        if len(parts) == 2:
            start_date = datetime.strptime(parts[0], "%Y-%m-%d")
            end_date = datetime.strptime(parts[1], "%Y-%m-%d")

            start_formatted = start_date.strftime("%Y-%B-%d")
            end_formatted = end_date.strftime("%Y-%B-%d")

            return f"{start_formatted} to {end_formatted}"
    except (ValueError, IndexError):
        pass

    return date_range


def estimate_baseline(boat_diff_percent: float, distance: float) -> int:
    """Estimate baseline boat count from difference percentage."""
    if boat_diff_percent == -100:
        return 0  # No boats when 100% decrease

    # Simple estimation - baseline boats when no change
    base_count = 50  # Default baseline
    if distance < 1000:
        base_count = 60  # More boats when closer
    elif distance > 1500:
        base_count = 30  # Fewer boats when farther

    # Adjust based on difference percentage
    adjusted_count = int(base_count * (1 + boat_diff_percent / 100))
    return max(0, adjusted_count)


def estimate_max_speed(avg_speed: float) -> float:
    """Estimate maximum storm speed from average."""
    return float(avg_speed) * 1.5


def estimate_max_wind(avg_speed: float) -> float:
    """Estimate maximum wind speed from average storm speed."""
    return float(avg_speed) * 5.0


def calculate_average_boats(boat_data: dict[str, Any]) -> float:
    """Calculate average boats across all grounds during typhoon."""
    total_boats = 0
    count = 0

    for ground in boat_data.values():
        baseline = ground["baseline"]
        difference_percent = ground["difference"]

        # Calculate actual boat count during typhoon
        if difference_percent == -100:
            actual_boats = 0
        else:
            actual_boats = baseline * (1 + difference_percent / 100)

        total_boats += actual_boats
        count += 1

    return round(total_boats / count, 1) if count > 0 else 0


def load_track_data(typhoon_name: str, track_files_dir: str) -> list[dict[str, Any]]:
    """Load track data for a typhoon from CSV file."""
    track_points = []

    # Look for track file with typhoon name
    track_file = os.path.join(track_files_dir, f"{typhoon_name.lower()}_track.csv")
    if not os.path.exists(track_file):
        # Try alternative naming patterns
        track_file = os.path.join(track_files_dir, "sample_track_2024.csv")

    if os.path.exists(track_file):
        try:
            df = pd.read_csv(track_file)

            # Filter for this typhoon if multiple typhoons in file
            if "NAME" in df.columns:
                df = df[df["NAME"].str.upper() == typhoon_name.upper()]

            for _, row in df.iterrows():
                # Extract coordinates from geometry or lat/lon columns
                lat = None
                lng = None

                if "LAT" in row and "LON" in row:
                    lat = float(row["LAT"])
                    lng = float(row["LON"])
                elif "geometry" in row:
                    # Parse POINT (lng lat) format
                    geom = row["geometry"]
                    if geom.startswith("POINT ("):
                        coords = geom[7:-1].split()
                        lng = float(coords[0])
                        lat = float(coords[1])

                if lat is not None and lng is not None:
                    # Format datetime
                    dt = datetime(row["year"], row["month"], row["day"], row["hour"], row["min"])
                    datetime_str = dt.strftime("%Y-%m-%d %H:%M")

                    # Handle NaN values
                    wind_speed = row.get("USA_WIND", 0)
                    cyclone_speed = row.get("STORM_SPD", 0)

                    # Convert NaN to 0
                    if pd.isna(wind_speed):
                        wind_speed = 0
                    if pd.isna(cyclone_speed):
                        cyclone_speed = 0

                    track_point = {
                        "lat": lat,
                        "lng": lng,
                        "datetime": datetime_str,
                        "windSpeed": int(wind_speed),
                        "cycloneSpeed": int(cyclone_speed),
                    }
                    track_points.append(track_point)

            # Sort by datetime
            track_points.sort(key=lambda x: x["datetime"])

        except Exception as e:
            print(f"Error loading track data for {typhoon_name}: {e}")

    return track_points


def create_historical_database(csv_paths: list[str], track_files_dir: str, db_path: str = "database/historical.json"):
    """Create historical database aligned with nowcast structure from multiple CSV files."""

    # Ensure database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Initialize TinyDB
    db = TinyDB(db_path)
    typhoons_table = db.table("typhoons")

    # Clear existing data
    typhoons_table.truncate()

    # Process each CSV file
    total_typhoon_count = 0

    for csv_path in csv_paths:
        if not os.path.exists(csv_path):
            print(f"Warning: CSV file not found at {csv_path}")
            continue

        print(f"Processing {csv_path}...")

        # Read CSV data
        df = pd.read_csv(csv_path)

        # Extract baseline values from "Ave Daily Boats" row
        baseline_values = {}
        baseline_row = df[df["Typhoon"] == "Ave Daily Boats"]
        if not baseline_row.empty:
            for i in range(6):  # Check up to G5
                baseline_col = f"G{i} (Boat Diff%)"
                if baseline_col in baseline_row.columns:
                    baseline_value = baseline_row.iloc[0][baseline_col]
                    if pd.notna(baseline_value):
                        baseline_values[f"ground{i}"] = float(baseline_value)

        print(f"Baseline values for {csv_path}: {baseline_values}")

        # Process each typhoon
        for _index, row in df.iterrows():
            if row["Typhoon"] == "Ave Daily Boats":  # Skip the baseline row
                continue

            total_typhoon_count += 1
            typhoon_id = total_typhoon_count

            # Transform CSV data to dashboard format
            dashboard_data = transform_csv_to_dashboard_format(row, baseline_values)

            # Load track data if available
            track_points = load_track_data(row["Typhoon"], track_files_dir)

            # Create typhoon record
            typhoon_record = {
                "uuid": str(uuid.uuid4()),
                "name": row["Typhoon"],
                "type": "TY",
                "track_points": track_points,
                "dashboard_data": dashboard_data,
                "created_at": datetime.now().isoformat(),
            }

            # Insert with numeric key
            typhoons_table.insert(typhoon_record)
            print(f"Created typhoon record {typhoon_id}: {row['Typhoon']} (Year: {dashboard_data['year']})")

    db.close()
    print(f"Historical database created with {total_typhoon_count} typhoons from {len(csv_paths)} files")


if __name__ == "__main__":
    # Define paths for all CSV files
    csv_paths = [
        "frontend/static/historical/data/phl_boatdiff2_2023.csv",
        "frontend/static/historical/data/phl_boatdiff2_2024.csv",
        "frontend/static/historical/data/phl_boatdiff2_2025.csv",
    ]
    track_files_dir = "frontend/static/historical/data/"
    db_path = "database/historical.json"

    create_historical_database(csv_paths, track_files_dir, db_path)
    print("Historical database created successfully!")
