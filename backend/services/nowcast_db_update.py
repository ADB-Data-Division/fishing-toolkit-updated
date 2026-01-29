"""
Database update functions for nowcast analysis.

This module handles extracting results from nowcast processing and updating the nowcast database.
"""

import hashlib
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd
from tinydb import TinyDB

from backend.utils.logger import get_logger

logger = get_logger(__name__)

# Wind speed thresholds for cyclone type classification (in knots)
CYCLONE_TYPE_THRESHOLDS = {
    "TD": (0, 33),  # Tropical Depression
    "TS": (34, 63),  # Tropical Storm
    "STS": (64, 82),  # Severe Tropical Storm
    "TY": (83, 119),  # Typhoon
    "STY": (120, 999),  # Super Typhoon
}


def generate_cyclone_uuid(name: str, year: int) -> str:
    """Generate deterministic UUID based on cyclone name and year.

    Args:
        name: Cyclone name (will be normalized)
        year: Year of the cyclone

    Returns:
        UUID string based on hash of name+year
    """
    # Normalize name (uppercase, strip whitespace)
    normalized_name = name.strip().upper()

    # Create deterministic hash from name+year
    hash_input = f"{normalized_name}_{year}".encode()
    hash_digest = hashlib.sha256(hash_input).hexdigest()

    # Convert hash to UUID format (use first 32 hex chars)
    uuid_str = f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

    return uuid_str


def classify_cyclone_type(max_wind_speed: float) -> str:
    """Classify cyclone type based on maximum wind speed.

    Args:
        max_wind_speed: Maximum wind speed in knots

    Returns:
        Cyclone type code (TD, TS, STS, TY, STY)
    """
    for cyclone_type, (min_speed, max_speed) in CYCLONE_TYPE_THRESHOLDS.items():
        if min_speed <= max_wind_speed <= max_speed:
            return cyclone_type

    # Default to TD if no match
    return "TD"


def calculate_activity_difference(baseline: float, predicted: float) -> str:
    """Calculate activity difference as percentage string.

    Args:
        baseline: Baseline boat count
        predicted: Predicted boat count

    Returns:
        Formatted percentage string (e.g., "+15.2%", "-10.5%")
    """
    if baseline == 0:
        if predicted == 0:
            return "+0.0%"
        else:
            return "+∞%"

    percentage = ((predicted - baseline) / baseline) * 100
    sign = "+" if percentage >= 0 else ""
    return f"{sign}{percentage:.1f}%"


def extract_track_points_from_gdf(filtered_gdf, cyclone_name: str) -> list[dict[str, Any]]:
    """Extract track points for a specific cyclone from GeoDataFrame.

    Args:
        filtered_gdf: GeoDataFrame containing cyclone tracks
        cyclone_name: Name of the cyclone to extract

    Returns:
        List of track point dictionaries
    """
    # Filter for this cyclone
    cyclone_data = filtered_gdf[filtered_gdf["NAME"] == cyclone_name].copy()

    if cyclone_data.empty:
        logger.warning(f"No track data found for cyclone: {cyclone_name}")
        return []

    # Sort by datetime
    cyclone_data = cyclone_data.sort_values("datetime")

    track_points = []
    for _, row in cyclone_data.iterrows():
        # Extract coordinates from geometry
        if hasattr(row.geometry, "y") and hasattr(row.geometry, "x"):
            lat = float(row.geometry.y)
            lng = float(row.geometry.x)
        elif "LAT" in row and "LON" in row:
            lat = float(row["LAT"])
            lng = float(row["LON"])
        else:
            logger.warning("Could not extract coordinates for track point")
            continue

        # Extract datetime
        dt = row["datetime"]
        if pd.isna(dt):
            dt = pd.to_datetime(row.get("ISO_TIME", ""), errors="coerce")

        if pd.isna(dt):
            logger.warning("Invalid datetime for track point")
            continue

        datetime_str = dt.strftime("%Y-%m-%d %H:%M")

        # Extract wind speed and cyclone speed
        wind_speed = row.get("USA_WIND", 0)
        if pd.isna(wind_speed):
            wind_speed = 0
        wind_speed = int(round(float(wind_speed)))

        cyclone_speed = row.get("STORM_SPD", 0)
        if pd.isna(cyclone_speed):
            cyclone_speed = 0
        cyclone_speed = int(round(float(cyclone_speed)))

        track_points.append(
            {
                "lat": lat,
                "lng": lng,
                "datetime": datetime_str,
                "windSpeed": wind_speed,
                "cycloneSpeed": cyclone_speed,
            }
        )

    return track_points


def build_daily_data_from_csv(final_csv_df: pd.DataFrame, cyclone_name: str) -> dict[str, dict[str, Any]]:
    """Build daily data structure for a cyclone from the output CSV.

    Args:
        final_csv_df: DataFrame from {country}_logdatadf_py_new_{year}.csv
        cyclone_name: Name of the cyclone

    Returns:
        Dictionary mapping dates to daily statistics
    """
    # Filter for this cyclone
    cyclone_data = final_csv_df[final_csv_df["NAME"] == cyclone_name].copy()

    if cyclone_data.empty:
        logger.warning(f"No data found in CSV for cyclone: {cyclone_name}")
        return {}

    daily_data = {}

    for _, row in cyclone_data.iterrows():
        # Extract date
        date = row["date_only"]
        if isinstance(date, pd.Timestamp):
            date_str = date.strftime("%Y-%m-%d")
        else:
            date_str = str(date)

        # Extract storm speeds
        avg_storm_speed = row.get("stm_spd_mean", 0)
        max_storm_speed = row.get("stm_spd_max", 0)
        max_wind_speed = row.get("USA_WIND", 0)

        # Build distances array (distance_0, distance_1, ...)
        distances = []
        fishing_ground_idx = 0
        while f"distance_{fishing_ground_idx}" in row:
            dist = row[f"distance_{fishing_ground_idx}"]
            if pd.notna(dist):
                distances.append(round(float(dist), 1))
            fishing_ground_idx += 1

        # Build baseline array (base_0, base_1, ...)
        baseline = []
        fishing_ground_idx = 0
        while f"base_{fishing_ground_idx}" in row:
            base = row[f"base_{fishing_ground_idx}"]
            if pd.notna(base):
                baseline.append(int(round(float(base))))
            fishing_ground_idx += 1

        # Build predicted array (predict_g0, predict_g1, ...)
        predicted = []
        fishing_ground_idx = 0
        while f"predict_g{fishing_ground_idx}" in row:
            pred = row[f"predict_g{fishing_ground_idx}"]
            if pd.notna(pred):
                predicted.append(int(round(float(pred))))
            fishing_ground_idx += 1

        # Calculate activity differences
        activity_diff = []
        for base, pred in zip(baseline, predicted, strict=False):
            diff = calculate_activity_difference(base, pred)
            activity_diff.append(diff)

        # Build daily data entry
        daily_data[date_str] = {
            "date": date_str,
            "avgStormSpeed": f"{avg_storm_speed:.1f} knots",
            "maxStormSpeed": f"{int(round(max_storm_speed))} knots",
            "maxWindSpeed": f"{int(round(max_wind_speed))} knots",
            "distances": distances,
            "boatCounts": {
                "baseline": baseline,
                "predicted": predicted,
            },
            "activityDifference": activity_diff,
        }

    return daily_data


def update_nowcast_database_from_run(
    country: str,
    year: int,
    output_path: str,
    results: dict[str, Any],
    db_path: str = "database/nowcast.json",
    progress_callback: Callable[[int, str, str], None] | None = None,
) -> dict[str, Any]:
    """Update nowcast database with results from a processing run.

    Args:
        country: Country code (e.g., "phl")
        year: Year of analysis
        output_path: Path to output directory
        results: Results dictionary from main() containing:
            - filtered_gdf_1: GeoDataFrame with cyclone tracks
            - daily_stats: DataFrame with storm speed statistics
            - final_result: Final output CSV DataFrame
        db_path: Path to nowcast database JSON file
        progress_callback: Optional callback for progress updates

    Returns:
        Dictionary with update status and counts
    """
    try:
        logger.info(f"Starting database update for {country} {year}")

        if progress_callback:
            progress_callback(5, "Creating visualizations and updating database...", "Loading output files...")

        # Extract data from results
        filtered_gdf_1 = results.get("filtered_gdf_1")
        final_result_df = results.get("final_result")

        if filtered_gdf_1 is None or final_result_df is None:
            raise ValueError("Missing required data in results: filtered_gdf_1 or final_result")

        # Also try to load from CSV file as backup
        csv_path = os.path.join(output_path, f"{country}_logdatadf_py_new_{year}.csv")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Output CSV not found: {csv_path}")

        # Load CSV if not already in results
        if final_result_df is None:
            final_result_df = pd.read_csv(csv_path)
            final_result_df["date_only"] = pd.to_datetime(final_result_df["date_only"])

        logger.info(f"Loaded output CSV with {len(final_result_df)} rows")

        if progress_callback:
            progress_callback(5, "Creating visualizations and updating database...", "Extracting cyclone data...")

        # Get unique cyclone names
        cyclone_names = final_result_df["NAME"].unique()
        logger.info(f"Found {len(cyclone_names)} cyclones: {', '.join(cyclone_names)}")

        if progress_callback:
            progress_callback(5, "Creating visualizations and updating database...", "Initializing database...")

        # Initialize TinyDB (same as historical)
        # Ensure database directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # TinyDB automatically creates the file if it doesn't exist
        db = TinyDB(db_path)
        typhoons_table = db.table("typhoons")

        # Get all existing typhoons for deduplication
        all_typhoons = typhoons_table.all()

        added_count = 0
        updated_count = 0

        # Process each cyclone
        for cyclone_name in cyclone_names:
            if progress_callback:
                progress_callback(
                    5, "Creating visualizations and updating database...", f"Processing cyclone: {cyclone_name}..."
                )

            logger.info(f"Processing cyclone: {cyclone_name}")

            # Generate UUID
            cyclone_uuid = generate_cyclone_uuid(cyclone_name, year)
            logger.debug(f"Generated UUID for {cyclone_name}: {cyclone_uuid}")

            # Extract track points
            track_points = extract_track_points_from_gdf(filtered_gdf_1, cyclone_name)
            if not track_points:
                logger.warning(f"No track points found for {cyclone_name}, skipping")
                continue

            # Determine cyclone type from max wind speed
            max_wind = max(point["windSpeed"] for point in track_points)
            cyclone_type = classify_cyclone_type(max_wind)
            logger.debug(f"Cyclone type for {cyclone_name}: {cyclone_type} (max wind: {max_wind} knots)")

            # Build daily data
            daily_data = build_daily_data_from_csv(final_result_df, cyclone_name)
            if not daily_data:
                logger.warning(f"No daily data found for {cyclone_name}, skipping")
                continue

            # Build database entry
            entry = {
                "uuid": cyclone_uuid,
                "name": cyclone_name,
                "type": cyclone_type,
                "track_points": track_points,
                "daily_data": daily_data,
                "created_at": datetime.now().isoformat(),
            }

            # Check if UUID already exists (deduplication)
            existing_doc = None
            for typhoon in all_typhoons:
                if typhoon.get("uuid") == cyclone_uuid:
                    existing_doc = typhoon
                    break

            if existing_doc:
                # Update existing entry using TinyDB doc_id
                typhoons_table.update(entry, doc_ids=[existing_doc.doc_id])
                updated_count += 1
                logger.info(f"Updated existing entry for {cyclone_name} (UUID: {cyclone_uuid})")
            else:
                # Insert new entry
                typhoons_table.insert(entry)
                added_count += 1
                logger.info(f"Added new entry for {cyclone_name} (UUID: {cyclone_uuid})")

        if progress_callback:
            progress_callback(5, "Creating visualizations and updating database...", "Saving database...")

        # Close database connection
        db.close()

        logger.info(f"Database updated successfully: {added_count} added, {updated_count} updated")

        return {
            "status": "success",
            "message": f"Database updated: {added_count} cyclones added, {updated_count} updated",
            "added_count": added_count,
            "updated_count": updated_count,
        }

    except Exception as e:
        logger.error(f"Error updating database: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Database update failed: {str(e)}",
            "added_count": 0,
            "updated_count": 0,
        }
