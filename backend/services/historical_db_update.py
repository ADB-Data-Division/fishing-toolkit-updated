"""
Database update service for historical analysis results.
Handles extraction of track data and updating the historical database.
"""

import os
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from tinydb import TinyDB

from backend.utils.logger import get_logger

logger = get_logger(__name__)


def extract_track_data_for_database(
    country: str, year: int, intermediate_path: str, tracks_output_path: str
) -> dict[str, str]:
    """
    Extract track data from processed lin11d CSV and save individual track files per typhoon.

    Args:
        country: Country code (e.g., 'phl')
        year: Year of analysis
        intermediate_path: Path to intermediate directory containing lin11d CSV
        tracks_output_path: Path to save track CSV files

    Returns:
        Dictionary mapping typhoon names to track file paths
    """
    track_file_mapping: dict[str, str] = {}

    # Read lin11d CSV file
    lin11d_file = os.path.join(intermediate_path, f"lin11d_{country}_{year}.csv")
    if not os.path.exists(lin11d_file):
        logger.warning(f"Track source file not found: {lin11d_file}")
        return track_file_mapping

    try:
        df = pd.read_csv(lin11d_file)

        # Ensure required columns exist
        required_columns = ["NAME", "ISO_TIME", "LAT", "LON"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns in track file: {missing_columns}")
            return track_file_mapping

        # Convert ISO_TIME to datetime
        df["ISO_TIME"] = pd.to_datetime(df["ISO_TIME"], errors="coerce")

        # Extract year, month, day, hour, minute for datetime formatting
        df["year"] = df["ISO_TIME"].dt.year
        df["month"] = df["ISO_TIME"].dt.month
        df["day"] = df["ISO_TIME"].dt.day
        df["hour"] = df["ISO_TIME"].dt.hour
        df["min"] = df["ISO_TIME"].dt.minute

        # Group by typhoon name
        typhoon_names = df["NAME"].unique()

        # Ensure tracks directory exists
        os.makedirs(tracks_output_path, exist_ok=True)

        for typhoon_name in typhoon_names:
            if pd.isna(typhoon_name) or typhoon_name == "UNNAMED" or typhoon_name == "NOT_NAMED":
                continue

            # Filter data for this typhoon
            typhoon_data = df[df["NAME"] == typhoon_name].copy()

            # Normalize typhoon name for filename (remove special chars, lowercase)
            normalized_name = typhoon_name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
            track_file = os.path.join(tracks_output_path, f"{normalized_name}_track.csv")

            # Prepare track data in format expected by load_track_data()
            track_df = pd.DataFrame(
                {
                    "NAME": typhoon_data["NAME"],
                    "LAT": typhoon_data["LAT"],
                    "LON": typhoon_data["LON"],
                    "year": typhoon_data["year"],
                    "month": typhoon_data["month"],
                    "day": typhoon_data["day"],
                    "hour": typhoon_data["hour"],
                    "min": typhoon_data["min"],
                    "USA_WIND": typhoon_data.get("USA_WIND", 0).fillna(0),
                    "STORM_SPD": typhoon_data.get("STORM_SPD", 0).fillna(0),
                }
            )

            # Sort by datetime
            track_df = track_df.sort_values(["year", "month", "day", "hour", "min"])

            # Save track file
            track_df.to_csv(track_file, index=False)
            track_file_mapping[typhoon_name] = track_file
            logger.info(f"Saved track file for {typhoon_name}: {track_file}")

        logger.info(f"Extracted {len(track_file_mapping)} track files")
        return track_file_mapping

    except Exception as e:
        logger.error(f"Error extracting track data: {e}", exc_info=True)
        return track_file_mapping


def update_historical_database_from_run(
    country: str,
    year: int,
    output_path: str,
    db_path: str = "database/historical.json",
    progress_callback=None,
) -> dict[str, Any]:
    """
    Update historical database with results from a processed run.

    Args:
        country: Country code (e.g., 'phl')
        year: Year of analysis
        output_path: Path to intermediate output directory
        db_path: Path to historical database file
        progress_callback: Optional callback function(phase, phase_name, message)

    Returns:
        Dictionary with update status and statistics
    """
    try:
        if progress_callback:
            progress_callback(5, "Generating visualizations and updating database...", "Preparing database update...")

        # Import ETL functions
        from scripts.etl_historical_data import (
            load_track_data,
            transform_csv_to_dashboard_format,
        )

        # Get analysis path (where boatdiff2 CSV should be)
        analysis_path = output_path.replace("intermediate", "analysis")
        os.makedirs(analysis_path, exist_ok=True)

        # Check if boatdiff2 file exists in intermediate (where it's currently saved)
        boatdiff2_file_intermediate = os.path.join(output_path, f"{country}_boatdiff2_{year}.csv")
        boatdiff2_file_analysis = os.path.join(analysis_path, f"{country}_boatdiff2_{year}.csv")

        # Use analysis path file if it exists, otherwise use intermediate
        if os.path.exists(boatdiff2_file_analysis):
            boatdiff2_file = boatdiff2_file_analysis
        elif os.path.exists(boatdiff2_file_intermediate):
            # Copy to analysis directory (keep original in intermediate)
            import shutil

            shutil.copy2(boatdiff2_file_intermediate, boatdiff2_file_analysis)
            boatdiff2_file = boatdiff2_file_analysis
            logger.info(f"Copied boatdiff2 file to analysis directory: {boatdiff2_file}")
        else:
            error_msg = f"Boatdiff2 CSV file not found in {boatdiff2_file_intermediate} or {boatdiff2_file_analysis}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

        if progress_callback:
            progress_callback(5, "Generating visualizations and updating database...", "Extracting track data...")

        # Extract track data
        # Tracks should be saved to: data/outputs/historical/{country}/{year}/tracks/
        base_output_path = output_path.replace("intermediate", "").rstrip("/")
        tracks_output_path = os.path.join(base_output_path, "tracks")
        track_file_mapping = extract_track_data_for_database(country, year, output_path, tracks_output_path)

        if progress_callback:
            progress_callback(5, "Generating visualizations and updating database...", "Reading CSV data...")

        # Read boatdiff2 CSV
        df = pd.read_csv(boatdiff2_file)

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

        logger.info(f"Baseline values extracted: {baseline_values}")

        if progress_callback:
            progress_callback(5, "Generating visualizations and updating database...", "Updating database...")

        # Initialize database
        # Ensure database directory exists (handles case where database doesn't exist yet)
        db_dir = os.path.dirname(db_path)
        if db_dir:  # Only create directory if path has a directory component
            os.makedirs(db_dir, exist_ok=True)

        # TinyDB automatically creates the file if it doesn't exist
        db = TinyDB(db_path)
        typhoons_table = db.table("typhoons")

        # Get all existing typhoons (returns empty list if database is new/empty)
        all_typhoons = typhoons_table.all()

        # Delete typhoons for this year (overwrite strategy)
        # If database is new, this will just skip (no typhoons to delete)
        deleted_count = 0
        for typhoon in all_typhoons:
            typhoon_year = typhoon.get("dashboard_data", {}).get("year")
            if typhoon_year == year:
                typhoons_table.remove(doc_ids=[typhoon.doc_id])
                deleted_count += 1

        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} existing typhoons for year {year}")
        else:
            logger.info(f"No existing typhoons found for year {year} (database may be new or empty)")

        # Process each typhoon from CSV
        inserted_count = 0
        for _index, row in df.iterrows():
            if row["Typhoon"] == "Ave Daily Boats":  # Skip the baseline row
                continue

            # Transform CSV data to dashboard format
            dashboard_data = transform_csv_to_dashboard_format(row, baseline_values)

            # Load track data if available
            typhoon_name = row["Typhoon"]
            track_points = []
            if typhoon_name in track_file_mapping:
                # Get the track file path from mapping
                track_file_path = track_file_mapping[typhoon_name]
                # Normalize name to match filename format (filename is normalized_name_track.csv)
                normalized_name = os.path.basename(track_file_path).replace("_track.csv", "")
                # Use the tracks_output_path as the directory for load_track_data
                track_points = load_track_data(normalized_name, tracks_output_path)
                # If not found with normalized name, try original name (lowercase)
                if not track_points:
                    track_points = load_track_data(typhoon_name.lower(), tracks_output_path)
            else:
                logger.warning(f"No track file found for {typhoon_name}")

            # Create typhoon record
            typhoon_record = {
                "uuid": str(uuid.uuid4()),
                "name": typhoon_name,
                "type": "TY",
                "track_points": track_points,
                "dashboard_data": dashboard_data,
                "created_at": datetime.now().isoformat(),
            }

            # Insert into database
            typhoons_table.insert(typhoon_record)
            inserted_count += 1
            logger.info(f"Inserted typhoon: {typhoon_name} (Year: {dashboard_data['year']})")

        db.close()

        result = {
            "status": "success",
            "message": "Database updated successfully",
            "deleted_count": deleted_count,
            "inserted_count": inserted_count,
            "year": year,
        }

        logger.info(f"Database update complete: {inserted_count} typhoons inserted, {deleted_count} deleted")
        return result

    except Exception as e:
        logger.error(f"Error updating historical database: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Failed to update database: {str(e)}",
        }
