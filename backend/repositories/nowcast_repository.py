"""Nowcast typhoon data repository."""

import uuid
from datetime import datetime
from typing import Any

import geopandas as gpd
import pandas as pd

from .base_repository import BaseRepository


class NowcastRepository(BaseRepository):
    """Repository for nowcast typhoon data operations."""

    def __init__(self, db_path: str = "database/nowcast.json"):
        """Initialize the nowcast repository.

        Args:
            db_path: Path to the nowcast database file
        """
        super().__init__(db_path, table_name="typhoons")

    def get_all(self) -> list[dict[str, Any]]:
        """Get all typhoon records.

        Returns:
            List of all typhoon dictionaries
        """
        # TinyDB returns Document objects which behave like dicts
        return [dict(doc) for doc in self.table.all()]

    def create_typhoon_record(self, name: str, csv_path: str, shapefile_path: str) -> str | None:
        """Create a new typhoon record from CSV and shapefile data.

        Args:
            name: Typhoon name
            csv_path: Path to CSV file with daily data
            shapefile_path: Path to shapefile with track points

        Returns:
            UUID of created typhoon or None if failed
        """
        try:
            # Generate UUID for the typhoon
            typhoon_uuid = str(uuid.uuid4())

            # Process data
            daily_data = self.process_csv_data(csv_path)
            track_points = self.process_shapefile_data(shapefile_path)

            if not daily_data or not track_points:
                raise ValueError("Failed to process CSV or shapefile data")

            # Create typhoon record
            typhoon_record = {
                "uuid": typhoon_uuid,
                "name": name,
                "type": "TY",  # Default type
                "track_points": track_points,
                "daily_data": {day["date"]: day for day in daily_data},
                "created_at": datetime.now().isoformat(),
            }

            # Save to database using TinyDB
            self.insert(typhoon_record)
            print(f"Typhoon record created with UUID: {typhoon_uuid}")

            return typhoon_uuid

        except Exception as e:
            print(f"Error creating typhoon record: {e}")
            return None

    def get_typhoon_list(self) -> list[dict[str, Any]]:
        """Get list of all typhoons with basic info.

        Returns:
            List of typhoon summary dictionaries
        """
        typhoons = self.get_all()
        return [
            {
                "uuid": t["uuid"],
                "name": t["name"],
                "type": t["type"],
                "date_range": self._get_date_range(t.get("daily_data", {})),
                "track_points_count": len(t.get("track_points", [])),
            }
            for t in typhoons
        ]

    def get_typhoon_by_uuid(self, typhoon_uuid: str) -> dict[str, Any] | None:
        """Get complete typhoon data by UUID.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            Typhoon data or None if not found
        """
        return self.get_by_field("uuid", typhoon_uuid)

    def get_typhoon_dates(self, typhoon_uuid: str) -> list[str]:
        """Get available dates for a specific typhoon.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            Sorted list of date strings
        """
        typhoon = self.get_typhoon_by_uuid(typhoon_uuid)
        if typhoon and "daily_data" in typhoon:
            return sorted(typhoon["daily_data"].keys())
        return []

    def delete_typhoon(self, typhoon_uuid: str) -> bool:
        """Delete a typhoon record by UUID.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            True if successful, False otherwise
        """
        try:
            self.delete_by_field("uuid", typhoon_uuid)
            print(f"Typhoon with UUID {typhoon_uuid} deleted")
            return True
        except Exception as e:
            print(f"Error deleting typhoon: {e}")
            return False

    def process_csv_data(self, csv_path: str) -> list[dict[str, Any]]:
        """Process CSV data and return structured daily data.

        Args:
            csv_path: Path to CSV file

        Returns:
            List of daily data dictionaries
        """
        try:
            df = pd.read_csv(csv_path)
            daily_data = []

            for _, row in df.iterrows():
                # Calculate activity differences
                baseline = [row["base_0"], row["base_1"], row["base_2"], row["base_3"]]
                predicted = [row["predict_g0"], row["predict_g1"], row["predict_g2"], row["predict_g3"]]

                activity_diff = []
                for base, pred in zip(baseline, predicted, strict=False):
                    if base == 0:
                        diff = "+0%" if pred == 0 else "+∞%"
                    else:
                        percentage = ((pred - base) / base) * 100
                        diff = f"{'+' if percentage >= 0 else ''}{percentage:.2f}%"
                    activity_diff.append(diff)

                daily_record = {
                    "date": row["date_only"],
                    "avgStormSpeed": f"{row['stm_spd_mean']:.1f} knots",
                    "maxStormSpeed": f"{row['stm_spd_max']} knots",
                    "maxWindSpeed": f"{row['USA_WIND']} knots",
                    "distances": [row["distance_0"], row["distance_1"], row["distance_2"], row["distance_3"]],
                    "boatCounts": {"baseline": baseline, "predicted": predicted},
                    "activityDifference": activity_diff,
                }
                daily_data.append(daily_record)

            return daily_data

        except Exception as e:
            print(f"Error processing CSV data: {e}")
            return []

    def process_shapefile_data(self, shapefile_path: str) -> list[dict[str, Any]]:
        """Process shapefile data and return track points.

        Args:
            shapefile_path: Path to shapefile

        Returns:
            List of track point dictionaries
        """
        try:
            gdf = gpd.read_file(shapefile_path)
            track_points = []

            for _, row in gdf.iterrows():
                # Extract geometry coordinates
                point = row.geometry
                lat, lng = point.y, point.x

                # Format datetime
                dt = datetime(row["year"], row["month"], row["day"], row["hour"], row["minute"])
                datetime_str = dt.strftime("%Y-%m-%d %H:%M")

                track_point = {
                    "lat": float(lat),
                    "lng": float(lng),
                    "datetime": datetime_str,
                    "windSpeed": int(row["USA_WIND"]),
                    "cycloneSpeed": int(row["STORM_SPD"]),
                }
                track_points.append(track_point)

            # Sort by datetime
            track_points.sort(key=lambda x: str(x["datetime"]))
            return track_points

        except Exception as e:
            print(f"Error processing shapefile data: {e}")
            return []

    def _get_date_range(self, daily_data: dict[str, Any]) -> str:
        """Get date range string from daily data.

        Args:
            daily_data: Dictionary of daily data keyed by date

        Returns:
            Date range string
        """
        if not daily_data:
            return "No data"

        dates = sorted(daily_data.keys())
        if len(dates) == 1:
            return dates[0]
        return f"{dates[0]} to {dates[-1]}"
