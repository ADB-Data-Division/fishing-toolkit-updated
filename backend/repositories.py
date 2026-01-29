"""
Data Manager for Typhoon Nowcast POC
Handles TinyDB operations, data processing, and transformation
"""

import os
import uuid
from datetime import datetime
from typing import Any

import geopandas as gpd
import pandas as pd
from tinydb import Query, TinyDB


class TyphoonDataManager:
    """Manages typhoon data storage and retrieval using TinyDB."""

    def __init__(self, db_path: str = "database/nowcast.json"):
        """Initialize the data manager with TinyDB connection."""
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Initialize TinyDB
        self.db = TinyDB(db_path)
        self.typhoons_table = self.db.table("typhoons")
        # Fishing grounds are now loaded directly from GeoJSON file

    def process_csv_data(self, csv_path: str) -> list[dict[str, Any]]:
        """Process CSV data and return structured daily data."""
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
        """Process shapefile data and return track points."""
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
            track_points.sort(key=lambda x: x["datetime"])
            return track_points

        except Exception as e:
            print(f"Error processing shapefile data: {e}")
            return []

    def create_typhoon_record(self, name: str, csv_path: str, shapefile_path: str) -> str:
        """Create a new typhoon record from CSV and shapefile data."""
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

            # Save to TinyDB
            self.typhoons_table.insert(typhoon_record)
            print(f"Typhoon record created with UUID: {typhoon_uuid}")

            return typhoon_uuid

        except Exception as e:
            print(f"Error creating typhoon record: {e}")
            return None

    def get_typhoon_list(self) -> list[dict[str, Any]]:
        """Get list of all typhoons with basic info."""
        typhoons = self.typhoons_table.all()
        return [
            {
                "uuid": t["uuid"],
                "name": t["name"],
                "type": t["type"],
                "date_range": self._get_date_range(t["daily_data"]),
                "track_points_count": len(t["track_points"]),
            }
            for t in typhoons
        ]

    def get_typhoon_data(self, typhoon_uuid: str) -> dict[str, Any] | None:
        """Get complete typhoon data by UUID."""
        Typhoon = Query()
        typhoon = self.typhoons_table.get(Typhoon.uuid == typhoon_uuid)
        return typhoon

    def get_typhoon_dates(self, typhoon_uuid: str) -> list[str]:
        """Get available dates for a specific typhoon."""
        typhoon = self.get_typhoon_data(typhoon_uuid)
        if typhoon and "daily_data" in typhoon:
            return sorted(typhoon["daily_data"].keys())
        return []

    def get_fishing_grounds(self) -> list[dict[str, Any]]:
        """Get fishing grounds data."""
        return self.fishing_grounds_table.all()

    def _get_date_range(self, daily_data: dict[str, Any]) -> str:
        """Get date range string from daily data."""
        if not daily_data:
            return "No data"

        dates = sorted(daily_data.keys())
        if len(dates) == 1:
            return dates[0]
        return f"{dates[0]} to {dates[-1]}"

    def delete_typhoon(self, typhoon_uuid: str) -> bool:
        """Delete a typhoon record by UUID."""
        try:
            Typhoon = Query()
            self.typhoons_table.remove(Typhoon.uuid == typhoon_uuid)
            print(f"Typhoon with UUID {typhoon_uuid} deleted")
            return True
        except Exception as e:
            print(f"Error deleting typhoon: {e}")
            return False

    def clear_all_data(self):
        """Clear all typhoon data (for testing/reset)."""
        self.typhoons_table.truncate()
        print("All typhoon data cleared")

    def close(self):
        """Close the database connection."""
        self.db.close()
