"""Historical typhoon data repository."""

import json
import os
from typing import Any

from .base_repository import BaseRepository


class HistoricalRepository(BaseRepository):
    """Repository for historical typhoon data operations."""

    def __init__(self, db_path: str = "database/historical.json"):
        """Initialize the historical repository.

        Args:
            db_path: Path to the historical database file
        """
        super().__init__(db_path, table_name="typhoons")

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
                "date_range": t.get("dashboard_data", {}).get("dates", "N/A"),
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

    def get_typhoon_by_name(self, name: str) -> dict[str, Any] | None:
        """Get typhoon data by name.

        Args:
            name: Typhoon name

        Returns:
            Typhoon data or None if not found
        """
        return self.get_by_field("name", name)

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get all typhoons in dashboard format.

        Returns:
            Dictionary with typhoons and fishing_grounds
        """
        typhoons = self.get_all()
        dashboard_typhoons = {}

        for typhoon in typhoons:
            # Use the actual typhoon name as key
            key = typhoon["name"]
            dashboard_typhoons[key] = typhoon.get("dashboard_data", {})

        # Try to load fishing grounds from GeoJSON file
        fishing_grounds_geojson = None

        # Get the most recent year from typhoons
        latest_year = None
        for typhoon in typhoons:
            year = typhoon.get("dashboard_data", {}).get("year")
            if year and (latest_year is None or year > latest_year):
                latest_year = year

        if latest_year:
            # Build path to GeoJSON file
            geojson_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data",
                "outputs",
                "historical",
                "phl",
                str(latest_year),
                "intermediate",
                f"phl_merged_dense_area_polygons_{latest_year}.geojson",
            )

            if os.path.exists(geojson_path):
                try:
                    with open(geojson_path) as f:
                        fishing_grounds_geojson = json.load(f)
                except Exception as e:
                    print(f"Error loading fishing grounds GeoJSON: {e}")

        return {
            "typhoons": dashboard_typhoons,
            "fishing_grounds_geojson": fishing_grounds_geojson,
            "latest_year": latest_year,
        }

    def get_typhoons_by_year(self, year: int) -> list[dict[str, Any]]:
        """Get all typhoons for a specific year.

        Args:
            year: Year to filter by

        Returns:
            List of typhoon records for the year
        """
        all_typhoons = self.get_all()
        filtered = []

        for typhoon in all_typhoons:
            typhoon_year = typhoon.get("dashboard_data", {}).get("year")
            if typhoon_year == year:
                filtered.append(typhoon)

        return filtered

    def get_available_years(self) -> list[int]:
        """Get all available years from the database.

        Returns:
            Sorted list of unique years
        """
        typhoons = self.get_all()
        years = set()

        for typhoon in typhoons:
            year = typhoon.get("dashboard_data", {}).get("year")
            if year:
                years.add(year)

        return sorted(years)
