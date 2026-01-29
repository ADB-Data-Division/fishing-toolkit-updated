"""Historical typhoon data repository."""

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

        # Add fishing grounds data
        fishing_grounds = [
            {"name": "Ground 0", "lat": 14.5, "lng": 120.5},
            {"name": "Ground 1", "lat": 13.5, "lng": 121.5},
            {"name": "Ground 2", "lat": 12.5, "lng": 122.5},
            {"name": "Ground 3", "lat": 11.5, "lng": 123.5},
            {"name": "Ground 4", "lat": 10.5, "lng": 124.5},
        ]

        return {"typhoons": dashboard_typhoons, "fishing_grounds": fishing_grounds}

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
