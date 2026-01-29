"""
PyWebView Bridge API for Typhoon Dashboard
Handles communication between Python backend and JavaScript frontend
"""

import json
import os
from datetime import datetime
from typing import Any

from data_manager import TyphoonDataManager


class TyphoonApi:
    """API class for webview typhoon dashboard communication."""

    def __init__(self, window=None):
        self.window = window
        self.data_manager = TyphoonDataManager()

    def get_typhoon_list(self) -> list[dict[str, Any]]:
        """Get list of all available typhoons."""
        try:
            typhoons = self.data_manager.get_typhoon_list()
            print(f"Retrieved {len(typhoons)} typhoons")
            return typhoons
        except Exception as e:
            print(f"Error getting typhoon list: {e}")
            return []

    def get_typhoon_data(self, typhoon_uuid: str) -> dict[str, Any] | None:
        """Get complete data for a specific typhoon."""
        try:
            typhoon = self.data_manager.get_typhoon_data(typhoon_uuid)
            if typhoon:
                print(f"Retrieved data for typhoon: {typhoon['name']}")
                return typhoon
            else:
                print(f"No typhoon found with UUID: {typhoon_uuid}")
                return None
        except Exception as e:
            print(f"Error getting typhoon data: {e}")
            return None

    def get_typhoon_dates(self, typhoon_uuid: str) -> list[str]:
        """Get available dates for a specific typhoon."""
        try:
            dates = self.data_manager.get_typhoon_dates(typhoon_uuid)
            print(f"Retrieved {len(dates)} dates for typhoon {typhoon_uuid}")
            return dates
        except Exception as e:
            print(f"Error getting typhoon dates: {e}")
            return []

    def get_fishing_grounds(self) -> list[dict[str, Any]]:
        """Get fishing grounds data from GeoJSON file."""
        try:
            # Load fishing grounds from GeoJSON file
            # Using restructured folder: data/inputs/gis/countries/{country}/fishing_grounds/
            geojson_path = os.path.join(
                "data", "inputs", "gis", "countries", "phl", "fishing_grounds", "fishing_grounds_nowcast.geojson"
            )

            if not os.path.exists(geojson_path):
                print(f"Fishing grounds GeoJSON file not found: {geojson_path}")
                return []

            with open(geojson_path) as f:
                geojson_data = json.load(f)

            # Transform GeoJSON features to the expected format
            fishing_grounds = []
            for feature in geojson_data.get("features", []):
                # Calculate centroid of the polygon for display
                coords = feature["geometry"]["coordinates"][0]  # First ring of polygon
                if coords:
                    # Calculate centroid (simple average)
                    lng_sum = sum(coord[0] for coord in coords)
                    lat_sum = sum(coord[1] for coord in coords)
                    centroid_lng = lng_sum / len(coords)
                    centroid_lat = lat_sum / len(coords)

                    ground = {
                        "id": feature["properties"]["contour_id"],
                        "name": f"Ground {feature['properties']['contour_id']}",
                        "lat": centroid_lat,
                        "lng": centroid_lng,
                        "description": f"Fishing ground {feature['properties']['contour_id']}",
                        "geometry": feature["geometry"],  # Keep full geometry for map display
                    }
                    fishing_grounds.append(ground)

            print(f"Retrieved {len(fishing_grounds)} fishing grounds from GeoJSON")
            return fishing_grounds

        except Exception as e:
            print(f"Error getting fishing grounds from GeoJSON: {e}")
            return []

    def create_typhoon_from_files(self, name: str, csv_path: str, shapefile_path: str) -> str | None:
        """Create a new typhoon record from CSV and shapefile."""
        try:
            typhoon_uuid = self.data_manager.create_typhoon_record(name, csv_path, shapefile_path)
            if typhoon_uuid:
                print(f"Created new typhoon: {name} with UUID: {typhoon_uuid}")
                # Notify the frontend that new data is available
                self._notify_data_update()
                return typhoon_uuid
            else:
                print(f"Failed to create typhoon: {name}")
                return None
        except Exception as e:
            print(f"Error creating typhoon: {e}")
            return None

    def delete_typhoon(self, typhoon_uuid: str) -> bool:
        """Delete a typhoon record."""
        try:
            success = self.data_manager.delete_typhoon(typhoon_uuid)
            if success:
                print(f"Deleted typhoon with UUID: {typhoon_uuid}")
                self._notify_data_update()
            return success
        except Exception as e:
            print(f"Error deleting typhoon: {e}")
            return False

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get all data needed for dashboard initialization."""
        try:
            # Get basic typhoon list for selection
            typhoons = self.get_typhoon_list()

            # Get fishing grounds
            fishing_grounds = self.get_fishing_grounds()

            # If there are typhoons, get the first one's data as default
            default_typhoon_data = None
            default_dates = []

            if typhoons:
                first_typhoon = typhoons[0]
                default_typhoon_data = self.get_typhoon_data(first_typhoon["uuid"])
                default_dates = self.get_typhoon_dates(first_typhoon["uuid"])

            dashboard_data = {
                "typhoons": typhoons,
                "fishing_grounds": fishing_grounds,
                "default_typhoon": default_typhoon_data,
                "default_dates": default_dates,
            }

            print("Dashboard data prepared successfully")
            return dashboard_data

        except Exception as e:
            print(f"Error preparing dashboard data: {e}")
            return {"typhoons": [], "fishing_grounds": [], "default_typhoon": None, "default_dates": []}

    def _notify_data_update(self):
        """Notify the frontend that data has been updated."""
        try:
            # Send a simple notification to the frontend
            self.window.evaluate_js(
                """
                if (window.dashboard && window.dashboard.onDataUpdate) {
                    window.dashboard.onDataUpdate();
                }
            """
            )
        except Exception as e:
            print(f"Error notifying frontend: {e}")

    def console_log(self, level: str, message: str):
        """Bridge console logs from frontend to Python."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] JS-{level.upper()}: {message}")

    def close(self):
        """Clean up resources."""
        if self.data_manager:
            self.data_manager.close()
