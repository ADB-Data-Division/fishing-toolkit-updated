"""
Cyclone Impact Toolkit - Main Application Entry Point
Provides unified interface for Historical and Nowcast analysis modes
"""

import atexit
import json
import os
import threading
import time
from datetime import datetime

import geopandas as gpd
import webview
from dotenv import load_dotenv
from shapely.geometry import Point

from backend.api.historical_api import HistoricalApi
from backend.api.nowcast_api import NowcastApi
from backend.utils.logger import get_logger
from backend.utils.utils import get_config_path

logger = get_logger(__name__)

# Load environment variables from config file at application startup
config_path = get_config_path()
load_dotenv(config_path, override=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t", "yes")
logger.info(f"DEBUG: {DEBUG}")

atexit.register(lambda: logger.debug("Python atexit handler called"))


class CleanAPI:
    """
    Clean API wrapper for pywebview that has NO webview references during introspection.
    This prevents pywebview from trying to access DOM properties before the window is initialized.
    Always update this when adding new API methods.
    """

    def __init__(self):
        """Initialize clean API - NO window references at this point."""
        # Create the real API but WITHOUT window reference
        # Use double underscore to hide from introspection
        self.__api = UnifiedApi(window=None)

    # Explicitly delegate all public API methods to make them visible to pywebview
    def test_api(self):
        return self.__api.test_api()

    def select_mode(self, mode: str):
        return self.__api.select_mode(mode)

    def back_to_welcome(self):
        return self.__api.back_to_welcome()

    def load_historical_dashboard(self):
        return self.__api.load_historical_dashboard()

    def load_nowcast_dashboard(self):
        return self.__api.load_nowcast_dashboard()

    def get_typhoon_list(self):
        return self.__api.get_typhoon_list()

    def get_typhoon_data(self, typhoon_uuid: str):
        return self.__api.get_typhoon_data(typhoon_uuid)

    def get_typhoon_dates(self, typhoon_uuid: str):
        return self.__api.get_typhoon_dates(typhoon_uuid)

    def get_dashboard_data(self):
        return self.__api.get_dashboard_data()

    def get_fishing_grounds(self):
        return self.__api.get_fishing_grounds()

    def create_typhoon_from_files(self, name: str, csv_path: str, shapefile_path: str):
        return self.__api.create_typhoon_from_files(name, csv_path, shapefile_path)

    def delete_typhoon(self, typhoon_uuid: str):
        return self.__api.delete_typhoon(typhoon_uuid)

    def get_available_years(self):
        return self.__api.get_available_years()

    def get_typhoons_by_year(self, year: int):
        return self.__api.get_typhoons_by_year(year)

    def get_dashboard_data_by_year(self, year: int):
        return self.__api.get_dashboard_data_by_year(year)

    def run_historical_analysis(self, country: str, year: int, overwrite: bool = False):
        return self.__api.run_historical_analysis(country, year, overwrite)

    def get_historical_analysis_status(self):
        return self.__api.get_historical_analysis_status()

    def cancel_historical_analysis(self):
        return self.__api.cancel_historical_analysis()

    def run_nowcast_analysis(
        self, country: str, year: int | None = None, local_zip_path: str | None = None, days: int | None = None
    ):
        return self.__api.run_nowcast_analysis(country, year, local_zip_path, days)

    def get_nowcast_analysis_status(self):
        return self.__api.get_nowcast_analysis_status()

    def cancel_nowcast_analysis(self):
        return self.__api.cancel_nowcast_analysis()

    def console_log(self, level: str, message: str):
        return self.__api.console_log(level, message)

    def close_app(self):
        return self.__api.close_app()

    def save_track(self, track_data_json: str):
        return self.__api.save_track(track_data_json)

    def get_saved_track_path(self):
        return self.__api.get_saved_track_path()

    def upload_cyclone_track(self, file_data: str, filename: str):
        return self.__api.upload_cyclone_track(file_data, filename)

    def set_window(self, window):
        """Set window reference after window creation - called from main()."""
        self.__api.set_window(window)

    def close(self):
        """Close and cleanup resources."""
        return self.__api.close()


class UnifiedApi:
    """Unified API that handles both welcome screen and dashboard modes."""

    def __init__(self, window=None):
        """Initialize all APIs at startup."""
        # Store window in a way that won't be serialized by pywebview
        # Use double underscore to hide from introspection
        self.__window = window
        self.current_mode = None
        self.saved_track_path = None
        self._navigation_threads = []  # Track navigation threads
        self._is_closing = False  # Flag to prevent navigation during shutdown

        # Initialize both APIs - pass window=None initially to avoid introspection issues
        self.nowcast_api = NowcastApi(window=window)
        self.historical_api = HistoricalApi(window=window)

        logger.info("All APIs initialized successfully")

    @property
    def window(self):
        """Get window reference safely."""
        return self.__window

    @window.setter
    def window(self, value):
        """Set window reference safely."""
        self.__window = value
        # Update window references in child APIs
        if self.nowcast_api:
            self.nowcast_api.window = value
        if self.historical_api:
            self.historical_api.window = value

    def set_window(self, window):
        """Set window reference after window creation."""
        self.__window = window
        # Update window references in child APIs
        if self.nowcast_api:
            self.nowcast_api.window = window
        if self.historical_api:
            self.historical_api.window = window

    def test_api(self):
        """Test method to verify API is working."""
        return {"status": "API is working", "mode": self.current_mode}

    def select_mode(self, mode: str):
        """Handle mode selection from welcome screen.

        Args:
            mode: Either 'historical' or 'nowcast'

        Uses a daemon thread to navigate after callback completes.
        Works in both dev and packaged environments.
        """
        logger.info(f"Mode selected: {mode}")

        try:
            # Set current mode
            self.current_mode = mode

            # Get the appropriate HTML path
            if mode == "historical":
                html_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "frontend", "static", "historical", "index.html")
                )
                title = "Typhoon Historical Dashboard"
            elif mode == "nowcast":
                html_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "frontend", "static", "nowcast", "index.html")
                )
                title = "Typhoon Nowcast Dashboard"
            else:
                logger.warning(f"Unknown mode: {mode}")
                return False

            if self.window:

                def _do_navigation():
                    time.sleep(0.05)  # 50ms delay to let callback complete
                    try:
                        self.window.title = title
                        self.window.load_url(html_path)
                        logger.info(f"{mode.title()} mode launched successfully")
                    except Exception as e:
                        logger.error(f"Error in navigation: {e}")

                thread = threading.Thread(target=_do_navigation, daemon=True)
                thread.start()
                return True

            return False

        except Exception as e:
            logger.error(f"Error launching {mode} mode: {e}")
            return False

    def back_to_welcome(self):
        """Return to welcome screen.

        Uses a daemon thread to navigate after callback completes.
        Works in both dev and packaged environments.
        """
        try:
            # Check if we're closing - don't navigate if so
            if self._is_closing:
                return False

            self.current_mode = None
            html_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "frontend", "static", "index.html"))

            if self.window:
                # Use a daemon thread to navigate after callback completes
                def _do_navigation():
                    # Check again before sleeping
                    if self._is_closing:
                        return

                    time.sleep(0.05)  # 50ms delay to let callback complete

                    # Check one more time before navigation
                    if self._is_closing:
                        return

                    try:
                        self.window.title = "Cyclone Impact Toolkit"
                        self.window.load_url(html_path)
                        logger.info("Returned to welcome screen")
                    except Exception as e:
                        logger.error(f"Error in navigation: {e}")

                thread = threading.Thread(target=_do_navigation, daemon=True)
                self._navigation_threads.append(thread)
                thread.start()
                return True

            return False

        except Exception as e:
            logger.error(f"Error returning to welcome screen: {e}")
            return False

    def load_historical_dashboard(self):
        """Load the full historical dashboard.

        Uses a daemon thread to navigate after callback completes.
        Works in both dev and packaged environments.
        """
        try:
            # Check if we're closing
            if self._is_closing:
                return False

            # Set mode before navigation so subsequent API calls route correctly
            self.current_mode = "historical"

            html_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "frontend", "static", "historical", "index.html")
            )

            if self.window:

                def _do_navigation():
                    if self._is_closing:
                        return
                    time.sleep(0.05)  # 50ms delay to let callback complete
                    if self._is_closing:
                        return
                    try:
                        self.window.title = "Typhoon Impact Dashboard"
                        self.window.load_url(html_path)
                        logger.info("Loaded historical dashboard")
                    except Exception as e:
                        logger.error(f"Error in navigation: {e}")

                thread = threading.Thread(target=_do_navigation, daemon=True)
                self._navigation_threads.append(thread)
                thread.start()
                return True

            return False

        except Exception as e:
            logger.error(f"Error loading historical dashboard: {e}")
            return False

    def load_nowcast_dashboard(self):
        """Load the full nowcast dashboard.

        Uses a daemon thread to navigate after callback completes.
        Works in both dev and packaged environments.
        """
        try:
            # Check if we're closing
            if self._is_closing:
                return False

            # Set mode before navigation so subsequent API calls route correctly
            self.current_mode = "nowcast"

            html_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "frontend", "static", "nowcast", "index.html")
            )

            if self.window:

                def _do_navigation():
                    if self._is_closing:
                        return
                    time.sleep(0.05)  # 50ms delay to let callback complete
                    if self._is_closing:
                        return
                    try:
                        self.window.title = "Typhoon Nowcast Dashboard"
                        self.window.load_url(html_path)
                        logger.info("Loaded nowcast dashboard")
                    except Exception as e:
                        logger.error(f"Error in navigation: {e}")

                thread = threading.Thread(target=_do_navigation, daemon=True)
                self._navigation_threads.append(thread)
                thread.start()
                return True

            return False

        except Exception as e:
            logger.error(f"Error loading nowcast dashboard: {e}")
            return False

    # Delegate API calls to the appropriate API based on current mode
    def get_typhoon_list(self):
        """Get typhoon list from current mode API."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.get_typhoon_list()
        elif self.current_mode == "historical":
            return self.historical_api.get_typhoon_list()
        return []

    def get_typhoon_data(self, typhoon_uuid: str):
        """Get typhoon data from current mode API."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.get_typhoon_data(typhoon_uuid)
        elif self.current_mode == "historical":
            return self.historical_api.get_typhoon_data(typhoon_uuid)
        return None

    def get_typhoon_dates(self, typhoon_uuid: str):
        """Get typhoon dates from current mode API."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.get_typhoon_dates(typhoon_uuid)
        elif self.current_mode == "historical":
            # Historical API doesn't have dates - return empty list
            return []
        return []

    def get_dashboard_data(self):
        """Get dashboard data from current mode API."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.get_dashboard_data()
        elif self.current_mode == "historical":
            return self.historical_api.get_dashboard_data()
        return {}

    def get_fishing_grounds(self):
        """Get fishing grounds from current mode API."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.get_fishing_grounds()
        elif self.current_mode == "historical":
            return self.historical_api.get_fishing_grounds()
        return []

    # Nowcast-specific methods
    def create_typhoon_from_files(self, name: str, csv_path: str, shapefile_path: str):
        """Create typhoon from files (nowcast only)."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.create_typhoon_from_files(name, csv_path, shapefile_path)
        return None

    def delete_typhoon(self, typhoon_uuid: str):
        """Delete typhoon (nowcast only)."""
        if self.current_mode == "nowcast":
            return self.nowcast_api.delete_typhoon(typhoon_uuid)
        return False

    # Historical-specific methods
    def get_available_years(self):
        """Get available years (historical only)."""
        if self.current_mode == "historical":
            return self.historical_api.get_available_years()
        return []

    def get_typhoons_by_year(self, year: int):
        """Get typhoons by year (historical only)."""
        if self.current_mode == "historical":
            return self.historical_api.get_typhoons_by_year(year)
        return {}

    def get_dashboard_data_by_year(self, year: int):
        """Get dashboard data by year (historical only)."""
        if self.current_mode == "historical":
            return self.historical_api.get_dashboard_data_by_year(year)
        return {"typhoons": {}, "fishing_grounds": []}

    # Historical analysis processing methods
    def run_historical_analysis(self, country: str, year: int, overwrite: bool = False):
        """Run historical analysis processing.

        Args:
            country: Country name (e.g., "philippines", "vietnam")
            year: Year for analysis
            overwrite: Whether to overwrite existing data files

        Returns:
            Dictionary with status information
        """
        if self.current_mode != "historical":
            self.current_mode = "historical"
        return self.historical_api.run_historical_analysis(country, year, overwrite)

    def get_historical_analysis_status(self):
        """Get status of historical analysis processing.

        Returns:
            Dictionary with current status
        """
        return self.historical_api.get_historical_analysis_status()

    def cancel_historical_analysis(self):
        """Cancel the current historical analysis if running.

        Returns:
            Dictionary with cancellation status
        """
        return self.historical_api.cancel_historical_analysis()

    # Nowcast analysis processing methods
    def run_nowcast_analysis(
        self, country: str, year: int | None = None, local_zip_path: str | None = None, days: int | None = None
    ):
        """Run nowcast analysis processing.

        Args:
            country: Country name (e.g., "philippines", "vietnam")
            year: Year for analysis (defaults to current year if None)
            local_zip_path: Optional path to uploaded cyclone track file
            days: Number of days to look back for IBTrACS data (defaults to 7 if None)

        Returns:
            Dictionary with status information
        """
        if self.current_mode != "nowcast":
            self.current_mode = "nowcast"
        return self.nowcast_api.run_nowcast_analysis(country, year, local_zip_path, days)

    def get_nowcast_analysis_status(self):
        """Get status of nowcast analysis processing.

        Returns:
            Dictionary with current status
        """
        return self.nowcast_api.get_nowcast_analysis_status()

    def cancel_nowcast_analysis(self):
        """Cancel the current nowcast analysis if running.

        Returns:
            Dictionary with cancellation status
        """
        return self.nowcast_api.cancel_nowcast_analysis()

    # Common methods
    def console_log(self, level: str, message: str):
        """Bridge console logs from frontend."""
        if self.current_mode == "nowcast":
            self.nowcast_api.console_log(level, message)
        elif self.current_mode == "historical":
            self.historical_api.console_log(level, message)
        else:
            # Default console logging for welcome screen
            from datetime import datetime

            timestamp = datetime.now().strftime("%H:%M:%S")
            logger.info(f"[{timestamp}] JS-{level.upper()}: {message}")

    def close_app(self):
        """Close the application."""
        try:
            # Set closing flag to prevent new navigation
            self._is_closing = True

            # Clean up APIs
            if self.nowcast_api:
                try:
                    self.nowcast_api.close()
                except Exception as e:
                    logger.error(f"Error closing nowcast API: {e}")

            if self.historical_api:
                try:
                    self.historical_api.close()
                except Exception as e:
                    logger.error(f"Error closing historical API: {e}")

            # Wait a brief moment for any navigation threads to finish
            active_threads = [t for t in self._navigation_threads if t.is_alive()]
            if active_threads:
                for thread in active_threads:
                    thread.join(timeout=0.1)

            # Clear the navigation threads list
            self._navigation_threads.clear()

            # Destroy window
            if self.window:
                try:
                    self.window.destroy()
                except Exception as e:
                    logger.error(f"Error in window.destroy(): {e}")
                    raise

            logger.info("Application closed")
        except Exception as e:
            logger.error(f"Error closing application: {e}")
            import traceback

            traceback.print_exc()

    def save_track(self, track_data_json: str) -> str | None:
        """Save drawn track data to shapefile.

        Args:
            track_data_json: JSON string with track points

        Returns:
            Path to saved shapefile or None if failed
        """
        try:
            # Parse JSON
            track_data = json.loads(track_data_json)
            points = track_data.get("points", [])

            if not points:
                logger.warning("No track points to save")
                return None

            # Create output directory if it doesn't exist
            # Align with restructured folder structure: data/inputs/uploads/temp/
            output_dir = os.path.join(os.path.dirname(__file__), "data", "inputs", "uploads", "temp")
            os.makedirs(output_dir, exist_ok=True)

            # Create lists to store point data
            geometries = []
            years = []
            months = []
            days = []
            hours = []
            minutes = []
            cyclone_speeds = []
            wind_speeds = []
            iso_times = []
            names = []

            # Process all points
            for point_data in points:
                coords = point_data["coordinates"]
                point = Point(coords[0], coords[1])  # lon, lat
                geometries.append(point)

                # Parse datetime
                dt = datetime.fromisoformat(point_data["date_time"].replace("T", " "))
                years.append(dt.year)
                months.append(dt.month)
                days.append(dt.day)
                hours.append(dt.hour)
                minutes.append(dt.minute)

                cyclone_speeds.append(point_data["cyclone_spd"])
                wind_speeds.append(point_data["wind_spd"])
                iso_times.append(point_data["date_time"])
                names.append("Drawn Track")

            # Create a GeoDataFrame with all point data
            gdf = gpd.GeoDataFrame(
                {
                    "geometry": geometries,
                    "year": years,
                    "month": months,
                    "day": days,
                    "hour": hours,
                    "minute": minutes,
                    "NAME": names,
                    "STORM_SPD": cyclone_speeds,
                    "USA_WIND": wind_speeds,
                    "ISO_TIME": iso_times,
                },
                crs="EPSG:4326",
            )

            # Add explicit LAT and LON columns from geometry
            gdf["LAT"] = gdf.geometry.y
            gdf["LON"] = gdf.geometry.x

            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"track_drawn_{timestamp}.shp"
            output_path = os.path.join(output_dir, filename)

            # Save as shapefile
            gdf.to_file(output_path)
            logger.info(f"Track saved to: {output_path}")

            # Store the path for use in nowcast analysis
            self.saved_track_path = output_path

            return output_path

        except Exception as e:
            logger.error(f"Error saving track: {e}")
            import traceback

            traceback.print_exc()
            return None

    def get_saved_track_path(self) -> str | None:
        """Get the path of the saved track.

        Returns:
            Path to saved track or None
        """
        return getattr(self, "saved_track_path", None)

    def upload_cyclone_track(self, file_data: str, filename: str) -> str:
        """Upload a cyclone track file (ZIP or shapefile) and extract/return shapefile path.

        Args:
            file_data: Base64 encoded file data
            filename: Original filename

        Returns:
            Full path to the extracted/uploaded shapefile
        """
        return self.nowcast_api.upload_cyclone_track(file_data, filename)

    def close(self):
        """Clean up resources."""
        self._is_closing = True

        # Clean up navigation threads first
        if hasattr(self, "_navigation_threads") and self._navigation_threads:
            active_nav = [t for t in self._navigation_threads if t.is_alive()]
            if active_nav:
                for thread in active_nav:
                    try:
                        thread.join(timeout=0.05)
                    except Exception as e:
                        logger.error(f"Error joining thread: {e}")
            self._navigation_threads.clear()

        if self.nowcast_api:
            try:
                self.nowcast_api.close()
            except Exception as e:
                logger.error(f"Error closing nowcast API: {e}")

        if self.historical_api:
            try:
                self.historical_api.close()
            except Exception as e:
                logger.error(f"Error closing historical API: {e}")

        # Clear window reference
        self.__window = None


def main():
    """Main application entry point."""
    # Get screen size
    screens = webview.screens if hasattr(webview, "screens") else []
    if screens:
        screen = screens[0]
        width = int(screen.width * 0.9)
        height = int(screen.height * 0.9)
    else:
        width = 1400
        height = 900

    # Get path to unified HTML
    html_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "frontend", "static", "index.html"))

    # Initialize CLEAN API (no window references) - this is what pywebview will introspect
    api = CleanAPI()

    # Create main window with clean API (pywebview will introspect CleanAPI, not UnifiedApi)
    window = webview.create_window(
        "Cyclone Impact Toolkit",
        url=html_path,
        js_api=api,  # Pass CleanAPI, which has no window references
        width=width,
        height=height,
        resizable=True,
    )

    # NOW it's safe to set window reference - introspection is complete
    api.set_window(window)

    # Start webview
    try:
        webview.start(debug=True)
    except Exception as e:
        logger.error(f"Error in webview.start(): {e}")
        import traceback

        traceback.print_exc()

    # Cleanup after webview exits
    try:
        api.close()
    except Exception as e:
        logger.error(f"Error during API cleanup: {e}")
        import traceback

        traceback.print_exc()

    # Handle PyWebView's internal HTTP server thread that may block exit
    # PyWebView creates this thread when serving local files via its internal HTTP server.
    # The thread is non-daemon, so Python waits for it, causing a 30-50 second hang.
    # PyWebView doesn't expose a clean shutdown method, so we use os._exit() as a last resort.
    final_threads = threading.enumerate()
    remaining_non_daemon = [
        t for t in final_threads if t != threading.current_thread() and not t.daemon and t.is_alive()
    ]

    if remaining_non_daemon:
        webview_threads = [
            t for t in remaining_non_daemon if "process_request" in t.name.lower() or "http" in t.name.lower()
        ]
        if webview_threads:
            logger.debug("PyWebView HTTP server thread(s) detected, attempting graceful shutdown...")

            # Give threads a short time to finish naturally (200ms)
            for t in webview_threads:
                t.join(timeout=0.2)

            # Check if still alive
            still_alive = [t for t in webview_threads if t.is_alive()]
            if still_alive:
                logger.warning(
                    f"PyWebView thread(s) still alive after timeout: {[t.name for t in still_alive]}. "
                    "Forcing exit to prevent hang."
                )
                # Force immediate exit to prevent 30-50 second hang
                # Note: This bypasses normal Python cleanup (atexit handlers, etc.)
                # but is necessary because PyWebView doesn't provide a clean shutdown method
                # for its internal HTTP server thread.
                os._exit(0)


if __name__ == "__main__":
    main()
