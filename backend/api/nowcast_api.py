"""Nowcast API for PyWebView communication."""

import json
import os
import threading
from datetime import datetime
from typing import Any

from backend.utils.logger import get_logger

from ..repositories.nowcast_repository import NowcastRepository
from ..utils.utils import get_database_path, get_resource_path
from .base_api import BaseApi

logger = get_logger(__name__)


class NowcastApi(BaseApi):
    """API class for nowcast typhoon dashboard communication."""

    def __init__(self, window=None):
        """Initialize the nowcast API.

        Args:
            window: PyWebView window instance
        """
        super().__init__(window)
        db_path = get_database_path("database/nowcast.json")
        self.repository = NowcastRepository(db_path=db_path)

        # Status tracking for nowcast analysis processing
        self._status_lock = threading.Lock()
        self._analysis_status = {
            "status": "idle",  # idle, running, completed, error, cancelled
            "current_phase": 0,
            "total_phases": 5,
            "phase_name": "",
            "message": "",
            "progress_percent": 0,
            "error_message": None,
        }
        self._processing_thread = None
        self._cancellation_flag = threading.Event()

        logger.info(f"Nowcast API initialized with database path: {db_path} and repository: {self.repository}")

    def get_typhoon_list(self) -> list[dict[str, Any]]:
        """Get list of all available typhoons.

        Returns:
            List of typhoon summaries
        """
        try:
            typhoons = self.repository.get_typhoon_list()
            logger.info(f"Retrieved {len(typhoons)} typhoons")
            return typhoons
        except Exception as e:
            logger.error(f"Error getting typhoon list: {e}")
            return []

    def get_typhoon_data(self, typhoon_uuid: str) -> dict[str, Any] | None:
        """Get complete data for a specific typhoon.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            Typhoon data or None if not found
        """
        try:
            typhoon = self.repository.get_typhoon_by_uuid(typhoon_uuid)
            if typhoon:
                logger.info(f"Retrieved data for typhoon: {typhoon['name']}")
                return typhoon
            else:
                logger.info(f"No typhoon found with UUID: {typhoon_uuid}")
                return None
        except Exception as e:
            logger.error(f"Error getting typhoon data: {e}")
            return None

    def get_typhoon_dates(self, typhoon_uuid: str) -> list[str]:
        """Get available dates for a specific typhoon.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            List of date strings
        """
        try:
            dates = self.repository.get_typhoon_dates(typhoon_uuid)
            logger.info(f"Retrieved {len(dates)} dates for typhoon {typhoon_uuid}")
            return dates
        except Exception as e:
            logger.error(f"Error getting typhoon dates: {e}")
            return []

    def get_fishing_grounds(self) -> list[dict[str, Any]]:
        """Get fishing grounds data from GeoJSON file.

        Returns:
            List of fishing ground dictionaries
        """
        try:
            # Load fishing grounds from GeoJSON file
            # Using restructured folder: data/inputs/gis/countries/{country}/fishing_grounds/
            geojson_path = get_resource_path(
                "data/inputs/gis/countries/phl/fishing_grounds/fishing_grounds_nowcast.geojson"
            )

            if not os.path.exists(geojson_path):
                logger.error(f"Fishing grounds GeoJSON file not found: {geojson_path}")
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

            logger.info(f"Retrieved {len(fishing_grounds)} fishing grounds from GeoJSON")
            return fishing_grounds

        except Exception as e:
            logger.error(f"Error getting fishing grounds from GeoJSON: {e}")
            return []

    def create_typhoon_from_files(self, name: str, csv_path: str, shapefile_path: str) -> str | None:
        """Create a new typhoon record from CSV and shapefile.

        Args:
            name: Typhoon name
            csv_path: Path to CSV file
            shapefile_path: Path to shapefile

        Returns:
            UUID of created typhoon or None if failed
        """
        try:
            typhoon_uuid = self.repository.create_typhoon_record(name, csv_path, shapefile_path)
            if typhoon_uuid:
                logger.info(f"Created new typhoon: {name} with UUID: {typhoon_uuid}")
                # Notify the frontend that new data is available
                self.notify_data_update()
                return typhoon_uuid
            else:
                logger.error(f"Failed to create typhoon: {name}")
                return None
        except Exception as e:
            logger.error(f"Error creating typhoon: {e}")
            return None

    def delete_typhoon(self, typhoon_uuid: str) -> bool:
        """Delete a typhoon record.

        Args:
            typhoon_uuid: Typhoon UUID

        Returns:
            True if successful, False otherwise
        """
        try:
            success = self.repository.delete_typhoon(typhoon_uuid)
            if success:
                logger.info(f"Deleted typhoon with UUID: {typhoon_uuid}")
                self.notify_data_update()
            return success
        except Exception as e:
            logger.error(f"Error deleting typhoon: {e}")
            return False

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get all data needed for dashboard initialization.

        Returns:
            Dictionary with typhoons, fishing grounds, and default data
        """
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

            logger.info("Dashboard data prepared successfully")
            return dashboard_data

        except Exception as e:
            logger.error(f"Error preparing dashboard data: {e}")
            return {"typhoons": [], "fishing_grounds": [], "default_typhoon": None, "default_dates": []}

    def run_nowcast_analysis(
        self, country: str, year: int | None = None, local_zip_path: str | None = None, days: int | None = None
    ) -> dict[str, Any]:
        """Start nowcast analysis processing.

        Args:
            country: Country name from UI (e.g., "philippines", "vietnam")
            year: Year for analysis (defaults to current year if None)
            local_zip_path: Optional path to uploaded cyclone track file
            days: Number of days to look back for IBTrACS data (defaults to 7 if None or if using synthetic)

        Returns:
            Dictionary with status information
        """
        try:
            # Check if another run is active
            with self._status_lock:
                if self._analysis_status["status"] == "running":
                    return {
                        "status": "error",
                        "message": "Another analysis is already running. Please wait or cancel it first.",
                    }

            # Use current year if not specified
            if year is None:
                year = datetime.now().year

            # Map country name to country code
            country_map = {
                "philippines": "phl",
                "vietnam": "vnm",
                "thailand": "tha",
                "fiji": "fji",
                "vanuatu": "vut",
                "bangladesh": "bgd",
                "indonesia": "idn",
            }
            country_code = country_map.get(country.lower(), country.lower())

            # Validate country code
            valid_countries = ["phl", "vnm", "tha", "fji", "vut", "bgd", "idn"]
            if country_code not in valid_countries:
                return {
                    "status": "error",
                    "message": f"Invalid country: {country}. Supported countries: {list(country_map.keys())}",
                }

            # Validate uploaded file if provided
            if local_zip_path and not os.path.exists(local_zip_path):
                return {
                    "status": "error",
                    "message": f"Uploaded file not found: {local_zip_path}",
                }

            # Validate and set default for days parameter (only used for IBTrACS)
            ibtracs_days = 7  # Default value
            if days is not None and not local_zip_path:  # Only validate if using IBTrACS (no local file)
                if not isinstance(days, int) or days < 1 or days > 90:
                    return {
                        "status": "error",
                        "message": f"Invalid days value: {days}. Must be between 1 and 90.",
                    }
                ibtracs_days = days
            elif local_zip_path:
                # If using synthetic data, days parameter is ignored
                ibtracs_days = 7

            # Reset status
            with self._status_lock:
                self._analysis_status = {
                    "status": "running",
                    "current_phase": 0,
                    "total_phases": 5,
                    "phase_name": "Initializing...",
                    "message": "Starting nowcast analysis...",
                    "progress_percent": 0,
                    "error_message": None,
                }
                self._cancellation_flag.clear()

            # Start processing in background thread
            def _run_analysis():
                try:
                    from backend.services.nowcast import NowcastConfig, main

                    # Create config
                    config = NowcastConfig.from_defaults(
                        country=country_code,
                        year_selected=year,
                        root_path="data",
                    )
                    config.local_zip_path = local_zip_path
                    config.ensure_paths_exist()

                    # Pass ibtracs_days to main function (only used if not using local file)
                    ibtracs_days_param = ibtracs_days if not local_zip_path else None

                    # Define progress callback
                    def progress_callback(phase: int, phase_name: str, message: str):
                        """Update status as processing progresses."""
                        # Check for cancellation
                        if self._cancellation_flag.is_set():
                            raise KeyboardInterrupt("Processing cancelled by user")
                        self._update_status(phase, phase_name, message)

                    # Run main processing with progress callback
                    results = main(
                        config=config,
                        local_zip_path=local_zip_path,
                        progress_callback=progress_callback,
                        ibtracs_days=ibtracs_days_param,
                    )

                    # Check for cancellation after main() completes
                    if self._cancellation_flag.is_set():
                        with self._status_lock:
                            self._analysis_status["status"] = "cancelled"
                            self._analysis_status["message"] = "Processing was cancelled"
                        return

                    # Update status: Phase 5 (database update)
                    self._update_status(5, "Creating visualizations and updating database...", "Updating database...")

                    # Call database update function
                    from backend.services.nowcast_db_update import update_nowcast_database_from_run

                    def db_progress_callback(phase: int, phase_name: str, message: str):
                        """Progress callback for database update."""
                        if self._cancellation_flag.is_set():
                            raise KeyboardInterrupt("Processing cancelled by user")
                        self._update_status(phase, phase_name, message)

                    db_update_result = update_nowcast_database_from_run(
                        country_code,
                        year,
                        config.output_path,
                        results,  # Pass results dict with filtered_gdf_1, daily_stats, etc.
                        db_path=get_database_path("database/nowcast.json"),
                        progress_callback=db_progress_callback,
                    )

                    if db_update_result["status"] == "error":
                        raise Exception(f"Database update failed: {db_update_result['message']}")

                    logger.info(
                        f"Database updated: {db_update_result.get('added_count', 0)} cyclones added, "
                        f"{db_update_result.get('updated_count', 0)} updated"
                    )

                    # Mark as completed
                    with self._status_lock:
                        self._analysis_status["status"] = "completed"
                        self._analysis_status["current_phase"] = 5
                        self._analysis_status["progress_percent"] = 100
                        self._analysis_status["message"] = "Analysis completed successfully"

                except KeyboardInterrupt:
                    # Handle cancellation
                    with self._status_lock:
                        self._analysis_status["status"] = "cancelled"
                        self._analysis_status["message"] = "Processing was cancelled"
                    logger.info("Nowcast analysis was cancelled")
                except Exception as e:
                    logger.error(f"Error in nowcast analysis: {e}", exc_info=True)
                    with self._status_lock:
                        self._analysis_status["status"] = "error"
                        self._analysis_status["error_message"] = str(e)
                        self._analysis_status["message"] = f"Error: {str(e)}"

            self._processing_thread = threading.Thread(target=_run_analysis, daemon=True)
            self._processing_thread.start()

            return {
                "status": "started",
                "message": "Processing started",
            }

        except Exception as e:
            logger.error(f"Error starting nowcast analysis: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to start analysis: {str(e)}",
            }

    def _update_status(self, phase: int, phase_name: str, message: str):
        """Update processing status (thread-safe).

        Args:
            phase: Current phase number (1-5)
            phase_name: Name of the current phase
            message: Detailed message about current step
        """
        with self._status_lock:
            self._analysis_status["current_phase"] = phase
            self._analysis_status["phase_name"] = phase_name
            self._analysis_status["message"] = message
            # Calculate progress: each phase is 20% (100% / 5 phases)
            self._analysis_status["progress_percent"] = int((phase / 5) * 100)

    def get_nowcast_analysis_status(self) -> dict[str, Any]:
        """Get current status of nowcast analysis processing.

        Returns:
            Dictionary with status information
        """
        with self._status_lock:
            return self._analysis_status.copy()

    def cancel_nowcast_analysis(self) -> dict[str, Any]:
        """Cancel the current nowcast analysis if running.

        Returns:
            Dictionary with cancellation status
        """
        try:
            with self._status_lock:
                if self._analysis_status["status"] != "running":
                    return {
                        "status": "error",
                        "message": "No analysis is currently running",
                    }

                # Set cancellation flag
                self._cancellation_flag.set()

                # Update status
                self._analysis_status["status"] = "cancelled"
                self._analysis_status["message"] = "Cancellation requested..."

            return {
                "status": "cancelled",
                "message": "Cancellation requested. Processing will stop after current step.",
            }

        except Exception as e:
            logger.error(f"Error cancelling nowcast analysis: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to cancel: {str(e)}",
            }

    def upload_cyclone_track(self, file_data: str, filename: str) -> str:
        """Upload a cyclone track ZIP file, extract it, and return shapefile path.

        Args:
            file_data: Base64 encoded ZIP file data
            filename: Original filename (must be .zip)

        Returns:
            Full path to the extracted shapefile (.shp)
        """
        try:
            import base64
            import shutil
            import zipfile

            # Validate file type
            if not filename.lower().endswith(".zip"):
                raise ValueError(f"Only ZIP files are accepted. Received: {filename}")

            # Get project root and create upload directory
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            upload_dir = os.path.join(project_root, "data", "inputs", "uploads", "temp")
            os.makedirs(upload_dir, exist_ok=True)

            # Decode base64 data
            file_bytes = base64.b64decode(file_data)

            # Create temporary directory for extraction (with timestamp to avoid conflicts)
            timestamp = int(datetime.now().timestamp())
            extract_dir = os.path.join(upload_dir, f"extracted_{timestamp}")
            os.makedirs(extract_dir, exist_ok=True)

            try:
                # Save ZIP file temporarily
                zip_path = os.path.join(extract_dir, filename)
                with open(zip_path, "wb") as f:
                    f.write(file_bytes)

                # Extract ZIP file
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)

                # Find .shp file in extracted contents
                shp_files = [f for f in os.listdir(extract_dir) if f.lower().endswith(".shp")]

                if not shp_files:
                    raise ValueError(f"No .shp file found in ZIP archive: {filename}")

                # Use the first .shp file found
                shp_filename = shp_files[0]
                shapefile_path = os.path.join(extract_dir, shp_filename)

                # Verify companion files exist
                shp_basename = os.path.splitext(shp_filename)[0]
                required_extensions = [".shx", ".dbf"]
                missing_files = []

                for ext in required_extensions:
                    companion_file = os.path.join(extract_dir, f"{shp_basename}{ext}")
                    if not os.path.exists(companion_file):
                        missing_files.append(f"{shp_basename}{ext}")

                if missing_files:
                    logger.warning(f"Missing shapefile companion files: {missing_files}")
                    logger.info("GeoPandas may still be able to read the file, but some features may not work.")

                logger.info(f"Extracted shapefile from ZIP: {shapefile_path}")
                return shapefile_path

            except Exception:
                # Clean up extraction directory on error
                if os.path.exists(extract_dir):
                    shutil.rmtree(extract_dir, ignore_errors=True)
                raise

        except Exception as e:
            logger.error(f"Error uploading cyclone track: {e}", exc_info=True)
            raise Exception(f"Failed to upload and extract ZIP file: {str(e)}") from e

    def close(self):
        """Clean up resources."""
        if self.repository:
            self.repository.close()
