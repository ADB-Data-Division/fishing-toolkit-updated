"""Historical API for PyWebView communication."""

import json
import os
import threading
from typing import Any

import pandas as pd

from backend.utils.logger import get_logger

from ..repositories.historical_repository import HistoricalRepository
from ..utils.utils import get_database_path
from .base_api import BaseApi

logger = get_logger(__name__)


class HistoricalApi(BaseApi):
    """API class for historical typhoon dashboard communication."""

    def __init__(self, window=None):
        """Initialize the historical API.

        Args:
            window: PyWebView window instance
        """
        super().__init__(window)
        db_path = get_database_path("database/historical.json")
        self.repository = HistoricalRepository(db_path=db_path)

        # Status tracking for historical analysis processing
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

        logger.info(f"Historical API initialized with database path: {db_path} and repository: {self.repository}")

    def get_typhoon_list(self) -> list[dict[str, Any]]:
        """Get list of all available typhoons.

        Returns:
            List of typhoon summaries
        """
        try:
            typhoons = self.repository.get_typhoon_list()
            logger.info(f"Retrieved {len(typhoons)} historical typhoons")
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

    def get_typhoon_by_name(self, name: str) -> dict[str, Any] | None:
        """Get typhoon data by name.

        Args:
            name: Typhoon name

        Returns:
            Typhoon data or None if not found
        """
        try:
            typhoon = self.repository.get_typhoon_by_name(name)
            if typhoon:
                logger.info(f"Retrieved data for typhoon: {typhoon['name']}")
                return typhoon
            else:
                logger.info(f"No typhoon found with name: {name}")
                return None
        except Exception as e:
            logger.error(f"Error getting typhoon data by name: {e}")
            return None

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get all data needed for dashboard initialization.

        Returns:
            Dictionary with typhoons and fishing grounds
        """
        try:
            # Get typhoons in dashboard format
            dashboard_data = self.repository.get_dashboard_data()

            # Get fishing grounds (empty list as they're created per run)
            fishing_grounds = self.get_fishing_grounds()

            # Update fishing grounds if we have them
            if fishing_grounds:
                dashboard_data["fishing_grounds"] = fishing_grounds

            logger.info("Historical dashboard data prepared successfully")
            return dashboard_data

        except Exception as e:
            logger.error(f"Error preparing dashboard data: {e}")
            return {"typhoons": {}, "fishing_grounds": []}

    def get_fishing_grounds(self) -> list[dict[str, Any]]:
        """Get fishing grounds data.

        Note: Historical fishing grounds are created per run and stored in typhoon data.
        This returns an empty list as a placeholder.

        Returns:
            Empty list (fishing grounds are per-typhoon in historical mode)
        """
        try:
            # Historical mode: fishing grounds are stored per typhoon
            # Return empty list as they should be extracted from typhoon data
            logger.info("Historical fishing grounds are per-typhoon")
            return []
        except Exception as e:
            logger.error(f"Error getting fishing grounds: {e}")
            return []

    def get_boat_detections_geojson(self, year: int, sample_size: int = 5000) -> dict[str, Any] | None:
        """Load boat detection points and convert to GeoJSON.

        Args:
            year: Year to load data for
            sample_size: Number of random points to sample (default 5000 for performance)

        Returns:
            GeoJSON FeatureCollection or None if file not found
        """
        try:
            # Build path to boat detections CSV
            boat_csv_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data",
                "outputs",
                "historical",
                "phl",
                str(year),
                "intermediate",
                f"df_all_b_phl_{year}.csv",
            )

            if not os.path.exists(boat_csv_path):
                logger.warning(f"Boat detections file not found: {boat_csv_path}")
                return None

            # Load CSV
            logger.info(f"Loading boat detections from {boat_csv_path}")
            df = pd.read_csv(boat_csv_path)

            # Sample data if too large
            if len(df) > sample_size:
                logger.info(f"Sampling {sample_size} points from {len(df)} total boat detections")
                df = df.sample(n=sample_size, random_state=42)

            # Convert to GeoJSON
            features = []
            for _, row in df.iterrows():
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [row["Lon_DNB"], row["Lat_DNB"]],
                    },
                    "properties": {"date": str(row["date_only"]) if pd.notna(row["date_only"]) else None},
                }
                features.append(feature)

            geojson = {"type": "FeatureCollection", "features": features}

            logger.info(f"Converted {len(features)} boat detections to GeoJSON")
            return geojson

        except Exception as e:
            logger.error(f"Error loading boat detections for year {year}: {e}")
            import traceback

            traceback.print_exc()
            return None

    def get_available_years(self) -> list[int]:
        """Get all available years from the database.

        Returns:
            Sorted list of unique years
        """
        try:
            years = self.repository.get_available_years()
            logger.info(f"Retrieved {len(years)} available years")
            return years
        except Exception as e:
            logger.error(f"Error getting available years: {e}")
            return []

    def get_typhoons_by_year(self, year: int) -> dict[str, Any]:
        """Get all typhoons for a specific year.

        Args:
            year: Year to filter by

        Returns:
            Dictionary of typhoons keyed by normalized name
        """
        try:
            typhoons = self.repository.get_typhoons_by_year(year)
            dashboard_typhoons = {}

            for typhoon in typhoons:
                # Use name as key (normalized)
                key = typhoon["name"].lower().replace("-", "")
                dashboard_typhoons[key] = typhoon.get("dashboard_data", {})

            logger.info(f"Retrieved {len(dashboard_typhoons)} typhoons for year {year}")
            return dashboard_typhoons
        except Exception as e:
            logger.error(f"Error getting typhoons for year {year}: {e}")
            return {}

    def get_dashboard_data_by_year(self, year: int) -> dict[str, Any]:
        """Get dashboard data filtered by year.

        Args:
            year: Year to filter by

        Returns:
            Dictionary with typhoons and fishing grounds for the year
        """
        try:
            # Get typhoons for specific year
            typhoons = self.get_typhoons_by_year(year)

            # Try to load fishing grounds GeoJSON for this year
            fishing_grounds_geojson = None
            boat_detections_path = None

            geojson_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data",
                "outputs",
                "historical",
                "phl",
                str(year),
                "intermediate",
                f"phl_merged_dense_area_polygons_{year}.geojson",
            )

            if os.path.exists(geojson_path):
                try:
                    with open(geojson_path) as f:
                        fishing_grounds_geojson = json.load(f)
                    logger.info(f"Loaded fishing grounds GeoJSON for year {year}")
                except Exception as e:
                    logger.error(f"Error loading fishing grounds GeoJSON for year {year}: {e}")

            # Build path to boat detections CSV
            boat_csv_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data",
                "outputs",
                "historical",
                "phl",
                str(year),
                "intermediate",
                f"df_all_b_phl_{year}.csv",
            )

            if os.path.exists(boat_csv_path):
                boat_detections_path = boat_csv_path
                logger.info(f"Found boat detections CSV for year {year}")

            dashboard_data = {
                "typhoons": typhoons,
                "fishing_grounds_geojson": fishing_grounds_geojson,
                "boat_detections_csv_path": boat_detections_path,
                "latest_year": year,
            }

            logger.info(f"Dashboard data prepared for year {year}")
            return dashboard_data
        except Exception as e:
            logger.error(f"Error preparing dashboard data for year {year}: {e}")
            return {
                "typhoons": {},
                "fishing_grounds_geojson": None,
                "boat_detections_csv_path": None,
                "latest_year": year,
            }

    def run_historical_analysis(self, country: str, year: int, overwrite: bool = False) -> dict[str, Any]:
        """Start historical analysis processing.

        Args:
            country: Country name from UI (e.g., "philippines", "vietnam")
            year: Year for analysis
            overwrite: Whether to overwrite existing data files

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

            # Map country name to country code
            country_map = {
                "philippines": "phl",
                "vietnam": "vnm",
                "thailand": "tha",
                "fiji": "fji",
                "vut": "vut",
                "bangladesh": "bgd",
                "indonesia": "idn",
            }
            country_code = country_map.get(country.lower(), country.lower())

            # Validate country code
            from config import cyclone_seasons

            if country_code not in cyclone_seasons:
                return {
                    "status": "error",
                    "message": f"Invalid country: {country}. Supported countries: {list(country_map.keys())}",
                }

            # Reset status
            with self._status_lock:
                self._analysis_status = {
                    "status": "running",
                    "current_phase": 0,
                    "total_phases": 5,
                    "phase_name": "Initializing...",
                    "message": "Starting historical analysis...",
                    "progress_percent": 0,
                    "error_message": None,
                }
                self._cancellation_flag.clear()

            # Start processing in background thread
            def _run_analysis():
                try:
                    from backend.services.historical import Config, main
                    from config import cyclone_seasons

                    # Create config
                    config = Config.from_defaults(
                        country=country_code,
                        year_selected=year,
                        cyclone_seasons=cyclone_seasons,
                        root_path="data",
                    )
                    config.ensure_paths_exist()

                    # Define progress callback
                    def progress_callback(phase: int, phase_name: str, message: str):
                        """Update status as processing progresses."""
                        # Check for cancellation
                        if self._cancellation_flag.is_set():
                            raise KeyboardInterrupt("Processing cancelled by user")
                        self._update_status(phase, phase_name, message)

                    # Run main processing with progress callback
                    main(
                        config,
                        overwrite=overwrite,
                        debug=False,
                        read_from_file=False,
                        progress_callback=progress_callback,
                    )

                    # Check for cancellation after main() completes
                    if self._cancellation_flag.is_set():
                        with self._status_lock:
                            self._analysis_status["status"] = "cancelled"
                            self._analysis_status["message"] = "Processing was cancelled"
                        return

                    # Update status: Phase 5 (database update)
                    self._update_status(5, "Generating visualizations and updating database...", "Updating database...")

                    # Call database update function
                    from backend.services.historical_db_update import update_historical_database_from_run

                    def db_progress_callback(phase: int, phase_name: str, message: str):
                        """Progress callback for database update."""
                        if self._cancellation_flag.is_set():
                            raise KeyboardInterrupt("Processing cancelled by user")
                        self._update_status(phase, phase_name, message)

                    db_update_result = update_historical_database_from_run(
                        country_code,
                        year,
                        config.output_path,
                        db_path=get_database_path("database/historical.json"),
                        progress_callback=db_progress_callback,
                    )

                    if db_update_result["status"] == "error":
                        raise Exception(f"Database update failed: {db_update_result['message']}")

                    logger.info(
                        f"Database updated: {db_update_result.get('inserted_count', 0)} typhoons inserted, "
                        f"{db_update_result.get('deleted_count', 0)} deleted"
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
                    logger.info("Historical analysis was cancelled")
                except Exception as e:
                    logger.error(f"Error in historical analysis: {e}", exc_info=True)
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
            logger.error(f"Error starting historical analysis: {e}", exc_info=True)
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

    def get_historical_analysis_status(self) -> dict[str, Any]:
        """Get current status of historical analysis processing.

        Returns:
            Dictionary with status information
        """
        with self._status_lock:
            return self._analysis_status.copy()

    def cancel_historical_analysis(self) -> dict[str, Any]:
        """Cancel the current historical analysis if running.

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
            logger.error(f"Error cancelling historical analysis: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to cancel: {str(e)}",
            }

    def close(self):
        """Clean up resources."""
        if self.repository:
            self.repository.close()
