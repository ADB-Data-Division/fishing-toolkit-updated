# flake8: noqa: E501
import math
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime

import contextily as ctx
import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from geopy.distance import geodesic
from matplotlib import patheffects

from backend.services.historical import get_shapefiles_from_gis
from backend.utils.logger import get_logger
from backend.utils.utils import get_config_path

# Environment variables are loaded in main.py at application startup
config_path = get_config_path()
load_dotenv(config_path, override=True)
REQUEST_TIMEOUT = int(os.getenv("HTTP_REQUEST_TIMEOUT", "30"))
logger = get_logger(__name__)


@dataclass
class NowcastConfig:
    country: str
    year_selected: int
    gis_path: str
    output_path: str
    baseline_csv_path: str
    local_zip_path: str | None = None

    @classmethod
    def from_defaults(cls, country: str, year_selected: int, root_path: str = "data", run_timestamp: str | None = None):
        """
        Create a NowcastConfig instance with default folder structure.

        Args:
            country: Country code (e.g., 'phl', 'vnm', 'idn')
            year_selected: Year for nowcast
            root_path: Root data directory (default: 'data')
            run_timestamp: Optional timestamp for the run (default: current datetime)
        """
        if run_timestamp is None:
            run_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        gis_path = os.path.join(root_path, "inputs", "gis")
        output_path = os.path.join(root_path, "outputs", "nowcast", country, "runs", run_timestamp)
        baseline_csv_path = os.path.join(
            root_path, "inputs", "gis", "countries", country, "baselines", "baseline_nowcast.csv"
        )

        return cls(
            country=country,
            year_selected=year_selected,
            gis_path=gis_path,
            output_path=output_path,
            baseline_csv_path=baseline_csv_path,
            local_zip_path=None,
        )

    @classmethod
    def from_date(cls, country: str, year_selected: int, run_date: str, root_path: str = "data"):
        """
        Create a NowcastConfig for a specific date (YYYY-MM-DD format).
        """
        return cls.from_defaults(country, year_selected, root_path, run_date)

    def get_country_gis_path(self) -> str:
        """Get the country-specific GIS path."""
        return os.path.join(self.gis_path, "countries", self.country)

    def get_cyclone_cache_path(self) -> str:
        """Get the path for cached cyclone tracks."""
        return os.path.join(self.gis_path, "cyclone_tracks", "cache")

    def get_upload_path(self) -> str:
        """Get the path for user-uploaded files."""
        return os.path.join(self.gis_path.replace("inputs/gis", "inputs/uploads"), "temp")

    def ensure_paths_exist(self):
        """Create all necessary directories if they don't exist."""
        paths = [
            self.output_path,
            self.get_cyclone_cache_path(),
            self.get_upload_path(),
        ]
        for path in paths:
            os.makedirs(path, exist_ok=True)

    def create_latest_symlink(self):
        """Create/update a 'latest' symlink pointing to this run's output."""
        latest_path = os.path.join(os.path.dirname(os.path.dirname(self.output_path)), "latest")
        try:
            if os.path.islink(latest_path):
                os.remove(latest_path)
            elif os.path.exists(latest_path):
                # If it's a directory or file, try to remove it
                import shutil

                if os.path.isdir(latest_path):
                    shutil.rmtree(latest_path)
                else:
                    os.remove(latest_path)
        except (PermissionError, OSError) as e:
            logger.warning(
                f"Could not remove existing latest symlink/path: {e}. Attempting to create new symlink anyway."
            )

        try:
            # Create relative symlink for better portability
            rel_output_path = os.path.relpath(self.output_path, os.path.dirname(latest_path))
            os.symlink(rel_output_path, latest_path)
            logger.info(f"Created latest symlink: {latest_path} -> {rel_output_path}")
        except (PermissionError, OSError) as e:
            logger.warning(f"Could not create latest symlink: {e}. Continuing without symlink.")


def process_cyclone_data(country, read_eez, gis_path, output_path, local_zip_path=None, ibtracs_days=7):
    """
    Simplifies cyclone data processing:
    - Downloads IBTrACS data.
    - Filters by country, year, and missing data threshold.
    - Clips to EEZ and calculates storm speed statistics.

    Parameters:
    - country (str): Country code (ISO3 format).
    - read_eez: GeoDataFrame containing the EEZ boundaries.
    - gis_path (str): Path to GIS directory.
    - output_path (str): Directory for saving outputs.
    - local_zip_path (str, optional): Path to local ZIP file if using uploaded data.
    - ibtracs_days (int, optional): Number of days to look back for IBTrACS data (default: 7).

    Returns:
    - DataFrames for filtered data and storm speed statistics.
    """
    # Step 1: Download IBTrACS data
    tracks_file_name = "IBTrACS.last3years.list.v04r01.points.zip"
    if local_zip_path:
        # Handle both file paths and directory paths
        # If it's a directory, look for .shp files in it
        # If it's a file, use it directly
        if os.path.isdir(local_zip_path):
            # Look for .shp files in the directory
            shp_files = [f for f in os.listdir(local_zip_path) if f.endswith(".shp")]
            if shp_files:
                tracks_file_path = os.path.join(local_zip_path, shp_files[0])
                logger.info(f"Found shapefile in directory: {tracks_file_path}")
            else:
                raise ValueError(f"No .shp files found in directory: {local_zip_path}")
        elif os.path.exists(local_zip_path):
            tracks_file_path = local_zip_path
        else:
            # Try to find the file in common locations
            # If local_zip_path is just a filename, search for it
            filename = os.path.basename(local_zip_path)
            possible_paths = [
                local_zip_path,  # Original path (might be full path)
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "frontend", "static", "nowcast", "data", filename
                ),  # Frontend data folder
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "data", "inputs", "uploads", "temp", filename
                ),  # Upload temp folder
                os.path.join(os.path.dirname(__file__), "..", "..", local_zip_path),  # Relative to project root
            ]
            tracks_file_path = None
            for path in possible_paths:
                abs_path = os.path.abspath(path)
                if os.path.exists(abs_path):
                    tracks_file_path = abs_path
                    logger.info(f"Found shapefile at: {tracks_file_path}")
                    break

            if not tracks_file_path:
                raise FileNotFoundError(
                    f"Shapefile not found: {local_zip_path}\n"
                    f"Searched in:\n" + "\n".join([f"  - {os.path.abspath(p)}" for p in possible_paths])
                )

        print(f"Using local file from: {tracks_file_path}")

        # Check if shapefile has all required companion files
        if tracks_file_path.endswith(".shp"):
            shp_dir = os.path.dirname(tracks_file_path)
            shp_basename = os.path.splitext(os.path.basename(tracks_file_path))[0]
            required_extensions = [".shx", ".dbf"]  # .prj is optional
            missing_files = []

            for ext in required_extensions:
                companion_file = os.path.join(shp_dir, f"{shp_basename}{ext}")
                if not os.path.exists(companion_file):
                    missing_files.append(f"{shp_basename}{ext}")

            if missing_files:
                logger.warning(f"Shapefile companion files missing: {missing_files}")
                logger.info(f"Looking in directory: {shp_dir}")
                logger.info(
                    f"Files in directory: {os.listdir(shp_dir) if os.path.exists(shp_dir) else 'Directory not found'}"
                )
                # Try to read anyway - GeoPandas might handle it or give a better error

        try:
            # Attempt to read the file using GeoPandas
            # GeoPandas will look for companion files in the same directory
            filtered_gdf_1 = gpd.read_file(tracks_file_path)

            # Log NAME field values from uploaded file
            if "NAME" in filtered_gdf_1.columns:
                unique_names = filtered_gdf_1["NAME"].unique()
                logger.info(f"Uploaded file contains NAME values: {unique_names}")
                print(f"📌 Uploaded file contains cyclones: {unique_names}")
            else:
                logger.warning("Uploaded file does not have NAME column - will use UNNAMED")
                print("⚠️ Uploaded file missing NAME column - will use UNNAMED")
        except Exception as e:
            # Provide more helpful error message
            error_msg = str(e)
            if "Failed to open dataset" in error_msg or "flags=68" in error_msg:
                shp_dir = os.path.dirname(tracks_file_path) if tracks_file_path else "unknown"
                raise ValueError(  # noqa: B904
                    f"Failed to read shapefile. Shapefiles require multiple files (.shp, .shx, .dbf, .prj) "
                    f"in the same directory. File: {tracks_file_path}\n"
                    f"Directory: {shp_dir}\n"
                    f"Ensure all shapefile components are present. Error: {e}"
                )
            else:
                raise ValueError(  # noqa: B904
                    f"Failed to read the uploaded file. Ensure it is a valid geospatial file (Shapefile, GeoJSON, etc.): {e}"
                )

    else:
        print(f"Downloading {tracks_file_name} from IBTrACS.")
        base_url = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/shapefile/"
        response = requests.get(base_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise Exception(f"Failed to access IBTrACS data source: status code {response.status_code}")

        soup = BeautifulSoup(response.content, "html.parser")
        zip_link = None
        for a in soup.find_all("a", href=True):
            if tracks_file_name in a["href"]:
                zip_link = base_url + a["href"]
                break

        if not zip_link:
            raise Exception(f"ZIP file link for {tracks_file_name} not found on the page.")

        tracks_file_path = os.path.join(gis_path, tracks_file_name)
        zip_response = requests.get(zip_link, timeout=REQUEST_TIMEOUT)
        if zip_response.status_code == 200:
            with open(tracks_file_path, "wb") as f:
                f.write(zip_response.content)
        else:
            raise Exception(f"Failed to download ZIP file: status code {zip_response.status_code}")

        if not zipfile.is_zipfile(tracks_file_path):
            raise Exception("The provided file is not a valid ZIP file.")

        with zipfile.ZipFile(tracks_file_path, "r") as zip_ref:
            zip_ref.extractall(gis_path)

        shapefile_path = os.path.join(gis_path, tracks_file_name.replace(".zip", ".shp"))
        filtered_gdf_1 = gpd.read_file(shapefile_path)

    filtered_gdf_1["datetime"] = pd.to_datetime(filtered_gdf_1["ISO_TIME"], errors="coerce")

    # Determine if IBTrACS or local input was used
    used_ibtracs = not (local_zip_path and os.path.exists(local_zip_path))

    # func for filtering the recent cyclones (expanded window for better coverage)
    now = pd.Timestamp.now()

    if used_ibtracs:
        today = now.normalize()  # Strip time part if needed
        # Use configurable days lookback period
        days_ago = today - pd.Timedelta(days=ibtracs_days)

        filtered_gdf_1 = filtered_gdf_1[
            (filtered_gdf_1["datetime"] >= days_ago) & (filtered_gdf_1["datetime"] <= today)
        ]
        logger.info(
            f"Filtered IBTrACS data to last {ibtracs_days} days: {len(filtered_gdf_1)} rows from {days_ago.date()} to {today.date()}"
        )
        print(filtered_gdf_1)
    else:
        # Local shapefile filter: now to 3 weeks in the future
        max_allowed = now + pd.Timedelta(days=21)
        filtered_gdf_1 = filtered_gdf_1[
            (filtered_gdf_1["datetime"] >= now) & (filtered_gdf_1["datetime"] <= max_allowed)
        ]

    # Validate the data
    required_columns = {"ISO_TIME", "STORM_SPD", "USA_WIND", "NAME", "geometry"}
    missing_columns = required_columns - set(filtered_gdf_1.columns)
    if missing_columns:
        raise ValueError(f"Uploaded data is missing required columns: {missing_columns}")

    filtered_gdf_1["year"] = pd.to_datetime(filtered_gdf_1["ISO_TIME"], errors="coerce").dt.year
    filtered_gdf_1["month"] = pd.to_datetime(filtered_gdf_1["ISO_TIME"], errors="coerce").dt.month

    filtered_gdf_1["date_only"] = pd.to_datetime(filtered_gdf_1["ISO_TIME"], errors="coerce")

    if filtered_gdf_1.empty:
        if used_ibtracs:
            raise ValueError(
                "🛑 No typhoon data qualified from IBTrACS. Only cyclones within the current month are processed."
            )
        else:
            raise ValueError(
                "🛑 No typhoon entries fall within the valid period. Local shapefiles must include dates from today up to 3 weeks in the future."
            )
        return None

    filtered_gdf_1.drop(columns=["date_only"], inplace=True)

    # Filter columns with less than 70% missing data
    filtered_gdf_1 = filtered_gdf_1.loc[:, filtered_gdf_1.isna().mean() < 0.7]

    gdf = filtered_gdf_1
    gdf["datetime"] = pd.to_datetime(gdf["ISO_TIME"], errors="coerce")
    gdf["date_only"] = gdf["datetime"].dt.date

    if gdf.empty:
        raise ValueError("No cyclones within the required pre-processing parameters.")
        print("No cyclones within the required pre-processing parameters.")
        return None

    # Step 3: Clip to EEZ
    # Ensure both GeoDataFrames have the same CRS (required for gpd.clip)
    if gdf.crs is None:
        logger.warning("gdf has no CRS, setting to EPSG:4326")
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    if read_eez.crs is None:
        logger.warning("read_eez has no CRS, setting to EPSG:4326")
        read_eez = read_eez.set_crs("EPSG:4326", allow_override=True)

    # Make sure both are in the same CRS
    if gdf.crs != read_eez.crs:
        logger.info(f"CRS mismatch: gdf CRS={gdf.crs}, read_eez CRS={read_eez.crs}. Converting gdf to match read_eez.")
        gdf = gdf.to_crs(read_eez.crs)

    # Use gpd.clip (same as historical service) - it works with point geometries too
    clipped_gdf = gpd.clip(gdf, read_eez)
    logger.info(f"After clipping to EEZ: {len(clipped_gdf)} rows from {len(gdf)} original rows")

    if clipped_gdf.empty:
        logger.error("No cyclone points found within EEZ after clipping!")
        logger.info(f"gdf bounds: {gdf.total_bounds if not gdf.empty else 'N/A'}")
        logger.info(f"read_eez bounds: {read_eez.total_bounds if not read_eez.empty else 'N/A'}")

    # print(clipped_gdf["NAME"].unique)

    clipped_gdf["datetime"] = pd.to_datetime(clipped_gdf["ISO_TIME"], errors="coerce")
    clipped_gdf["date_only"] = clipped_gdf["datetime"].dt.date

    # Step 4: Calculate storm speed statistics
    clipped_gdf["storm_speed"] = clipped_gdf["STORM_SPD"]

    # Calculate the minimum and maximum dates for each cyclone
    # Preserve NAME field - only fill NaN values
    if "NAME" not in clipped_gdf.columns:
        logger.warning("NAME column missing after clipping - creating UNNAMED entries")
        clipped_gdf["NAME"] = "UNNAMED"
    else:
        # Log unique names before filling NaN
        unique_names_before = clipped_gdf["NAME"].unique()
        logger.info(f"Cyclone names found in EEZ: {unique_names_before}")

        # Only fill NaN/None values, preserve existing names
        clipped_gdf["NAME"] = clipped_gdf["NAME"].fillna("UNNAMED")

        # Log unique names after filling NaN
        unique_names_after = clipped_gdf["NAME"].unique()
        logger.info(f"Cyclone names after processing: {unique_names_after}")
        print(f"📌 Processing cyclones: {unique_names_after}")

    # Spatially filter cyclone points within the EEZ (additional check after clipping)
    # Note: Since clipped_gdf is already clipped to EEZ, this spatial join should include all points
    cyclones_inside_eez = gpd.sjoin(clipped_gdf, read_eez, predicate="within", how="inner")
    logger.info(f"After spatial join (within): {len(cyclones_inside_eez)} points")

    if cyclones_inside_eez.empty:
        logger.warning("No points passed 'within' predicate. Trying 'intersects' instead...")
        cyclones_inside_eez = gpd.sjoin(clipped_gdf, read_eez, predicate="intersects", how="inner")
        logger.info(f"After spatial join (intersects): {len(cyclones_inside_eez)} points")

    cyclone_duration_eez = cyclones_inside_eez.groupby("NAME").agg(
        start_dt=("datetime", "min"), end_dt=("datetime", "max")
    )

    # Calculate duration in hours (not days)
    cyclone_duration_eez["hours_inside_eez"] = (
        cyclone_duration_eez["end_dt"] - cyclone_duration_eez["start_dt"]
    ).dt.total_seconds() / 3600

    # Keep cyclones that span at least 24 hours inside EEZ
    valid_cyclones_eez = cyclone_duration_eez[cyclone_duration_eez["hours_inside_eez"] >= 24].index

    # Log which cyclones passed the 24-hour filter
    logger.info(f"Cyclones meeting 24-hour EEZ requirement: {list(valid_cyclones_eez)}")

    # Log which cyclones were filtered out
    all_cyclone_names = cyclones_inside_eez["NAME"].unique()
    filtered_out = set(all_cyclone_names) - set(valid_cyclones_eez)
    if filtered_out:
        logger.warning(f"Cyclones filtered out (less than 24 hours in EEZ): {list(filtered_out)}")

    filtered_gdf_1 = cyclones_inside_eez[cyclones_inside_eez["NAME"].isin(valid_cyclones_eez)]

    # print(filtered_gdf_1["NAME"].unique)
    if filtered_gdf_1.empty:
        raise ValueError(
            "Typhoons found, but none intersect with the EEZ for at least 24hrs."
            "You may upload a new typhoon track or try again later."
        )
        return None

    daily_stats = (
        filtered_gdf_1.groupby(["date_only", "NAME"])
        .agg(
            stm_spd_mean=("storm_speed", "mean"),
            stm_spd_max=("storm_speed", "max"),
            USA_WIND=("USA_WIND", "max"),
        )
        .reset_index()
    )
    daily_stats["stm_spd_mean"] = daily_stats["stm_spd_mean"].round(1)
    daily_stats["stm_spd_max"] = daily_stats["stm_spd_max"].round(1)
    daily_stats["USA_WIND"] = daily_stats["USA_WIND"].round(1)
    print("📌 Columns in filtered_gdf_1:", filtered_gdf_1.columns.tolist())
    print("📌 daily_stats columns:", daily_stats.columns.tolist())

    # Save results
    storm_speed_stats_path = os.path.join(output_path, "storm_speed_stats.csv")
    daily_stats.to_csv(storm_speed_stats_path, index=False)
    logger.info(f"Saved storm speed statistics to: {storm_speed_stats_path}")

    return gdf, daily_stats, filtered_gdf_1


def compute_baseline_from_static_csv(
    baseline_csv_path: str,
    filtered_gdf_1: gpd.GeoDataFrame,
    days_needed: int = 30,
    max_future_lag: int = 21,
) -> pd.DataFrame:
    """
    Compute per-cyclone baseline averages from a static CSV of boat counts.

    CSV format:
        date_only,0,1,2,3,...
    where 0,1,2,3,... are fishing-ground IDs and each cell is the boat count.

    Logic per cyclone NAME:
      - If start_dt <= last_baseline_date:
            use the 30 MOST RECENT available rows with date_only < start_dt.
      - If start_dt > last_baseline_date but <= last_baseline_date + max_future_lag:
            use the 30 MOST RECENT available rows up to last_baseline_date.
      - If start_dt > last_baseline_date + max_future_lag:
            skip (no reliable baseline).

    Returns one row per NAME:
        NAME, base_0, base_1, ..., base_boats_fishing
    """

    if not os.path.exists(baseline_csv_path):
        raise FileNotFoundError(f"Baseline CSV not found: {baseline_csv_path}")

    df_base = pd.read_csv(baseline_csv_path)
    # handle formats like "1-Jun", "6/5/25", etc.
    # First try parsing with day-month format (assumes current year)
    try:
        # Try day-month format (e.g., "1-Jun")
        parsed_dates = pd.to_datetime(df_base["date_only"], format="%d-%b", errors="coerce")
        # If successful, update year to current year or year_selected
        if not parsed_dates.isna().all():
            current_year = datetime.now().year
            df_base["date_only"] = parsed_dates.apply(lambda x: x.replace(year=current_year) if pd.notna(x) else x)
        else:
            # Try other formats
            df_base["date_only"] = pd.to_datetime(df_base["date_only"], errors="coerce")
    except Exception:
        # Fallback to default parsing
        df_base["date_only"] = pd.to_datetime(df_base["date_only"], errors="coerce")

    # Convert to date object
    df_base["date_only"] = df_base["date_only"].dt.date

    # fishing-ground columns (e.g. '0','1','2','3',...)
    fishing_cols = [c for c in df_base.columns if c != "date_only"]

    last_baseline_date = df_base["date_only"].max()
    print(f"📅 Last baseline date in static CSV: {last_baseline_date}")

    rows = []

    for name, group in filtered_gdf_1.groupby("NAME"):
        start_dt = group["datetime"].min().date()
        print(f"\n🌀 Cyclone {name}: start {start_dt}")

        # ---- CASE 1: cyclone within baseline range (past or present) ----
        if start_dt <= last_baseline_date:
            mask = df_base["date_only"] < start_dt
            ref_for_log = start_dt
            print(f"   ▶ Using 30 days BEFORE {start_dt} as reference window.")

        # ---- CASE 2: cyclone is in the future but not too far ----
        else:
            delta_days = (start_dt - last_baseline_date).days
            print(f"   ▶ Cyclone is {delta_days} days after last baseline date " f"({last_baseline_date}).")

            if delta_days > max_future_lag:
                print(f"   ⏭️ Skipping {name}: more than {max_future_lag} days after " f"last baseline date.")
                continue

            # use last 30 baseline rows up to last_baseline_date
            mask = df_base["date_only"] <= last_baseline_date
            ref_for_log = last_baseline_date
            print(f"   ▶ Using 30 days up to {last_baseline_date} as reference window.")

        # take the MOST RECENT `days_needed` rows among the masked baseline days
        baseline_window = df_base.loc[mask].sort_values("date_only").tail(days_needed)

        if baseline_window.empty:
            print(f"   ❌ No baseline rows found for {name}.")
            continue

        if len(baseline_window) < days_needed:
            print(
                f"   ⚠️ Only {len(baseline_window)} available baseline rows for {name} "
                f"before {ref_for_log} (requested {days_needed})."
            )

        # per-ground mean over those rows
        mean_vals = baseline_window[fishing_cols].mean()
        total_boats = mean_vals.sum()

        row = {"NAME": name, "base_boats_fishing": total_boats}
        for col in fishing_cols:
            # e.g. col '0' -> 'base_0'
            row[f"base_{col}"] = mean_vals[col]

        rows.append(row)

    if not rows:
        raise ValueError("No baseline values could be computed from static CSV.")

    base_averages = pd.DataFrame(rows)

    # round base_* columns (except total)
    for col in base_averages.columns:
        if col.startswith("base_") and col != "base_boats_fishing":
            base_averages[col] = base_averages[col].round()

    logger.info("Finished computing static CSV baselines per cyclone")
    logger.debug(f"Baseline averages:\n{base_averages.head()}")

    return base_averages


def calculate_min_distance(daily_stats, gdf, centers_df_latest, output_path):
    """
    Compute the minimum distance between cyclones and fishing grounds for each date.

    Parameters:
    - daily_stats: DataFrame containing storm speed data with 'date_only' column.
    - gdf: DataFrame containing cyclone points with 'date_only' column.
    - centers_df_latest: GeoDataFrame containing centroids of fishing grounds with 'lat' and 'lon' columns.
    - output_path: Path to save output files.

    Returns:
    - pivot_table3: DataFrame containing the minimum distance per date and contour_id.
    """

    # Convert date columns to datetime format
    daily_stats["date_only"] = pd.to_datetime(daily_stats["date_only"])
    gdf["date_only"] = pd.to_datetime(gdf["date_only"])

    fishing_centroids = centers_df_latest

    # Define a helper function to process a single storm speed DataFrame
    def process_storm_speed(storm_spd_mean_df, fishing_centroids_copy):
        if not storm_spd_mean_df.empty:
            # Merge storm speed data with cyclone data
            typhoon_criteria = pd.merge(storm_spd_mean_df, gdf, on=["date_only", "NAME"])
            logger.debug(f"Typhoon criteria:\n{typhoon_criteria}")

            if not typhoon_criteria.empty and not fishing_centroids_copy.empty:
                # Perform one-to-many merge between centroids and cyclones
                daily_boats_typhoons2 = pd.merge(
                    fishing_centroids_copy.assign(key=1),
                    typhoon_criteria.assign(key=1),
                    on="key",
                    how="outer",
                ).drop(columns="key")

                # Calculate the minimum distance using the Haversine formula
                def haversine_distance(lat, lon, LAT, LON):
                    start_coords = (lat, lon)
                    end_coords = (LAT, LON)
                    return geodesic(start_coords, end_coords).kilometers

                # Add the "distance" column
                daily_boats_typhoons2["distance"] = daily_boats_typhoons2.apply(
                    lambda row: haversine_distance(row["lat"], row["lon"], row["LAT"], row["LON"]),
                    axis=1,
                ).round(1)

                # Group by date and name, find the minimum distance
                grouped = daily_boats_typhoons2.groupby(["date_only", "contour_id", "NAME"])
                result = grouped["distance"].min()

                # Create a pivot table
                pivot_table2 = result.reset_index().pivot(
                    index=["date_only", "NAME"], columns="contour_id", values="distance"
                )
                pivot_table2 = pivot_table2.reset_index()
                return pivot_table2

        # Return an empty DataFrame if the input DataFrame or merge conditions are empty
        return pd.DataFrame(columns=["date_only", "NAME"])

    # Process both DataFrames with independent copies
    fishing_centroids_copy = fishing_centroids.copy()
    pivot_table3 = process_storm_speed(daily_stats, fishing_centroids_copy)

    # Save the results
    pivot_table_path = os.path.join(output_path, "pivot_table_test.csv")
    pivot_table3.to_csv(pivot_table_path, index=False)
    logger.info(f"Saved pivot table to: {pivot_table_path}")
    return pivot_table3


def merge_dfs(daily_stats, pivot_table3):
    daily_stats["date_only"] = pd.to_datetime(daily_stats["date_only"])
    pivot_table3["date_only"] = pd.to_datetime(pivot_table3["date_only"])

    merged_df = daily_stats.merge(pivot_table3, on=["NAME", "date_only"], how="left")
    print("🧪 Columns after merge:", merged_df.columns.tolist())

    columns_to_rename = merged_df.columns[5:]

    # Create a mapping for the last 5 columns
    column_mapping = {col: "distance_" + str(col) for col in columns_to_rename}

    # Rename the columns
    merged_df.rename(columns=column_mapping, inplace=True)

    return merged_df


def calculate_boat_count(row, coefficients):
    boat_counts = {}
    wind = row.get("USA_WIND", 0)
    stm_spd = row.get("stm_spd_mean", 0)

    for g in range(6):
        if f"g{g}" not in coefficients.columns or pd.isnull(coefficients[f"g{g}"][0]):
            continue

        intercept = coefficients.loc[coefficients["model"] == "intercept", f"g{g}"].values[0]
        distance_coeff = coefficients.loc[coefficients["model"] == "distance", f"g{g}"].values[0]
        stm_spd_coeff = coefficients.loc[coefficients["model"] == "stm_spd_mean", f"g{g}"].values[0]
        wind_coeff = coefficients.loc[coefficients["model"] == "USA_WIND", f"g{g}"].values[0]
        wind2 = coefficients.loc[coefficients["model"] == "wind2", f"g{g}"].values[0]
        wind3 = coefficients.loc[coefficients["model"] == "wind3", f"g{g}"].values[0]

        try:
            distance = row[f"distance_{g}"]
        except KeyError:
            distance = 0

        log_boats = (
            intercept
            + (distance_coeff or 0) * distance
            + (stm_spd_coeff or 0) * stm_spd
            + (wind_coeff or 0) * wind
            + (wind2 or 0) * (wind**2)
            + (wind3 or 0) * (wind**3)
        )
        boat_counts[f"predict_g{g}"] = round(math.exp(log_boats))

    return pd.Series(boat_counts)


def nowcast_table(merged_df, base_averages, output_path, country, current_year, coefficients):
    """
    Compares actual boat counts during cyclones with average baseline from clean dates.

    Parameters:
    - merged_df: DataFrame with boat counts during cyclone days.
    - base_averages: Clean period summary with base averages (per cyclone).
    - output_path: Directory where final CSV will be saved.
    - country: ISO3 code (e.g., "phl").
    - current_year: Integer year (e.g., 2025).
    - coefficients: Dictionary of regression coefficients used in modeled count.

    Returns:
    - final_result: DataFrame with base_ prefixed columns and nowcast values.
    """
    for col in base_averages.columns:
        if col.startswith("base_") and col != "base_boats_fishing":
            base_averages[col] = base_averages[col].round()

    pre_final_result = pd.merge(merged_df, base_averages, on="NAME", how="left")

    result = merged_df.apply(calculate_boat_count, axis=1, coefficients=coefficients)

    final_result = pd.concat([pre_final_result, result], axis=1)

    final_result_path = os.path.join(output_path, f"{country}_logdatadf_py_new_{current_year}.csv")
    final_result.to_csv(final_result_path, index=False)
    logger.info(f"Saved final nowcast table to: {final_result_path}")

    return final_result


def generate_map(read_eez, year_selected, country, filtered_gdf_1, fg_df_latest, output_path):
    """
    Generate a map visualization of fishing grounds and typhoon tracks.

    Parameters:
    - read_eez: GeoDataFrame containing EEZ boundaries
    - year_selected: Year for the analysis
    - country: Country code
    - filtered_gdf_1: GeoDataFrame containing filtered cyclone tracks
    - fg_df_latest: GeoDataFrame containing fishing grounds
    - output_path: Directory to save the map

    Returns:
    - map_path: Path to the saved map file
    """
    # Plot the data
    fig, ax = plt.subplots(figsize=(10, 10))

    fg_df_latest.plot(ax=ax, edgecolor="black", facecolor="none")
    filtered_gdf_1.plot(ax=ax, markersize=5, color="red", alpha=0.5, label="Typhoons")

    minx, miny, maxx, maxy = read_eez.total_bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    # Add labels with halo effect for each fishing ground
    for _, row in fg_df_latest.iterrows():
        x, y = (
            row.geometry.centroid.x,
            row.geometry.centroid.y,
        )  # Use centroid of each feature for label placement
        contour_id = row["contour_id"]  # Access contour_id for labeling

        # Add label with halo effect
        ax.annotate(
            text=contour_id,
            xy=(x, y),
            ha="center",
            color="white",  # Text color
            fontsize=8,
            path_effects=[patheffects.withStroke(linewidth=2, foreground="black")],  # Halo effect
        )

    # Try to add OpenStreetMap basemap, with a fallback mechanism
    try:
        ctx.add_basemap(
            ax,
            crs=filtered_gdf_1.crs.to_string(),
            attribution="Map data © OpenStreetMap contributors",
        )
    except Exception as e:
        logger.warning(f"Failed to load OpenStreetMap basemap: {e}")
        logger.info("Switching to CartoDB Positron basemap...")
        try:
            ctx.add_basemap(
                ax,
                crs=filtered_gdf_1.crs.to_string(),
                source=ctx.providers.CartoDB.Positron,
            )
        except Exception as e2:
            logger.warning(f"Failed to load CartoDB Positron basemap: {e2}")
            logger.info("No basemap will be added.")
    # Custom legend entry for the dashed fishing grounds
    fishing_grounds_legend = mpatches.Patch(edgecolor="black", facecolor="none", label="Fishing Grounds")

    # Add legend with custom entry for fishing grounds
    handles, labels = ax.get_legend_handles_labels()
    handles.append(fishing_grounds_legend)
    labels.append("Fishing Grounds (Dashed Outline)")
    ax.legend(
        handles=handles,
        loc="upper left",  # Position the legend in the upper right corner of the map
        borderaxespad=0.5,  # Space between legend and map edge
        frameon=True,  # Add a frame to make it more visible
        fontsize=10,
    )
    current_year = datetime.now().year

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Fishing Grounds in {current_year}")
    map_path = os.path.join(output_path, f"{country}_{current_year}_map.png")
    plt.savefig(map_path)
    plt.close()
    logger.info(f"Map saved to: {map_path}")

    # Return the results
    return map_path


def main(
    config: NowcastConfig, local_zip_path: str | None = None, progress_callback=None, ibtracs_days: int | None = None
):
    """
    Main function for nowcast analysis processing.

    Args:
        config: NowcastConfig object with paths and settings
        local_zip_path: Optional path to local ZIP file for uploaded cyclone tracks
        progress_callback: Optional callback function for progress updates
        ibtracs_days: Optional number of days to look back for IBTrACS data (defaults to 7)
    """
    country = config.country
    year_selected = config.year_selected
    gis_path = config.gis_path
    output_path = config.output_path
    baseline_csv_path = config.baseline_csv_path
    current_year = pd.Timestamp.now().year

    logger.info(f"Starting nowcast analysis for {country} in {year_selected}")
    logger.info(f"Config: {config}")

    def update_progress(phase: int, phase_name: str, message: str):
        """Helper to call progress callback if provided."""
        if progress_callback:
            try:
                progress_callback(phase, phase_name, message)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")

    # Ensure paths exist
    config.ensure_paths_exist()

    # Cubic model coefficients
    data_coefficients = {
        "model": [
            "intercept",
            "distance",
            "stm_spd_mean",
            "USA_WIND",
            "wind2",
            "wind3",
        ],
        "g0": [3.862524, -0.000233, -0.006981, -0.000301, 0.000157, -0.000001],
        "g1": [2.824298, 0.000638, 0.001755, -0.014819, 0.000173, -0.000001],
        "g2": [4.140008, 0.001268, -0.020501, -0.014269, 0.000184, -0.000001],
        "g3": [4.216685, 0.000409, -0.002017, -0.010676, 0.000148, -0.000001],
        "g4": [None, None, None, None, None, None],
        "g5": [None, None, None, None, None, None],
    }
    coefficients = pd.DataFrame(data_coefficients)

    update_progress(1, "Loading GIS data...", "Getting shapefiles from GIS...")
    logger.info("Step 1: Get shapefiles from GIS")
    (
        read_eez,
        _,
        _,
        centers_df_latest,
        _,
        no_ty_file_pivoted_avg_per_contour,
        fg_df_latest,
    ) = get_shapefiles_from_gis(gis_path, country)

    update_progress(2, "Processing cyclone data...", "Processing cyclone data...")
    logger.info("Step 2: Process cyclone data")
    # Use provided ibtracs_days or default to 7
    days_to_use = ibtracs_days if ibtracs_days is not None else 7
    gdf, daily_stats, filtered_gdf_1 = process_cyclone_data(
        country, read_eez, gis_path, output_path, local_zip_path, ibtracs_days=days_to_use
    )

    update_progress(3, "Computing baselines...", "Computing baselines from static CSV...")
    logger.info("Step 3: Compute baselines from static CSV")
    base_averages = compute_baseline_from_static_csv(
        baseline_csv_path=baseline_csv_path,
        filtered_gdf_1=filtered_gdf_1,
        days_needed=30,
    )

    update_progress(3, "Calculating baselines and distances...", "Calculating minimum distances...")
    logger.info("Step 4: Calculate minimum distances")
    pivot_table3 = calculate_min_distance(daily_stats, gdf, centers_df_latest, output_path)

    update_progress(4, "Generating predictions...", "Merging dataframes...")
    logger.info("Step 5: Merge dataframes")
    merged_df = merge_dfs(daily_stats, pivot_table3)

    update_progress(4, "Generating predictions...", "Generating nowcast table...")
    logger.info("Step 6: Generate nowcast table")
    final_result = nowcast_table(merged_df, base_averages, output_path, country, current_year, coefficients)

    update_progress(5, "Creating visualizations...", "Generating map visualization...")
    logger.info("Step 7: Generate map")
    map_path = generate_map(read_eez, year_selected, country, filtered_gdf_1, fg_df_latest, output_path)

    # Create latest symlink
    config.create_latest_symlink()
    logger.info(f"Created latest symlink pointing to: {output_path}")

    logger.info("Nowcast analysis completed successfully")
    return {
        "output_path": output_path,
        "map_path": map_path,
        "final_result_path": os.path.join(output_path, f"{country}_logdatadf_py_new_{current_year}.csv"),
        "storm_speed_stats_path": os.path.join(output_path, "storm_speed_stats.csv"),
        "pivot_table_path": os.path.join(output_path, "pivot_table_test.csv"),
        # Return data needed for database update
        "filtered_gdf_1": filtered_gdf_1,
        "daily_stats": daily_stats,
        "final_result": final_result,
    }


if __name__ == "__main__":
    # Example usage with the new folder structure
    country = "phl"
    year_selected = 2024

    # Use the new NowcastConfig.from_defaults() method
    config = NowcastConfig.from_defaults(
        country=country,
        year_selected=year_selected,
        root_path="data",  # Uses the new data/ folder structure
    )

    # Run the main analysis
    # Set local_zip_path if you have an uploaded cyclone track file
    results = main(config=config, local_zip_path=None)

    logger.info("Output files:")
    for key, path in results.items():
        logger.info(f"  {key}: {path}")
