# flake8: noqa: E501
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass

import contextily as ctx
import geojson
import geopandas as gpd
import matplotlib

from backend.utils.auth import get_access_token

# This is added so that matplotlib does not try to open a window when plotting
# as it will cause an issue with the main GUI window
matplotlib.use("Agg")
from datetime import datetime

import imageio
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from geopy.distance import geodesic
from matplotlib import patheffects
from scipy import stats
from shapely.errors import TopologicalError
from shapely.geometry import Point, Polygon, box, shape
from shapely.validation import make_valid

from backend.utils.helper import (
    get_eog_access_token,
    time_execution,
    update_eog_access_token,
    update_last_run_cyclone_list,
    update_last_run_num_grounds,
)
from backend.utils.logger import get_logger
from config import cyclone_seasons

urllib3.disable_warnings()
logger = get_logger(__name__)
REQUEST_TIMEOUT = int(os.getenv("HTTP_REQUEST_TIMEOUT", "30"))


# from config import (
#     country,
#     cyclone_seasons,
#     gis_path,
#     graphs_path,
#     output_path,
#     viirs_path,
#     year_selected,
# )


@time_execution("downloading VIIRS data")
def download_viirs_data(year_selected, country, viirs_path, cyclone_seasons, overwrite=False):
    """
    Downloads VIIRS daily data files for a specified country and year based on defined cyclone season periods.

    Parameters:
    - year_selected (int): The year for which data is being downloaded.
    - country (str): The country code for which data is needed.
    - viirs_path (str): The local directory path to save downloaded files.
    - cyclone_seasons (dict): Dictionary of cyclone seasons per country, specifying start and end months.

    This function constructs a URL from the VIIRS data source and scrapes available data files matching
    the specified cyclone season. For each matching file, it attempts to download and save the CSV
    to the specified path, logging successes and any download errors.
    """

    # Get current year and month to handle current year data constraints
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

    # Set URLs from the data source (VIIRS)
    access_token = get_access_token()
    headers = {"Authorization": "Bearer " + access_token}
    data_url = f"https://eogdata.mines.edu/wwwdata/viirs_products/vbd/v23/{country}/final/daily/"
    response = requests.get(data_url, headers=headers, timeout=REQUEST_TIMEOUT)
    logger.info(f"Response: {response}")

    if response.status_code != 200:
        logger.error(f"Failed to access data URL: {data_url}. Status code: {response.status_code}")
        return

    html = response.text
    logger.info(f"VIIRS Path: {viirs_path}")

    csv_files = []

    if country in cyclone_seasons:
        seasons = cyclone_seasons[country] if isinstance(cyclone_seasons[country], list) else [cyclone_seasons[country]]

        for season in seasons:
            start_month = season["start_month"]
            end_month = season["end_month"]

            if year_selected < current_year:
                # For past years, download data for the full cyclone season
                if start_month <= end_month:
                    months_pattern = f"({'|'.join(f'{month:02d}' for month in range(start_month, end_month + 1))})"
                    pattern = rf'href="(VBD_npp_d{year_selected}{months_pattern}(0[1-9]|[12][0-9]|3[01])_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    csv_files.extend(re.findall(pattern, html))
                else:
                    months_pattern_current_year = f"({'|'.join(f'{month:02d}' for month in range(start_month, 13))})"
                    months_pattern_next_year = f"({'|'.join(f'{month:02d}' for month in range(1, end_month + 1))})"
                    pattern_current_year = rf'href="(VBD_npp_d{year_selected}{months_pattern_current_year}(0[1-9]|[12][0-9]|3[01])_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    pattern_next_year = rf'href="(VBD_npp_d{year_selected + 1}{months_pattern_next_year}(0[1-9]|[12][0-9]|3[01])_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    csv_files.extend(re.findall(pattern_current_year, html) + re.findall(pattern_next_year, html))
            else:
                # For the current year, limit to cyclone season months up to current month/day
                if start_month <= end_month:
                    # If within a single year
                    months_pattern = f"({'|'.join(f'{month:02d}' for month in range(start_month, current_month + 1))})"
                    days_pattern = (
                        "(0[1-9]|[12][0-9]|3[01])"
                        if current_month > end_month
                        else f"(0[1-9]|[12][0-9]|{current_day:02d})"
                    )
                    pattern = rf'href="(VBD_npp_d{year_selected}{months_pattern}{days_pattern}_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    csv_files.extend(re.findall(pattern, html))
                else:
                    # Handles year-spanning seasons
                    months_pattern_current_year = f"({'|'.join(f'{month:02d}' for month in range(start_month, 13))})"
                    days_pattern_current = (
                        "(0[1-9]|[12][0-9]|3[01])" if current_month > 12 else f"(0[1-9]|[12][0-9]|{current_day:02d})"
                    )
                    months_pattern_next_year = f"({'|'.join(f'{month:02d}' for month in range(1, end_month + 1))})"
                    pattern_current_year = rf'href="(VBD_npp_d{year_selected}{months_pattern_current_year}{days_pattern_current}_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    pattern_next_year = rf'href="(VBD_npp_d{year_selected + 1}{months_pattern_next_year}(0[1-9]|[12][0-9]|3[01])_{country}_noaa_ops_v23\.csv(\.gz)?)"'
                    csv_files.extend(re.findall(pattern_current_year, html) + re.findall(pattern_next_year, html))

    # Check if any files were found; if none, log an error and exit
    if not csv_files:
        logger.error(
            f"No VIIRS data files available for {country} in {year_selected} within the defined cyclone period."
        )
        raise Exception(
            f"No VIIRS data files available for {country} in {year_selected} within the defined cyclone period."
        )

    for csv_file in set(csv_files):
        csv_file_name = csv_file[0]
        logger.info(f"Found file: {csv_file_name}")
        csv_url = data_url + csv_file_name
        csv_file_path = os.path.join(viirs_path, csv_file_name)
        logger.info(f"CSV file path: {csv_file_path}")

        if not overwrite and os.path.exists(csv_file_path):
            logger.info("File already exists and overwrite is set to False. Skipping...")
            continue

        response = requests.get(csv_url, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code == 200:
            os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

            with open(csv_file_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded and saved: {csv_file_path}")
        else:
            logger.error(f"Failed to download {csv_url}. Status code: {response.status_code}")


# Step 2: Merge VIIRS data
@time_execution("merging VIIRS data")
def merge_viirs_data(viirs_path, year_selected, country, output_path):
    """
    Merges downloaded VIIRS data files for a specified country and year into a single DataFrame and saves the result.

    Parameters:
    - viirs_path (str): Directory path containing the downloaded VIIRS data files.
    - year_selected (int): The year of data to be merged.
    - country (str): The country code to filter files.
    - output_path (str): Directory path to save the merged output file.

    This function checks for files in the specified directory that match the provided year and country pattern.
    It reads each file, concatenates them into a single DataFrame, and saves the merged result as a CSV.
    """

    df_append = pd.DataFrame()
    list_df = []

    # Create a regex pattern to match filenames with the selected year and country
    file_pattern_csv = f"VBD_npp_d{year_selected}.*_{country}_noaa_ops_v23\\.csv"
    file_pattern_gz = f"VBD_npp_d{year_selected}.*_{country}_noaa_ops_v23\\.csv\\.gz"

    # Check if the directory exists
    if os.path.exists(viirs_path):
        viirs_files = os.listdir(viirs_path)
        try:
            for file in viirs_files:
                # Check if the file matches either the .csv or .csv.gz pattern
                if re.match(file_pattern_csv, file) or re.match(file_pattern_gz, file):
                    full_file_path = os.path.join(viirs_path, file)
                    if file.endswith(".gz"):
                        data = pd.read_csv(full_file_path, compression="gzip")
                    else:
                        data = pd.read_csv(full_file_path)
                    list_df.append(data)

            if list_df:  # If there are matching files
                # Merge all VIIRS data into one dataframe
                df_append = pd.concat(list_df)
                df_append.to_csv(os.path.join(output_path, f"df_append_{country}_{year_selected}.csv"))
                logger.info(df_append.head(3))  # Print the head of the dataframe
            else:
                logger.info(f"No files found for country: {country} and year: {year_selected}")

        except FileNotFoundError as e:
            logger.error(e)

    else:
        logger.info(f"The directory {viirs_path} does not exist.")

    return df_append


# Step 3: Download and process cyclone data
@time_execution("downloading and processing cyclone data")
def download_and_process_cyclone_data(gis_path, cyclone_seasons, country, year_selected):
    """
    Downloads and processes IBTrACS cyclone data for a specified country and year, filtering it by cyclone season.

    Parameters:
    - gis_path (str): Path for storing GIS data, including shapefiles.
    - cyclone_seasons (dict): Dictionary with cyclone season start and end months for each country.
    - country (str): Country code for which cyclone data is being processed.
    - year_selected (int): Year for the data processing and filtering.

    This function downloads the IBTrACS cyclone (pts) shapefile data, extracts the ZIP, and filters records to the specified
    country's cyclone season for the chosen year. The filtered data is saved as a new shapefile and returned as a
    GeoDataFrame. An error is raised if the cyclone season or ZIP file link is missing.
    """

    # Determine URL based on current/last 3 years or previous year
    current_year = datetime.now().year
    if year_selected >= current_year - 2:
        tracks_file_name = "IBTrACS.last3years.list.v04r01.points.zip"
    else:
        tracks_file_name = "IBTrACS.since1980.list.v04r01.points.zip"

    logger.info(f"Downloading {tracks_file_name} for {country} in {year_selected}.")

    # Base URL for downloading IBTrACS data
    base_url = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/shapefile/"

    # Path to the downloaded zip file
    shapefile_path = os.path.join(gis_path, tracks_file_name.replace(".zip", ".shp"))

    t0 = time.perf_counter()

    # Download the HTML page
    response = requests.get(base_url, timeout=REQUEST_TIMEOUT)
    if response.status_code != 200:
        logger.error(f"Failed to download page: status code {response.status_code}")
        raise Exception(f"Failed to download page: status code {response.status_code}")

    # Parse the HTML page to find the ZIP file link
    soup = BeautifulSoup(response.content, "html.parser")
    zip_link = None
    for a in soup.find_all("a", href=True):
        if tracks_file_name in a["href"]:
            zip_link = base_url + a["href"]
            break

    if not zip_link:
        logger.error("ZIP file link not found on the page.")
        raise Exception("ZIP file link for {tracks_file_name} not found on the page.")

    # Download the ZIP file
    logger.info(f"Downloading ZIP file: {zip_link}")
    zip_response = requests.get(zip_link, timeout=REQUEST_TIMEOUT)
    if zip_response.status_code == 200:
        tracks_file_path = os.path.join(gis_path, tracks_file_name)
        logger.info(f"Saving ZIP file to: {tracks_file_path}")
        with open(tracks_file_path, "wb") as f:
            f.write(zip_response.content)
    else:
        logger.error(f"Failed to download ZIP file: status code {zip_response.status_code}")
        raise Exception(f"Failed to download ZIP file: status code {zip_response.status_code}")
    t1 = time.perf_counter()
    logger.info(f"Downloaded ZIP file in {t1 - t0:.2f} seconds.")

    t0 = time.perf_counter()
    # Ensure the downloaded file is a valid ZIP file
    if zipfile.is_zipfile(tracks_file_path):
        # Unzip the file inside the shapefile directory
        with zipfile.ZipFile(tracks_file_path, "r") as zip_ref:
            zip_ref.extractall(gis_path)
    else:
        raise Exception("The downloaded file is not a valid ZIP file.")
    t1 = time.perf_counter()
    logger.info(f"Unzipped file in {t1 - t0:.2f} seconds.")

    logger.info("Processing cyclone data.")
    t0 = time.perf_counter()
    gdf = gpd.read_file(shapefile_path)
    t1 = time.perf_counter()
    logger.info(f"Read shapefile in {t1 - t0:.2f} seconds.")

    # Convert ISO_TIME to datetime and extract year and month
    gdf["datetime"] = pd.to_datetime(gdf["ISO_TIME"], errors="coerce")
    gdf["year"] = gdf["datetime"].dt.year
    gdf["month"] = gdf["datetime"].dt.month

    # Get the cyclone season for the selected country
    if country not in cyclone_seasons:
        raise ValueError(f"Country '{country}' is not defined in the cyclone seasons.")
    season_start_month = cyclone_seasons[country]["start_month"]
    season_end_month = cyclone_seasons[country]["end_month"]

    # Define a function to filter for a specific cyclone season
    def filter_for_season(gdf, start_month, end_month, year_selected):
        def is_within_season(row):
            month = row["month"]
            year = row["year"]
            # Check if the month falls within the cyclone season period
            if start_month <= end_month:
                # For seasons within the same year
                return year == year_selected and start_month <= month <= end_month
            else:
                # For seasons spanning two years
                return (year == year_selected and month >= start_month) or (
                    year == year_selected + 1 and month <= end_month
                )

        return gdf[gdf.apply(is_within_season, axis=1)]

    # Apply cyclone season filter and create the GeoDataFrame
    logger.info("Filtering for the cyclone season.")
    t0 = time.perf_counter()
    filtered_gdf = filter_for_season(gdf, season_start_month, season_end_month, year_selected)
    filtered_gdf["row_id"] = filtered_gdf.index + 1
    filtered_gdf.reset_index(drop=True, inplace=True)
    filtered_gdf.set_index("row_id", inplace=True)

    filtered_gdf = filtered_gdf.set_crs("EPSG:4326", allow_override=True)

    # Drop 'datetime' column before saving to avoid issues with shapefile format
    filtered_gdf.drop(columns=["datetime"], inplace=True)
    t1 = time.perf_counter()
    logger.info(f"Filtered for cyclone season in {t1 - t0:.2f} seconds.")

    # Define the path for saving the filtered shapefile
    filtered_tracks_file_path = os.path.join(gis_path, f"IBTrACS_{year_selected}.shp")

    logger.info("Saving processed track to shapefile.")
    # Save the filtered shapefile
    t0 = time.perf_counter()
    filtered_gdf.to_file(filtered_tracks_file_path)
    t1 = time.perf_counter()
    logger.info(f"Saving done in {t1 - t0:.2f} seconds.")
    return filtered_gdf

    # Read the filtered shapefile
    if os.path.exists(filtered_tracks_file_path):
        read_filtered_tracks = gpd.read_file(filtered_tracks_file_path)
        return read_filtered_tracks
        # print(read_filtered_tracks)
    else:
        raise FileNotFoundError(f"Filtered tracks file does not exist: {filtered_tracks_file_path}")


# Function to get the shapefiles from GIS directory
@time_execution("getting shapefiles from GIS directory")
def get_shapefiles_from_gis(gis_path, country):
    """
    Retrieves specific shapefiles (country's EEZ, wrddsf, wrdph) from a specified GIS directory.

    Parameters:
    - gis_path (str): Path to the directory containing GIS shapefiles.

    This function scans the directory for shapefiles, loading the EEZ shapefile for the specified country,
    as well as `wrddsf` and `wrdph` files if they are present. Each shapefile is read into a GeoDataFrame
    and returned, or `None` if the file is not found.
    """

    # If application is packaged, use internal gis path
    if hasattr(sys, "_MEIPASS"):
        gis_path = os.path.join(sys._MEIPASS, "gis")

    (
        read_eez,
        wrddsf,
        wrdph,
        centers_df_latest,
        avg_daily_latest,
        no_ty_file_pivoted_avg_per_contour,
        fg_df_latest,
    ) = (
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )

    # Define paths for organized structure
    country_gis_path = os.path.join(gis_path, "countries", country)
    eez_path = os.path.join(country_gis_path, "eez")
    fishing_grounds_path = os.path.join(country_gis_path, "fishing_grounds")
    centroids_path = os.path.join(country_gis_path, "centroids")
    baselines_path = os.path.join(country_gis_path, "baselines")

    # Load EEZ shapefile
    eez_file = os.path.join(eez_path, f"{country}_eez.shp")
    if os.path.exists(eez_file):
        read_eez = gpd.read_file(eez_file).set_crs("EPSG:4326", allow_override=True)
        logger.info(f"Loaded EEZ for {country}")

    # Load wrddsf and wrdph from root gis directory (shared across countries)
    wrddsf_file = os.path.join(gis_path, "wrddsf.shp")
    if os.path.exists(wrddsf_file):
        wrddsf = gpd.read_file(wrddsf_file)
        logger.info("Loaded wrddsf")

    wrdph_file = os.path.join(gis_path, "wrdph.shp")
    if os.path.exists(wrdph_file):
        wrdph = gpd.read_file(wrdph_file)
        logger.info("Loaded wrdph")

    # Load centroids
    centroids_file = os.path.join(centroids_path, "polygon_centroids_historical.csv")
    if os.path.exists(centroids_file):
        centers_df_latest = pd.read_csv(centroids_file)
        print("CSV file 'polygon_centroids_historical.csv' loaded.")
        print(centers_df_latest)

    # Load baseline files
    avg_daily_file = os.path.join(baselines_path, "avg_daily_boats_noty_phl_2023.csv")
    if os.path.exists(avg_daily_file):
        avg_daily_latest = pd.read_csv(avg_daily_file)
        print(avg_daily_latest)
        print("CSV file 'avg_daily_boats_noty_phl_2023.csv' loaded.")

    no_ty_file = os.path.join(baselines_path, "no_ty_file_pivoted_avg_per_contour.csv")
    if os.path.exists(no_ty_file):
        no_ty_file_pivoted_avg_per_contour = pd.read_csv(no_ty_file)
        print(no_ty_file_pivoted_avg_per_contour)
        print("CSV file 'no_ty_file_pivoted_avg_per_contour' loaded.")

    # Load fishing grounds (look for any geojson file in fishing_grounds directory)
    if os.path.exists(fishing_grounds_path):
        geojson_files = [f for f in os.listdir(fishing_grounds_path) if f.endswith(".geojson")]
        if geojson_files:
            fg_file = os.path.join(fishing_grounds_path, geojson_files[0])
            fg_df_latest = gpd.read_file(fg_file)
            print(fg_df_latest)
            print(f"Fishing grounds loaded from: {geojson_files[0]}")

    return (
        read_eez,
        wrddsf,
        wrdph,
        centers_df_latest,
        avg_daily_latest,
        no_ty_file_pivoted_avg_per_contour,
        fg_df_latest,
    )


# Function to read the filtered IBTrACS shapefile
@time_execution("reading filtered IBTrACS shapefile")
def read_filtered_tracks(gis_path, year_selected):
    """
    Reads the filtered IBTrACS cyclone shapefile for a specified year from the GIS directory.

    Parameters:
    - gis_path (str): Path to the GIS directory containing the filtered shapefile.
    - year_selected (int): Year of the cyclone data to read.

    This function checks for the existence of the filtered shapefile (`IBTrACS_{year_selected}.shp`),
    loading it as a GeoDataFrame if found. If the file does not exist, it raises a `FileNotFoundError`.
    """

    filtered_tracks_file_path = os.path.join(gis_path, f"IBTrACS_{year_selected}.shp")
    if os.path.exists(filtered_tracks_file_path):
        read_filtered_tracks = gpd.read_file(filtered_tracks_file_path)
        return read_filtered_tracks
    else:
        raise FileNotFoundError(f"Filtered tracks file does not exist: {filtered_tracks_file_path}")


# Function to post-process the VIIRS data
@time_execution("post-processing VIIRS data")
def post_process_viirs_data(df_append, output_path):
    """
    Post-processes the VIIRS data by cleaning, transforming, and saving it.

    Parameters:
    df_append (pd.DataFrame): Raw VIIRS data.
    output_path (str): Path to save the processed data.
    """
    df_append = df_append.dropna(subset=["Date_Mscan"])
    df_append = df_append[df_append["QF_Detect"].isin([1, 2, 3, 8, 10])]
    df_append = df_append.drop_duplicates()
    df_all_sf = gpd.GeoDataFrame(
        df_append.set_geometry(
            gpd.points_from_xy(df_append["Lon_DNB"], df_append["Lat_DNB"]),
            crs="EPSG:4326",
        )
    )
    df_all_sf.rename({"Date_Mscan": "ISO_TIME"}, axis=1, inplace=True)

    t = df_all_sf.reset_index(drop=True)
    t["ISO_TIME"] = pd.to_datetime(t["ISO_TIME"])
    t["date_only"] = t["ISO_TIME"].dt.date
    t["date_only"] = pd.to_datetime(t["date_only"])
    t.to_csv(os.path.join(output_path, "t_processed.csv"), index=False)

    return t


# Function to initiate post-processing of IBTrACS data
def create_lin11d(lin11, year_selected, country, cyclone_seasons):
    """
    Filters cyclone track data for a specified country, year, and defined cyclone season. This also removes all cyclones without a name ('NOT_NAMED' under `NAME`).

    Parameters:
    - lin11 (DataFrame): Data containing cyclone track records with an `ISO_TIME` column.
    - year_selected (int): The primary year to filter the cyclone data.
    - country (str): Country code used to look up the cyclone season.
    - cyclone_seasons (dict): Dictionary with start and end months for each country's cyclone season.

    This function converts `ISO_TIME` to a datetime format, then filters records to include only those
    within the cyclone season (either within the same year or spanning to the next year). It returns
    a filtered DataFrame with an additional `date_only` column for the date portion of `ISO_TIME`, and excluding unnamed cyclones.
    """

    season = cyclone_seasons[country]
    start_month = season["start_month"]
    end_month = season["end_month"]
    end_year = year_selected + 1 if start_month > end_month else year_selected

    logger.info(f"Filtering for country: {country}, Year: {year_selected}")
    logger.info(f"Start month: {start_month}, End month: {end_month}, End year: {end_year}")
    # logger.info(lin11)
    logger.info(f"ISO_TIME dtype before conversion: {lin11['year'].dtype}")

    lin11["ISO_TIME"] = pd.to_datetime(lin11["ISO_TIME"], errors="coerce")
    logger.info(f"ISO_TIME dtype after conversion: {lin11['ISO_TIME'].dtype}")
    logger.info(f"lin11 after datetime conversion: {lin11.head(3)}\n")

    lin11 = lin11[lin11["NAME"] != "UNNAMED"]

    lin11d = lin11[
        ((lin11["year"] == year_selected) & (lin11["month"] >= start_month))
        | ((lin11["year"] == end_year) & (lin11["month"] <= end_month))
    ]

    lin11d["ISO_TIME"] = pd.to_datetime(lin11d["ISO_TIME"])

    logger.info(f"Rows after filtering: {lin11d.shape[0]}")

    lin11d["date_only"] = lin11d["ISO_TIME"].dt.date
    lin11d["date_only"] = pd.to_datetime(lin11d["date_only"])
    # logger.info(lin11d)
    logger.info(f"lin11 after datetime conversion: {lin11.head(3)}\n")
    return lin11d


# Function to post-process the IBTrACS data
@time_execution("post-processing IBTrACS data")
def post_process_typhoon_tracks(filtered_tracks, cyclone_seasons, year_selected, country, read_eez, output_path):
    """
    Post-processes typhoon tracks data by filtering and preparing it for analysis within a specified region (EEZ).

    Parameters:
    - filtered_tracks (GeoDataFrame): Typhoon track data to be processed.
    - cyclone_seasons (dict): Dictionary specifying cyclone season start and end months for each country.
    - year_selected (int): Year of interest for filtering.
    - country (str): Country code used to look up cyclone season details.
    - read_eez (GeoDataFrame): EEZ boundary used for spatial clipping.
    - output_path (str): Path for saving the processed output, if needed.

    This function removes columns from `filtered_tracks` with more than 70% missing values, converts `ISO_TIME` to datetime,
    filters records by cyclone season, and spatially clips the data to the specified EEZ region. The function returns
    the filtered DataFrame (`lin11d`) and the spatially clipped GeoDataFrame (`lin11b`).
    """
    lin11 = filtered_tracks.loc[:, filtered_tracks.isna().mean() < 0.7]
    logger.info(f"Initial filtered_tracks shape: {filtered_tracks.shape}")
    logger.info(f"Shape after removing columns with >70% missing values: {lin11.shape}")

    lin11["ISO_TIME"] = pd.to_datetime(lin11["ISO_TIME"])
    logger.info(f"Number of rows in lin11 after datetime conversion: {lin11.shape[0]}")

    lin11d = create_lin11d(lin11, year_selected, country, cyclone_seasons)
    lin11d.to_csv(os.path.join(output_path, f"lin11d_{country}_{year_selected}.csv"))
    lin11d["date_only"] = pd.to_datetime(lin11d["date_only"])
    # lin11d = convert_to_geodataframe(lin11d)

    logger.info(f"Number of rows in lin11d: {lin11d.shape[0]}")

    lin11b = gpd.clip(lin11d, read_eez)
    logger.info(f"Number of rows in lin11b: {lin11b.shape[0]}")

    # return lin11d, unique_dates_td, td
    return lin11d, lin11b


def get_start_date(year_selected, start_month):
    """
    Generates a start date based on the specified year and month.

    Parameters:
    - year_selected (int): The year for the start date.
    - start_month (int): The month for the start date.

    This function returns a `date` object representing the first day of the specified month and year.
    """

    start_date = pd.to_datetime(f"{year_selected}-{start_month:02d}-01").date()
    logger.info(f"Dynamic start_date: {start_date}")
    return start_date


# Post-process the data into the selected country
@time_execution("processing data for country")
def process_data_for_country(country, year_selected, t, lin11d, read_eez, cyclone_seasons, output_path):
    """
    Processes cyclone track data for a specified country and year, filtering data by cyclone season and region (EEZ).

    Parameters:
    - country (str): Country code for which data is processed.
    - year_selected (int): The target year for data processing.
    - t (GeoDataFrame): Original cyclone track data for the given country and year.
    - lin11d (DataFrame): Cyclone data filtered by month and year.
    - read_eez (GeoDataFrame): EEZ boundary used for spatial clipping.
    - cyclone_seasons (dict): Dictionary defining cyclone season start and end months for each country.
    - output_path (str): Path to save processed files.

    This function filters data based on the cyclone season and spatially clips it to the EEZ. It creates a DataFrame
    of filtered points (`td`) and returns unique dates from `td`, along with the spatially filtered and seasonally filtered data.
    """

    start_month = cyclone_seasons[country]["start_month"]
    end_month = cyclone_seasons[country]["end_month"]
    start_date = get_start_date(year_selected, start_month)
    end_year = year_selected + 1 if start_month > end_month else year_selected

    df_all_b = gpd.clip(t, read_eez)
    logger.info(f"Number of rows in df_all_b_1: {df_all_b.shape[0]}")
    df_all_b["ISO_TIME"] = pd.to_datetime(df_all_b["ISO_TIME"], utc=False)
    df_all_b["date_only"] = df_all_b["ISO_TIME"].dt.date
    df_all_b.to_csv(
        os.path.join(output_path, f"df_all_b_{country}_{year_selected}.csv"),
        index=False,
    )

    # Filtering data within the cyclone season
    all_filtered = pd.DataFrame()
    for year in range(year_selected, end_year + 1):
        for month in range(
            start_month if year == year_selected else 1,
            end_month + 1 if year == end_year else 13,
        ):
            filtered_cyclone_points = filter_cyclone_points(lin11d, read_eez, month, output_path, year_selected)
            all_filtered = pd.concat([all_filtered, filtered_cyclone_points], ignore_index=True)

    # Save to intermediate output directory
    all_filtered.to_csv(os.path.join(output_path, f"all_filtered_{year_selected}.csv"), index=False)

    td = df_all_b[df_all_b["date_only"] >= start_date]

    if "date_only" not in td.columns:
        td["date_only"] = td["ISO_TIME"].dt.date

    td.to_csv(os.path.join(output_path, f"td_{country}_{year_selected}.csv"), index=False)
    logger.info(f"Number of rows in td: {td.shape[0]}")

    # Get unique dates in the 'date_only' column of td
    unique_dates_td = td["date_only"].unique()

    return unique_dates_td, td, all_filtered, df_all_b


# Function to post-process boats and typhoons -> basically get the statistics of boats given the typhoon presence
@time_execution("post-processing boats and typhoons")
def post_process_boats_and_typhoons(lin11d, td, read_eez, year_selected, country, output_path):
    """Post-processing the boats and typhoons in preparation for the generation of fishing grounds."""

    # Get the bounding box of the EEZ
    bbox2 = read_eez.total_bounds
    logger.info(f"Bounding Box Coordinates: {bbox2}")
    bbox_geom2 = box(*bbox2)  # Create a bounding box geometry
    logger.info(f"Bounding Box Geometry: {bbox_geom2}")

    # Clip the data using the bounding box
    # logger.info(lin11d)
    logger.info(f"Number of rows in lin11d: {lin11d.shape[0]}")
    lin11d_clipped = lin11d[lin11d.intersects(bbox_geom2)]
    lin11d_clipped.to_csv(os.path.join(output_path, f"lin1dd_clipped_{country}_{year_selected}.csv"))
    logger.info(f"Number of rows in lin11d_clipped: {lin11d_clipped.shape[0]}")

    lin11d_clipped["ISO_TIME"] = pd.to_datetime(lin11d_clipped["ISO_TIME"], errors="coerce")

    # Ensure 'date_only' column exists and is in datetime format
    if "date_only" not in lin11d_clipped.columns:
        lin11d_clipped["date_only"] = lin11d_clipped["ISO_TIME"].dt.date
    else:
        # Convert 'date_only' to datetime format if it already exists
        lin11d_clipped["date_only"] = pd.to_datetime(lin11d_clipped["date_only"], errors="coerce").dt.date

    # lin11d_clipped["ISO_TIME"] = pd.to_datetime(lin11d_clipped["date_only"]).dt.date
    unique_dates = lin11d_clipped["date_only"].astype(str).unique()
    logger.info(f"Number of rows in unique_dates: {unique_dates.shape[0]}")
    # Getting the clipped boats without typhoon experience
    print(td.head())
    td["date_only"] = pd.to_datetime(td["date_only"]).dt.date

    unique_dates = pd.to_datetime(unique_dates).date
    logger.info(f"Number of rows in unique_dates: {len(unique_dates)}")

    boats_no_typhoons = td[~td["date_only"].isin(unique_dates)]
    boats_no_typhoons.to_csv(os.path.join(output_path, f"{country}_{year_selected}_boats_no_ty.csv"))

    # Getting the clipped boats with typhoon experience
    boats_typhoons = td[td["date_only"].isin(unique_dates)]
    boats_typhoons.to_csv(os.path.join(output_path, f"{country}_{year_selected}_boats_ty.csv"))

    logger.info(f"Number of boats without typhoon occurrence: {boats_no_typhoons.shape[0]}")
    logger.info(f"Number of boats with typhoon occurrence: {boats_typhoons.shape[0]}")

    # Generating statistics of monthly number of boats with and without typhoon experience
    boats_no_typhoons["date_only"] = pd.to_datetime(boats_no_typhoons["date_only"])
    boats_per_month = boats_no_typhoons.groupby(boats_no_typhoons["date_only"].dt.to_period("M")).size().reset_index()
    boats_per_month.columns = ["Month", "Boats_Count_No_ty"]
    boats_per_month.to_csv(os.path.join(output_path, f"{country}_{year_selected}_sum_boats_no_ty.csv"))

    boats_typhoons["date_only"] = pd.to_datetime(boats_typhoons["date_only"])
    boats_ty_per_month = boats_typhoons.groupby(boats_typhoons["date_only"].dt.to_period("M")).size().reset_index()
    boats_ty_per_month.columns = ["Month", "Boats_Count_With_ty"]
    boats_ty_per_month.to_csv(os.path.join(output_path, f"{country}_{year_selected}_sum_boats_with_ty.csv"))

    # Count of boats per dateonly (no_typhoons happened)
    boats_per_date = boats_no_typhoons["date_only"].value_counts().reset_index()
    boats_per_date.columns = ["date_only", "count"]

    # Count of boats per dateonly (typhoons happened)
    boats_per_date_ty = boats_typhoons["date_only"].value_counts().reset_index()
    boats_per_date_ty.columns = ["date_only", "count"]

    return (
        lin11d_clipped,
        boats_no_typhoons,
        boats_typhoons,
        boats_per_month,
        boats_ty_per_month,
        boats_per_date,
        boats_per_date_ty,
    )


# Function to determine fishing grounds
@time_execution("determining fishing grounds")
def determine_fishing_grounds(boats_no_typhoons, year_selected, country, fg_df_latest, output_path):
    """
    Identifies fishing grounds based on the density of fishing boats detected in the absence of typhoons, using Kernel Density Estimation (KDE).

    Parameters:
    - boats_no_typhoons (DataFrame): Data containing fishing boat locations (latitude and longitude) when no typhoons occurred.
    - year_selected (int): Year of data analysis.
    - country (str): Country code used for naming outputs.

    This function filters out negative longitude values and applies KDE to fishing boat locations to identify high-density areas.
    Polygons representing fishing grounds are saved as GeoJSON files and merged to create a map of identified fishing grounds.
    The function returns the merged GeoDataFrame, clipped GeoDataFrame, and the path to the saved map image.
    """

    # Filter out negative longitude values
    boats_no_typhoons_filtered = boats_no_typhoons[boats_no_typhoons["Lon_DNB"] > 0]

    # Setting the long and lat from the boats_no_typhoons dataframe
    x_coords = boats_no_typhoons_filtered["Lon_DNB"].values
    y_coords = boats_no_typhoons_filtered["Lat_DNB"].values

    current_year = pd.Timestamp.now().year

    # If the current year is chosen, use the provided GeoDataFrame directly
    if year_selected == current_year:
        merged_gdf = fg_df_latest
        logger.info("Using 'grounds_latest.geojson' for current year clipping.")
    else:
        # Perform kernel density estimation
        kde = stats.gaussian_kde([x_coords, y_coords])

        # Define the grid over which to evaluate the KDE
        x_grid, y_grid = np.mgrid[
            x_coords.min() : x_coords.max() : 100j,
            y_coords.min() : y_coords.max() : 100j,
        ]

        # Evaluate the KDE on the grid
        kde_values = kde(np.vstack([x_grid.ravel(), y_grid.ravel()]))

        # Reshape the KDE values back to the grid shape
        kde_values = kde_values.reshape(x_grid.shape)

        # Set the density threshold to identify highest density points -> main fishing grounds, contour form, using 50% threshold
        density_threshold = np.percentile(kde_values, 90)

        # Find the contour lines
        contour_lines = plt.contour(x_grid, y_grid, kde_values, levels=[density_threshold])

        # Initialize a list to hold polygons
        polygons = []

        # Loop through each contour path and extract the vertices
        # Note: In matplotlib 3.8+, use get_paths() directly instead of collections[0].get_paths()
        try:
            # Try new API first (matplotlib 3.8+)
            paths = contour_lines.get_paths()
        except AttributeError:
            # Fallback to old API (matplotlib < 3.8)
            if len(contour_lines.collections) > 0:
                paths = contour_lines.collections[0].get_paths()
            else:
                logger.warning("No contour collections found")
                paths = []

        for contour_path in paths:
            polygon_vertices = contour_path.vertices
            # Only create polygon if we have at least 3 vertices
            if len(polygon_vertices) >= 3:
                polygons.append(Polygon(polygon_vertices))
            else:
                logger.warning(f"Skipping contour with only {len(polygon_vertices)} vertices")

        # Validate and repair geometries
        valid_polygons = [make_valid(p) for p in polygons]

        # Save number of polygon grounds
        logger.info(f"Number of fishing grounds: {len(valid_polygons)}")
        update_last_run_num_grounds(len(valid_polygons))

        # Create a GeoDataFrame from valid polygons
        shapely_polygons = [shape(p) for p in valid_polygons]
        merged_features = []
        merged_dict = {i: False for i in range(len(shapely_polygons))}  # Track merged polygons
        contour_id = 0  # Initialize contour ID

        i = 0
        while i < len(shapely_polygons):
            if merged_dict[i]:
                i += 1
                continue

            current_polygon = shapely_polygons[i]
            j = i + 1
            while j < len(shapely_polygons):
                if merged_dict[j]:
                    j += 1
                    continue

                if current_polygon.intersects(shapely_polygons[j]):
                    try:
                        current_polygon = current_polygon.union(shapely_polygons[j])
                        merged_dict[j] = True
                    except TopologicalError as e:
                        print(f"Error merging polygons {i} and {j}: {e}")
                j += 1

            # Add merged or individual polygon to features with contour ID
            merged_features.append(geojson.Feature(geometry=current_polygon, properties={"contour_id": contour_id}))
            merged_dict[i] = True
            contour_id += 1
            i += 1

        # Create a GeoDataFrame from the merged features
        merged_gdf = gpd.GeoDataFrame.from_features(merged_features, crs="EPSG:4326")
        print("Using generated merged polygons for past year clipping with 'contour_id' labeling.")

        # Output the merged polygons as a GeoJSON file
        merged_geojson_file = os.path.join(output_path, f"{country}_merged_dense_area_polygons_{year_selected}.geojson")
        merged_gdf.to_file(merged_geojson_file, driver="GeoJSON")
        print(f"Merged polygons GeoJSON saved as '{merged_geojson_file}'")

    # Create a GeoDataFrame for the original DataFrame
    original_gdf = gpd.GeoDataFrame(
        boats_no_typhoons,
        geometry=gpd.points_from_xy(boats_no_typhoons["Lon_DNB"], boats_no_typhoons["Lat_DNB"]),
        crs="EPSG:4326",
    )

    # Perform a spatial join to clip the original DataFrame based on the selected polygons
    clipped_gdf = gpd.sjoin(original_gdf, merged_gdf, predicate="within")

    # Save the clipped data as a new CSV file to intermediate directory
    clipped_csv_file = os.path.join(output_path, f"clipped_original_data_{country}_{year_selected}.csv")
    clipped_gdf.to_csv(clipped_csv_file, index=False)
    print(f"Clipped data saved as '{clipped_csv_file}'")

    # Create GeoDataFrame for the scatter points
    gdf_scatter = gpd.GeoDataFrame(geometry=[Point(x, y) for x, y in zip(x_coords, y_coords, strict=False)])
    gdf_scatter.set_crs(epsg=4326, inplace=True)

    # Simulate GeoDataFrame for clipped_gdf
    clipped_gdf.set_crs(epsg=4326, inplace=True)

    # Validate geometry types before plotting
    logger.info(f"clipped_gdf geometry types: {clipped_gdf.geometry.type.value_counts().to_dict()}")
    logger.info(f"merged_gdf geometry types: {merged_gdf.geometry.type.value_counts().to_dict()}")

    # Ensure clipped_gdf only contains Point geometries for proper visualization
    if not all(clipped_gdf.geometry.type == "Point"):
        logger.warning("clipped_gdf contains non-Point geometries, filtering to Points only")
        clipped_gdf = clipped_gdf[clipped_gdf.geometry.type == "Point"]

    # Plot the data
    fig, ax = plt.subplots(figsize=(10, 10))

    gdf_scatter.plot(ax=ax, markersize=5, color="blue", alpha=0.5, label="Fishing Boats Detected")
    # Plot clipped boats as points, not polygons
    clipped_gdf.plot(ax=ax, color="purple", markersize=3, alpha=0.6)
    merged_gdf.plot(ax=ax, facecolor="none", edgecolor="black")

    minx, miny, maxx, maxy = gdf_scatter.total_bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    # Add labels with halo effect for each fishing ground
    for _, row in merged_gdf.iterrows():
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

    # Add OpenStreetMap basemap
    # basemap_added = False
    try:
        ctx.add_basemap(
            ax,
            crs=gdf_scatter.crs.to_string(),
            attribution="Map data © OpenStreetMap contributors",
        )
        # basemap_added = True
    except Exception as e:
        logger.exception(f"Failed to load OpenStreetMap basemap: {e}", stack_info=True)
        logger.info("Switching to CartoDB Positron basemap...")
        try:
            ctx.add_basemap(
                ax,
                crs=gdf_scatter.crs.to_string(),
                source=ctx.providers.CartoDB.Positron,
            )
        except Exception as e2:
            logger.exception(f"Failed to load CartoDB Positro basemap: {e2}", stack_info=True)
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

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Fishing Grounds in {year_selected}")
    # Save map to visualizations directory
    map_path = os.path.join(output_path.replace("intermediate", "visualizations"), f"{country}_{year_selected}_map.png")
    plt.savefig(map_path)
    plt.close()
    print(f"Map saved to {map_path}")

    # Return the results
    return merged_gdf, clipped_gdf, map_path


# Function to clip boats with typhoon occurrence using the main fishing grounds
@time_execution("clipping boats with typhoon occurrence")
def clip_boats_with_typhoon_occurrence(
    boats_typhoons, boats_no_typhoons, merged_gdf, year_selected, country, output_path
):
    """
    Clips fishing boat locations that experienced typhoon events to the main fishing grounds and computes boat counts for each typhoon occurrence date.

    Parameters:
    - boats_typhoons (DataFrame): Data containing fishing boat locations with typhoon occurrences.
    - boats_no_typhoons (DataFrame): Data of fishing boats without typhoon occurrences.
    - merged_gdf (GeoDataFrame): Main fishing grounds defined by KDE-derived contours.
    - year_selected (int): Year of data analysis.
    - country (str): Country code used for naming outputs.

    This function performs spatial clipping of boats with typhoon occurrences using main fishing ground polygons.
    It calculates daily and monthly averages of boat counts with and without typhoon occurrences, saving results to CSV files and returning the clipped GeoDataFrame, daily boat counts, and merged boat data.
    """

    # Create a GeoDataFrame for the original DataFrame: Boats with typhoon experience
    typhoons_gdf = gpd.GeoDataFrame(
        boats_typhoons,
        geometry=gpd.points_from_xy(boats_typhoons["Lon_DNB"], boats_typhoons["Lat_DNB"]),
        crs="EPSG:4326",
    )

    # Perform a spatial join to clip the original DataFrame based on the merged polygons
    clipped_ty_gdf = gpd.sjoin(typhoons_gdf, merged_gdf, predicate="within")

    # Save the clipped data as a new CSV file -> clipped boats typhoon with contour id
    clipped_ty_gdf.to_csv(
        os.path.join(output_path, f"clipped_ty_gdf_{country}_{year_selected}.csv"),
        index=False,
    )
    # Calculate the total number of boats during typhoon occurrence (per date)
    boats_fishing_grounds = boats_typhoons["date_only"].value_counts().reset_index()
    boats_fishing_grounds.columns = ["date_only", "boats_fishing"]
    boats_fishing_grounds = boats_fishing_grounds.sort_values(by="date_only")
    boats_fishing_grounds.to_csv(
        os.path.join(output_path, f"boats_fishing_grounds_{country}_{year_selected}.csv"),
        index=False,
    )

    # Calculate the average number of boats during typhoon occurrence
    boatsfg_per_month = (
        boats_fishing_grounds.groupby(boats_fishing_grounds["date_only"].dt.to_period("M"))["boats_fishing"]
        .mean()
        .reset_index(name="monthly_ave")
    )
    boatsfg_per_month.to_csv(
        os.path.join(output_path, f"mean_boatsfg_ty_{country}_{year_selected}.csv"),
        index=False,
    )

    # Calculate the total number of boats without typhoon occurrence (per date)
    boats_fishing_grounds_noty = boats_no_typhoons["date_only"].value_counts().reset_index()
    boats_fishing_grounds_noty.columns = ["date_only", "boats_fishing"]
    boats_fishing_grounds_noty = boats_fishing_grounds_noty.sort_values(by="date_only")
    boats_fishing_grounds_noty.to_csv(
        os.path.join(output_path, f"boats_fishing_grounds_noty_{country}_{year_selected}.csv"),
        index=False,
    )

    # Calculate the average number of boats without typhoon occurrence
    boatsfg_per_month_noty = (
        boats_fishing_grounds_noty.groupby(boats_fishing_grounds_noty["date_only"].dt.to_period("M"))["boats_fishing"]
        .mean()
        .reset_index(name="monthly_ave")
    )
    boatsfg_per_month_noty.to_csv(
        os.path.join(output_path, f"mean_boatsfg_noty{country}_{year_selected}.csv"),
        index=False,
    )

    merge_boats_num = pd.concat([boats_fishing_grounds_noty, boats_fishing_grounds], ignore_index=True)
    merge_boats_num = merge_boats_num.sort_values(by="date_only")
    merge_boats_num.to_csv(
        os.path.join(output_path, f"merge_boats_num_{country}_{year_selected}.csv"),
        index=False,
    )
    # logger.info(merge_boats_num)
    return clipped_ty_gdf, boats_fishing_grounds, merge_boats_num


@time_execution("computing clipped boats")
def compute_clipped_boats(
    clipped_ty_gdf,
    boats_fishing_grounds,
    merged_gdf,
    year_selected,
    country,
    output_path,
):
    """
    Compute the number of clipped boats per fishing ground and the total number of boats per typhoon date.

    Parameters:
    - clipped_ty_gdf: GeoDataFrame containing boats clipped with typhoon occurrence.
    - boats_fishing_grounds: DataFrame containing the total number of boats per date of typhoon occurrence.
    - year_selected: The selected year for the analysis.
    - country: The country code for the analysis.

    Returns:
    - pivot_table: DataFrame containing the pivot table with clipped boat counts per fishing ground and total boats per date.
    """

    # Convert the 'date' column to a datetime object
    clipped_ty_gdf["date_only"] = pd.to_datetime(clipped_ty_gdf["date_only"])

    # Extract the date only from the datetime column
    clipped_ty_gdf["date_only"] = clipped_ty_gdf["date_only"].dt.date

    # Group by 'contour_id' and 'date_only', and then count rows in each group
    grouped_counts = clipped_ty_gdf.groupby(["contour_id", "date_only"]).size().reset_index(name="row_count")

    grouped_counts["date_only"] = pd.to_datetime(grouped_counts["date_only"])

    # Convert 'date_only' column in boats_fishing_grounds to datetime
    boats_fishing_grounds["date_only"] = pd.to_datetime(boats_fishing_grounds["date_only"])

    # Merge the grouped counts with boats_fishing_grounds
    grouped_counts = pd.merge(
        grouped_counts,
        boats_fishing_grounds[["date_only", "boats_fishing"]],
        on="date_only",
        how="left",
    )

    # Get all unique contour_ids from merged_gdf
    all_contour_ids = pd.DataFrame({"contour_id": merged_gdf["contour_id"].unique()})

    # Merge with grouped_counts to ensure all contour_ids are included
    grouped_counts = all_contour_ids.merge(grouped_counts, on="contour_id", how="left")

    # Create a pivot table with 'date_only' as index and 'contour_id' as columns
    pivot_table = grouped_counts.pivot(index="date_only", columns=["contour_id"], values="row_count")

    # Add the total number of boats per date
    pivot_table["boats_fishing"] = grouped_counts.groupby("date_only")["boats_fishing"].first()

    # Reset the index of the pivot table
    pivot_table = pivot_table.reset_index()

    # Save the pivot table as a CSV file
    pivot_table.to_csv(
        os.path.join(output_path, f"count_grounds_{country}_{year_selected}.csv"),
        index=False,
    )
    # logger.info(pivot_table)
    return pivot_table


@time_execution("computing clipped boats without typhoon")
def compute_clipped_boats_no_typhoon(clipped_gdf, boats_no_typhoons, year_selected, country, output_path):
    """
    Compute the number of clipped boats per fishing ground without typhoon occurrence, and the total number of boats per date of typhoon occurrence.

    Parameters:
    - clipped_gdf: GeoDataFrame containing boats without typhoon occurrence.
    - boats_no_typhoons: DataFrame containing the number of boats per date without typhoon occurrence.
    - year_selected: The selected year for the analysis.
    - country: The country code for the analysis.

    Returns:
    - pivot_table2: DataFrame containing the pivot table with clipped boat counts per fishing ground and total boats per date.
    """

    # Convert the 'date' column to a datetime object
    clipped_gdf["date_only"] = pd.to_datetime(clipped_gdf["date_only"])

    # Extract the date only from the datetime column
    clipped_gdf["date_only"] = clipped_gdf["date_only"].dt.date

    # Group by 'contour_id' and 'date_only', and then count rows in each group
    grouped_counts2 = clipped_gdf.groupby(["contour_id", "date_only"]).size().reset_index(name="row_count")

    # Convert 'date_only' column in grouped_counts2 to datetime
    grouped_counts2["date_only"] = pd.to_datetime(grouped_counts2["date_only"])

    # Calculate the total number of boats without typhoon occurrence (per date)
    boats_fishing_grounds_no_ty = boats_no_typhoons["date_only"].value_counts().reset_index()
    boats_fishing_grounds_no_ty.columns = ["date_only", "boats_fishing"]
    boats_fishing_grounds_no_ty = boats_fishing_grounds_no_ty.sort_values(by="date_only")

    # Convert 'date_only' column in boats_fishing_grounds_no_ty to datetime
    boats_fishing_grounds_no_ty["date_only"] = pd.to_datetime(boats_fishing_grounds_no_ty["date_only"])

    # Merge the grouped counts with boats_fishing_grounds_no_ty
    grouped_counts2 = pd.merge(
        grouped_counts2,
        boats_fishing_grounds_no_ty[["date_only", "boats_fishing"]],
        on="date_only",
        how="left",
    )

    # Create a pivot table with 'date_only' as index and 'contour_id' as columns
    pivot_table2 = grouped_counts2.pivot(index="date_only", columns=["contour_id"], values="row_count")

    # Add the total number of boats per date
    pivot_table2["boats_fishing"] = grouped_counts2.groupby("date_only")["boats_fishing"].first()

    # Reset the index of the pivot table
    pivot_table2 = pivot_table2.reset_index()

    # Save the pivot table as a CSV file
    pivot_table2.to_csv(
        os.path.join(output_path, f"count_grounds_no_ty_{country}_{year_selected}.csv"),
        index=False,
    )
    # Melt the DataFrame to convert contour_id columns into rows
    melted_df = pivot_table2.melt(id_vars=["date_only"], var_name="contour_id", value_name="fishing_boats")

    # Convert contour_id to numeric to handle dynamically detected ids
    melted_df["contour_id"] = pd.to_numeric(melted_df["contour_id"], errors="coerce")

    # Drop NaN values to handle blanks in the original table
    melted_df = melted_df.dropna(subset=["fishing_boats"])

    # Calculate the average daily count of boats for each contour_id
    average_daily_counts = melted_df.groupby("contour_id")["fishing_boats"].mean().round().astype(int).reset_index()

    # Rename columns for clarity
    average_daily_counts.columns = ["contour_id", "avg_daily_boats"]

    # Save the average daily count table as a CSV file
    average_daily_counts.to_csv(
        os.path.join(output_path, f"avg_daily_boats_noty_{country}_{year_selected}.csv"),
        index=False,
    )

    return pivot_table2, average_daily_counts


@time_execution("calculating centroids")
def calculate_centroids(merged_gdf, output_path, country, year_selected):
    """
    Calculates the centroids of Polygon and MultiPolygon geometries in a GeoDataFrame.

    Parameters:
    - merged_gdf (GeoDataFrame): GeoDataFrame containing Polygon and MultiPolygon geometries.

    This function computes the centroid for each geometry, adding `lat` and `lon` columns for the centroid coordinates.
    The resulting GeoDataFrame, including centroid coordinates, is saved to 'centers.csv' and returned.
    """

    read_poly = merged_gdf.copy()

    # Function to compute centroids for Polygon or MultiPolygon geometries
    def compute_centroid(geom):
        if geom.geom_type == "Polygon":
            return geom.centroid
        elif geom.geom_type == "MultiPolygon":
            return geom.representative_point()  # Use a representative point for MultiPolygon
        else:
            raise ValueError(f"Unsupported geometry type: {geom.geom_type}")

    # Calculate the centroid for each geometry
    read_poly["centroid"] = read_poly.geometry.apply(compute_centroid)
    read_poly["lat"] = read_poly["centroid"].y
    read_poly["lon"] = read_poly["centroid"].x

    # Save to CSV
    read_poly.to_csv(os.path.join(output_path, f"centers_{country}_{year_selected}.csv"), index=False)
    return read_poly


@time_execution("filtering cyclone points")
def filter_cyclone_points(lin11d, read_eez, month, output_path=None, year_selected=None):
    """
    Filter cyclone points that are within the EEZ and meet the criteria of being present for at least one day.

    Parameters:
    - lin11d: DataFrame containing cyclone points.
    - read_eez: GeoDataFrame containing the EEZ boundaries.
    - month: Integer representing the month for which to filter cyclone points.
    - output_path: Optional path for saving debug files.
    - year_selected: Optional year for naming debug files.

    Returns:
    - filtered_points: DataFrame containing filtered cyclone points.
    """

    lin11d["ISO_TIME"] = pd.to_datetime(lin11d["ISO_TIME"], errors="coerce")
    lin11d["date_only"] = lin11d["ISO_TIME"].dt.date
    lin11d["date_only"] = pd.to_datetime(lin11d["date_only"])
    lin11d["month"] = pd.to_numeric(lin11d["month"], errors="coerce", downcast="integer")

    # cyclone_points_month_df = lin11d[lin11d['date_only'].dt.month == month]
    cyclone_points_month_df = lin11d[lin11d["month"] == month]

    # Identify cyclone names that have points within the EEZ
    cyclone_names_inside_eez = set()
    for _, cyclone_point in cyclone_points_month_df.iterrows():
        cyclone_name = cyclone_point["NAME"]
        if any(read_eez.geometry.contains(cyclone_point.geometry)):
            cyclone_names_inside_eez.add(cyclone_name)

    cyclones_inside_eez = cyclone_points_month_df[cyclone_points_month_df["NAME"].isin(cyclone_names_inside_eez)]

    points_inside_eez = gpd.sjoin(cyclones_inside_eez, read_eez, predicate="within", how="inner")

    # Calculate entry and exit dates within the EEZ
    points_inside_eez["entered_eez_date"] = points_inside_eez.groupby("NAME")["date_only"].transform("min")
    points_inside_eez["within_eez_date"] = points_inside_eez.groupby("NAME")["date_only"].transform("max")

    # New criteria (a) = minimum of 1 day inside EEZ
    # Merge the processed dates back to cyclones_inside_eez
    cyclones_inside_eez2 = cyclones_inside_eez.merge(
        points_inside_eez[["NAME", "entered_eez_date", "within_eez_date"]],
        on="NAME",
        how="left",
    )
    cyclones_inside_eez2["days_inside_eez"] = (
        cyclones_inside_eez2["within_eez_date"] - cyclones_inside_eez2["entered_eez_date"]
    ).dt.days

    # Exclude cyclone points within EEZ for less than 1 day
    filtered_points = cyclones_inside_eez2[cyclones_inside_eez2["days_inside_eez"] >= 1]
    # Save to intermediate output directory (optional, for debugging)
    if output_path and year_selected:
        filtered_points.to_csv(os.path.join(output_path, f"filtered_{year_selected}.csv"), index=False)

    return filtered_points


@time_execution("preparing storm speed data")
def prepare_storm_speed_data(all_filtered, clipped_ty_gdf, output_path, country, year_selected):
    """
    Prepare dataset for the generation of maximum storm speed per month.

    Parameters:
    - all_filtered: DataFrame containing filtered cyclone points from the cyclone season.
    - clipped_ty_gdf: GeoDataFrame containing boats within the typhoon period.

    Returns:
    - test_stmspeed: DataFrame combining filtered cyclones and boats within the typhoon period.
    """
    # Convert date columns to datetime format
    all_filtered["date_only"] = pd.to_datetime(all_filtered["date_only"])
    clipped_ty_gdf["date_only"] = pd.to_datetime(clipped_ty_gdf["date_only"])

    # Combine the filtered cyclones and the boats within the typhoon period
    test_stmspeed = pd.merge(all_filtered, clipped_ty_gdf, on="date_only")
    logger.info(test_stmspeed.head(3))

    full_merge = pd.merge(all_filtered, clipped_ty_gdf, on="date_only", how="outer", indicator=True)

    # Initialize unmatched_rows as None for flexibility
    unmatched_rows = None

    # Check if there are unmatched rows
    if "left_only" in full_merge["_merge"].unique():
        # Filter rows that are only in all_filtered (not matched with clipped_ty_gdf)
        unmatched_rows = full_merge[full_merge["_merge"] == "left_only"].drop(columns=["_merge"])
        logger.info("Unmatched rows found:")
        # Save unmatched rows dataframe
        unmatched_rows.to_csv(
            os.path.join(output_path, f"unmatched_rows_{country}_{year_selected}.csv"),
            index=False,
        )
    else:
        logger.info("No unmatched rows found.")

    # Print the unmatched rows
    logger.info(unmatched_rows.head())
    return test_stmspeed, unmatched_rows


@time_execution("calculating storm speed")
def calculate_storm_speed(all_filtered, clipped_ty_gdf, unmatched_rows, output_path, country, year_selected):
    """
    Calculates the mean and maximum storm speeds for dates of typhoon occurrences.

    Parameters:
    - all_filtered (DataFrame): Data containing storm speed and other attributes, with 'date_only' column.
    - clipped_ty_gdf (GeoDataFrame): Clipped data of boats with typhoon occurrences, containing 'date_only' column.

    This function merges storm speed data with typhoon occurrence dates, then calculates the daily mean storm speed.
    Monthly maximum storm speeds are also computed and saved to CSV files. The function returns DataFrames of daily mean
    and monthly maximum storm speeds.
    """

    all_filtered["date_only"] = pd.to_datetime(all_filtered["date_only"])
    clipped_ty_gdf["date_only"] = pd.to_datetime(clipped_ty_gdf["date_only"])

    test_stmspeed = pd.merge(all_filtered, clipped_ty_gdf, on="date_only")

    if test_stmspeed.empty:
        storm_spd_mean_df0 = pd.DataFrame(columns=["date_only", "stm_spd_mean", "NAME"])
        max_stmspd0 = pd.DataFrame(columns=["date_only", "storm_speed", "NAME"])
    else:
        # Group by date and calculate mean storm speed, keeping 'NAME'
        storm_spd_mean0 = test_stmspeed.groupby(["date_only", "NAME"])["STORM_SPD"].mean().round(1)
        storm_spd_mean_df0 = storm_spd_mean0.reset_index()
        storm_spd_mean_df0.columns = ["date_only", "NAME", "stm_spd_mean"]

        # Ensure 'date_only' remains a datetime object
        storm_spd_mean_df0["date_only"] = pd.to_datetime(storm_spd_mean_df0["date_only"]).dt.date
        storm_spd_mean_df0["date_only"] = pd.to_datetime(storm_spd_mean_df0["date_only"])

        storm_spd_mean00 = unmatched_rows.groupby(["date_only", "NAME"])["STORM_SPD"].mean().round(1)
        storm_spd_mean_df00 = storm_spd_mean00.reset_index()
        storm_spd_mean_df00.columns = ["date_only", "NAME", "stm_spd_mean"]

        # Ensure 'date_only' remains a datetime object
        storm_spd_mean_df00["date_only"] = pd.to_datetime(storm_spd_mean_df00["date_only"]).dt.date
        storm_spd_mean_df00["date_only"] = pd.to_datetime(storm_spd_mean_df00["date_only"])

        # Find the maximum storm speed for each month
        result0 = storm_spd_mean_df0.groupby(storm_spd_mean_df0["date_only"].dt.to_period("M")).apply(
            lambda x: x.loc[x["stm_spd_mean"].idxmax()]
        )
        result0.reset_index(drop=True, inplace=True)
        result0["storm_speed"] = result0["stm_spd_mean"]

        # Select the relevant columns
        max_stmspd_cols0 = ["date_only", "storm_speed", "NAME"]
        max_stmspd0 = result0[max_stmspd_cols0]

    storm_spd_mean_df0.to_csv(
        os.path.join(output_path, f"storm_spd_mean_df0_{country}_{year_selected}.csv"),
        index=False,
    )
    storm_spd_mean_df00.to_csv(
        os.path.join(output_path, f"storm_spd_mean_df00{country}_{year_selected}.csv"),
        index=False,
    )
    max_stmspd0.to_csv(
        os.path.join(output_path, f"max_stmspd0_{country}_{year_selected}.csv"),
        index=False,
    )

    # logger.info(max_stmspd0)
    return storm_spd_mean_df0, storm_spd_mean_df00, max_stmspd0


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the Haversine distance between two geographical points.

    Parameters:
    - lat1, lon1: Latitude and longitude of the first point.
    - lat2, lon2: Latitude and longitude of the second point.

    Returns:
    - Distance in kilometers between the two points.
    """
    start_coords = (lat1, lon1)
    end_coords = (lat2, lon2)
    return geodesic(start_coords, end_coords).kilometers


@time_execution("calculating minimum distance")
def calculate_min_distance(
    storm_spd_mean_df0,
    storm_spd_mean_df00,
    lin11d,
    clipped_ty_gdf,
    read_poly,
    centers_df_latest,
    year_selected,
):
    """
    Compute the minimum distance between cyclones and fishing grounds for each date.

    Parameters:
    - storm_spd_mean_df0: DataFrame containing storm speed data with 'date_only' column.
    - lin11d: DataFrame containing cyclone points with 'date_only' column.
    - clipped_ty_gdf: DataFrame containing clipped typhoon data with 'date_only' column.
    - read_poly: GeoDataFrame containing centroids of fishing grounds with 'lat' and 'lon' columns.

    Returns:
    - pivot_table3: DataFrame containing the minimum distance per date and contour_id.
    """

    # Convert date columns to datetime format
    storm_spd_mean_df0["date_only"] = pd.to_datetime(storm_spd_mean_df0["date_only"])
    storm_spd_mean_df00["date_only"] = pd.to_datetime(storm_spd_mean_df00["date_only"])
    lin11d["date_only"] = pd.to_datetime(lin11d["date_only"])
    clipped_ty_gdf["date_only"] = pd.to_datetime(clipped_ty_gdf["date_only"])

    # Determine the DataFrame to use based on the year
    current_year = pd.Timestamp.now().year
    fishing_centroids = centers_df_latest if year_selected == current_year else read_poly

    # Define a helper function to process a single storm speed DataFrame
    def process_storm_speed(storm_spd_mean_df, fishing_centroids_copy):
        if not storm_spd_mean_df.empty and not lin11d.empty:
            # Merge storm speed data with cyclone data
            typhoon_criteria = pd.merge(storm_spd_mean_df, lin11d, on=["date_only", "NAME"])
            print("this is typhoon criteria")
            print(typhoon_criteria)

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
    pivot_table3 = process_storm_speed(storm_spd_mean_df0, fishing_centroids_copy)

    fishing_centroids_copy = fishing_centroids.copy()
    pivot_table3_df00 = process_storm_speed(storm_spd_mean_df00, fishing_centroids_copy)

    # Return the two pivot tables
    return pivot_table3, pivot_table3_df00


# New function to finalize data for ingestion
@time_execution("finalizing data for ingestion")
def finalize_data_for_ingestion(
    test_stmspeed,
    unmatched_rows,
    storm_spd_mean_df0,
    storm_spd_mean_df00,
    pivot_table,
    pivot_table3,
    pivot_table3_df00,
    country,
    year_selected,
    output_path,
):
    """
    Calculate the maximum storm speed per date and merge with other dataframes.

    Parameters:
    - test_stmspeed: DataFrame containing merged storm speed data with 'date_only' and 'STORM_SPD' columns.
    - unmatched_rows: DataFrame containing rows from `all_filtered` that did not merge.
    - storm_spd_mean_df0: DataFrame containing storm speed mean data with 'date_only' column.
    - storm_spd_mean_df00: Additional DataFrame containing storm speed mean data.
    - pivot_table: DataFrame containing ground data for further merging.
    - pivot_table3: DataFrame containing distance data for storm_spd_mean_df0.
    - pivot_table3_df00: DataFrame containing distance data for storm_spd_mean_df00.
    - country: Country code for naming the output CSV file.
    - year_selected: Selected year for naming the output CSV file.

    Returns:
    - final_df0: Final DataFrame for storm_spd_mean_df0.
    - final_df00: Final DataFrame for storm_spd_mean_df00.
    - unmatched_df: Combined DataFrame for unmatched rows and their respective processing results.
    """

    # Convert date columns to datetime format
    # logger.info(test_stmspeed)
    test_stmspeed["date_only"] = pd.to_datetime(test_stmspeed["date_only"])
    storm_spd_mean_df0["date_only"] = pd.to_datetime(storm_spd_mean_df0["date_only"])

    def process_data(storm_spd_mean_df, pivot_table3_current, test_stmspeed_local):
        # Check if test_stmspeed_local and the current storm speed DataFrame are not empty
        if not test_stmspeed_local.empty and not storm_spd_mean_df.empty:
            # Convert date columns to datetime format
            print("last step")
            test_stmspeed_local["date_only"] = pd.to_datetime(test_stmspeed_local["date_only"])
            print(test_stmspeed_local["date_only"].unique())
            storm_spd_mean_df["date_only"] = pd.to_datetime(storm_spd_mean_df["date_only"])
            print(storm_spd_mean_df["date_only"].unique())
            pivot_table3_current["date_only"] = pd.to_datetime(pivot_table3_current["date_only"])
            print(pivot_table3_current["date_only"].unique())

            # Calculating the max storm speed per date
            storm_spd_max = test_stmspeed_local.groupby(["date_only", "NAME"])["STORM_SPD"].max().reset_index()
            storm_spd_max.columns = ["date_only", "NAME", "stm_spd_max"]
            storm_spd_max["date_only"] = pd.to_datetime(storm_spd_max["date_only"])

            # Merge with storm_spd_mean_df on both 'date_only' and 'NAME' to retain the storm name
            storm_spd_max = pd.merge(storm_spd_max, storm_spd_mean_df, on=["date_only", "NAME"])
            print(storm_spd_max)

            if not pivot_table3_current.empty:
                # Combine dataframes
                combined = pd.merge(storm_spd_max, pivot_table3_current, on=["date_only", "NAME"])
                if pivot_table is not None and not pivot_table.empty and pivot_table3_current is not pivot_table3_df00:
                    combined = pd.merge(combined, pivot_table, on="date_only")

                # Dynamic column renaming for distances and grounds
                column_names = combined.columns.tolist()
                if pivot_table3_current is pivot_table3:
                    # For pivot_table_final: Divide columns into distance_ and ground_
                    midpoint = len(column_names[4:-1]) // 2
                    distance_columns = column_names[4 : 4 + midpoint]
                    ground_columns = column_names[4 + midpoint : -1]
                else:
                    # For other cases: Treat all as distance_
                    distance_columns = column_names[4:-1]
                    ground_columns = []

                # Create a dictionary to rename columns dynamically
                new_column_names = {}
                for i, col in enumerate(distance_columns):
                    new_column_names[col] = f"distance_{i}"
                for i, col in enumerate(ground_columns):
                    new_column_names[col] = f"ground_{i}"

                combined = combined.rename(columns=new_column_names)

                # Fill NaN values
                combined = combined.fillna(0)

                # Calculate total distance and filter for minimum total_distance per date
                distance_columns = [str(col) for col in combined.columns if str(col).startswith("distance_")]
                combined["total_distance"] = combined[distance_columns].sum(axis=1)
                combined = combined.loc[combined.groupby("date_only")["total_distance"].idxmin()]
                combined = combined.drop(columns=["total_distance"])

                return combined
            else:
                print("pivot_table3_current or pivot_table is empty. Cannot merge.")
                return pd.DataFrame()
        else:
            print("test_stmspeed or storm_spd_mean_df is empty. Cannot perform calculations.")
            return pd.DataFrame()

    # Process matched rows (test_stmspeed) with `storm_spd_mean_df0`
    pivot_table_final = process_data(storm_spd_mean_df0, pivot_table3, test_stmspeed)
    final_output_path = os.path.join(output_path, f"{country}_logdatadf_py_new_{year_selected}_all.csv")
    pivot_table_final.to_csv(final_output_path, index=False)

    # Process unmatched rows with `storm_spd_mean_df00`
    pivot_table_final0 = process_data(storm_spd_mean_df00, pivot_table3_df00, unmatched_rows)
    # Filter `pivot_table_final0` to start after the max date in `pivot_table_final`
    if not pivot_table_final.empty and not pivot_table_final0.empty:
        max_date = pivot_table_final["date_only"].max()
        pivot_table_final0 = pivot_table_final0[pivot_table_final0["date_only"] > max_date]

        # Rename columns dynamically for `distance_` prefix
        distance_columns = pivot_table_final0.columns.tolist()
        columns_to_rename = {
            col: f"distance_{i}" for i, col in enumerate(distance_columns[4:], start=0)
        }  # Renaming after the first 4 columns
        pivot_table_final0.rename(columns=columns_to_rename, inplace=True)

    # Save `pivot_table_final0` separately
    final_output_path2 = os.path.join(output_path, f"{country}_logdatadf0_py_new_{year_selected}_all.csv")
    pivot_table_final0.to_csv(final_output_path2, index=False)
    print(f"Final combined data saved to: {final_output_path}")

    return pivot_table_final, pivot_table_final0


# def calculate_cyclone_metrics(pivot_table_final, average_daily_counts):
def calculate_boat_difference(
    pivot_table_final,
    average_daily_counts,
    year_selected,
    output_path,
    country,
    avg_daily_latest=None,
):
    """
    Calculate cyclone metrics dynamically for detected grounds.

    Parameters:
        pivot_table_final (pd.DataFrame): The main DataFrame containing cyclone data with columns 'NAME', 'stm_spd_mean',
                           'date_only', 'distance_{ground}', and 'ground_{ground}'.
        ave_daily_count_df (pd.DataFrame): DataFrame containing average daily boat counts with columns
                                           'contour_id' and 'avg_daily_boats'.

    Returns:
        pd.DataFrame: A DataFrame summarizing cyclone metrics, including average storm speed, date range,
                      average distance per ground, and percentage boat differences.
    """

    # Use preloaded centers_df_latest if provided
    if year_selected == datetime.now().year:
        if avg_daily_latest is None:
            raise ValueError("For the current year (2024), avg_daily_latest must be provided.")
        print("Using preloaded DataFrame for the current year (2024).")
        average_daily_counts = avg_daily_latest
    else:
        if average_daily_counts is None:
            raise ValueError(f"For past years ({year_selected}), average_daily_counts must be provided.")
        print(f"Processing past year ({year_selected}), using provided average daily counts DataFrame.")

    # Detect the number of ground columns dynamically
    ground_columns = [col for col in pivot_table_final.columns if "ground_" in col]
    num_grounds = len(ground_columns)  # Count the number of ground columns dynamically

    # Initialize results list
    results = []
    cyclones = pivot_table_final["NAME"].unique()

    # Loop through each cyclone
    for cyclone in cyclones:
        cyclone_data = pivot_table_final[pivot_table_final["NAME"] == cyclone]
        if cyclone_data.empty:
            print(f"No data found for cyclone: {cyclone}. Skipping.")
            continue

        cyclone_data["date_only"] = pd.to_datetime(cyclone_data["date_only"]).dt.date
        ave_storm_speed = cyclone_data["stm_spd_mean"].mean().round(2)
        date_range = f"{cyclone_data['date_only'].min()} to {cyclone_data['date_only'].max()}"  # Get date range

        # Calculate metrics for each ground dynamically
        ground_metrics = {}
        for ground in range(num_grounds):
            dist_col = f"distance_{ground}"
            ground_col = f"ground_{ground}"

            if cyclone_data[dist_col].isna().all():
                print(f"All values for {dist_col} are NaN. Skipping ground {ground}.")
                continue

            # Find the minimum distance
            min_distance = cyclone_data[dist_col].min()

            # Get all rows with the minimum distance
            min_dist_rows = cyclone_data[cyclone_data[dist_col] == min_distance]

            # Calculate the average distance for the ground
            ave_distance = cyclone_data[dist_col].mean().round(2)

            # Get number of boats on the minimum distance date for the ground
            boats_min_distance_avg = min_dist_rows[ground_col].mean()

            # Retrieve contour_id corresponding to the ground column
            contour_id = ground  # Assuming ground column directly maps to contour_id
            if contour_id not in average_daily_counts["contour_id"].values:
                print(f"Contour ID {contour_id} not found in average_daily_counts. Skipping ground {ground}.")
                continue

            ave_daily_count = average_daily_counts.loc[
                average_daily_counts["contour_id"] == contour_id, "avg_daily_boats"
            ].values[0]

            if pd.notna(ave_daily_count) and ave_daily_count != 0:
                boat_diff = ((boats_min_distance_avg - ave_daily_count) / ave_daily_count) * 100
            else:
                boat_diff = None

            # Store results
            ground_metrics[f"G{ground} Distance (km)"] = ave_distance
            ground_metrics[f"G{ground} (Boat Diff%)"] = round(boat_diff, 2) if boat_diff is not None else "N/A"

        # Append result row
        results.append(
            {
                "Typhoon": cyclone.title(),
                "Ave. Stm Speed (knot)": ave_storm_speed,
                "Date Range": date_range,
                **ground_metrics,  # Interleaved columns
            }
        )

    # Convert results to a DataFrame
    final_table_boatdiff = pd.DataFrame(results)
    final_table_path = os.path.join(output_path, f"{country}_boatdiff_{year_selected}.csv")
    final_table_boatdiff.to_csv(final_table_path, index=False)
    return final_table_boatdiff


def append_average_daily_count(final_table_boatdiff, average_daily_counts, year_selected, output_path, country):
    """
    Append a row of average daily counts below the header in the DataFrame.

    Parameters:
        final_table_boatdiff (pd.DataFrame): The main results DataFrame.
        average_daily_counts (pd.DataFrame): DataFrame containing average daily boat counts with 'contour_id' and 'avg_daily_boats'.

    Returns:
        pd.DataFrame: The modified DataFrame with the average daily count row added.
    """
    # Detect the number of grounds dynamically based on "Boat Difference %" columns
    num_grounds = len([col for col in final_table_boatdiff.columns if "Boat Diff" in col])

    # Create an empty row with the same columns as the final table
    avg_daily_row = {col: "" for col in final_table_boatdiff.columns}
    avg_daily_row["Typhoon"] = "Ave Daily Boats"

    # Populate the average daily count for each "Boat Difference %" column
    for ground in range(num_grounds):
        boat_diff_col = f"G{ground} (Boat Diff%)"
        contour_id = ground  # Assume ground index corresponds to contour_id

        # Retrieve the average daily count for the ground
        ave_daily_count = average_daily_counts.loc[average_daily_counts["contour_id"] == contour_id, "avg_daily_boats"]
        avg_daily_row[boat_diff_col] = round(ave_daily_count.values[0], 2) if not ave_daily_count.empty else ""

    # Append the row below the header
    avg_daily_row_df = pd.DataFrame([avg_daily_row])
    final_table_boatdiff2 = pd.concat([avg_daily_row_df, final_table_boatdiff], ignore_index=True)
    final_table_path = os.path.join(output_path, f"{country}_boatdiff2_{year_selected}.csv")
    final_table_boatdiff2.to_csv(final_table_path, index=False)

    return final_table_boatdiff2


def generate_visualizations_by_cyclone(
    pivot_table_final,
    df_all_b,
    lin11d,
    pivot_table2,
    storm_spd_mean_df0,
    wrdph,
    merged_gdf,
    graphs_path,
):
    """
    Generate visualizations for each cyclone by name and date range.

    Parameters:
    pivot_table_final (DataFrame): DataFrame containing 'date_only' and 'cyclone_name'.
    Other parameters as explained in generate_visualizations().
    """
    grouped = pivot_table_final.groupby("NAME")
    cyclones = list(pivot_table_final[pivot_table_final["NAME"] != "NOT_NAMED"]["NAME"].unique())

    for cyclone_name, group in grouped:
        formatted_name = " ".join(word.capitalize() for word in cyclone_name.strip().split())

        # dates = group['date_only'].dt.date.unique()
        min_date = group["date_only"].min()
        max_date = group["date_only"].max()
        date_range = pd.date_range(start=min_date, end=max_date)
        date_range = [d.date() for d in pd.date_range(start=min_date, end=max_date)]

        # for date in dates:
        for date in date_range:
            visualize_data(
                date,
                df_all_b,
                lin11d,
                pivot_table2,
                storm_spd_mean_df0,
                wrdph,
                merged_gdf,
                graphs_path,
                formatted_name,
            )

        # Create a GIF for each cyclone
        create_gif(graphs_path, formatted_name, fps=0.1)
    # Update last run cyclone list with properly formatted names
    formatted_cyclones = [" ".join(word.capitalize() for word in name.strip().split()) for name in cyclones]
    update_last_run_cyclone_list(formatted_cyclones)


def visualize_data(
    date,
    df_all_b,
    lin11d,
    pivot_table2,
    storm_spd_mean_df0,
    wrdph,
    merged_gdf,
    graphs_path,
    cyclone_name,
):
    """
    Visualize and save a plot for a specific date and cyclone.

    Parameters:
    date (datetime): The date for which to generate the visualization.
    cyclone_name (str): Name of the cyclone for the current visualization.
    Other DataFrames and parameters as explained in generate_visualizations().
    """
    try:
        # Convert date to match date-only comparison format
        date = pd.to_datetime(date).date()
        df_all_b.loc[:, "date_only"] = pd.to_datetime(df_all_b["ISO_TIME"]).dt.date
        lin11d.loc[:, "date_only"] = pd.to_datetime(lin11d["ISO_TIME"]).dt.date

        # Masking for boats data on the specific date
        boats_on_date = df_all_b[df_all_b["date_only"] == date]

        lin11d["date_only"] = pd.to_datetime(lin11d["ISO_TIME"]).dt.date
        lin11d["NAME"] = lin11d["NAME"].str.strip().str.title()

        # Masking for cyclone data that matches the date and cyclone name
        cyclone_on_date = lin11d[(lin11d["date_only"] == date) & (lin11d["NAME"] == cyclone_name)]

        # Plotting
        fig, ax = plt.subplots()

        # Check if wrdph and merged_gdf are not None before plotting
        if wrdph is not None and not wrdph.empty:
            wrdph.plot(facecolor="none", edgecolor="black", ax=ax)
        else:
            print(f"'wrdph' is empty or None for cyclone {cyclone_name} on date {date}")

        if merged_gdf is not None and not merged_gdf.empty:
            merged_gdf.plot(ax=ax, edgecolor="green", linestyle="--", linewidth=1, facecolor="none")
        else:
            print(f"'merged_gdf' is empty or None for cyclone {cyclone_name} on date {date}")

        # Plot boats and cyclone points if data exists
        if not boats_on_date.empty:
            ax.scatter(
                boats_on_date["Lon_DNB"],
                boats_on_date["Lat_DNB"],
                c="blue",
                label="Boats",
                s=10,
            )
        if not cyclone_on_date.empty:
            ax.scatter(
                cyclone_on_date["LON"],
                cyclone_on_date["LAT"],
                c="red",
                label="Typhoon",
                s=20,
            )

        # Add only the date text
        ax.text(110, 26, f"Date: {date}", fontsize=10, fontstyle="italic")

        ax.set_xlim(110, 130)
        ax.set_ylim(0, 25)
        ax.set_aspect("equal")

        # Create a manual entry for the fishing grounds to the legend
        fishing_grounds_legend = mpatches.Patch(
            edgecolor="green",
            linestyle="--",
            linewidth=1,
            facecolor="none",
            label="Fishing Grounds",
        )

        # Add legend with custom entry for fishing grounds
        handles, labels = ax.get_legend_handles_labels()
        handles.append(fishing_grounds_legend)
        labels.append("Fishing Grounds")
        ax.legend(
            handles=handles,
            title="",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            borderaxespad=0,
        )

        # Save cyclone frame PNG to maps subdirectory
        map_output_path = os.path.join(graphs_path, "maps", f"{cyclone_name}_{date}.png")
        os.makedirs(os.path.dirname(map_output_path), exist_ok=True)
        plt.savefig(
            map_output_path,
            dpi=300,
            bbox_inches="tight",
        )
        plt.close(fig)

    except Exception as e:
        print(f"Error processing date {date} for cyclone {cyclone_name}: {e}")
        print("Variables at the time of error:")
        print(f"wrdph: {wrdph}")
        print(f"merged_gdf: {merged_gdf}")
        print(f"boats_on_date: {boats_on_date}")
        print(f"cyclone_on_date: {cyclone_on_date}")


def create_gif(graphs_path, cyclone_name, fps=1):
    """
    Create a GIF from a collection of saved images for a specific cyclone.

    Parameters:
    graphs_path (str): Path where images are stored.
    cyclone_name (str): Name of the cyclone for naming the GIF.
    """

    # Look for images in the maps subdirectory
    maps_path = os.path.join(graphs_path, "maps")
    if not os.path.exists(maps_path):
        print(f"Maps directory not found: {maps_path}")
        return

    filenames = [
        os.path.join(maps_path, file)
        for file in os.listdir(maps_path)
        if file.startswith(cyclone_name) or file.startswith(cyclone_name.upper())
    ]
    filenames.sort()

    # if filenames:
    #     images = [imageio.imread(f) for f in filenames]
    #     imageio.mimsave(
    #         os.path.join(graphs_path, f"gif_{cyclone_name}.gif"), images, duration=0.05
    #     )
    # else:
    #     print(f"No images found for cyclone {cyclone_name}, skipping GIF creation.")

    if filenames:
        images = [imageio.imread(f) for f in filenames]

        # Calculate duration per frame based on FPS
        duration = 1 / fps

        # Save GIF to gifs subdirectory
        gif_output_path = os.path.join(graphs_path, "gifs", f"gif_{cyclone_name}.gif")
        os.makedirs(os.path.dirname(gif_output_path), exist_ok=True)
        imageio.mimsave(
            gif_output_path,
            images,
            duration=duration,
        )
        print(f"GIF saved to: {gif_output_path}")
    else:
        print(f"No images found for cyclone {cyclone_name}, skipping GIF creation.")


@dataclass
class Config:
    country: str
    year_selected: int
    viirs_path: str
    cyclone_seasons: dict[str, dict[str, int]]
    gis_path: str
    output_path: str
    graphs_path: str

    @classmethod
    def from_defaults(cls, country: str, year_selected: int, cyclone_seasons: dict, root_path: str = "data"):
        """
        Create a Config instance with default folder structure.

        Args:
            country: Country code (e.g., 'phl', 'vnm', 'idn')
            year_selected: Year for analysis
            cyclone_seasons: Dictionary of cyclone seasons by country
            root_path: Root data directory (default: 'data')
        """
        return cls(
            country=country,
            year_selected=year_selected,
            viirs_path=os.path.join(root_path, "inputs", "viirs", country, str(year_selected)),
            cyclone_seasons=cyclone_seasons,
            gis_path=os.path.join(root_path, "inputs", "gis"),
            output_path=os.path.join(root_path, "outputs", "historical", country, str(year_selected), "intermediate"),
            graphs_path=os.path.join(root_path, "outputs", "historical", country, str(year_selected), "visualizations"),
        )

    def get_analysis_path(self) -> str:
        """Get the path for final analysis outputs."""
        base_path = self.output_path.replace("intermediate", "analysis")
        os.makedirs(base_path, exist_ok=True)
        return base_path

    def get_country_gis_path(self) -> str:
        """Get the country-specific GIS path."""
        return os.path.join(self.gis_path, "countries", self.country)

    def ensure_paths_exist(self):
        """Create all necessary directories if they don't exist."""
        paths = [
            self.viirs_path,
            self.output_path,
            self.graphs_path,
            os.path.join(self.graphs_path, "maps"),
            os.path.join(self.graphs_path, "gifs"),
            self.get_analysis_path(),
        ]
        for path in paths:
            os.makedirs(path, exist_ok=True)


# Combining all
def main(config: Config, overwrite: bool, debug: bool = False, read_from_file=False, progress_callback=None):
    """
    Main function for historical analysis processing.

    Args:
        config: Config object with paths and settings
        overwrite: Whether to overwrite existing data files
        debug: Debug mode flag
        read_from_file: Whether to read pre-processed files
        progress_callback: Optional callback function(phase, phase_name, message) for progress updates
    """
    # try:
    country = config.country
    year_selected = config.year_selected
    viirs_path = config.viirs_path
    cyclone_seasons = config.cyclone_seasons
    gis_path = config.gis_path
    output_path = config.output_path
    graphs_path = config.graphs_path
    logger.info(f"Config: {config}")

    def update_progress(phase: int, phase_name: str, message: str):
        """Helper to call progress callback if provided."""
        if progress_callback:
            try:
                progress_callback(phase, phase_name, message)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")

    curr_eog_access_token = get_eog_access_token()
    if not curr_eog_access_token:
        logger.info("Logging in to the EOG server. Please wait...")
        access_token = get_access_token()
        if not access_token:
            update_eog_access_token(None)
            raise Exception("Failed to login to the EOG server. Please check your credentials.")
        update_eog_access_token(access_token)
        logger.info("Successfully logged in to the EOG server.")

    if debug:
        return "phl_2023_map.png"

    # Phase 1: Data Download & Preparation (Steps 1-4)
    update_progress(1, "Downloading and preparing data...", "Downloading VIIRS data...")
    logger.info("Step 1: Download VIIRS data")
    download_viirs_data(year_selected, country, viirs_path, cyclone_seasons, overwrite)

    update_progress(1, "Downloading and preparing data...", "Merging VIIRS data...")
    logger.info("Step 2: Merge VIIRS data")
    df_append = merge_viirs_data(viirs_path, year_selected, country, output_path)

    update_progress(1, "Downloading and preparing data...", "Downloading and processing cyclone data...")
    logger.info("Step 3: Download and process cyclone data")
    if read_from_file:
        logger.info("Reading filtered tracks from file.")
        filtered_tracks = read_filtered_tracks(gis_path, year_selected)
    else:
        logger.info("Filtered tracks not found. Downloading and processing cyclone data.")
        filtered_tracks = download_and_process_cyclone_data(gis_path, cyclone_seasons, country, year_selected)

    update_progress(1, "Downloading and preparing data...", "Getting shapefiles from GIS...")
    logger.info("Step 4: Get shapefiles from GIS")
    (
        read_eez,
        wrddsf,
        wrdph,
        centers_df_latest,
        avg_daily_latest,
        no_ty_file_pivoted_avg_per_contour,
        fg_df_latest,
    ) = get_shapefiles_from_gis(gis_path, country)

    # Phase 2: Data Processing (Steps 5-8)
    update_progress(2, "Processing boat and cyclone data...", "Post-processing VIIRS data...")
    logger.info("Step 5: Post-process VIIRS data")
    t = post_process_viirs_data(df_append, output_path)

    update_progress(2, "Processing boat and cyclone data...", "Post-processing typhoon tracks...")
    logger.info("Step 6: Post-process typhoon tracks")
    lin11d, lin11b = post_process_typhoon_tracks(
        filtered_tracks,
        cyclone_seasons,
        year_selected,
        country,
        read_eez,
        output_path,
    )

    update_progress(2, "Processing boat and cyclone data...", "Processing data for country...")
    logger.info("Step 7: Process data for country")
    unique_dates_td, td, all_filtered, df_all_b = process_data_for_country(
        country, year_selected, t, lin11d, read_eez, cyclone_seasons, output_path
    )

    update_progress(2, "Processing boat and cyclone data...", "Post-processing boats and typhoons...")
    logger.info("Step 8: Post-process boats and typhoons")
    (
        lin11d_clipped,
        boats_no_typhoons,
        boats_typhoons,
        boats_per_month,
        boats_ty_per_month,
        boats_per_date,
        boats_per_date_ty,
    ) = post_process_boats_and_typhoons(lin11d, td, read_eez, year_selected, country, output_path)

    # Phase 3: Analysis & Grounds (Steps 9-13)
    update_progress(3, "Analyzing fishing grounds...", "Determining fishing grounds...")
    logger.info("Step 9: Determine fishing grounds")
    merged_gdf, clipped_gdf, map_path = determine_fishing_grounds(
        boats_no_typhoons, year_selected, country, fg_df_latest, output_path
    )

    update_progress(3, "Analyzing fishing grounds...", "Clipping boats with typhoon occurrence...")
    logger.info("Step 10: Clip boats with typhoon occurrence")
    (
        clipped_ty_gdf,
        boats_fishing_grounds,
        merge_boats_num,
    ) = clip_boats_with_typhoon_occurrence(
        boats_typhoons,
        boats_no_typhoons,
        merged_gdf,
        year_selected,
        country,
        output_path,
    )

    update_progress(3, "Analyzing fishing grounds...", "Calculating centroids...")
    logger.info("Step 11: Calculate centroids")
    read_poly = calculate_centroids(merged_gdf, output_path, country, year_selected)

    update_progress(3, "Analyzing fishing grounds...", "Computing clipped boats...")
    logger.info("Step 12: Compute clipped boats")
    pivot_table = compute_clipped_boats(
        clipped_ty_gdf,
        boats_fishing_grounds,
        merged_gdf,
        year_selected,
        country,
        output_path,
    )

    update_progress(3, "Analyzing fishing grounds...", "Computing clipped boats without typhoon...")
    logger.info("Step 13: Compute clipped boats no typhoon")
    pivot_table2, average_daily_counts = compute_clipped_boats_no_typhoon(
        clipped_gdf, boats_no_typhoons, year_selected, country, output_path
    )

    # Phase 4: Final Calculations (Steps 14-18)
    update_progress(4, "Calculating impact metrics...", "Preparing storm speed data...")
    logger.info("Step 14: Prepare storm speed data")
    test_stmspeed, unmatched_rows = prepare_storm_speed_data(
        all_filtered, clipped_ty_gdf, output_path, country, year_selected
    )

    update_progress(4, "Calculating impact metrics...", "Calculating storm speed...")
    logger.info("Step 15: Calculate storm speed")
    storm_spd_mean_df0, storm_spd_mean_df00, max_stmspd0 = calculate_storm_speed(
        all_filtered,
        clipped_ty_gdf,
        unmatched_rows,
        output_path,
        country,
        year_selected,
    )

    update_progress(4, "Calculating impact metrics...", "Calculating minimum distance...")
    logger.info("Step 16: Calculate minimum distance")
    pivot_table3, pivot_table3_df00 = calculate_min_distance(
        storm_spd_mean_df0,
        storm_spd_mean_df00,
        lin11d,
        clipped_ty_gdf,
        read_poly,
        centers_df_latest,
        year_selected,
    )

    update_progress(4, "Calculating impact metrics...", "Finalizing data for ingestion...")
    logger.info("Step 17: Finalize data for ingestion")
    pivot_table_final, pivot_table_final0 = finalize_data_for_ingestion(
        test_stmspeed,
        unmatched_rows,
        storm_spd_mean_df0,
        storm_spd_mean_df00,
        pivot_table,
        pivot_table3,
        pivot_table3_df00,
        country,
        year_selected,
        output_path,
    )

    update_progress(4, "Calculating impact metrics...", "Creating final boat difference table...")
    logger.info("Step 18: Final table for boat difference")
    final_table_boatdiff = calculate_boat_difference(
        pivot_table_final,
        average_daily_counts,
        year_selected,
        output_path,
        country,
        avg_daily_latest,
    )

    _ = append_average_daily_count(final_table_boatdiff, average_daily_counts, year_selected, output_path, country)

    # Phase 5: Visualizations & Database (Step 19)
    update_progress(5, "Generating visualizations and updating database...", "Generating GIFs...")
    logger.info("Step 19: GIFs generation")
    generate_visualizations_by_cyclone(
        pivot_table_final,
        df_all_b,
        lin11d,
        pivot_table2,
        storm_spd_mean_df0,
        wrdph,
        merged_gdf,
        graphs_path,
    )

    logger.info("All steps completed successfully")
    return map_path


# except Exception as e:
#     print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage with the new folder structure
    country = "phl"
    year_selected = 2024

    # Use the new Config.from_defaults() method
    config = Config.from_defaults(
        country=country,
        year_selected=year_selected,
        cyclone_seasons=cyclone_seasons,
        root_path="data",  # Uses the new data/ folder structure
    )

    # Ensure all directories exist
    config.ensure_paths_exist()

    # Run the main analysis
    # Set overwrite=True to re-download VIIRS data, False to use cached data
    # Set debug=True to skip processing and return quickly
    # Set read_from_file=True to read pre-processed files instead of downloading
    main(config=config, overwrite=False, debug=False, read_from_file=False)
