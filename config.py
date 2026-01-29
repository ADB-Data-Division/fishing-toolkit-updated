import os

from backend.utils.logger import get_logger

logger = get_logger(__name__)

default_year_selected = 2023
default_country = "fji"

# Get values from environment variables or use defaults
year_selected = int(os.getenv("year_selected", default_year_selected))
country = os.getenv("country", default_country)

# year_selected = 2020
# country = 'phl'
# Print values for debugging
# print(f"Year selected from config: {year_selected}")
# print(f"Country from config: {country}")

root_path = os.path.dirname(os.path.abspath("__file__"))
logger.info(f"Root path: {root_path}")

viirs_path = os.path.join(root_path, f"{year_selected}")
# print(viirs_path)
gis_path = os.path.join(root_path, "gis")
output_path = os.path.join(root_path, "output")
graphs_path = os.path.join(root_path, "graphs")

try:
    # os.makedirs(viirs_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(graphs_path, exist_ok=True)
    os.makedirs(gis_path, exist_ok=True)
except Exception as e:
    print(e)

# List files in directories (Optional for debugging)
viirs_files = os.listdir(viirs_path) if os.path.exists(viirs_path) else []
gis_files = os.listdir(gis_path) if os.path.exists(gis_path) else []

cyclone_seasons = {
    "vnm": {"start_month": 6, "end_month": 12},
    "fji": {"start_month": 11, "end_month": 4},
    "vut": {"start_month": 1, "end_month": 6},
    "phl": {"start_month": 6, "end_month": 12},
    "bgd": {"start_month": 3, "end_month": 12},
    "idn": {"start_month": 11, "end_month": 4},
    "tha-khm": {"start_month": 4, "end_month": 11},
    # 'bgd': [
    #     {'start_month': 3, 'end_month': 7},
    #     {'start_month': 9, 'end_month': 12}
    # ]
}

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t", "yes")
# Country code mapping
country_map = {"Fiji": "fji", "Philippines": "phl", "Vietnam": "vnm"}

# Reverse mapping for display
reverse_country_map = {v: k for k, v in country_map.items()}

# Config for statistics image
statistic_img_mapping = {
    "all": "hierlasso_boats_fishing_model_plot_all_grounds.png",
    "ground": "hierlasso_model_plot_{id}.png",
}

# Windows related configurations
MAX_WIDTH = 1400
MAX_HEIGHT = 900
