import functools
import os
import time

from PIL import Image, ImageFilter

from .logger import get_logger

logger = get_logger(__name__)

# In memory database
SCALE = 1.0
MAP_IMAGE_PATH = ""
LAST_RUN_COUNTRY = "phl"
LAST_RUN_YEAR = "2023"
LAST_RUN_NUM_GROUNDS = 0
LAST_RUN_CYCLONE_LIST = None
TABLE_HEADERS = None
EOG_ACCESS_TOKEN = None
CURRENT_SIMULATION_TYPE = "historical"  # Default to historical

# Cache for resized images
resized_image_cache: dict[str, str] = {}


def get_scale_value():
    """Get the scale value for the canvas."""
    return SCALE


def update_scale_value(value):
    """Update the scale value for the canvas."""
    global SCALE
    SCALE = value


def get_map_image_path():
    """Get the map image path."""
    return MAP_IMAGE_PATH


def update_map_image_path(path):
    """Update the map image path."""
    global MAP_IMAGE_PATH
    MAP_IMAGE_PATH = path


def get_last_run_country():
    """Get the last ran country."""
    return LAST_RUN_COUNTRY


def update_last_run_country(country):
    """Update the last ran country."""
    global LAST_RUN_COUNTRY
    LAST_RUN_COUNTRY = country


def get_last_run_year():
    """Get the last ran year."""
    return LAST_RUN_YEAR


def update_last_run_year(year):
    """Update the last ran year."""
    global LAST_RUN_YEAR
    LAST_RUN_YEAR = year


def get_last_run_num_grounds():
    """Get the last ran number of grounds."""
    return LAST_RUN_NUM_GROUNDS


def update_last_run_num_grounds(num_grounds):
    """Update the last ran number of grounds."""
    global LAST_RUN_NUM_GROUNDS
    LAST_RUN_NUM_GROUNDS = num_grounds


def get_last_run_cyclone_list():
    """Get the last ran cyclone list."""
    return LAST_RUN_CYCLONE_LIST


def update_last_run_cyclone_list(cyclone_list):
    """Update the last ran cyclone list."""
    global LAST_RUN_CYCLONE_LIST
    LAST_RUN_CYCLONE_LIST = cyclone_list


def get_table_headers():
    """Get the table headers."""
    return TABLE_HEADERS


def update_table_headers(headers):
    """Update the table headers."""
    global TABLE_HEADERS
    TABLE_HEADERS = headers


def get_eog_access_token():
    """Get the EOG access token."""
    return EOG_ACCESS_TOKEN


def update_eog_access_token(token):
    """Update the EOG access token."""
    global EOG_ACCESS_TOKEN
    EOG_ACCESS_TOKEN = token


def get_current_simulation_type():
    """Get the current simulation type."""
    return CURRENT_SIMULATION_TYPE


def update_current_simulation_type(simulation_type):
    """Update the current simulation type."""
    global CURRENT_SIMULATION_TYPE
    CURRENT_SIMULATION_TYPE = simulation_type


def time_execution(log_message=""):
    def decorator_time_execution(func):
        @functools.wraps(func)
        def wrapper_time_execution(*args, **kwargs):
            start_time = time.perf_counter()
            value = func(*args, **kwargs)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            logger.info(
                f"Function '{log_message if log_message else func.__name__}' " f"executed in {elapsed_time:.2f} seconds"
            )
            return value

        return wrapper_time_execution

    return decorator_time_execution


def get_statistics_image_fname(statistic):
    """Get the statistics image path."""
    from config import statistic_img_mapping

    if statistic == "all":
        return statistic_img_mapping["all"]
    elif "ground" in statistic.lower():
        id = statistic.split("_")[-1]
        return statistic_img_mapping["ground"].format(id=id)
    else:
        return None


def resize_image(image_path, target_size):
    """Resize the image and apply sharpening."""
    if image_path in resized_image_cache:
        logger.info("Using cached resized image...")
        return resized_image_cache[image_path]

    with Image.open(image_path) as image:
        # Convert to RGB if the image is in palette mode
        if image.mode == "P":
            image = image.convert("RGB")

        # Resize the image
        image = image.resize(target_size, Image.LANCZOS)

        # Apply sharpening filter
        image = image.filter(ImageFilter.SHARPEN)

        # Save resized image
        resized_image_path = os.path.join(os.path.dirname(image_path), "resized_" + os.path.basename(image_path))
        image.save(resized_image_path)

        # Cache and return the resized image path
        resized_image_cache[image_path] = resized_image_path
        return resized_image_path


def get_size_statistics_tab(window):
    """Get the size of the statistics tab, handling zero size."""
    retry_count = 5  # Max retries to avoid infinite loops
    scrollbar_width = 20
    window.refresh()

    for _ in range(retry_count):
        width, height = window["statistics_column"].get_size()
        if width > 1 and height > 1:
            logger.info(f"Statistics tab size: {width}x{height}")
            return width - scrollbar_width, height - scrollbar_width

    logger.warning("Statistics tab size could not be determined, using default values.")
    return 948, 599  # Default fallback size
