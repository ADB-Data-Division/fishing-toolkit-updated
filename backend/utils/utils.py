"""
Utility functions for path handling, especially for PyInstaller compatibility.
"""

import os
import shutil
import sys


def get_base_path():
    """
    Get the base path of the application.

    When running as a PyInstaller bundle, returns the temporary directory
    where files are extracted. Otherwise, returns the directory containing main.py.

    Returns:
        str: Base path of the application
    """
    if getattr(sys, "frozen", False):
        # Running as a PyInstaller bundle
        # _MEIPASS is the temporary directory where PyInstaller extracts files
        return sys._MEIPASS
    else:
        # Running as a normal Python script
        # Return the directory containing main.py (project root)
        # __file__ is backend/utils/utils.py, so go up 3 levels to reach project root
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_resource_path(relative_path: str) -> str:
    """
    Get the absolute path to a resource file.

    Works both in development and when packaged as a PyInstaller executable.

    Args:
        relative_path: Path relative to the application base directory

    Returns:
        str: Absolute path to the resource
    """
    base_path = get_base_path()
    return os.path.join(base_path, relative_path)


def is_running_as_executable() -> bool:
    """
    Check if the application is running as a PyInstaller executable.

    Returns:
        bool: True if running as executable, False otherwise
    """
    return getattr(sys, "frozen", False)


def get_config_path() -> str:
    """
    Get the path to the .app_config file, handling both development
    and PyInstaller environments.

    Returns:
        str: Path to the .app_config file
    """
    if getattr(sys, "frozen", False):
        # If the application is running as a bundle (PyInstaller)
        base_path = sys._MEIPASS
        return os.path.join(base_path, ".app_config")
    else:
        # Development environment
        return ".app_config"


def get_database_path(relative_db_path: str) -> str:
    """
    Get the absolute path to a database file.

    When running as executable, database files are saved next to the executable.
    If the database file doesn't exist, it will be copied from the bundle.
    When running as script, database files are saved in the project directory.

    Args:
        relative_db_path: Path relative to the database directory (e.g., "database/nowcast.json")

    Returns:
        str: Absolute path to the database file
    """
    if getattr(sys, "frozen", False):
        # Running as executable - save database next to executable
        base_dir = os.path.dirname(sys.executable)
        db_path = os.path.join(base_dir, relative_db_path)

        # If database doesn't exist, copy from bundle
        if not os.path.exists(db_path):
            # Get path in bundle
            bundle_db_path = os.path.join(sys._MEIPASS, relative_db_path)

            # Copy from bundle if it exists
            if os.path.exists(bundle_db_path):
                # Ensure directory exists
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                # Copy file
                shutil.copy2(bundle_db_path, db_path)
                print(f"Copied database file from bundle: {relative_db_path}")
            else:
                # File doesn't exist in bundle either, create directory
                os.makedirs(os.path.dirname(db_path), exist_ok=True)

        return db_path
    else:
        # Running as script - use project root
        base_path = get_base_path()
        return os.path.join(base_path, relative_db_path)
