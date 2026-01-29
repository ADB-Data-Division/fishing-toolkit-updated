import os

import requests

from backend.utils.logger import get_logger

logger = get_logger(__name__)

# Environment variables are loaded in main.py at application startup
REQUEST_TIMEOUT = int(os.getenv("HTTP_REQUEST_TIMEOUT", "30"))
TOKEN_URL = os.getenv("EOG_TOKEN_URL", "https://eogauth-new.mines.edu/realms/eog/protocol/openid-connect/token")


def get_access_token():
    try:
        username = os.getenv("EOG_USERNAME")
        password = os.getenv("EOG_PASSWORD")
        if not username or not password:
            raise ValueError("Username or password environment variables not set")

        params = {
            "client_id": os.getenv("EOG_CLIENT_ID", "eogdata_oidc"),
            "client_secret": os.getenv("EOG_CLIENT_SECRET", ""),
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        token_url = TOKEN_URL
        if not token_url:
            raise ValueError("EOG_TOKEN_URL environment variable not set")
        response = requests.post(token_url, data=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        access_token_dict = response.json()
        return access_token_dict.get("access_token")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    return None
