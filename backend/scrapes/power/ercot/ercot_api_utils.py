"""
Shared ERCOT API helpers for authentication, request handling, and response parsing.

Used by all ERCOT scrape scripts that call the ERCOT Public Reports API
(https://api.ercot.com/api/public-reports/).

The energy_storage_resources_daily script uses a different dashboard endpoint
and does not use this module.
"""

import json
import logging
import time

import pandas as pd
import requests

from backend import secrets

ERCOT_BASE_URL = "https://api.ercot.com/api/public-reports"
ERCOT_AUTH_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)


def get_authentication_headers(
    logger: logging.Logger | None = None,
    username: str = secrets.ERCOT_USERNAME,
    passcode: str = secrets.ERCOT_PASSCODE,
    api_key: str = secrets.ERCOT_API_KEY,
) -> dict:
    """Authenticate with ERCOT B2C and return request headers."""
    auth_url = (
        f"{ERCOT_AUTH_URL}"
        f"?username={username}"
        f"&password={passcode}"
        f"&grant_type=password"
        f"&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access"
        f"&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70"
        f"&response_type=id_token"
    )

    response = requests.post(auth_url)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        if logger:
            logger.info("ERCOT authentication successful")
        return {
            "accept": "application/json",
            "Ocp-Apim-Subscription-Key": api_key,
            "Authorization": access_token,
        }

    msg = f"Failed to authenticate: {response.status_code} - {response.text}"
    if logger:
        logger.error(msg)
    raise Exception(msg)


def make_request(
    endpoint: str,
    params: dict,
    logger: logging.Logger | None = None,
    base_url: str = ERCOT_BASE_URL,
    max_retries: int = 30,
    retry_delay: int = 5,
) -> requests.Response:
    """Make an authenticated request to the ERCOT API with retry logic.

    Authenticates once per call, then retries the GET request up to
    ``max_retries`` times if the response contains no data or is malformed.
    """
    headers = get_authentication_headers(logger=logger)
    url = f"{base_url}/{endpoint}"

    # Ensure a large page size so results are not silently truncated.
    params.setdefault("size", 1_000_000)

    if logger:
        logger.info(f"Endpoint: {endpoint} | params={params}")

    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, params=params)

        try:
            response_json = response.content.decode("utf-8")
            response_dict = json.loads(response_json)

            if response_dict.get("data") is not None:
                if logger:
                    logger.info(f"Data received from {endpoint}")
                return response

            if logger:
                logger.info(
                    f"Attempt {attempt + 1}/{max_retries}: "
                    f"Data not yet available. Retrying in {retry_delay}s..."
                )
            time.sleep(retry_delay)

        except (json.JSONDecodeError, KeyError) as e:
            if logger:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries}: "
                    f"Error processing response: {e}. Retrying in {retry_delay}s..."
                )
            time.sleep(retry_delay)

    raise Exception(
        f"Failed to get valid response after {max_retries} attempts from {endpoint}"
    )


def parse_response(response: requests.Response) -> pd.DataFrame:
    """Parse the standard ERCOT API JSON response into a DataFrame.

    The ERCOT Public Reports API returns ``{"fields": [...], "data": [...]}``.
    """
    body = response.json()
    columns = [field["name"] for field in body["fields"]]
    data = body["data"]
    return pd.DataFrame(data, columns=columns)
