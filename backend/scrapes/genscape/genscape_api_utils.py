"""
Shared Genscape API request helper.

Provides a rate-limit-aware HTTP client used by all Genscape ingestion scripts.
"""

import time

import pandas as pd
import requests

from backend import secrets


def make_request(
    url: str,
    params: dict,
    logger,
    max_attempts: int = 10,
    rate_limit: int = 10,
) -> pd.DataFrame:
    """Make a Genscape API GET request with retry logic for rate limiting.

    Args:
        url: Genscape API endpoint URL.
        params: Query parameters for the request.
        logger: Logger instance for structured output.
        max_attempts: Maximum number of retry attempts.
        rate_limit: Seconds to wait on 429 responses.

    Returns:
        DataFrame of the JSON response data.

    Raises:
        RuntimeError: If all attempts fail or no data is returned.
    """
    headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache",
        "Gen-Api-Key": secrets.GEN_API_KEY,
    }

    attempt = 0
    response = None
    while attempt < max_attempts:
        response = requests.get(url=url, headers=headers, params=params)

        if response.status_code == 200:
            logger.info(
                f"API request succeeded on attempt {attempt + 1} | "
                f"url={url} | params={params}"
            )
            break

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", rate_limit)
            wait_seconds = int(retry_after)
            logger.warning(
                f"Rate limited (429) on attempt {attempt + 1}/{max_attempts} | "
                f"retry_after={wait_seconds}s | response={response.text}"
            )
            time.sleep(wait_seconds)
        else:
            logger.warning(
                f"Request failed on attempt {attempt + 1}/{max_attempts} | "
                f"status={response.status_code} | response={response.text}"
            )
            time.sleep(rate_limit)

        attempt += 1

    if response is None or response.status_code != 200:
        raise RuntimeError(
            f"Failed after {max_attempts} attempts | url={url} | params={params}"
        )

    data = response.json()["data"]
    df = pd.DataFrame(data)

    if len(df) == 0:
        raise RuntimeError(f"No data returned | url={url} | params={params}")

    logger.info(f"Received {len(df)} rows from API")
    return df
