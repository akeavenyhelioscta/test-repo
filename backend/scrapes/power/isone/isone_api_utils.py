"""Shared utilities for ISO-NE isoexpress CSV scraping.

All ISONE scrape scripts download public CSV reports from the ISO-NE
isoexpress site.  A session cookie must be established before the CSV
endpoints return data, so every request first visits a page on the site
to warm up the session.
"""

import io
import time

import pandas as pd
import requests

ISONE_BASE_URL = "https://www.iso-ne.com"
COOKIE_WARMUP_URL = (
    f"{ISONE_BASE_URL}/isoexpress/web/reports/operations/-/tree/gen-fuel-mix"
)

# Throttle between requests to avoid ISO-NE rate limiting.
REQUEST_DELAY_SECONDS = 1.0


def make_request(url: str, logger=None, retries: int = 3) -> requests.Response:
    """Make a request to ISO-NE with cookie warmup and retry logic."""
    last_error = None
    for attempt in range(retries):
        try:
            with requests.Session() as s:
                s.get(COOKIE_WARMUP_URL)
                response = s.get(url)
                content_type = response.headers.get("Content-Type", "")
                if logger:
                    logger.info(f"Pulling from ... {url}")
                    logger.info(
                        f"Status Code: {response.status_code} ... "
                        f"Content Type: {content_type}"
                    )
                if response.status_code == 200 and "text/csv" in content_type:
                    time.sleep(REQUEST_DELAY_SECONDS)
                    return response
                last_error = f"status={response.status_code}, content_type={content_type}"
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            if logger:
                logger.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")

        time.sleep(REQUEST_DELAY_SECONDS)

    raise RuntimeError(
        f"Failed to get data from {url} after {retries} attempts ({last_error})"
    )


def parse_csv_response(
    response: requests.Response,
    skiprows=None,
    skipfooter: int = 1,
) -> pd.DataFrame:
    """Parse a CSV response from ISO-NE into a DataFrame."""
    if skiprows is None:
        skiprows = [0, 1, 2, 3, 5]
    return pd.read_csv(
        io.StringIO(response.content.decode("utf8")),
        skiprows=skiprows,
        skipfooter=skipfooter,
        engine="python",
    )
