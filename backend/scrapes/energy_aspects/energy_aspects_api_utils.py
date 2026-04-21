"""
Energy Aspects API utilities.

Base URL: https://api.energyaspects.com/data/
Auth: API key passed as `api_key` query parameter.
Docs: https://developer.energyaspects.com/reference/quickstart-guide
"""

import hashlib
import re
import requests
import logging
from io import StringIO

import pandas as pd

from backend import secrets

logger = logging.getLogger(__name__)

BASE_URL = "https://api.energyaspects.com/data"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63


def _get_api_key() -> str:
    key = secrets.ENERGY_ASPECTS_API_KEY
    if not key:
        raise ValueError(
            "ENERGY_ASPECTS_API_KEY not set. "
            "Add it to your .env file or environment variables."
        )
    return key


def get(endpoint: str, params: dict | None = None, timeout: int = 60) -> requests.Response:
    """
    Make an authenticated GET request to the Energy Aspects API.

    Args:
        endpoint: Path relative to BASE_URL (e.g. "/datasets/timeseries").
        params: Additional query parameters (api_key is added automatically).
        timeout: Request timeout in seconds.

    Returns:
        requests.Response
    """
    url = f"{BASE_URL}{endpoint}"
    request_params = {"api_key": _get_api_key()}
    if params:
        request_params.update(params)

    logger.debug(f"GET {url} params={_redact_params(request_params)}")
    response = requests.get(url, params=request_params, timeout=timeout)
    response.raise_for_status()
    return response


def get_json(endpoint: str, params: dict | None = None, timeout: int = 60) -> dict | list:
    """GET request returning parsed JSON."""
    return get(endpoint, params=params, timeout=timeout).json()


def get_paginated(
    endpoint: str,
    params: dict | None = None,
    records_per_page: int = 5000,
    max_pages: int = 100,
    timeout: int = 60,
) -> list[dict]:
    """
    Fetch all pages from a paginated Energy Aspects endpoint.

    Pagination endpoints use /pagination/ path variants with `page` and
    `records_per_page` query parameters.

    Returns:
        Combined list of all records across pages.
    """
    all_records = []
    page = 1
    request_params = {"records_per_page": records_per_page}
    if params:
        request_params.update(params)

    while page <= max_pages:
        request_params["page"] = page
        data = get_json(endpoint, params=request_params, timeout=timeout)

        if isinstance(data, list):
            if not data:
                break
            all_records.extend(data)
            if len(data) < records_per_page:
                break
        elif isinstance(data, dict):
            # Some endpoints wrap results in a key
            records = data.get("data", data.get("results", []))
            if not records:
                break
            all_records.extend(records)
            if len(records) < records_per_page:
                break
        else:
            break

        page += 1

    return all_records


def pull_timeseries(
    dataset_ids: list[int],
    date_from: str | None = None,
    date_to: str | None = None,
    batch_size: int = 50,
    timeout: int = 120,
) -> pd.DataFrame:
    """
    Pull timeseries data for multiple dataset_ids and return a wide DataFrame.

    Uses /timeseries/csv with column_header=dataset_id, then renames columns
    from dataset_id integers to descriptive names from the API metadata.

    Args:
        dataset_ids: List of EA dataset_id integers.
        date_from: Start date (YYYY-MM-DD) or None for all available.
        date_to: End date (YYYY-MM-DD) or None for all available.
        batch_size: Max dataset_ids per API call (avoids URL length limits).
        timeout: Request timeout in seconds.

    Returns:
        DataFrame with 'date' index and one column per dataset_id.
        Column names are the dataset_id integers (as strings).
    """
    all_dfs = []

    for i in range(0, len(dataset_ids), batch_size):
        batch = dataset_ids[i : i + batch_size]
        params = {
            "dataset_id": ",".join(str(d) for d in batch),
            "column_header": "dataset_id",
        }
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to

        resp = get("/timeseries/csv", params=params, timeout=timeout)
        if not resp.text.strip():
            continue

        df = pd.read_csv(StringIO(resp.text))
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    # Merge batches on Date column
    result = all_dfs[0]
    for df in all_dfs[1:]:
        result = result.merge(df, on="Date", how="outer")

    # Normalize date column
    result.rename(columns={"Date": "date"}, inplace=True)
    result["date"] = pd.to_datetime(result["date"])
    result.sort_values("date", inplace=True)
    result.reset_index(drop=True, inplace=True)

    # Convert column names to strings for consistent handling
    result.columns = [str(c) for c in result.columns]

    return result


def get_dataset_metadata(dataset_ids: list[int], timeout: int = 120) -> dict[int, dict]:
    """
    Fetch metadata for a list of dataset_ids.

    Returns:
        Dict mapping dataset_id → metadata dict (description, unit, etc.)
    """
    data = get_json("/timeseries", params={
        "dataset_id": ",".join(str(d) for d in dataset_ids),
        "date_from": "2025-01-01",
        "date_to": "2025-01-01",
    }, timeout=timeout)

    return {
        item["dataset_id"]: item["metadata"]
        for item in data
        if isinstance(item, dict) and "dataset_id" in item
    }


def description_to_column(desc: str) -> str:
    """
    Convert an EA API metadata description to a snake_case column name.

    Examples:
        "Monthly EA forecast for on-peak heat rate in ERCOT-North in mmbtu/MWh"
        → "fcst_on_peak_heat_rate_in_ercot_north_in_mmbtu_per_mwh"
    """
    s = desc
    # Strip common prefixes
    s = re.sub(
        r"^Monthly EA (forecast for |actual )",
        lambda m: "fcst_" if "forecast" in m.group() else "ea_actual_",
        s,
    )
    # Abbreviations (order matters — longer phrases first)
    replacements = [
        ("natural gas", "ng"),
        ("installed capacity", "installed_capacity"),
        ("dispatch cost", "dis_cost"),
        ("fuel cost", "fuel_cost"),
        ("heat rate", "heat_rate"),
        ("spark spread", "spark_spread"),
        ("power prices", "power_prices"),
        ("oil products", "oil_products"),
        ("onshore wind", "onshore_wind"),
        ("offshore wind", "offshore_wind"),
        ("bituminous coal", "bit_coal"),
        ("sub-bituminous coal", "sub_bit_coal"),
        ("normal weather", "norm_weather"),
        ("forward price", "forward_price"),
        ("(included)", "inc"),
        ("(CCA included)", "cca_inc"),
        ("(RGGI included)", "rggi_inc"),
        ("(RGGI and GWSA included)", "rggi_and_gwsa_inc"),
        (" for ", " "),
    ]
    for old, new in replacements:
        s = s.replace(old, new)
    # Unit replacements (before general cleanup)
    s = s.replace("$/MWh", "usd_mwh")
    s = s.replace("mmbtu/MWh", "mmbtu_per_mwh")
    s = s.replace("bcf/d", "bcf_per_d")
    # Region cleanup
    s = s.replace(" in US: ", " ")
    s = s.replace(" in US:", " ")
    # Lowercase and sanitize
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s


def normalize_postgres_identifier(
    name: str,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> str:
    """
    Convert a string to a PostgreSQL-safe identifier.

    PostgreSQL truncates identifiers to 63 characters. For long names we keep
    readable start/end segments and append a short hash for deterministic
    uniqueness.
    """
    identifier = re.sub(r"[^a-z0-9_]", "_", str(name).lower())
    identifier = re.sub(r"_+", "_", identifier).strip("_")

    if not identifier:
        identifier = "col"
    if identifier[0].isdigit():
        identifier = f"col_{identifier}"
    if len(identifier) <= max_length:
        return identifier

    digest = hashlib.sha1(identifier.encode("utf-8")).hexdigest()[:8]
    tail_length = min(16, max_length - len(digest) - 2)
    head_length = max_length - len(digest) - tail_length - 2

    prefix = identifier[:head_length].rstrip("_")
    suffix = identifier[-tail_length:].lstrip("_")

    return f"{prefix}_{suffix}_{digest}"


def make_unique_postgres_identifiers(
    names: list[str],
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> list[str]:
    """Return PostgreSQL-safe identifiers with deterministic de-duplication."""
    used: set[str] = set()
    safe_names: list[str] = []

    for name in names:
        base = normalize_postgres_identifier(name, max_length=max_length)
        candidate = base
        counter = 2

        while candidate in used:
            suffix = f"_{counter}"
            trimmed_base = base[: max_length - len(suffix)].rstrip("_")
            candidate = f"{trimmed_base}{suffix}"
            counter += 1

        used.add(candidate)
        safe_names.append(candidate)

    return safe_names


def make_postgres_safe_columns(
    df: pd.DataFrame,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> pd.DataFrame:
    """Return a copy of *df* with PostgreSQL-safe, unique column names."""
    safe_columns = make_unique_postgres_identifiers(
        [str(col) for col in df.columns],
        max_length=max_length,
    )
    if list(df.columns) == safe_columns:
        return df

    result = df.copy()
    result.columns = safe_columns
    return result


def build_column_map(dataset_ids: list[int], timeout: int = 120) -> dict[str, str]:
    """
    Build a {str(dataset_id): column_name} mapping by fetching metadata
    and converting descriptions to snake_case column names.
    """
    meta = get_dataset_metadata(dataset_ids, timeout=timeout)
    raw_map = {
        str(did): description_to_column(meta[did]["description"])
        for did in dataset_ids
        if did in meta
    }
    safe_names = make_unique_postgres_identifiers(list(raw_map.values()))
    return dict(zip(raw_map.keys(), safe_names))


def _redact_params(params: dict) -> dict:
    """Redact api_key from params for logging."""
    return {k: ("***" if k == "api_key" else v) for k, v in params.items()}
