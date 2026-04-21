"""
Energy Aspects API — discover available data sources and endpoints.

This is a one-time / on-demand utility script that catalogs:
  1. All known API endpoints (static, from documentation).
  2. Available timeseries datasets (dynamic, from /datasets/timeseries).
  3. Valid metadata filter values (dynamic, from /metadata/timeseries).
  4. Website-to-API dataset mappings (dynamic, from /dataset_mappings).

Output: JSON file written to backend/scrapes/energy_aspects/output/catalog.json

Docs: https://developer.energyaspects.com/reference/quickstart-guide
"""

import json
from datetime import datetime
from pathlib import Path

from backend import secrets
from backend.utils import logging_utils

from backend.scrapes.energy_aspects import energy_aspects_api_utils as ea_api

# ── Config ──────────────────────────────────────────────────────────────────

API_SCRAPE_NAME = "energy_aspects_discover_catalog"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

OUTPUT_DIR = Path(__file__).parent / "output"

# ── Static endpoint catalog (from API documentation) ────────────────────────

ENDPOINT_CATALOG = [
    {
        "category": "Datasets",
        "endpoint": "List timeseries datasets",
        "method": "GET",
        "path": "/datasets/timeseries",
        "description": "List all available timeseries datasets. Supports pagination via /datasets/timeseries with page and records_per_page params.",
        "parameters": "api_key (required), page, records_per_page",
        "notes": "Returns dataset_id, name, metadata fields for each dataset.",
    },
    {
        "category": "Datasets",
        "endpoint": "Get dataset detail",
        "method": "GET",
        "path": "/datasets/timeseries/{dataset_id}",
        "description": "Get detailed info about a specific timeseries dataset by its dataset_id.",
        "parameters": "api_key (required), dataset_id (path)",
        "notes": "Use dataset_id values from the list endpoint.",
    },
    {
        "category": "Dataset Mappings",
        "endpoint": "Dataset mappings",
        "method": "GET",
        "path": "/dataset_mappings",
        "description": "Maps Energy Aspects website file categories to their corresponding dataset_id values.",
        "parameters": "api_key (required)",
        "notes": "Bridges the website UI to the API. Useful for translating known file downloads to API dataset_ids.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data (JSON)",
        "method": "GET",
        "path": "/timeseries",
        "description": "Retrieve timeseries data in JSON format (default).",
        "parameters": "api_key (required), dataset_id, date_from, date_to, release_date, geography, frequency, lifecycle_stage, category, category_detail, aspect, aspect_detail",
        "notes": "Auth via api_key query param. Dates as YYYY-MM-DD or dynamic notation (e.g. -1y, -3Mb). release_date retrieves historic revisions.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data (CSV)",
        "method": "GET",
        "path": "/timeseries/csv",
        "description": "Retrieve timeseries data in CSV format.",
        "parameters": "Same as /timeseries, plus column_header (metadata field for CSV column headers)",
        "notes": "Append /csv to the timeseries path for CSV output.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data (XLSX)",
        "method": "GET",
        "path": "/timeseries/xlsx",
        "description": "Retrieve timeseries data in Excel format.",
        "parameters": "Same as /timeseries, plus column_header",
        "notes": "Append /xlsx for Excel output.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data (paginated)",
        "method": "GET",
        "path": "/timeseries/pagination/",
        "description": "Paginated timeseries data retrieval. Also available as /pagination/json and /pagination/csv.",
        "parameters": "Same as /timeseries, plus page (default 1), records_per_page (default 5000)",
        "notes": "Non-paginated endpoints return all matching data at once. Use pagination for large result sets.",
    },
    {
        "category": "Changelog",
        "endpoint": "Changelog (JSON)",
        "method": "GET",
        "path": "/changelog",
        "description": "Dataset changelog showing when datasets were updated. Also available as /changelog/csv, /changelog/xlsx, /changelog/html.",
        "parameters": "api_key (required), date_from (required), date_to (required), dataset_id, page, records_per_page",
        "notes": "date_from and date_to are REQUIRED for changelog endpoints. Pagination available via /changelog/pagination/.",
    },
    {
        "category": "Metadata",
        "endpoint": "Timeseries metadata",
        "method": "GET",
        "path": "/metadata/timeseries",
        "description": "Returns valid parameter values for timeseries filtering (categories, aspects, geographies, frequencies, lifecycle stages).",
        "parameters": "api_key (required)",
        "notes": "Call this first to discover valid filter values before querying /timeseries.",
    },
]

# ── Dynamic discovery functions ─────────────────────────────────────────────


def _pull_metadata() -> dict:
    """Fetch valid timeseries filter values from /metadata/timeseries."""
    logger.section("Fetching metadata/timeseries (valid filter values)...")
    data = ea_api.get_json("/metadata/timeseries")
    logger.info(f"  Metadata response type: {type(data).__name__}")
    return data


def _pull_datasets() -> list[dict]:
    """Fetch all available timeseries datasets from /datasets/timeseries."""
    logger.section("Fetching datasets/timeseries (paginated)...")
    datasets = ea_api.get_paginated("/datasets/timeseries", records_per_page=5000)
    logger.info(f"  Found {len(datasets)} datasets")
    return datasets


def _pull_dataset_mappings() -> dict | list:
    """Fetch website-to-API dataset mappings from /dataset_mappings."""
    logger.section("Fetching dataset_mappings...")
    data = ea_api.get_json("/dataset_mappings")
    count = len(data) if isinstance(data, list) else "N/A"
    logger.info(f"  Mappings response type: {type(data).__name__}, count: {count}")
    return data


def _format_catalog(
    metadata: dict,
    datasets: list[dict],
    dataset_mappings: dict | list,
) -> dict:
    """Assemble the full catalog as a single JSON-serializable dict."""
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "api_base_url": ea_api.BASE_URL,
        "auth_method": "api_key query parameter",
        "static_endpoints": ENDPOINT_CATALOG,
        "dynamic": {
            "metadata_filter_values": metadata,
            "datasets": datasets,
            "dataset_count": len(datasets),
            "dataset_mappings": dataset_mappings,
        },
    }


def _save_catalog(catalog: dict) -> Path:
    """Write catalog to JSON file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "catalog.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)
    return output_path


# ── Orchestration ───────────────────────────────────────────────────────────


def main():
    """
    Discover and catalog all Energy Aspects API data sources.

    Outputs:
        - backend/scrapes/energy_aspects/output/catalog.json
        - Console summary of datasets found
    """
    try:
        logger.header(API_SCRAPE_NAME)

        # Pull from all discovery endpoints
        metadata = _pull_metadata()
        datasets = _pull_datasets()
        dataset_mappings = _pull_dataset_mappings()

        # Assemble catalog
        logger.section("Assembling catalog...")
        catalog = _format_catalog(metadata, datasets, dataset_mappings)

        # Save
        output_path = _save_catalog(catalog)
        logger.success(f"Catalog written to {output_path}")

        # Console summary
        logger.section("Summary")
        logger.info(f"  Static endpoints documented: {len(ENDPOINT_CATALOG)}")
        logger.info(f"  Datasets discovered:         {catalog['dynamic']['dataset_count']}")

        if datasets:
            # Show first few datasets as a preview
            logger.info("  First 10 datasets:")
            for ds in datasets[:10]:
                ds_id = ds.get("dataset_id", "?")
                meta = ds.get("metadata", {})
                desc = meta.get("description", "?")
                cat = meta.get("category", "")
                freq = meta.get("frequency", "")
                logger.info(f"    - [{ds_id}] {desc} ({cat}, {freq})")
            if len(datasets) > 10:
                logger.info(f"    ... and {len(datasets) - 10} more (see catalog.json)")

        return catalog

    except Exception as e:
        logger.exception(f"Catalog discovery failed: {e}")
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    catalog = main()
