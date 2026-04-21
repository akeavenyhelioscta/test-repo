"""
Meteologica: USA US48 power demand long term hourly

Content ID: 8485
Content Name: USA US48 power demand long term hourly
Source: https://api-markets.meteologica.com/api/v1/
"""

from pathlib import Path

import pandas as pd


from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.meteologica.auth import make_get_request

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

API_SCRAPE_NAME = "usa_us48_power_demand_long_term_hourly"
CONTENT_ID = 8485

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# Column mapping: raw API names -> clean snake_case
# This content type has Year columns (Year 1979..Year 2026), Average, Bottom, Top.
# Year columns are renamed dynamically in _format() to avoid listing all 48 here.
COLUMN_RENAME_MAP = {
    "From yyyy-mm-dd hh:mm": "forecast_period_start",
    "To yyyy-mm-dd hh:mm": "forecast_period_end",
    "UTC offset from (UTC+/-hhmm)": "utc_offset_from",
    "UTC offset to (UTC+/-hhmm)": "utc_offset_to",
    "Average": "average_mw",
    "Bottom": "bottom_mw",
    "Top": "top_mw",
}

# Desired column order in the final DataFrame (year columns appended dynamically)
COLUMN_ORDER = [
    "content_id",
    "content_name",
    "update_id",
    "issue_date",
    "forecast_period_start",
    "forecast_period_end",
    "utc_offset_from",
    "utc_offset_to",
    "average_mw",
    "bottom_mw",
    "top_mw",
]


# --------------------------------------------------------------------------- #
# _pull
# --------------------------------------------------------------------------- #

def _pull(content_id: int = CONTENT_ID) -> tuple[pd.DataFrame, dict]:
    """
    Fetch the latest forecast data from the Meteologica API.

    Calls GET /contents/{content_id}/data (no update_id = latest update).

    Returns:
        tuple: (raw DataFrame from the "data" array, metadata dict with
                content_id, content_name, update_id, issue_date, timezone, unit)
    """
    response = make_get_request(f"contents/{content_id}/data")
    payload = response.json()

    metadata = {
        "content_id": payload["content_id"],
        "content_name": payload["content_name"],
        "update_id": payload["update_id"],
        "issue_date": payload["issue_date"],
        "timezone": payload.get("timezone"),
        "unit": payload.get("unit"),
    }

    df = pd.DataFrame(payload["data"])

    logger.info(
        f"Pulled {len(df)} rows | update_id={metadata['update_id']} | "
        f"issue_date={metadata['issue_date']}"
    )

    return df, metadata


# --------------------------------------------------------------------------- #
# _format
# --------------------------------------------------------------------------- #

def _format(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    """
    Rename columns to snake_case, cast dtypes, and add metadata columns.

    Notes:
        - This content type has "Year XXXX" columns (Year 1979..Year 2026)
          which are renamed to "year_XXXX" for valid SQL column names.
        - Average, Bottom, Top are renamed to average_mw, bottom_mw, top_mw.
    """
    # Rename "Year XXXX" columns to "year_XXXX" (valid SQL identifiers)
    year_rename = {c: c.lower().replace(" ", "_") for c in df.columns if c.startswith("Year ")}
    df = df.rename(columns=year_rename)

    # Rename standard columns
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Add metadata columns
    df["content_id"] = metadata["content_id"]
    df["content_name"] = metadata["content_name"]
    df["update_id"] = metadata["update_id"]
    df["issue_date"] = metadata["issue_date"]

    # Cast numeric columns (API returns these as strings)
    numeric_cols = ["average_mw", "bottom_mw", "top_mw"] + [c for c in df.columns if c.startswith("year_")]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast datetime columns
    for col in ["forecast_period_start", "forecast_period_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M", errors="coerce")

    # Reorder columns (only include columns that exist)
    ordered_cols = [c for c in COLUMN_ORDER if c in df.columns]
    extra_cols = [c for c in df.columns if c not in COLUMN_ORDER]
    df = df[ordered_cols + extra_cols]

    logger.info(f"Formatted DataFrame: {len(df)} rows x {len(df.columns)} cols")

    return df


# --------------------------------------------------------------------------- #
# _upsert
# --------------------------------------------------------------------------- #

def _upsert(
    df: pd.DataFrame,
    database: str = "helioscta",
    schema: str = "meteologica",
    table_name: str = API_SCRAPE_NAME,
) -> None:
    """
    Upsert the formatted DataFrame to Azure PostgreSQL.

    Primary key: (update_id, forecast_period_start, forecast_period_end)
        - Includes forecast_period_end to handle DST fall-back duplicate hours.
    """
    primary_keys = ["update_id", "forecast_period_start", "forecast_period_end"]
    data_types = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_keys,
    )


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main():
    """
    Orchestrate the Meteologica forecast pull -> format -> upsert pipeline.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"meteologica.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")

        # pull
        logger.section("Pulling latest forecast data...")
        df, metadata = _pull()

        # format
        logger.section("Formatting data...")
        df = _format(df, metadata)

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(
            f"Successfully pulled and upserted {len(df)} rows | "
            f"update_id={metadata['update_id']}"
        )

        run.success(rows_processed=len(df))

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df


if __name__ == "__main__":
    
    df, metadata = _pull()
    df = _format(df, metadata)
    _upsert(df)
    print(f"Done -- {len(df)} rows upserted (update_id={metadata['update_id']})")
