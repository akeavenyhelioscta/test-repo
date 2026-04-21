"""
Meteologica: USA US48 power demand forecast ECMWF ENS hourly

Content ID: 4213
Content Name: USA US48 power demand forecast ECMWF ENS hourly
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

API_SCRAPE_NAME = "usa_us48_power_demand_forecast_ecmwf_ens_hourly"
CONTENT_ID = 4213

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# Column mapping: raw API names -> clean snake_case
COLUMN_RENAME_MAP = {
    "From yyyy-mm-dd hh:mm": "forecast_period_start",
    "To yyyy-mm-dd hh:mm": "forecast_period_end",
    "ARPEGE RUN": "arpege_run",
    "ECMWF ENS RUN": "ecmwf_ens_run",
    "ECMWF HRES RUN": "ecmwf_hres_run",
    "GFS RUN": "gfs_run",
    "NAM RUN": "nam_run",
    "UTC offset from (UTC+/-hhmm)": "utc_offset_from",
    "UTC offset to (UTC+/-hhmm)": "utc_offset_to",
    "forecast": "forecast_mw",
    "perc10": "perc10_mw",
    "perc90": "perc90_mw",
}

# Desired column order in the final DataFrame
COLUMN_ORDER = [
    "content_id",
    "content_name",
    "update_id",
    "issue_date",
    "forecast_period_start",
    "forecast_period_end",
    "utc_offset_from",
    "utc_offset_to",
    "forecast_mw",
    "perc10_mw",
    "perc90_mw",
    "arpege_run",
    "ecmwf_ens_run",
    "ecmwf_hres_run",
    "gfs_run",
    "nam_run",
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
        - Weather model run columns (arpege_run, gfs_run, etc.) may be absent
          depending on the content type and forecast horizon.
        - forecast_mw, perc10_mw, perc90_mw arrive as strings from the API and
          are cast to float.
    """
    # Rename columns
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Add metadata columns
    df["content_id"] = metadata["content_id"]
    df["content_name"] = metadata["content_name"]
    df["update_id"] = metadata["update_id"]
    df["issue_date"] = metadata["issue_date"]

    # Cast numeric columns (API returns these as strings)
    for col in ["forecast_mw", "perc10_mw", "perc90_mw"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast datetime columns
    for col in ["forecast_period_start", "forecast_period_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M", errors="coerce")

    # Model run datetime columns -- keep as VARCHAR strings.
    # The upsert utility calls df.fillna(0) which would corrupt TIMESTAMP NaT
    # values into "0". By keeping these as strings, missing values become empty.
    model_run_cols = ["arpege_run", "ecmwf_ens_run", "ecmwf_hres_run", "gfs_run", "nam_run"]
    for col in model_run_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

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

    Primary key: (update_id, forecast_period_start)
        - update_id uniquely identifies the forecast batch (includes model run info)
        - forecast_period_start uniquely identifies the hour within that batch
        - Together they allow storing multiple forecast updates and deduplicating
          if the same update is fetched again.
    """
    primary_keys = ["update_id", "forecast_period_start"]
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
