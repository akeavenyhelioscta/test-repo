"""
Energy Aspects — Lower 48 Installed Capacity by fuel type (MW).

US48 installed capacity datasets.
Source: .refactor/energy_aspects_v1_2025_dec_28/lower_48_installed_capacity_mw.csv
"""

from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.energy_aspects import energy_aspects_api_utils as ea_api

API_SCRAPE_NAME = "lower_48_installed_capacity_mw"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# US48 installed capacity datasets
DATASET_IDS = [24423, 24428, 24422, 24424, 24425, 24427, 24426]

COLUMN_MAP = {
    "24423": "coal_mw",
    "24428": "hydro_mw",
    "24422": "natural_gas_mw",
    "24424": "nuclear_mw",
    "24425": "oil_mw",
    "24427": "onshore_wind_mw",
    "24426": "solar_mw",
}


def _pull(
    date_from: str = "2012-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} datasets from EA API...")
    df = ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAP)
    return ea_api.make_postgres_safe_columns(df)


def _upsert(
    df: pd.DataFrame,
    schema: str = "energy_aspects",
    table_name: str = API_SCRAPE_NAME,
) -> None:
    data_types = azure_postgresql.infer_sql_data_types(df=df)
    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=["date"],
    )


def main():
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="energy_aspects",
        target_table=f"energy_aspects.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        df = _pull()

        if df.empty:
            logger.section("No data returned, skipping upsert.")
        else:
            df = _format(df)
            logger.section(f"Upserting {len(df)} rows, {len(df.columns)} columns...")
            _upsert(df)
            logger.success(f"Upserted {len(df)} rows.")

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
