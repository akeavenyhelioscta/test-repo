"""
Energy Aspects — ISO Dispatch Costs.

Mapping ID: 157
95 datasets covering dispatch costs by fuel type, plant type, and ISO region.
Source: .refactor/energy_aspects_v1_2025_dec_28/iso_dispatch_costs.csv
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

API_SCRAPE_NAME = "iso_dispatch_costs"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# From dataset_mappings mapping_id=157
DATASET_IDS = [
    9143, 9144, 9145, 9146, 9147, 9148, 9149, 9150, 9151, 9152, 9153,
    9162, 9163, 9164, 9165, 9166, 9167, 9168, 9169, 9170, 9171, 9172,
    9173, 9174, 9175, 9176, 9177,
    9183, 9184, 9185, 9186, 9187, 9188, 9189, 9190, 9191, 9192, 9193,
    9202, 9203, 9204, 9205, 9206, 9207, 9208, 9209, 9210, 9211, 9212,
    9213, 9214, 9215, 9216, 9217, 9218, 9219, 9220, 9221, 9222, 9223,
    9224, 9225,
    9229, 9230, 9231, 9232, 9233, 9234, 9235, 9236,
    9243, 9244, 9245, 9246, 9247, 9248, 9249, 9250, 9251, 9252, 9253, 9254,
    9818,
    10052, 10053, 10054, 10055, 10056, 10057, 10058, 10059, 10060,
    11948, 11949, 11950,
]


def _pull(
    date_from: str = "2018-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} datasets from EA API...")
    df = ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
    # Auto-generate column names from API metadata descriptions
    logger.info("Building column name mapping from API metadata...")
    column_map = ea_api.build_column_map(DATASET_IDS)
    df = df.rename(columns=column_map)
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
