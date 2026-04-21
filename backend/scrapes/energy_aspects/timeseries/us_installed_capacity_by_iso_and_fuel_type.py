"""
Energy Aspects — US Installed Capacity by ISO and Fuel Type.

Mapping ID: 270
84 datasets covering installed capacity by fuel type across all ISOs/regions.
Source: .refactor/energy_aspects_v1_2025_dec_28/us_installed_capacity_by_iso_and_fuel_type.csv
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

API_SCRAPE_NAME = "us_installed_capacity_by_iso_and_fuel_type"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# From dataset_mappings mapping_id=270
DATASET_IDS = [
    24359, 24360, 24361, 24362, 24363, 24364, 24365, 24366, 24367, 24368,
    24369, 24370, 24371, 24372, 24373, 24374, 24375, 24376, 24377, 24378,
    24379, 24380, 24381, 24382, 24383, 24384, 24385, 24386, 24387, 24388,
    24389, 24390, 24391, 24392, 24393, 24394, 24395, 24396, 24397, 24398,
    24399, 24400, 24401, 24402, 24403, 24404, 24405, 24406, 24407, 24408,
    24409, 24410, 24411, 24412, 24413, 24414, 24415, 24416, 24417, 24418,
    24419, 24420, 24421, 24422, 24423, 24424, 24425, 24426, 24427, 24428,
    582720, 582721, 582722, 582723, 582724, 582725, 582726, 582727, 582728,
    582729, 582730, 582731, 582732, 582733,
]


def _pull(
    date_from: str = "2012-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} datasets from EA API...")
    df = ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
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
