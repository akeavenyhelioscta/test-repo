"""
Energy Aspects — North American power price, heat rate and spark forecast.

Mapping ID: 45
Dataset IDs: 806-809, 814-817, 9088-9091, 15385-15386, 15390
Source: .refactor/energy_aspects_v1_2025_dec_28/na_power_price_heat_rate_spark_forecasts.csv
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

API_SCRAPE_NAME = "na_power_price_heat_rate_spark_forecasts"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

DATASET_IDS = [806, 807, 808, 809, 814, 815, 816, 817, 9088, 9089, 9090, 9091, 15385, 15386, 15390]

COLUMN_MAP = {
    "806": "fcst_on_peak_heat_rate_in_ercot_north_in_mmbtu_per_mwh",
    "807": "fcst_on_peak_heat_rate_in_isone_mass_in_mmbtu_per_mwh",
    "808": "fcst_on_peak_heat_rate_in_nyiso_g_in_mmbtu_per_mwh",
    "809": "fcst_on_peak_heat_rate_in_pjm_west_in_mmbtu_per_mwh",
    "814": "fcst_on_peak_power_prices_in_ercot_north_in_usd_mwh",
    "815": "fcst_on_peak_power_prices_in_isone_mass_in_usd_mwh",
    "816": "fcst_on_peak_power_prices_in_nyiso_g_in_usd_mwh",
    "817": "fcst_on_peak_power_prices_in_pjm_west_in_usd_mwh",
    "9088": "fcst_on_peak_clean_spark_spreads_in_isone_mass_in_usd_mwh",
    "9089": "fcst_on_peak_clean_spark_spreads_in_nyiso_g_in_usd_mwh",
    "9090": "fcst_on_peak_dirty_spark_spreads_in_pjm_west_in_usd_mwh",
    "9091": "fcst_on_peak_dirty_spark_spreads_in_ercot_north_in_usd_mwh",
    "15385": "fcst_on_peak_power_prices_in_caiso_sp15_in_usd_mwh",
    "15386": "fcst_on_peak_heat_rate_in_caiso_sp15_in_mmbtu_per_mwh",
    "15390": "fcst_on_peak_clean_spark_spreads_in_caiso_sp15_in_usd_mwh",
}


def _pull(
    date_from: str = "2020-01-01",
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
