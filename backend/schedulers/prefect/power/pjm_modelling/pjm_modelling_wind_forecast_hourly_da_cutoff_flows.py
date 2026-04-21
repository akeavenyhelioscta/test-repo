import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.caching.sync_to_blob import sync_to_blob
from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")

MART_NAME = "pjm_modelling_wind_forecast_hourly_da_cutoff"
SCHEMA = "pjm_modelling"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax."""
    dbt_logger = logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )
    dbt_logger.header("dbt")
    dbt_logger.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke([
        "run",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ])
    if not result.success:
        dbt_logger.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    dbt_logger.info(f"dbt run completed successfully: select={select}")


@flow(name="PJM Modelling Wind Forecast Hourly DA Cutoff")
def pjm_modelling_wind_forecast_hourly_da_cutoff():
    """Refresh the pjm_modelling wind forecast DA cutoff table and sync to Azure Blob parquet."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=MART_NAME, source="power",
    )
    run.start()
    try:
        # ────── 1. Rebuild the table (and any ephemeral upstream logic) ──────
        run_dbt(f"+{MART_NAME}")

        # ────── 2. Sync to Azure Blob Storage ──────
        blob_logger = logging_utils.init_logging(
            name="SYNC_TO_BLOB",
            log_dir=Path(__file__).parent / "logs",
            log_to_file=True,
            delete_if_no_errors=True,
        )
        blob_logger.header("Azure Blob Storage")
        blob_logger.section(f"Syncing {SCHEMA}.{MART_NAME}...")
        blob_path = sync_to_blob(schema=SCHEMA, table=MART_NAME)
        blob_logger.info(f"Synced to {blob_path}")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_modelling_wind_forecast_hourly_da_cutoff()
