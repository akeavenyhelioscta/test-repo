import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils

logger = logging.getLogger(__name__)

MART = "pjm_dates_daily"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_dates_daily')."""
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


@flow(name="PJM Dates Daily")
def pjm_dates_daily():
    """Rebuild the PJM daily calendar view and publish parquet to Azure Blob."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_dates_daily", source="power",
    )
    run.start()
    try:
        # ────── 1. Rebuild dates view (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 2. Pull mart from Postgres and export to parquet ──────
        df = azure_postgresql_utils.pull_from_db(f"SELECT * FROM {DBT_SCHEMA}.{MART}")
        model_cache_utils.write_mart_cache(df, mart=MART, pipeline_name=__name__)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_dates_daily()
