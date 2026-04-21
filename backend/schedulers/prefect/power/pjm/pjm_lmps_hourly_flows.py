import importlib
import logging
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from dbt.cli.main import dbtRunner
from prefect import flow

from backend.schedulers.prefect.power.pjm.pjm_lmps_hourly_notifications import notify_da_lmps
from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_lmps_hourly')."""
    logger = logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )
    logger.header("dbt")
    logger.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke([
        "run",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ])
    if not result.success:
        logger.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    logger.info(f"dbt run completed successfully: select={select}")


@flow(name="PJM LMPs Hourly")
def pjm_lmps_hourly():
    """Day-Ahead Hourly LMPs — poll PJM API with tenacity retries, upsert to PostgreSQL, run dbt."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_lmps_hourly", source="power",
    )
    run.start()
    try:
        # ────── 1. Poll PJM API and upsert raw data to PostgreSQL ──────
        mod = importlib.import_module("backend.orchestration.power.pjm.da_hrl_lmps")
        mod.main()

        # ────── 2. Send Slack notification with LMP summary ──────
        target_date = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d")
        notify_da_lmps(target_date)

        # ────── 3. Run dbt transformations (upstream only — no downstream pjm_modelling) ──────
        run_dbt("+pjm_lmps_hourly")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


@flow(name="PJM LMPs Hourly Backfill")
def pjm_lmps_hourly_backfill():
    """Day-Ahead Hourly LMPs — 7-day lookback backfill, no polling."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_lmps_hourly_backfill", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape 7-day lookback from PJM API and upsert to PostgreSQL ──────
        mod = importlib.import_module("backend.scrapes.power.pjm.da_hrl_lmps")
        mod.main()

        # ────── 2. Run dbt transformations (upstream only) ──────
        run_dbt("+pjm_lmps_hourly")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_lmps_hourly_backfill()
