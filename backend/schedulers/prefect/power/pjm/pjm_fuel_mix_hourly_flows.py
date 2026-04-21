import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_fuel_mix_hourly')."""
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


@flow(name="PJM Fuel Mix Hourly")
def pjm_fuel_mix_hourly():
    """GridStatus PJM fuel mix — scrape latest hourly fuel mix, run dbt incremental merge."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_fuel_mix_hourly", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape GridStatus fuel mix and upsert to PostgreSQL ──────
        mod = importlib.import_module("backend.scrapes.power.gridstatus.pjm.pjm_fuel_mix_hourly")
        mod.main()

        # ────── 2. Run dbt transformations (upstream only — no downstream pjm_modelling) ──────
        run_dbt("+pjm_fuel_mix_hourly")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_fuel_mix_hourly()
