import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_load_rt_hourly')."""
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


@flow(name="PJM Load RT Hourly")
def pjm_load_rt_hourly():
    """RT Hourly Load — scrape metered + prelim + 5-min instantaneous from PJM API, run dbt."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_load_rt_hourly", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape metered hourly load and upsert to PostgreSQL ──────
        mod = importlib.import_module("backend.scrapes.power.pjm.hourly_load_metered")
        mod.main()

        # ────── 2. Scrape prelim hourly load and upsert to PostgreSQL ──────
        mod = importlib.import_module("backend.scrapes.power.pjm.hourly_load_prelim")
        mod.main()

        # ────── 3. Scrape 5-min instantaneous load and upsert to PostgreSQL ──────
        mod = importlib.import_module("backend.scrapes.power.pjm.five_min_instantaneous_load_v1_2025_OCT_15")
        mod.main()

        # ────── 4. Run dbt transformations (upstream only — no downstream pjm_modelling) ──────
        run_dbt("+pjm_load_rt_hourly")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_load_rt_hourly()
