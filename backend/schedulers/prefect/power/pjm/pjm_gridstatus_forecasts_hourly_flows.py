import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")

SCRAPES = [
    "backend.scrapes.power.gridstatus.pjm.pjm_load_forecast",
    "backend.scrapes.power.gridstatus.pjm.pjm_solar_forecast_hourly",
    "backend.scrapes.power.gridstatus.pjm.pjm_wind_forecast_hourly",
]

MARTS = [
    "pjm_gridstatus_load_forecast_hourly",
    "pjm_gridstatus_solar_forecast_hourly",
    "pjm_gridstatus_wind_forecast_hourly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_gridstatus_load_forecast_hourly')."""
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


@flow(name="PJM GridStatus Forecasts Hourly")
def pjm_gridstatus_forecasts_hourly():
    """GridStatus PJM load/solar/wind forecasts — scrape, run dbt."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_gridstatus_forecasts_hourly", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape each GridStatus forecast and upsert to PostgreSQL ──────
        for module_path in SCRAPES:
            mod = importlib.import_module(module_path)
            mod.main()

        # ────── 2. Run dbt transformations (upstream only — no downstream pjm_modelling) ──────
        run_dbt(" ".join(f"+{mart}" for mart in MARTS))

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_gridstatus_forecasts_hourly()
