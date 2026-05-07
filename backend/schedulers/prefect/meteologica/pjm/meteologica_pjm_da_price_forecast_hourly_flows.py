import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import (
    logging_utils,
    pipeline_run_logger,
    azure_postgresql_utils,
    model_cache_utils,
)


logger = logging.getLogger(__name__)

SCRAPES = [
    # ────── Western-Hub DA price forecast — deterministic point ──────
    (
        "backend.scrapes.meteologica.pjm.usa_pjm_western_hub_da_power_price_forecast_hourly",
        "da_price_det",
    ),
    # ────── Western-Hub DA price forecast — ECMWF ensemble (51 members) ──────
    (
        "backend.scrapes.meteologica.pjm.usa_pjm_western_hub_da_power_price_forecast_ecmwf_ens_hourly",
        "da_price_ens",
    ),
]

MARTS = [
    "meteologica_pjm_da_price_forecast_hourly_da_cutoff_historical",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+meteologica_pjm_da_price_forecast_hourly_da_cutoff_historical')."""
    dbt_logger = logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )
    dbt_logger.header("dbt")
    dbt_logger.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke(
        [
            "run",
            "--select",
            select,
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
        ]
    )
    if not result.success:
        dbt_logger.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    dbt_logger.info(f"dbt run completed successfully: select={select}")


@task(name="scrape", retries=1)
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="Meteologica PJM DA Price Forecast Hourly")
def meteologica_pjm_da_price_forecast_hourly():
    """Hourly umbrella flow — scrape Meteologica Western-Hub DA price forecasts
    (deterministic + ECMWF ensemble), build the dbt DA-cutoff price mart,
    export parquet to backend cache, modelling cache, and Azure blob.

    Scrapes are loosely coupled: a failure in one does not block the other or dbt,
    so the mart still rebuilds from whatever fresh inputs landed.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="meteologica_pjm_da_price_forecast_hourly",
        source="power",
    )
    run.start()
    scrape_failures: list[str] = []
    try:
        # ────── 1. Scrape latest forecasts ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:
                scrape_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Run dbt for the DA-cutoff price mart in one invocation ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        # ────── 3. Pull mart from Postgres and export to parquet (caches + blob) ──────
        for mart in MARTS:
            df = azure_postgresql_utils.pull_from_db(
                f"SELECT * FROM {DBT_SCHEMA}.{mart}"
            )
            model_cache_utils.write_mart_cache(df, mart=mart, pipeline_name=__name__)

        if scrape_failures:
            raise RuntimeError(
                f"Flow completed but {len(scrape_failures)} scrape(s) failed: {scrape_failures}"
            )

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    meteologica_pjm_da_price_forecast_hourly()
