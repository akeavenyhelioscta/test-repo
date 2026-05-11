import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

MART = "clear_street_trades"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. 'clear_street_trades+')."""
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


@flow(name="Clear Street Trades")
def clear_street_trades():
    """Daily rebuild of clear_street_trades + downstream scorecard.

    Trades land in clear_street.helios_transactions_v2_* via the sister
    helioscta-backend repo's overnight SFTP pipeline. This flow does NOT
    ingest — it just refreshes the dbt-owned table after that ingest
    completes (scheduled at 5am MT for safety).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="clear_street_trades",
        source="positions_and_trades",
    )
    run.start()
    try:
        # Trailing `+` rebuilds the table AND downstream scorecard so the
        # scorecard reflects the new fills as soon as the table refreshes.
        run_dbt(f"{MART}+")
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    clear_street_trades()
