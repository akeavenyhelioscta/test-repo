import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.utils import logging_utils, pipeline_run_logger

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "dbt" / "dbt_azure_postgresql")

# ────── 36 Meteologica PJM demand forecast contents feeding the mart ──────
# Order mirrors staging_v1_meteologica_pjm_demand_forecast_hourly (RTO, 3 macro,
# 17 MidAtl sub, 1 South sub, 14 West sub).
SCRAPE_MODULES = [
    # RTO + 3 macro regions
    "backend.scrapes.meteologica.pjm.usa_pjm_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_south_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_power_demand_forecast_hourly",

    # Mid-Atlantic sub-regions (17)
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_ae_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_bc_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_dpl_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_dpl_dplco_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_dpl_easton_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_jc_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_me_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pe_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pep_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pep_pepco_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pep_smeco_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pl_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pl_plco_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pl_ugi_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_pn_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_ps_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_midatlantic_reco_power_demand_forecast_hourly",

    # South sub-regions (1)
    "backend.scrapes.meteologica.pjm.usa_pjm_south_dom_power_demand_forecast_hourly",

    # West sub-regions (14)
    "backend.scrapes.meteologica.pjm.usa_pjm_west_aep_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_aep_aepapt_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_aep_aepimp_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_aep_aepkpt_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_aep_aepopt_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_ap_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_atsi_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_atsi_oe_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_atsi_papwr_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_ce_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_day_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_deok_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_duq_power_demand_forecast_hourly",
    "backend.scrapes.meteologica.pjm.usa_pjm_west_ekpc_power_demand_forecast_hourly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+meteologica_pjm_demand_forecast_hourly')."""
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


@flow(name="Meteologica PJM Demand Forecast Hourly")
def meteologica_pjm_demand_forecast_hourly():
    """Meteologica PJM demand forecast — scrape 36 regional contents, run dbt incremental merge."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="meteologica_pjm_demand_forecast_hourly", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape all 36 Meteologica PJM demand forecast contents ──────
        for module_name in SCRAPE_MODULES:
            mod = importlib.import_module(module_name)
            mod.main()

        # ────── 2. Run dbt transformations (upstream only — no downstream pjm_modelling) ──────
        run_dbt("+meteologica_pjm_demand_forecast_hourly")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    meteologica_pjm_demand_forecast_hourly()
