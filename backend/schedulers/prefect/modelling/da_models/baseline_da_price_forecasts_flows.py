"""Dedicated Prefect flows for the two baseline Meteologica DA-price forecasters.

Two independent deployments (see the .yaml) so each baseline model schedules and
fails on its own clock:

  - ``pjm_baseline_da_price_next_14_days`` -> the multi-day horizon pipeline
    (``pipelines/forecast_next_14_days.py``): one ``forecast_runs`` row per
    forward delivery date, all sharing one run_id.
  - ``pjm_baseline_da_price_ice_anchored_single_day`` -> the ICE-anchored
    single-day pipeline (``pipelines/forecast_single_day_ice_anchored.py``):
    tomorrow's row, anchored to the ICE PDA D1-IUS VWAP.

Each flow runs that pipeline's standalone preflight first
(``baseline_meteo_da_price/data_validation/...``). The preflight raises
``DataValidationError`` on a hard input problem, which fails the Prefect run
*before* anything is published -- no garbage row in ``pjm_model_outputs.forecast_runs``.
WARN-level findings (stale ICE ticker, short vintage, ...) are printed and the
flow proceeds, exactly as the pipelines themselves degrade.

Companion: ``like_day_pjm_rto_hourly_flows.py`` (the KNN like-day forecaster on
the same preflight-then-publish pattern). The old ``da_forecasts_daily``
umbrella that fired every forecaster from one flow is retired -- each pipeline
has its own preflight-gated deployment now.
"""

import importlib
import logging

from prefect import flow, task

from backend.utils import pipeline_run_logger

logger = logging.getLogger(__name__)


def _resolve(target: str):
    """``"module.path:callable"`` -> the callable."""
    module_path, _, attr = target.partition(":")
    return getattr(importlib.import_module(module_path), attr)


@task(name="run-preflight", retries=0)
def run_preflight(label: str, target: str) -> None:
    """Run a pipeline's data-validation preflight; raises on a hard input fault."""
    logger.info("Preflight: %s", label)
    _resolve(target)(quiet=False)  # prints the per-check report; raises on ERROR
    logger.info("Preflight passed: %s", label)


@task(name="run-forecast", retries=1)
def run_forecast(label: str, target: str) -> None:
    """Run a baseline pipeline's ``run(publish=True)``."""
    logger.info("Running forecaster: %s", label)
    _resolve(target)(publish=True, quiet=True)
    logger.info("Forecaster published: %s", label)


def _preflight_then_forecast(
    *,
    pipeline_name: str,
    label: str,
    preflight_target: str,
    forecast_target: str,
) -> None:
    """Shared body: PipelineRunLogger -> preflight task -> forecast task."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=pipeline_name,
        source="modelling",
    )
    run.start()
    try:
        run_preflight(label, preflight_target)
        run_forecast(label, forecast_target)
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


@flow(name="PJM Baseline DA-Price -- Next 14 Days")
def pjm_baseline_da_price_next_14_days():
    """Preflight + publish the 14-day Meteologica DA-price baseline horizon."""
    _preflight_then_forecast(
        pipeline_name="pjm_baseline_da_price_next_14_days",
        label="baseline_meteo_da_price next_14_days",
        preflight_target=(
            "backend.modelling.da_models.baseline_meteo_da_price"
            ".data_validation.forecast_next_14_days:run"
        ),
        forecast_target=(
            "backend.modelling.da_models.baseline_meteo_da_price"
            ".pipelines.forecast_next_14_days:run"
        ),
    )


@flow(name="PJM Baseline DA-Price -- ICE-Anchored Single Day")
def pjm_baseline_da_price_ice_anchored_single_day():
    """Preflight + publish the ICE-anchored single-day Meteologica DA-price baseline."""
    _preflight_then_forecast(
        pipeline_name="pjm_baseline_da_price_ice_anchored_single_day",
        label="baseline_meteo_da_price ice_anchored single_day",
        preflight_target=(
            "backend.modelling.da_models.baseline_meteo_da_price"
            ".data_validation.forecast_single_day_ice_anchored:run"
        ),
        forecast_target=(
            "backend.modelling.da_models.baseline_meteo_da_price"
            ".pipelines.forecast_single_day_ice_anchored:run"
        ),
    )


if __name__ == "__main__":
    pjm_baseline_da_price_next_14_days()
    pjm_baseline_da_price_ice_anchored_single_day()
