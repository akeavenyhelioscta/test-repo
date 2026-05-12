"""Dedicated Prefect flow for the ``like_day_model_knn`` pjm_rto_hourly forecaster.

One deployment (see ``like_day_pjm_rto_hourly.yaml``):

  - ``like_day_pjm_rto_hourly`` -> the like-day KNN analog pipeline
    (``like_day_model_knn/pjm_rto_hourly/pipelines/forecast_single_day.py``):
    tomorrow's hourly DA-LMP forecast at the model hub, published as one row in
    ``pjm_model_outputs.forecast_runs``.

The flow runs the pipeline's standalone preflight first
(``like_day_model_knn/data_validation/forecast_single_day.py``). The preflight
raises ``DataValidationError`` on a hard input problem, which fails the Prefect
run *before* anything is published -- no garbage row. WARN-level findings (the
known DA-LMP source double-publish, etc.) are printed and the flow proceeds.

Mirrors ``baseline_da_price_forecasts_flows.py``; one deployment per pipeline,
the old ``da_forecasts_daily`` umbrella is retired.
"""

import importlib
import logging

from prefect import flow, task

from backend.utils import pipeline_run_logger

logger = logging.getLogger(__name__)

_PREFLIGHT_TARGET = (
    "backend.modelling.da_models.like_day_model_knn"
    ".data_validation.forecast_single_day:run"
)
_FORECAST_TARGET = (
    "backend.modelling.da_models.like_day_model_knn"
    ".pjm_rto_hourly.pipelines.forecast_single_day:run"
)
_LABEL = "like_day_model_knn pjm_rto_hourly forecast_single_day"


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
    """Run the pipeline's ``run(publish=True)``."""
    logger.info("Running forecaster: %s", label)
    _resolve(target)(publish=True, quiet=True)
    logger.info("Forecaster published: %s", label)


@flow(name="PJM Like-Day pjm_rto_hourly")
def like_day_pjm_rto_hourly():
    """Preflight + publish the like-day KNN single-day DA-price forecast."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="like_day_pjm_rto_hourly",
        source="modelling",
    )
    run.start()
    try:
        run_preflight(_LABEL, _PREFLIGHT_TARGET)
        run_forecast(_LABEL, _FORECAST_TARGET)
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    like_day_pjm_rto_hourly()
