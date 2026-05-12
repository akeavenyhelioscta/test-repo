"""Standalone data-validation preflights for the ``like_day_model_knn`` pipelines.

One script per forecast pipeline, run *before* that pipeline and never imported
by it. Currently:

  - ``forecast_single_day`` — preflight for
    ``pjm_rto_hourly/pipelines/forecast_single_day.py`` (the DA LMP label pool,
    the SEP alt label source, the PJM calendar incl. the target-date row, and
    the RT-load / observed-weather / solar / wind hourly feeds the analog query
    row is built from).

Mirrors ``baseline_meteo_da_price/data_validation/``. There's only one pipeline
in this family today, so there's no ``_shared.py`` — add one if a second
pipeline appears and the two would otherwise duplicate a check list.

Run a preflight directly::

    python -m backend.modelling.da_models.like_day_model_knn.data_validation.forecast_single_day
"""
