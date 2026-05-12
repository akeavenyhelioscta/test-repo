"""Standalone data-validation preflights for the ``baseline_meteo_da_price`` pipelines.

One script per forecast pipeline, run *before* that pipeline and never imported
by it (a bad-data abort must never half-run a forecast; the two have separate
change-cycles). Each script loads exactly the inputs its pipeline consumes via
``common/data/loader.py``, runs a battery of checks from ``common/validation``,
prints a per-check report, and raises ``DataValidationError`` on any ERROR.

  - ``forecast_single_day_ice_anchored`` — preflight for
    ``pipelines/forecast_single_day_ice_anchored.py`` (the single-day Meteologica
    lead-1 + settled-DA-LMP checks plus ICE-ticker WARN checks; anchoring
    degrades gracefully when ICE is unavailable, so nothing ICE-related is ERROR)
  - ``forecast_next_14_days`` — preflight for ``pipelines/forecast_next_14_days.py``
    (validates the latest-vintage frame and that every forward delivery date is
    complete)

The shared single-day check list (the Meteologica lead-1 + settled-DA-LMP
checks) and the constants the pipelines mirror live in ``_shared.py`` —
``meteo_single_day_specs`` so a future plain-single-day preflight can reuse it
without drifting from the ICE-anchored one.

Run a preflight directly, e.g.::

    python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_next_14_days
    python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_single_day_ice_anchored
"""
