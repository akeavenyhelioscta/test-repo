# backend/modelling

Promoted home for the PJM DA-price forecasters — the code that runs on a
Prefect schedule. It is a near-pure relocation of `modelling/da_models/`:

- **Import root** is `backend.modelling.da_models.*` (not `da_models.*`).
  Every internal import uses the `backend.` prefix so the package resolves
  under a Prefect worker rooted at `/app`, with no `sys.path` hacks.
- **Parquet data** comes from `backend/cache/` via
  `backend.modelling.da_models.common.configs.CACHE_DIR`, which reads
  `backend.settings.CACHE_DIR`. `DA_MODELS_CACHE_DIR` still overrides it
  (one env var repoints both the backend and the modelling code).
- **Publishing — this tree only.** Families compose
  `build_payload -> extract_onpeak_forecast -> publish_forecast_run`, and
  there is exactly one `publish_forecast_run` symbol —
  `backend.modelling.da_models.common.publish`. It upserts one row per run
  into `pjm_model_outputs.forecast_runs` (the seam the frontend reads).
  `backend/modelling/` is the **sole writer** of that table; the `modelling/`
  tree at the repo top is research/compute-only and writes nothing.
- **Family-import rule still holds**: a family (`baseline_meteo_da_price`,
  `like_day_model_knn`) may import from `common/`, never from a sibling family.

`modelling/` (top of repo) stays as the research / standalone tree — the
two copies coexist for now. The scheduled deployments live under
`backend/schedulers/prefect/modelling/da_models/` — one yaml per family
(`baseline_da_price_forecasts.yaml`, `like_day_pjm_rto_hourly.yaml`); each
deployment runs the pipeline's data-validation preflight first, then
`run(publish=True)`. See the "Preflight data validation" section below.

Run a forecaster directly:

```
python -m backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day
```

## Preflight data validation

Each forecast pipeline has a **standalone** preflight that runs *before* it and
is never imported by it (so a bad-data abort never half-runs a forecast, and the
two have separate change-cycles). A preflight loads exactly the inputs its
pipeline consumes (via `common/data/loader.py` — it never re-reads parquet
itself), runs a battery of checks, prints a per-check report, and raises
`common.validation.DataValidationError` if anything reached ERROR severity. It
collects **all** results before deciding to raise, so one run surfaces every
problem. Exit code is 0 when healthy, non-zero on failure.

Layout:

- Checks live once in `common/validation/` (`checks.py` primitives — including
  `check_forecast_execution_recent`, `check_freshness`, `check_row_count_per_day`,
  …; `runner.py` `run_checks` / `ValidationReport` / `print_report`; `errors.py`
  `DataValidationError`). The package imports no model family — keep it that way.
- `<family>/data_validation/` — one script per forecast pipeline, named after
  the pipeline's `forecast_*.py`. `baseline_meteo_da_price/data_validation/` has
  `forecast_single_day_ice_anchored` + `forecast_next_14_days` plus a `_shared.py`
  (`meteo_single_day_specs` + the constants the pipelines mirror — kept separate
  so the two single-day variants don't drift). `like_day_model_knn/data_validation/`
  has `forecast_single_day` (covers the DA-LMP label pool, the SEP alt label
  source, the PJM calendar incl. the target-date row, and the RT-load / weather /
  solar / wind hourly feeds; the softer per-domain feeds in `domains.py` degrade
  gracefully and are left for later — there's a TODO in that file). One pipeline
  in that family today, so no `_shared.py` there yet.

Two severities: **ERROR** aborts (missing target date, all-NaN series, wrong
vintage, out-of-range $/MWh, an incomplete forward delivery date, a stale feature
feed); **WARN** is printed but does not abort (a stale forecast-execution stamp,
an aged-but-present vintage, a known source double-publish the analog pool's
`pivot_table` mean absorbs, ICE-ticker gaps the ICE-anchored pipeline already
falls back from). Sanity bounds (`DA_LMP_MIN_USD` / `MAX_USD`, `LOAD_MW_*`,
`NET_LOAD_MW_*`, `FRESHNESS_WARN_DAYS`, `FORECAST_EXEC_WARN_HOURS`) are
deliberately wide constants in `checks.py` — tighten when a real bug slips
through.

Run a preflight directly:

```
python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_single_day_ice_anchored
python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_next_14_days
python -m backend.modelling.da_models.like_day_model_knn.data_validation.forecast_single_day
```

The Prefect deployments run the matching preflight as the first task, so a bad
input fails the run before anything is published — `baseline_da_price_forecasts.yaml`
(the 14-day and ICE-anchored baseline) and `like_day_pjm_rto_hourly.yaml` under
`backend/schedulers/prefect/modelling/da_models/`.
