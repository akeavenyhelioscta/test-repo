"""Data-validation preflight for ``pipelines/forecast_next_14_days.py``.

The multi-day pipeline loads the single most-recent Meteologica DA-price vintage
(``latest_only=True`` -- one ``as_of_date``, ~7-14 forward days at full 24-hour
coverage) and publishes one ``forecast_runs`` row per forward delivery date.
This preflight loads that same frame and asserts:

  ERROR (the pipeline publishes nothing or garbage otherwise):
    - the vintage frame is non-empty;
    - it carries exactly one ``as_of_date`` (the ``latest_only`` contract);
    - there is at least one forward delivery date after ``run_date``;
    - every forward delivery date in the horizon has 24 distinct hours;
    - the price series are not all-NaN on any forward date;
    - the deterministic series is in a sane $/MWh range across the horizon.

  WARN (degraded but it still runs):
    - the forecast-execution timestamp is recent (a re-publish of a stale
      horizon is worse than a stale single day -- promote to ERROR if desired);
    - the ``as_of_date`` is fresh;
    - the horizon is at least ``MIN_EXPECTED_HORIZON`` forward days.

Writes nothing; never imported by the forecast pipeline.

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_next_14_days

Exit code is 0 when inputs are healthy, non-zero (DataValidationError) otherwise.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.baseline_meteo_da_price.data_validation import (  # noqa: E402
    _shared,
)
from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.validation import (  # noqa: E402
    CheckResult,
    CheckStatus,
    ValidationReport,
    check_forecast_execution_recent,
    check_freshness,
    check_frame_non_empty,
    check_no_all_nan,
    check_row_count_per_day,
    check_value_range,
    print_report,
    run_checks,
)
from backend.modelling.da_models.common.validation.checks import (  # noqa: E402
    DA_LMP_MAX_USD,
    DA_LMP_MIN_USD,
)
from backend.utils.logging_utils import init_logging, print_header  # noqa: E402

# ── Defaults (mirror pipelines/forecast_next_14_days.py) ───────────────────
RUN_DATE: date | None = None  # None -> today (the vintage date)
HORIZON_DAYS: int | None = 14  # cap on forward delivery dates; None -> all
HUB: str = _shared.HUB
CACHE_DIR: Path | None = _shared.CACHE_DIR
LOG_DIR: Path = _shared.LOG_DIR
# A healthy Meteologica DA-price vintage reaches ~7-14 forward days; fewer than
# this is a WARN (short publish), not a failure.
MIN_EXPECTED_HORIZON: int = 7


def _forward_dates(
    meteo: pd.DataFrame, run_date: date, horizon_days: int | None
) -> list[date]:
    if meteo.empty:
        return []
    parsed = pd.to_datetime(meteo["date"], errors="coerce").dt.date
    fwd = sorted({d for d in parsed if d is not None and pd.notna(d) and d > run_date})
    return fwd[: int(horizon_days)] if horizon_days is not None else fwd


def _single_as_of(meteo: pd.DataFrame) -> CheckResult:
    name = "meteo_da_price: single latest vintage (one as_of_date)"
    if "as_of_date" not in meteo.columns:
        return CheckResult(
            name, CheckStatus.PASS, "no as_of_date column; nothing to check"
        )
    aod = sorted(
        set(pd.to_datetime(meteo["as_of_date"], errors="coerce").dt.date.dropna())
    )
    if len(aod) != 1:
        return CheckResult(
            name,
            CheckStatus.ERROR,
            f"latest_only frame should carry one as_of_date, found {aod}",
        )
    return CheckResult(name, CheckStatus.PASS, f"as_of_date == {aod[0]}")


def _has_forward_dates(forward: list[date], run_date: date) -> CheckResult:
    name = "meteo_da_price: forward delivery dates exist"
    if not forward:
        return CheckResult(
            name,
            CheckStatus.ERROR,
            f"no Meteologica delivery dates after run_date {run_date} -- nothing to publish",
        )
    return CheckResult(
        name,
        CheckStatus.PASS,
        f"{len(forward)} forward date(s): {forward[0]} .. {forward[-1]}",
    )


def _horizon_depth(forward: list[date]) -> CheckResult:
    name = "meteo_da_price: horizon depth"
    if len(forward) < MIN_EXPECTED_HORIZON:
        return CheckResult(
            name,
            CheckStatus.WARN,
            f"only {len(forward)} forward day(s) (< {MIN_EXPECTED_HORIZON}); short vintage",
        )
    return CheckResult(name, CheckStatus.PASS, f"{len(forward)} forward day(s)")


def validate(
    run_date: date | None = RUN_DATE,
    *,
    horizon_days: int | None = HORIZON_DAYS,
    cache_dir: Path | None = CACHE_DIR,
) -> ValidationReport:
    """Load the latest Meteologica vintage and validate the forward horizon."""
    resolved_run_date = _shared.resolve_run_date(run_date)
    meteo = loader.load_meteologica_da_price_forecast(
        cache_dir=cache_dir, latest_only=True
    )
    forward = _forward_dates(meteo, resolved_run_date, horizon_days)
    meteo_fwd = (
        meteo[pd.to_datetime(meteo["date"], errors="coerce").dt.date.isin(set(forward))]
        if forward
        else meteo.iloc[0:0]
    )

    specs: list = [
        lambda: check_frame_non_empty("meteo_da_price: frame non-empty", meteo),
        lambda: _single_as_of(meteo),
        lambda: _has_forward_dates(forward, resolved_run_date),
        lambda: _horizon_depth(forward),
        lambda: check_forecast_execution_recent(
            "meteo_da_price: forecast execution recent",
            meteo,
            exec_cols=_shared.EXEC_COLS,
            reference=resolved_run_date,
        ),
        lambda: check_freshness(
            "meteo_da_price: vintage freshness",
            meteo,
            reference_date=resolved_run_date,
            max_age_days=2,  # latest vintage's as_of should be run_date - 1
        ),
        lambda: check_value_range(
            "meteo_da_price: deterministic in sane $/MWh range (horizon)",
            meteo_fwd,
            "da_price_deterministic",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
        ),
    ]
    # Per-forward-date completeness + not-all-NaN.
    for fd in forward:
        specs.append(
            lambda fd=fd: check_row_count_per_day(
                f"meteo_da_price: 24 hours on {fd}", meteo, fd
            )
        )
        specs.append(
            lambda fd=fd: check_no_all_nan(
                f"meteo_da_price: price series not all-NaN on {fd}",
                meteo,
                _shared.PRICE_SERIES,
                target_date=fd,
            )
        )
    return run_checks(specs)


def run(
    run_date: date | None = RUN_DATE,
    *,
    horizon_days: int | None = HORIZON_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> ValidationReport:
    """Preflight entrypoint: validate, print the report, raise if it failed."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="preflight_baseline_next_14", log_dir=LOG_DIR)
    try:
        resolved_run_date = _shared.resolve_run_date(run_date)
        if not quiet:
            print_header(
                f"PREFLIGHT - forecast_next_14_days | {HUB} | run_date {resolved_run_date} "
                f"| horizon <= {horizon_days}",
                "=",
                100,
            )
        report = validate(
            run_date=run_date, horizon_days=horizon_days, cache_dir=cache_dir
        )
        if not quiet:
            print_report(report, logger=pl)
        report.raise_if_failed()
        return report
    finally:
        pl.close()


if __name__ == "__main__":
    run()
