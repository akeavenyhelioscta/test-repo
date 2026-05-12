"""Shared bits for the ``baseline_meteo_da_price`` data-validation scripts.

Holds the constants every preflight here mirrors from the pipelines, the
date-resolution helpers, and ``meteo_single_day_specs`` — the common check
list both the plain single-day and the ICE-anchored single-day preflights run
(they consume the identical Meteologica lead-1 + settled-DA-LMP inputs).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from backend.modelling.da_models.common.validation import (
    CheckResult,
    CheckStatus,
    check_forecast_execution_recent,
    check_freshness,
    check_frame_non_empty,
    check_lead_days,
    check_no_all_nan,
    check_no_duplicate_keys,
    check_row_count_per_day,
    check_target_date_present,
    check_value_range,
)
from backend.modelling.da_models.common.validation.checks import (
    DA_LMP_MAX_USD,
    DA_LMP_MIN_USD,
)

# ── Defaults (mirror the baseline_meteo_da_price pipelines) ────────────────
HUB: str = "WESTERN HUB"
LEAD_DAYS: int | None = 1  # DA-cutoff vintage
CACHE_DIR: Path | None = None  # None -> configs.CACHE_DIR via the loader
LOG_DIR: Path = Path(__file__).resolve().parents[5] / "backend" / "modelling" / "logs"

# The deterministic point + ENS summary series Meteologica publishes per hour.
PRICE_SERIES: tuple[str, ...] = (
    "da_price_deterministic",
    "da_price_ens_average",
    "da_price_ens_bottom",
    "da_price_ens_top",
)
EXEC_COLS: tuple[str, ...] = (
    "det_forecast_execution_datetime_local",
    "ens_forecast_execution_datetime_local",
)


def resolve_target_date(target_date: date | None) -> date:
    """Mirror the pipelines: ``None`` -> tomorrow."""
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def resolve_run_date(run_date: date | None) -> date:
    """Mirror the pipelines: ``None`` -> today."""
    return run_date if run_date is not None else date.today()


def settled_lmp_absent_warn(hub: str) -> CheckResult:
    """The 'no settled DA LMP yet' WARN — expected for a future target date."""
    return CheckResult(
        name="settled DA LMP at hub: sane $/MWh range when present",
        status=CheckStatus.WARN,
        detail=(
            f"no settled DA LMP rows for hub {hub} yet "
            f"(expected for a future target date)"
        ),
    )


def meteo_single_day_specs(
    meteo,
    lmps_at_hub,
    resolved_date: date,
    *,
    lead_days: int | None,
    hub: str,
):
    """Check thunks shared by the single-day and ICE-anchored preflights.

    ``meteo`` is the Meteologica DA-price frame already filtered to the
    ``lead_days`` vintage (not yet to ``resolved_date``); ``lmps_at_hub`` is
    the settled DA LMP frame filtered to ``hub`` (may be empty for a future
    target date).
    """
    return [
        lambda: check_frame_non_empty("meteo_da_price: frame non-empty", meteo),
        lambda: check_target_date_present(
            "meteo_da_price: target date present", meteo, resolved_date
        ),
        lambda: check_row_count_per_day(
            "meteo_da_price: 24 hours on target date", meteo, resolved_date
        ),
        lambda: check_no_duplicate_keys(
            "meteo_da_price: unique (date, hour_ending)",
            meteo,
            ["date", "hour_ending"],
        ),
        lambda: check_lead_days(
            "meteo_da_price: DA-cutoff vintage",
            meteo,
            resolved_date,
            lead_days=lead_days if lead_days is not None else 1,
        ),
        lambda: check_no_all_nan(
            "meteo_da_price: price series not all-NaN",
            meteo,
            PRICE_SERIES,
            target_date=resolved_date,
        ),
        lambda: check_value_range(
            "meteo_da_price: deterministic in sane $/MWh range",
            meteo,
            "da_price_deterministic",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
            target_date=resolved_date,
        ),
        lambda: check_value_range(
            "meteo_da_price: ENS average in sane $/MWh range",
            meteo,
            "da_price_ens_average",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
            target_date=resolved_date,
        ),
        lambda: check_forecast_execution_recent(
            "meteo_da_price: forecast execution recent",
            meteo,
            exec_cols=EXEC_COLS,
            reference=resolved_date,
        ),
        lambda: check_freshness(
            "meteo_da_price: vintage freshness",
            meteo,
            reference_date=resolved_date,
            # as_of_date == date - lead_days, so the newest as_of is roughly the
            # target date minus lead; allow lead + slack before warning.
            max_age_days=(lead_days or 1) + 3,
        ),
        # Settled DA LMP only exists once the market clears (the typical
        # tomorrow case has none) -> a missing/empty hub frame is a WARN, not an
        # ERROR, because the forecast still runs without it.
        lambda: (
            check_value_range(
                "settled DA LMP at hub: sane $/MWh range when present",
                lmps_at_hub,
                "lmp",
                low=DA_LMP_MIN_USD,
                high=DA_LMP_MAX_USD,
            )
            if not lmps_at_hub.empty
            else settled_lmp_absent_warn(hub)
        ),
    ]
