"""Single-day naive baseline forecast pipeline.

Wraps the pure forecast functions (``forecast_epf_naive`` /
``forecast_d7``) with shared output-table + metric utilities lifted
from the like-day model so the result dict matches that pipeline's
contract field-for-field.

Usage::

    python -m da_models.naive_baselines.pjm_rto_hourly.pipelines.forecast_single_day
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.common.forecast.output import (  # noqa: E402
    actuals_from_pool,
    build_output_table,
)
from da_models.like_day_model_knn import configs as like_day_configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.metrics import (  # noqa: E402
    evaluate_forecast,
)
from da_models.naive_baselines._shared import build_lmp_only_pool  # noqa: E402
from da_models.naive_baselines.pjm_rto_hourly.forecast import (  # noqa: E402
    forecast_d7,
    forecast_epf_naive,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None             # None -> tomorrow (date.today() + 1d)
BASELINE: str = "epf"                       # "epf" | "d7"
CACHE_DIR: Path | None = None
HUB: str = like_day_configs.HUB

_BASELINES = {
    "epf": forecast_epf_naive,
    "d7": forecast_d7,
}


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def run(
    target_date: date | None = TARGET_DATE,
    baseline: str = BASELINE,
    pool: pd.DataFrame | None = None,
    cache_dir: Path | None = CACHE_DIR,
    hub: str = HUB,
    quiet: bool = False,
) -> dict:
    """Run the naive forecast for one target date and return a result dict.

    Result keys:
        output_table       — Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat
                             with rows: Actual (if available), Forecast,
                             Error (if Actual available).
        metrics            — dict from ``evaluate_forecast(quantiles=[])``;
                             empty if actuals unavailable.
        forecast_date      — ISO string.
        has_actuals        — bool.
        n_pool             — pool row count.
        baseline_name      — ``"epf_naive"`` or ``"d7_naive"``.
        df_forecast        — raw 24-row frame from the forecast function.

    ``quiet`` — suppresses the terminal report (used by backtest harnesses
    that print their own cross-baseline summary).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if baseline not in _BASELINES:
        raise ValueError(
            f"baseline={baseline!r} not in {tuple(_BASELINES.keys())}"
        )

    resolved_date = _resolve_target_date(target_date)

    if pool is None:
        pool = build_lmp_only_pool(hub=hub, cache_dir=cache_dir)

    df_forecast = _BASELINES[baseline](pool, resolved_date)
    actuals = actuals_from_pool(pool, resolved_date)
    has_actuals = actuals is not None
    output_table = build_output_table(resolved_date, df_forecast, actuals)

    metrics: dict = {}
    if has_actuals and len(df_forecast) > 0:
        merged = df_forecast.copy()
        merged["actual_lmp"] = merged["hour_ending"].map(actuals)
        merged = merged.dropna(subset=["actual_lmp"])
        if len(merged) > 0:
            y_true = merged["actual_lmp"].to_numpy(dtype=float)
            metrics = evaluate_forecast(y_true, merged, quantiles=[])

    baseline_name = f"{baseline}_naive"
    result = {
        "output_table": output_table,
        "metrics": metrics,
        "forecast_date": str(resolved_date),
        "has_actuals": has_actuals,
        "n_pool": int(len(pool)),
        "baseline_name": baseline_name,
        "df_forecast": df_forecast,
    }

    if not quiet:
        _print_report(result, baseline=baseline, hub=hub)

    return result


_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _print_config_block(
    *, baseline: str, baseline_name: str, hub: str,
    forecast_date: date, n_pool: int, has_actuals: bool,
) -> None:
    """Naive equivalent of the like-day FORECAST CONFIGURATION block."""
    target_dow = _DOW_ABBR[forecast_date.weekday()]
    lag_days = 1 if (baseline == "epf" and forecast_date.weekday() in (1, 2, 3, 4)) else 7

    print("\n" + "=" * 90)
    print("  FORECAST CONFIGURATION")
    print("=" * 90)

    print(f"\n  Target        {forecast_date} ({target_dow})")
    print(f"  Hub           {hub}")
    print(f"  Baseline      {baseline_name}")
    print(f"  Lag           {lag_days} day(s)")
    print(f"  Pool size     {n_pool:,} dates")
    print(f"  Has actuals   {'yes' if has_actuals else 'no'}")

    print("\n" + "=" * 90)


def _print_forecast_block(
    output_table: pd.DataFrame, metrics: dict, *, hub: str, baseline_name: str,
) -> None:
    """Mirrors like-day's print_forecast: one unified Actual/Forecast/Error
    table (120-char banner) plus a metrics block."""
    print("\n" + "=" * 120)
    print(f"  DA LMP {baseline_name.upper()} FORECAST — {hub} ($/MWh)")
    print("=" * 120)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in output_table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        print(line)

    print("-" * len(header))

    if metrics:
        if {"mae", "rmse", "mape"}.issubset(metrics.keys()):
            print(
                f"  MAE: ${metrics['mae']:.2f}/MWh  |  "
                f"RMSE: ${metrics['rmse']:.2f}/MWh  |  "
                f"MAPE: {metrics['mape']:.1f}%"
            )
        if metrics.get("rmae") is not None and not pd.isna(metrics["rmae"]):
            verdict = "better" if metrics["rmae"] < 1 else "worse"
            print(
                f"  rMAE vs naive: {metrics['rmae']:.3f} ({verdict} than naive)"
            )

    print("=" * 120 + "\n")


def _print_report(result: dict, *, baseline: str, hub: str) -> None:
    forecast_date = date.fromisoformat(result["forecast_date"])
    output_table = result.get("output_table")

    _print_config_block(
        baseline=baseline,
        baseline_name=result["baseline_name"],
        hub=hub,
        forecast_date=forecast_date,
        n_pool=result["n_pool"],
        has_actuals=result["has_actuals"],
    )

    if output_table is None or len(output_table) == 0:
        print("\n[no forecast produced - lag source row missing from pool]\n")
        return

    _print_forecast_block(
        output_table,
        result.get("metrics") or {},
        hub=hub,
        baseline_name=result["baseline_name"],
    )


if __name__ == "__main__":
    run()
