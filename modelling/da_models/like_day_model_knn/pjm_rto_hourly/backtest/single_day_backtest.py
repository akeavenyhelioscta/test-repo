"""Single-day multi-scenario backtest for pjm_rto_hourly.

Runs many (weight + knob) scenarios against ONE target date with known
actuals, then ranks them by MAE / rMAE / CRPS and surfaces each
scenario's forecast OnPeak / OffPeak / Flat alongside the actuals.

Differs from ``backtest/param_sweep.py``:

  - param_sweep walks across MANY weekday targets and aggregates
    metrics — used to pick weights that generalize.
  - single_day_backtest pins ONE target date and shows per-scenario
    forecast detail — used for "how would each config have done on
    this specific day?"

Default target is YESTERDAY (``date.today() - 1d``); edit
``TARGET_DATE`` at the top to pick any past date.

Hard-fails if the target has no actuals in the pool — a backtest
without actuals is meaningless.

Tunable defaults at the top — edit and re-run, no CLI flags. Add your
weight perturbations to ``SCENARIOS``.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.single_day_backtest
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/single_day_backtest.py
"""
from __future__ import annotations

import sys
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402
from colorama import Fore, Style, init as colorama_init  # noqa: E402

colorama_init()

from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.backtest.scenarios import (  # noqa: E402
    SCENARIOS,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool, build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.forecast import (  # noqa: E402
    actuals_from_pool,
)
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as forecast_run,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = date(2026, 4, 30)   # None -> yesterday (date.today() - 1d)
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SPEC.name
# Scenarios live in scenarios.py (shared with param_sweep). Edit there
# to add/remove perturbations; both backtest scripts pick up the change.

_RUN_KWARGS_PASSTHROUGH: tuple[str, ...] = (
    "flt_radius", "n_analogs", "season_window_days", "min_pool_size",
)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() - timedelta(days=1)


def _scenario_overrides_for_run(scenario: dict) -> dict:
    overrides = scenario.get("overrides") or {}
    return {k: v for k, v in overrides.items() if k in _RUN_KWARGS_PASSTHROUGH}


def _execute_scenario(
    *,
    scenario_name: str,
    scenario: dict,
    target_date: date,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
    model_name: str,
) -> dict:
    started = time.perf_counter()
    base: dict = {
        "scenario_name": scenario_name,
        "status": "ok",
        "error_message": None,
    }
    try:
        result = forecast_run(
            target_date=target_date,
            model_name=model_name,
            pool=pool, query=query, dates_meta=dates_meta,
            feature_group_weights_override=scenario.get("weights"),
            quiet=True,
            write_analog_store=False,
            **_scenario_overrides_for_run(scenario),
        )
    except Exception as exc:
        base.update({
            "status": "failed",
            "error_message": f"{type(exc).__name__}: {exc}",
            "duration_s": round(time.perf_counter() - started, 3),
        })
        return base

    metrics = result.get("metrics") or {}
    output_table = result.get("output_table")
    quantiles_table = result.get("quantiles_table")
    forecast_row = (
        output_table[output_table["Type"] == "Forecast"].iloc[0]
        if output_table is not None and len(output_table) else None
    )
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Actual").any() else None
    )
    error_row = (
        output_table[output_table["Type"] == "Error"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Error").any() else None
    )

    # Per-HE arrays for the hourly metrics table
    hourly_forecast: list[float | None] = (
        [float(forecast_row[f"HE{h}"]) if pd.notna(forecast_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if forecast_row is not None else [None] * 24
    )
    hourly_actual: list[float | None] = (
        [float(actual_row[f"HE{h}"]) if pd.notna(actual_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if actual_row is not None else [None] * 24
    )
    hourly_abs_error: list[float | None] = (
        [abs(float(error_row[f"HE{h}"])) if pd.notna(error_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if error_row is not None else [None] * 24
    )

    # Per-HE 90% PI coverage (from P05/P95 rows in the quantiles table)
    hourly_in_90pi: list[bool | None] = [None] * 24
    if quantiles_table is not None and actual_row is not None:
        p05_rows = quantiles_table[quantiles_table["Type"] == "P05"]
        p95_rows = quantiles_table[quantiles_table["Type"] == "P95"]
        if len(p05_rows) and len(p95_rows):
            p05 = p05_rows.iloc[0]
            p95 = p95_rows.iloc[0]
            for i, h in enumerate(range(1, 25)):
                a = hourly_actual[i]
                lo = float(p05[f"HE{h}"]) if pd.notna(p05[f"HE{h}"]) else None
                hi = float(p95[f"HE{h}"]) if pd.notna(p95[f"HE{h}"]) else None
                if a is None or lo is None or hi is None:
                    continue
                hourly_in_90pi[i] = lo <= a <= hi

    base.update({
        "n_analogs_used": result.get("n_analogs_used"),
        "mae": metrics.get("mae"),
        "rmse": metrics.get("rmse"),
        "mape": metrics.get("mape"),
        "rmae": metrics.get("rmae"),
        "crps": metrics.get("crps"),
        "mean_pinball": metrics.get("mean_pinball"),
        "coverage_80pct": metrics.get("coverage_80pct"),
        "coverage_90pct": metrics.get("coverage_90pct"),
        "coverage_98pct": metrics.get("coverage_98pct"),
        "sharpness_90pct": metrics.get("sharpness_90pct"),
        "forecast_onpeak": float(forecast_row["OnPeak"]) if forecast_row is not None else None,
        "forecast_offpeak": float(forecast_row["OffPeak"]) if forecast_row is not None else None,
        "forecast_flat": float(forecast_row["Flat"]) if forecast_row is not None else None,
        "hourly_forecast": hourly_forecast,
        "hourly_actual": hourly_actual,
        "hourly_abs_error": hourly_abs_error,
        "hourly_in_90pi": hourly_in_90pi,
        "output_table": output_table,
        "duration_s": round(time.perf_counter() - started, 3),
    })
    return base


def _print_comparison(
    rows: list[dict], target_date: date, actuals_summary: dict[str, float],
) -> None:
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"].copy()
    failed = df[df["status"] == "failed"]

    print("\n" + "=" * 110)
    print(f"  SINGLE-DAY BACKTEST — {target_date} ({target_date.strftime('%a')})  |  Hub: WESTERN HUB")
    print(f"  Actuals  OnPeak={actuals_summary['onpeak']:>7.2f}  "
          f"OffPeak={actuals_summary['offpeak']:>7.2f}  "
          f"Flat={actuals_summary['flat']:>7.2f}  ($/MWh)")
    print(f"  Scenarios: {len(df)} ({len(ok)} ok, {len(failed)} failed)")
    print("=" * 110)

    if len(ok) == 0:
        print("\n  No successful scenarios; nothing to summarize.\n")
    else:
        if "default" in ok["scenario_name"].values:
            default_mae = float(ok.loc[ok["scenario_name"] == "default", "mae"].iloc[0])
            ok["delta_mae_vs_default"] = ok["mae"] - default_mae
        else:
            ok["delta_mae_vs_default"] = None

        ok = ok.sort_values("mae", ascending=True, na_position="last").reset_index(drop=True)

        cols = [
            "scenario_name", "mae", "rmse", "rmae", "crps",
            "coverage_90pct", "sharpness_90pct",
            "forecast_onpeak", "forecast_offpeak", "forecast_flat",
            "delta_mae_vs_default",
        ]
        formatters = {
            "mae": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "rmse": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "rmae": lambda v: f"{v:>6.3f}" if pd.notna(v) else "   n/a",
            "crps": lambda v: f"{v:>7.4f}" if pd.notna(v) else "    n/a",
            "coverage_90pct": lambda v: f"{v:>6.1%}" if pd.notna(v) else "   n/a",
            "sharpness_90pct": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_onpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_offpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_flat": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "delta_mae_vs_default": lambda v: f"{v:+7.2f}" if pd.notna(v) else "       -",
        }

        with pd.option_context("display.max_rows", None, "display.width", None):
            print()
            print(ok[cols].to_string(index=False, formatters=formatters))

        if len(ok) >= 2:
            best = ok.iloc[0]
            print(f"\n  Best by MAE: {best['scenario_name']} "
                  f"(MAE ${best['mae']:.2f}, rMAE {best['rmae']:.3f})")

    if len(failed) > 0:
        print("\n  Failed scenarios:")
        for _, r in failed.iterrows():
            print(f"    {r['scenario_name']}: {r['error_message']}")

    print()


_HL_FORECAST_LOCAL = Style.BRIGHT + Fore.RED
_RS_LOCAL = Style.RESET_ALL


def _print_per_scenario_detail(rows: list[dict], target_date: date) -> None:
    """One Actual / Forecast / Error block per scenario.

    Mirrors the per-run table that ``forecast_single_day.print_forecast``
    prints (Date | Type | HE1..HE24 | OnPk | OffPk | Flat). Forecast row
    is colorized bright red — same visual cue as the live forecast.
    """
    ok = [r for r in rows if r["status"] == "ok"]
    if not ok:
        return

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    sep_len = len(header)

    for r in ok:
        table = r.get("output_table")
        if table is None or len(table) == 0:
            continue

        print("=" * 120)
        print(f"  HOURLY DETAIL: {r['scenario_name']}  —  Western Hub ($/MWh) — {target_date}")
        if r.get("mae") is not None:
            print(f"  MAE: ${r['mae']:.2f}/MWh  |  RMSE: ${r['rmse']:.2f}/MWh  |  rMAE: {r['rmae']:.3f}")
        print("=" * 120)
        print(header)
        print("-" * sep_len)

        for _, row in table.iterrows():
            line = f"{str(row['Date']):<12} {row['Type']:<10}"
            for h in range(1, 25):
                val = row.get(f"HE{h}")
                line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
            for col in ("OnPeak", "OffPeak", "Flat"):
                v = row.get(col)
                line += f" {v:>7.2f}" if pd.notna(v) else f" {'':>7}"
            if row["Type"] == "Forecast":
                line = f"{_HL_FORECAST_LOCAL}{line}{_RS_LOCAL}"
            print(line)

        print("-" * sep_len)
        print()


def run(
    target_date: date | None = TARGET_DATE,
    scenarios: dict | None = None,
    model_name: str = MODEL_NAME,
) -> list[dict]:
    """Run every scenario against a single target date, then print a sorted
    comparison table. Returns the list of result dicts (one per scenario).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if scenarios is None:
        scenarios = SCENARIOS
    if not scenarios:
        raise ValueError("SCENARIOS is empty; add at least one entry.")

    resolved_date = _resolve_target_date(target_date)
    if resolved_date >= date.today() + timedelta(days=1):
        raise ValueError(
            f"Backtest needs a past or current date with actuals; got {resolved_date}. "
            f"Use forecast_single_day for live/future forecasts."
        )

    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print(f"\n[backtest {resolved_date}] building pool + dates_meta...")
    t0 = time.perf_counter()
    pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)
    print(f"[backtest {resolved_date}] pool built in {time.perf_counter() - t0:.1f}s "
          f"({len(pool)} rows)")

    actuals = actuals_from_pool(pool, resolved_date)
    if actuals is None:
        raise RuntimeError(
            f"No actuals in the pool for target_date={resolved_date}. "
            "The DA LMP cache may not yet cover this date — try an earlier "
            "target_date, or refresh the pjm_lmps_hourly parquet."
        )
    actuals_summary = {
        "onpeak": float(sum(actuals[h] for h in range(8, 24)) / 16),
        "offpeak": float(sum(actuals[h] for h in list(range(1, 8)) + [24]) / 8),
        "flat": float(sum(actuals.values()) / 24),
    }

    print(f"[backtest {resolved_date}] building query for target...")
    query = build_query_row(
        target_date=resolved_date, cache_dir=configs.CACHE_DIR, spec=spec_for_build,
    )

    print(f"[backtest {resolved_date}] running {len(scenarios)} scenario(s)...")
    rows: list[dict] = []
    for scenario_name, scenario in scenarios.items():
        row = _execute_scenario(
            scenario_name=scenario_name,
            scenario=scenario,
            target_date=resolved_date,
            pool=pool, query=query, dates_meta=dates_meta,
            model_name=model_name,
        )
        tag = "OK" if row["status"] == "ok" else "FAIL"
        mae = row.get("mae")
        mae_str = f"MAE={mae:.2f}" if isinstance(mae, (int, float)) and pd.notna(mae) else "MAE=n/a"
        print(f"[backtest {resolved_date}]   {scenario_name:<24} {tag:<4} "
              f"{mae_str}  ({row['duration_s']:.2f}s)")
        rows.append(row)

    _print_comparison(rows, resolved_date, actuals_summary)
    _print_per_scenario_detail(rows, resolved_date)
    return rows


if __name__ == "__main__":
    run()
