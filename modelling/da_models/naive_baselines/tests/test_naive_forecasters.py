"""Smoke tests for the naive baseline forecasters.

Inline ``__main__`` block, exits nonzero on failure (matching the
convention in ``like_day_model_knn.pjm_rto_hourly.metrics``). Six cases:

  1. EPF lag rule, all 7 DOWs (synthetic).
  2. d-7 lag rule, all 7 DOWs (synthetic).
  3. Missing lag source returns empty frame, no raise.
  4. Real-data smoke for target_date = 2026-04-30.
  5. Output schema invariant.
  6. build_output_table compatibility.

Usage::

    python -m da_models.naive_baselines.tests.test_naive_forecasters
    python modelling/da_models/naive_baselines/tests/test_naive_forecasters.py
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.naive_baselines._shared import build_lmp_only_pool  # noqa: E402
from da_models.naive_baselines.pjm_rto_hourly.forecast import (  # noqa: E402
    _epf_naive_lag_days,
    forecast_d7,
    forecast_epf_naive,
)

_HOURS = list(range(1, 25))
_LMP_COLS = [f"lmp_h{h}" for h in _HOURS]


def _synthetic_pool(start: date, n_days: int) -> pd.DataFrame:
    """Pool where lmp_h{h} on a given date encodes (dow, h) deterministically."""
    rows: list[dict] = []
    for k in range(n_days):
        d = start + timedelta(days=k)
        dow = d.weekday()
        row = {"date": d}
        for h in _HOURS:
            row[f"lmp_h{h}"] = 100.0 * dow + float(h)
        rows.append(row)
    return pd.DataFrame(rows, columns=["date"] + _LMP_COLS)


_FAILURES: list[str] = []


def _check(name: str, cond: bool, detail: str = "") -> None:
    marker = "PASS" if cond else "FAIL"
    line = f"  [{marker}] {name}"
    if detail:
        line += f"  - {detail}"
    print(line)
    if not cond:
        _FAILURES.append(name)


def case_epf_lag_rule() -> None:
    print("\n(1) EPF lag rule, all 7 DOWs")
    pool = _synthetic_pool(date(2026, 4, 1), 14)
    for k in range(7, 14):
        target = date(2026, 4, 1) + timedelta(days=k)
        lag = _epf_naive_lag_days(target.weekday())
        lag_date = target - timedelta(days=lag)
        out = forecast_epf_naive(pool, target)
        expected = [100.0 * lag_date.weekday() + h for h in _HOURS]
        actual = out["point_forecast"].tolist()
        _check(
            f"target {target} ({target.strftime('%a')}) lag={lag}d -> matches {lag_date}",
            actual == expected,
            f"first hr: got {actual[0]} expected {expected[0]}",
        )


def case_d7_lag_rule() -> None:
    print("\n(2) d-7 lag rule, all 7 DOWs")
    pool = _synthetic_pool(date(2026, 4, 1), 14)
    for k in range(7, 14):
        target = date(2026, 4, 1) + timedelta(days=k)
        lag_date = target - timedelta(days=7)
        out = forecast_d7(pool, target)
        expected = [100.0 * lag_date.weekday() + h for h in _HOURS]
        actual = out["point_forecast"].tolist()
        _check(
            f"target {target} ({target.strftime('%a')}) -> matches {lag_date}",
            actual == expected,
            f"first hr: got {actual[0]} expected {expected[0]}",
        )


def case_missing_lag_source() -> None:
    print("\n(3) Missing lag source returns empty frame, no raise")
    target = date(2026, 4, 10)
    pool = _synthetic_pool(target - timedelta(days=20), 5)
    try:
        out = forecast_epf_naive(pool, target)
    except Exception as exc:
        _check("forecast_epf_naive does not raise", False, f"raised {type(exc).__name__}")
        return
    _check("returns DataFrame", isinstance(out, pd.DataFrame))
    _check("zero rows", len(out) == 0, f"got {len(out)} rows")
    _check(
        "columns intact",
        list(out.columns) == ["hour_ending", "point_forecast"],
        f"got {list(out.columns)}",
    )
    _check(
        "hour_ending dtype int64",
        str(out["hour_ending"].dtype) == "int64",
        f"got {out['hour_ending'].dtype}",
    )
    _check(
        "point_forecast dtype float64",
        str(out["point_forecast"].dtype) == "float64",
        f"got {out['point_forecast'].dtype}",
    )


def case_real_data_smoke() -> None:
    print("\n(4) Real-data smoke - target_date 2026-04-30 (Thursday, EPF lag=1d)")
    try:
        pool = build_lmp_only_pool()
    except Exception as exc:
        print(f"  WARN: cache missing or unreadable - skipping ({type(exc).__name__}: {exc})")
        return
    if len(pool) == 0:
        print("  WARN: pool is empty - skipping")
        return

    target = date(2026, 4, 30)
    if (pool["date"] == target - timedelta(days=1)).sum() == 0:
        print(f"  WARN: pool missing {target - timedelta(days=1)} - skipping")
        return

    out_epf = forecast_epf_naive(pool, target)
    out_d7 = forecast_d7(pool, target)

    _check("EPF returns 24 rows", len(out_epf) == 24, f"got {len(out_epf)}")
    _check("d7 returns 24 rows", len(out_d7) == 24, f"got {len(out_d7)}")
    if len(out_epf) == 24:
        _check(
            "EPF hour_ending = 1..24",
            out_epf["hour_ending"].tolist() == _HOURS,
            f"got {out_epf['hour_ending'].tolist()}",
        )
        _check(
            "EPF all finite",
            np.isfinite(out_epf["point_forecast"].to_numpy()).all(),
        )
        onpeak = float(out_epf.iloc[7:23]["point_forecast"].mean())
        _check(
            f"EPF OnPeak in [$5, $300] (got ${onpeak:.2f})",
            5.0 <= onpeak <= 300.0,
        )


def case_output_schema() -> None:
    print("\n(5) Output schema invariant")
    pool = _synthetic_pool(date(2026, 4, 1), 14)
    out = forecast_d7(pool, date(2026, 4, 10))
    _check(
        "columns exactly hour_ending, point_forecast",
        list(out.columns) == ["hour_ending", "point_forecast"],
        f"got {list(out.columns)}",
    )
    _check(
        "hour_ending dtype int64",
        str(out["hour_ending"].dtype) == "int64",
        f"got {out['hour_ending'].dtype}",
    )
    _check(
        "point_forecast dtype float64",
        str(out["point_forecast"].dtype) == "float64",
        f"got {out['point_forecast'].dtype}",
    )
    _check(
        "hours = [1..24]",
        out["hour_ending"].tolist() == _HOURS,
        f"got {out['hour_ending'].tolist()}",
    )


def case_build_output_table_compat() -> None:
    print("\n(6) build_output_table compatibility (round-trip)")
    from da_models.common.forecast.output import build_output_table

    pool = _synthetic_pool(date(2026, 4, 1), 14)
    target = date(2026, 4, 10)
    out = forecast_d7(pool, target)
    table = build_output_table(target, out, actuals_hourly=None)
    _check(
        "build_output_table returns non-empty",
        isinstance(table, pd.DataFrame) and len(table) >= 1,
        f"got {type(table).__name__} len={len(table) if isinstance(table, pd.DataFrame) else 0}",
    )
    forecast_rows = table[table["Type"] == "Forecast"] if "Type" in table.columns else pd.DataFrame()
    _check(
        "Type==Forecast row exists",
        len(forecast_rows) == 1,
        f"got {len(forecast_rows)}",
    )
    if len(forecast_rows) == 1:
        row = forecast_rows.iloc[0]
        for h in _HOURS:
            cell = row.get(f"HE{h}")
            if cell is None or pd.isna(cell):
                _check(f"HE{h} populated", False, "got null")
                return
        _check("all 24 HE cells populated in Forecast row", True)


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    case_epf_lag_rule()
    case_d7_lag_rule()
    case_missing_lag_source()
    case_real_data_smoke()
    case_output_schema()
    case_build_output_table_compat()

    print()
    if _FAILURES:
        print(f"FAILED: {len(_FAILURES)} smoke test(s): {', '.join(_FAILURES)}")
        return 1
    print("OK: all smoke tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
