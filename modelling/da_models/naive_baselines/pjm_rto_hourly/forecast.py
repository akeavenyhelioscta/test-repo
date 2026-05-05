"""Pure naive-baseline forecast functions for PJM DA LMPs.

Two variants:

  - ``forecast_epf_naive`` — DOW-conditional persistence per
    Lago/Nogales/Conejo: target DOW in {Mon, Sat, Sun} -> d-7;
    target DOW in {Tue, Wed, Thu, Fri} -> d-1.
  - ``forecast_d7`` — pure same-hour-last-week persistence.

Both consume the wide LMP pool (``date`` + ``lmp_h1..lmp_h24``)
produced by ``naive_baselines._shared.build_lmp_only_pool`` and emit a
length-24 frame with columns ``hour_ending`` (int64, 1..24) and
``point_forecast`` (float64, $/MWh).

If the lag source row is missing from the pool (e.g. d-7 lands on
missing Feb 29, or the target is too early in the cache), an empty
DataFrame with correct schema is returned. Callers must handle that
case; this module does not raise.

Limitations (v1):

  - No holiday handling. A Tuesday following a Monday holiday will
    copy the holiday's prices verbatim under the EPF rule. The
    Lago/Nogales/Conejo paper treats this as accepted noise; revisit
    if a holiday-aware variant is needed.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

_HOURS: tuple[int, ...] = tuple(range(1, 25))
_LMP_COLS: tuple[str, ...] = tuple(f"lmp_h{h}" for h in _HOURS)
_OUTPUT_COLUMNS: tuple[str, ...] = ("hour_ending", "point_forecast")


def _empty_forecast() -> pd.DataFrame:
    return pd.DataFrame({
        "hour_ending": pd.Series([], dtype="int64"),
        "point_forecast": pd.Series([], dtype="float64"),
    })


def _epf_naive_lag_days(target_dow: int) -> int:
    """Lag in days for the EPF naive baseline given a target weekday.

    ``target_dow`` follows ``datetime.date.weekday()`` (Mon=0..Sun=6).
    Returns 7 for Mon/Sat/Sun, 1 for Tue-Fri. Documented in
    Lago/Nogales/Conejo's "Forecasting day-ahead electricity prices"
    survey as the standard EPF naive.
    """
    if target_dow in (0, 5, 6):
        return 7
    return 1


def _lookup_lag_row(pool: pd.DataFrame, lag_date: date) -> pd.Series | None:
    if pool is None or len(pool) == 0:
        return None
    matches = pool[pool["date"] == lag_date]
    if len(matches) == 0:
        return None
    row = matches.iloc[0]
    values = row[list(_LMP_COLS)]
    if values.isna().all():
        return None
    return row


def _row_to_forecast(row: pd.Series) -> pd.DataFrame:
    hours = np.asarray(_HOURS, dtype=np.int64)
    values = np.asarray(
        [row.get(f"lmp_h{h}") for h in _HOURS], dtype=np.float64,
    )
    return pd.DataFrame({
        "hour_ending": hours,
        "point_forecast": values,
    })


def forecast_epf_naive(pool: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """EPF naive (Lago/Nogales/Conejo): DOW-conditional persistence.

    Mon/Sat/Sun -> forecast(d, h) = LMP(d-7, h).
    Tue/Wed/Thu/Fri -> forecast(d, h) = LMP(d-1, h).

    Returns a 24-row frame with ``hour_ending`` (int64) and
    ``point_forecast`` (float64). When the lag source row is absent
    from the pool returns an empty frame with the same schema.

    Limitation: no holiday handling — Tuesday after a Monday holiday
    will copy the holiday's prices.
    """
    lag = _epf_naive_lag_days(target_date.weekday())
    lag_date = target_date - timedelta(days=lag)
    row = _lookup_lag_row(pool, lag_date)
    if row is None:
        return _empty_forecast()
    return _row_to_forecast(row)


def forecast_d7(pool: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Same-hour-last-week persistence: forecast(d, h) = LMP(d-7, h)."""
    lag_date = target_date - timedelta(days=7)
    row = _lookup_lag_row(pool, lag_date)
    if row is None:
        return _empty_forecast()
    return _row_to_forecast(row)
