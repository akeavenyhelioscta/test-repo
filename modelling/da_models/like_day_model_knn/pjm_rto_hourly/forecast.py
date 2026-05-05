"""Hourly forecast aggregation for pjm_rto_hourly - per-hour analogs.

Analogs are per-(date, hour) tuples, so this module groups by hour_ending
and computes weighted averages within each hour's own ensemble.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from da_models.common.configs import HOURS
from da_models.common.forecast.output import add_summary_cols


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


def hourly_forecast_from_hour_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    """Aggregate per-(hour, rank) analog tuples into a 24-hour forecast.

    Expects ``analogs`` with columns: hour_ending, weight, lmp.
    Group by hour_ending and produce a weighted point + quantiles per HE.
    """
    if len(analogs) == 0 or not {"hour_ending", "weight", "lmp"}.issubset(analogs.columns):
        return pd.DataFrame()

    rows: list[dict] = []
    for h in HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        values = sub["lmp"].to_numpy(dtype=float)
        w = sub["weight"].to_numpy(dtype=float)
        if w.sum() <= 0:
            continue
        w = w / w.sum()
        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=w))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, w, q)
        rows.append(row)
    return pd.DataFrame(rows)


def build_quantiles_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    display_quantiles: list[float] = (0.25, 0.375, 0.50, 0.625, 0.75),
) -> pd.DataFrame:
    """Pivot quantile bands into the same wide shape as build_output_table.

    Rows: P{q*100} bands in ascending order with the point Forecast row
    inserted between P50 and the next-higher quantile (matches the
    reference ``_print_quantiles`` layout).
    """
    if len(df_forecast) == 0:
        cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
        return pd.DataFrame(columns=cols)

    # P-rows
    rows: list[dict] = []
    for q in sorted(display_quantiles):
        col = f"q_{q:.2f}"
        if col not in df_forecast.columns:
            continue
        label = _quantile_label(q)
        row = {"Date": target_date, "Type": label}
        for _, r in df_forecast.iterrows():
            row[f"HE{int(r['hour_ending'])}"] = float(r[col]) if pd.notna(r[col]) else None
        rows.append(add_summary_cols(row))

    # Insert Forecast row between P50 and the next-higher band.
    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for _, r in df_forecast.iterrows():
        forecast_row[f"HE{int(r['hour_ending'])}"] = (
            float(r["point_forecast"]) if pd.notna(r.get("point_forecast")) else None
        )
    forecast_row = add_summary_cols(forecast_row)

    insert_at = next(
        (i for i, row in enumerate(rows) if row["Type"] == "P50"),
        len(rows) // 2,
    )
    rows.insert(insert_at + 1, forecast_row)

    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _quantile_label(q: float) -> str:
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")
