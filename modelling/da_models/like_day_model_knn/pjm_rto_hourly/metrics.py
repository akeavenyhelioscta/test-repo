"""Probabilistic forecast evaluation metrics.

Ported from helioscta-pjm-da/backend/src/like_day_forecast/evaluation/metrics.py
to keep the terminal report numbers comparable. GEFCom2014 conventions.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, q: float) -> float:
    """Pinball (quantile) loss — GEFCom2014 official metric."""
    delta = y_true - y_pred
    return float(np.mean(np.maximum(q * delta, (q - 1) * delta)))


def mean_pinball_loss(
    y_true: np.ndarray, y_pred_df: pd.DataFrame, quantiles: list[float],
) -> float:
    losses: list[float] = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            losses.append(pinball_loss(y_true, y_pred_df[col].to_numpy(dtype=float), q))
    return float(np.mean(losses)) if losses else float("nan")


def rmae(y_true: np.ndarray, y_pred: np.ndarray, y_naive: np.ndarray) -> float:
    """Relative MAE: MAE(model) / MAE(naive). <1 means model beats naive."""
    mae_model = float(np.mean(np.abs(y_true - y_pred)))
    mae_naive = float(np.mean(np.abs(y_true - y_naive)))
    if mae_naive == 0:
        return float("inf")
    return mae_model / mae_naive


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true != 0
    if not np.any(mask):
        return float("nan")
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


def coverage(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    """% of actuals within [lower, upper]."""
    return float(np.mean((y_true >= lower) & (y_true <= upper)))


def sharpness(lower: np.ndarray, upper: np.ndarray) -> float:
    """Average prediction interval width."""
    return float(np.mean(upper - lower))


def crps(y_true: np.ndarray, y_pred_df: pd.DataFrame, quantiles: list[float]) -> float:
    """CRPS approximated via trapezoidal integration of pinball losses."""
    losses: list[tuple[float, float]] = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            losses.append((q, pinball_loss(y_true, y_pred_df[col].to_numpy(dtype=float), q)))
    if len(losses) < 2:
        return float("nan")
    losses.sort(key=lambda x: x[0])
    qs = [l[0] for l in losses]
    pls = [l[1] for l in losses]
    return float(np.trapz(pls, qs))


def evaluate_forecast(
    y_true: np.ndarray,
    y_pred_df: pd.DataFrame,
    quantiles: list[float],
    y_naive: np.ndarray | None = None,
) -> dict:
    """Compute all metrics for a quantile forecast.

    ``y_pred_df`` must have a ``point_forecast`` column (or ``q_0.50``)
    and ``q_{q:.2f}`` columns for each quantile in ``quantiles``.
    """
    results: dict[str, float] = {}

    if "point_forecast" in y_pred_df.columns:
        y_point = y_pred_df["point_forecast"].to_numpy(dtype=float)
    elif "q_0.50" in y_pred_df.columns:
        y_point = y_pred_df["q_0.50"].to_numpy(dtype=float)
    else:
        y_point = None

    if y_point is not None:
        results["mae"] = mae(y_true, y_point)
        results["rmse"] = rmse(y_true, y_point)
        results["mape"] = mape(y_true, y_point)
        if y_naive is not None:
            results["rmae"] = rmae(y_true, y_point, y_naive)

    results["mean_pinball"] = mean_pinball_loss(y_true, y_pred_df, quantiles)
    results["crps"] = crps(y_true, y_pred_df, quantiles)

    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            results[f"pinball_{q:.2f}"] = pinball_loss(
                y_true, y_pred_df[col].to_numpy(dtype=float), q,
            )

    for name, q_lo, q_hi in (
        ("80pct", 0.10, 0.90),
        ("90pct", 0.05, 0.95),
        ("98pct", 0.01, 0.99),
    ):
        col_lo = f"q_{q_lo:.2f}"
        col_hi = f"q_{q_hi:.2f}"
        if col_lo in y_pred_df.columns and col_hi in y_pred_df.columns:
            results[f"coverage_{name}"] = coverage(
                y_true,
                y_pred_df[col_lo].to_numpy(dtype=float),
                y_pred_df[col_hi].to_numpy(dtype=float),
            )
            results[f"sharpness_{name}"] = sharpness(
                y_pred_df[col_lo].to_numpy(dtype=float),
                y_pred_df[col_hi].to_numpy(dtype=float),
            )

    return results


# ── Hour-agnostic shape metrics ───────────────────────────────────────────
# Specification locked in @TODO/pjm-research-for-modelling/backtest_eval_metrics.md.


_SHAPE_KEYS: tuple[str, ...] = (
    "peak_height_err",
    "peak_at_actual_hour_err",
    "time_of_peak_err",
    "valley_height_err",
    "valley_at_actual_hour_err",
    "time_of_valley_err",
    "peak_window_mae",
    "first_diff_mae",
    "variogram_score_p05",
)


def variogram_score(
    actual: np.ndarray, forecast: np.ndarray, p: float = 0.5,
) -> float:
    """Variogram score with inverse-lag weights and order p, normalized
    by sum of weights so values are comparable across days.

    Translation-invariant by metric form: returns ~0 for any constant
    additive bias regardless of weight choice. **Pair with a level
    metric (MAE/CRPS); never use as a standalone forecast-quality
    objective.**
    """
    n = len(actual)
    i, j = np.triu_indices(n, k=1)
    lag = (j - i).astype(float)
    w = 1.0 / lag
    a = np.abs(actual[i] - actual[j]) ** p
    f = np.abs(forecast[i] - forecast[j]) ** p
    return float((w * (a - f) ** 2).sum() / w.sum())


def _all_nan_shape() -> dict[str, float]:
    return {k: float("nan") for k in _SHAPE_KEYS}


def evaluate_shape(
    actual: np.ndarray, forecast: np.ndarray,
) -> dict[str, float]:
    """Hour-agnostic shape/ramp metrics for a 24-hour profile forecast.

    Both inputs must be length-24 numpy arrays. Returns a flat dict of
    float scalars; nullables are ``float("nan")`` (never ``None`` /
    never absent keys) to match ``evaluate_forecast`` and round-trip
    safely through parquet.

    Keys:
        peak_height_err           max(forecast) - max(actual)
        peak_at_actual_hour_err   forecast[argmax(actual)] - actual[argmax(actual)]
        time_of_peak_err          argmax(forecast) - argmax(actual)  (signed, hours)
        valley_height_err         min(forecast) - min(actual)
        valley_at_actual_hour_err forecast[argmin(actual)] - actual[argmin(actual)]
        time_of_valley_err        argmin(forecast) - argmin(actual)  (signed, hours)
        peak_window_mae           mean |err| over [argmax(actual)-1, +1], edge-clipped
        first_diff_mae            MAE on first-difference profile
        variogram_score_p05       shape-only scoring rule (secondary KPI)

    NaN handling: if either array contains NaN, returns all-NaN dict.
    Logs once on forecast-NaN (actual-NaN is normal — target may have
    no actuals yet).

    Tie-breaking: numpy ``argmax`` returns the first occurrence.
    """
    actual = np.asarray(actual, dtype=float)
    forecast = np.asarray(forecast, dtype=float)
    if actual.shape != (24,) or forecast.shape != (24,):
        raise ValueError(
            f"evaluate_shape expects length-24 arrays; got "
            f"actual={actual.shape}, forecast={forecast.shape}"
        )
    if np.isnan(actual).any():
        return _all_nan_shape()
    if np.isnan(forecast).any():
        logger.warning("evaluate_shape: forecast contains NaN; returning all-NaN")
        return _all_nan_shape()

    a_argmax = int(np.argmax(actual))
    f_argmax = int(np.argmax(forecast))
    a_argmin = int(np.argmin(actual))
    f_argmin = int(np.argmin(forecast))

    lo = max(0, a_argmax - 1)
    hi = min(24, a_argmax + 2)
    peak_window_mae = float(np.mean(np.abs(forecast[lo:hi] - actual[lo:hi])))

    first_diff_mae = float(np.mean(np.abs(np.diff(forecast) - np.diff(actual))))

    return {
        "peak_height_err": float(forecast.max() - actual.max()),
        "peak_at_actual_hour_err": float(forecast[a_argmax] - actual[a_argmax]),
        "time_of_peak_err": float(f_argmax - a_argmax),
        "valley_height_err": float(forecast.min() - actual.min()),
        "valley_at_actual_hour_err": float(forecast[a_argmin] - actual[a_argmin]),
        "time_of_valley_err": float(f_argmin - a_argmin),
        "peak_window_mae": peak_window_mae,
        "first_diff_mae": first_diff_mae,
        "variogram_score_p05": variogram_score(actual, forecast, p=0.5),
    }


if __name__ == "__main__":
    # Smoke tests for evaluate_shape / variogram_score on three canonical
    # synthetic cases per @TODO/pjm-research-for-modelling/backtest_eval_metrics.md.
    import sys as _sys

    actual = np.array([
        24, 22, 21, 20, 21, 25, 32, 41, 45, 44, 42, 40,
        39, 38, 39, 42, 48, 58, 68, 70, 65, 55, 42, 32,
    ], dtype=float)
    # actual.argmax() == 19 (HE20, value 70); actual.argmin() == 3 (HE4, value 20)

    failures: list[str] = []

    def _check(name: str, cond: bool, detail: str = "") -> None:
        marker = "PASS" if cond else "FAIL"
        line = f"  [{marker}] {name}"
        if detail:
            line += f"  - {detail}"
        print(line)
        if not cond:
            failures.append(name)

    print("\n(a) constant offset - forecast = actual + $10")
    s = evaluate_shape(actual, actual + 10.0)
    _check("variogram_score_p05 ~= 0",
           abs(s["variogram_score_p05"]) < 1e-9,
           f"got {s['variogram_score_p05']:.3e}")
    _check("peak_height_err == 10",
           abs(s["peak_height_err"] - 10) < 1e-9,
           f"got {s['peak_height_err']:.4f}")
    _check("peak_at_actual_hour_err == 10",
           abs(s["peak_at_actual_hour_err"] - 10) < 1e-9,
           f"got {s['peak_at_actual_hour_err']:.4f}")
    _check("time_of_peak_err == 0",
           s["time_of_peak_err"] == 0.0,
           f"got {s['time_of_peak_err']}")
    _check("first_diff_mae ~= 0",
           abs(s["first_diff_mae"]) < 1e-9,
           f"got {s['first_diff_mae']:.3e}")

    print("\n(b) amplitude flatten - forecast = 0.5*actual + 0.5*mean(actual)")
    flat = 0.5 * actual + 0.5 * actual.mean()
    s = evaluate_shape(actual, flat)
    _check("variogram_score_p05 > 0",
           s["variogram_score_p05"] > 0.0,
           f"got {s['variogram_score_p05']:.4f}")
    _check("peak_height_err < 0  (squashed peak)",
           s["peak_height_err"] < 0.0,
           f"got {s['peak_height_err']:.4f}")
    _check("valley_height_err > 0  (raised valley)",
           s["valley_height_err"] > 0.0,
           f"got {s['valley_height_err']:.4f}")
    _check("time_of_peak_err == 0",
           s["time_of_peak_err"] == 0.0,
           f"got {s['time_of_peak_err']}")
    _check("first_diff_mae > 0",
           s["first_diff_mae"] > 0.0,
           f"got {s['first_diff_mae']:.4f}")

    print("\n(c) timing shift +2h - forecast = np.roll(actual, +2)")
    s = evaluate_shape(actual, np.roll(actual, 2))
    _check("variogram_score_p05 > 0",
           s["variogram_score_p05"] > 0.0,
           f"got {s['variogram_score_p05']:.4f}")
    _check("peak_height_err == 0  (max preserved by roll)",
           abs(s["peak_height_err"]) < 1e-9,
           f"got {s['peak_height_err']:.4f}")
    _check("valley_height_err == 0",
           abs(s["valley_height_err"]) < 1e-9,
           f"got {s['valley_height_err']:.4f}")
    _check("time_of_peak_err == 2",
           s["time_of_peak_err"] == 2.0,
           f"got {s['time_of_peak_err']}")
    _check("time_of_valley_err == 2",
           s["time_of_valley_err"] == 2.0,
           f"got {s['time_of_valley_err']}")

    print("\n(d) NaN actuals - all keys NaN")
    nan_actual = actual.copy()
    nan_actual[5] = np.nan
    s = evaluate_shape(nan_actual, actual)
    _check("all keys NaN",
           all(np.isnan(v) for v in s.values()),
           f"got {sum(np.isnan(v) for v in s.values())}/{len(s)} NaN")

    print("\n(e) edge case - peak at HE1 (argmax=0, window clipped to [0,2))")
    edge = np.zeros(24)
    edge[0] = 100.0
    s = evaluate_shape(edge, edge)
    _check("peak_window_mae == 0  (perfect forecast)",
           s["peak_window_mae"] == 0.0,
           f"got {s['peak_window_mae']}")
    _check("variogram_score_p05 == 0",
           s["variogram_score_p05"] == 0.0,
           f"got {s['variogram_score_p05']:.4f}")

    print()
    if failures:
        print(f"FAILED: {len(failures)} smoke test(s): {', '.join(failures)}")
        _sys.exit(1)
    print("OK: all smoke tests passed.")
