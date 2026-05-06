"""Evaluation metrics for probabilistic and point forecasts."""

from __future__ import annotations

import logging
from typing import Mapping, Sequence

import numpy as np

logger = logging.getLogger(__name__)


def _to_numpy(values: Sequence[float] | np.ndarray) -> np.ndarray:
    return np.asarray(values, dtype=float)


def pinball_loss(
    y_true: Sequence[float] | np.ndarray,
    y_pred_q: Sequence[float] | np.ndarray,
    quantile: float,
) -> float:
    """Average pinball loss for a single quantile forecast."""
    if not 0.0 < quantile < 1.0:
        raise ValueError("quantile must be between 0 and 1.")

    actual = _to_numpy(y_true)
    pred = _to_numpy(y_pred_q)
    if actual.shape != pred.shape:
        raise ValueError("y_true and y_pred_q must have the same shape.")

    error = actual - pred
    loss = np.maximum(quantile * error, (quantile - 1.0) * error)
    return float(np.mean(loss))


def crps_from_quantiles(
    y_true: Sequence[float] | np.ndarray,
    quantile_predictions: Mapping[float, Sequence[float] | np.ndarray],
) -> float:
    """
    Approximate CRPS from a set of quantile forecasts.

    Approximation uses: CRPS ≈ 2 * average(pinball(q)).
    """
    if not quantile_predictions:
        raise ValueError("quantile_predictions must not be empty.")

    losses = []
    for quantile, predictions in sorted(quantile_predictions.items()):
        losses.append(pinball_loss(y_true, predictions, float(quantile)))
    return float(2.0 * np.mean(losses))


def coverage(
    y_true: Sequence[float] | np.ndarray,
    lower: Sequence[float] | np.ndarray,
    upper: Sequence[float] | np.ndarray,
) -> float:
    """Empirical interval coverage."""
    actual = _to_numpy(y_true)
    lo = _to_numpy(lower)
    hi = _to_numpy(upper)
    if not (actual.shape == lo.shape == hi.shape):
        raise ValueError("y_true, lower, and upper must have the same shape.")

    return float(np.mean((actual >= lo) & (actual <= hi)))


def point_errors(
    y_true: Sequence[float] | np.ndarray,
    y_pred: Sequence[float] | np.ndarray,
) -> dict[str, float]:
    """Point forecast error summary."""
    actual = _to_numpy(y_true)
    pred = _to_numpy(y_pred)
    if actual.shape != pred.shape:
        raise ValueError("y_true and y_pred must have the same shape.")

    error = actual - pred
    abs_error = np.abs(error)
    squared_error = error**2
    denom = np.where(np.abs(actual) < 1e-12, np.nan, np.abs(actual))
    ape = abs_error / denom

    return {
        "mae": float(np.nanmean(abs_error)),
        "rmse": float(np.sqrt(np.nanmean(squared_error))),
        "mape": float(np.nanmean(ape) * 100.0),
        "bias": float(np.nanmean(error)),
    }


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


_SHAPE_ONPEAK_KEYS: tuple[str, ...] = (
    "block_mean_err",
    "block_mean_abs_err",
    "peak_height_err_onpeak",
    "peak_at_actual_hour_err_onpeak",
    "time_of_peak_err_onpeak",
    "valley_height_err_onpeak",
    "valley_at_actual_hour_err_onpeak",
    "time_of_valley_err_onpeak",
    "peak_window_mae_onpeak",
    "first_diff_mae_onpeak",
    "variogram_score_p05_onpeak",
    "peak_outside_onpeak",
    "valley_outside_onpeak",
)


def variogram_score(
    actual: np.ndarray,
    forecast: np.ndarray,
    p: float = 0.5,
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


def _all_nan_shape_onpeak() -> dict[str, float]:
    return {k: float("nan") for k in _SHAPE_ONPEAK_KEYS}


def evaluate_shape(
    actual: np.ndarray,
    forecast: np.ndarray,
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


def evaluate_shape_onpeak(
    actual: np.ndarray,
    forecast: np.ndarray,
) -> dict[str, float]:
    """Shape metrics restricted to the PJM on-peak block (HE8-23).

    Both inputs must be length-24 numpy arrays indexed
    ``[HE1, ..., HE24]``. The function takes the slice ``[7:23]``
    (16 values, HE8-23) and applies the locked 24-hour formula
    verbatim — no re-tuning of variogram p or weight scheme. Caller
    is responsible for restricting to weekdays ex NERC holidays.

    Keys:
        block_mean_err                  mean(forecast[7:23]) - mean(actual[7:23])
        block_mean_abs_err              abs(block_mean_err)
        peak_height_err_onpeak          max over slice
        peak_at_actual_hour_err_onpeak  forecast at slice-argmax of actual
        time_of_peak_err_onpeak         signed hours, computed on slice
        valley_height_err_onpeak        analogous min over slice
        valley_at_actual_hour_err_onpeak analogous
        time_of_valley_err_onpeak       analogous
        peak_window_mae_onpeak          slice argmax +/- 1, clipped to [0, 16]
        first_diff_mae_onpeak           MAE on slice first-differences
        variogram_score_p05_onpeak      variogram_score on slice
        peak_outside_onpeak             1.0 if full-day argmax outside [7, 22]
        valley_outside_onpeak           1.0 if full-day argmin outside [7, 22]

    NaN handling matches ``evaluate_shape``: actual-NaN -> all-NaN dict;
    forecast-NaN -> all-NaN dict + logged warning.

    Tie-breaking: numpy ``argmax`` returns the first occurrence.
    """
    actual = np.asarray(actual, dtype=float)
    forecast = np.asarray(forecast, dtype=float)
    if actual.shape != (24,) or forecast.shape != (24,):
        raise ValueError(
            f"evaluate_shape_onpeak expects length-24 arrays; got "
            f"actual={actual.shape}, forecast={forecast.shape}"
        )
    if np.isnan(actual).any():
        return _all_nan_shape_onpeak()
    if np.isnan(forecast).any():
        logger.warning(
            "evaluate_shape_onpeak: forecast contains NaN; returning all-NaN"
        )
        return _all_nan_shape_onpeak()

    a_slice = actual[7:23]
    f_slice = forecast[7:23]

    block_mean_err = float(f_slice.mean() - a_slice.mean())

    a_argmax_full = int(np.argmax(actual))
    a_argmin_full = int(np.argmin(actual))
    peak_outside = 0.0 if 7 <= a_argmax_full <= 22 else 1.0
    valley_outside = 0.0 if 7 <= a_argmin_full <= 22 else 1.0

    a_argmax = int(np.argmax(a_slice))
    f_argmax = int(np.argmax(f_slice))
    a_argmin = int(np.argmin(a_slice))
    f_argmin = int(np.argmin(f_slice))

    lo = max(0, a_argmax - 1)
    hi = min(16, a_argmax + 2)
    peak_window_mae = float(np.mean(np.abs(f_slice[lo:hi] - a_slice[lo:hi])))

    first_diff_mae = float(np.mean(np.abs(np.diff(f_slice) - np.diff(a_slice))))

    return {
        "block_mean_err": block_mean_err,
        "block_mean_abs_err": float(abs(block_mean_err)),
        "peak_height_err_onpeak": float(f_slice.max() - a_slice.max()),
        "peak_at_actual_hour_err_onpeak": float(f_slice[a_argmax] - a_slice[a_argmax]),
        "time_of_peak_err_onpeak": float(f_argmax - a_argmax),
        "valley_height_err_onpeak": float(f_slice.min() - a_slice.min()),
        "valley_at_actual_hour_err_onpeak": float(
            f_slice[a_argmin] - a_slice[a_argmin]
        ),
        "time_of_valley_err_onpeak": float(f_argmin - a_argmin),
        "peak_window_mae_onpeak": peak_window_mae,
        "first_diff_mae_onpeak": first_diff_mae,
        "variogram_score_p05_onpeak": variogram_score(a_slice, f_slice, p=0.5),
        "peak_outside_onpeak": peak_outside,
        "valley_outside_onpeak": valley_outside,
    }


if __name__ == "__main__":
    import sys as _sys

    actual = np.array(
        [
            24,
            22,
            21,
            20,
            21,
            25,
            32,
            41,
            45,
            44,
            42,
            40,
            39,
            38,
            39,
            42,
            48,
            58,
            68,
            70,
            65,
            55,
            42,
            32,
        ],
        dtype=float,
    )

    failures: list[str] = []

    def _check(name: str, cond: bool, detail: str = "") -> None:
        marker = "PASS" if cond else "FAIL"
        line = f"  [{marker}] {name}"
        if detail:
            line += f"  - {detail}"
        print(line)
        if not cond:
            failures.append(name)

    print("\n=== evaluate_shape (24h) ===")

    print("\n(a) constant offset - forecast = actual + $10")
    s = evaluate_shape(actual, actual + 10.0)
    _check(
        "variogram_score_p05 ~= 0",
        abs(s["variogram_score_p05"]) < 1e-9,
        f"got {s['variogram_score_p05']:.3e}",
    )
    _check(
        "peak_height_err == 10",
        abs(s["peak_height_err"] - 10) < 1e-9,
        f"got {s['peak_height_err']:.4f}",
    )
    _check(
        "peak_at_actual_hour_err == 10",
        abs(s["peak_at_actual_hour_err"] - 10) < 1e-9,
        f"got {s['peak_at_actual_hour_err']:.4f}",
    )
    _check(
        "time_of_peak_err == 0",
        s["time_of_peak_err"] == 0.0,
        f"got {s['time_of_peak_err']}",
    )
    _check(
        "first_diff_mae ~= 0",
        abs(s["first_diff_mae"]) < 1e-9,
        f"got {s['first_diff_mae']:.3e}",
    )

    print("\n(b) amplitude flatten - forecast = 0.5*actual + 0.5*mean(actual)")
    flat = 0.5 * actual + 0.5 * actual.mean()
    s = evaluate_shape(actual, flat)
    _check(
        "variogram_score_p05 > 0",
        s["variogram_score_p05"] > 0.0,
        f"got {s['variogram_score_p05']:.4f}",
    )
    _check(
        "peak_height_err < 0  (squashed peak)",
        s["peak_height_err"] < 0.0,
        f"got {s['peak_height_err']:.4f}",
    )
    _check(
        "valley_height_err > 0  (raised valley)",
        s["valley_height_err"] > 0.0,
        f"got {s['valley_height_err']:.4f}",
    )
    _check(
        "time_of_peak_err == 0",
        s["time_of_peak_err"] == 0.0,
        f"got {s['time_of_peak_err']}",
    )
    _check(
        "first_diff_mae > 0",
        s["first_diff_mae"] > 0.0,
        f"got {s['first_diff_mae']:.4f}",
    )

    print("\n(c) timing shift +2h - forecast = np.roll(actual, +2)")
    s = evaluate_shape(actual, np.roll(actual, 2))
    _check(
        "variogram_score_p05 > 0",
        s["variogram_score_p05"] > 0.0,
        f"got {s['variogram_score_p05']:.4f}",
    )
    _check(
        "peak_height_err == 0  (max preserved by roll)",
        abs(s["peak_height_err"]) < 1e-9,
        f"got {s['peak_height_err']:.4f}",
    )
    _check(
        "valley_height_err == 0",
        abs(s["valley_height_err"]) < 1e-9,
        f"got {s['valley_height_err']:.4f}",
    )
    _check(
        "time_of_peak_err == 2",
        s["time_of_peak_err"] == 2.0,
        f"got {s['time_of_peak_err']}",
    )
    _check(
        "time_of_valley_err == 2",
        s["time_of_valley_err"] == 2.0,
        f"got {s['time_of_valley_err']}",
    )

    print("\n(d) NaN actuals - all keys NaN")
    nan_actual = actual.copy()
    nan_actual[5] = np.nan
    s = evaluate_shape(nan_actual, actual)
    _check(
        "all keys NaN",
        all(np.isnan(v) for v in s.values()),
        f"got {sum(np.isnan(v) for v in s.values())}/{len(s)} NaN",
    )

    print("\n(e) edge case - peak at HE1 (argmax=0, window clipped to [0,2))")
    edge = np.zeros(24)
    edge[0] = 100.0
    s = evaluate_shape(edge, edge)
    _check(
        "peak_window_mae == 0  (perfect forecast)",
        s["peak_window_mae"] == 0.0,
        f"got {s['peak_window_mae']}",
    )
    _check(
        "variogram_score_p05 == 0",
        s["variogram_score_p05"] == 0.0,
        f"got {s['variogram_score_p05']:.4f}",
    )

    print("\n=== evaluate_shape_onpeak (HE8-23 slice) ===")

    print("\n(oa) constant offset on slice - forecast = actual + $10")
    s = evaluate_shape_onpeak(actual, actual + 10.0)
    _check(
        "variogram_score_p05_onpeak ~= 0",
        abs(s["variogram_score_p05_onpeak"]) < 1e-9,
        f"got {s['variogram_score_p05_onpeak']:.3e}",
    )
    _check(
        "block_mean_err == 10.0",
        abs(s["block_mean_err"] - 10.0) < 1e-9,
        f"got {s['block_mean_err']:.4f}",
    )
    _check(
        "block_mean_abs_err == 10.0",
        abs(s["block_mean_abs_err"] - 10.0) < 1e-9,
        f"got {s['block_mean_abs_err']:.4f}",
    )
    _check(
        "peak_height_err_onpeak == 10",
        abs(s["peak_height_err_onpeak"] - 10) < 1e-9,
        f"got {s['peak_height_err_onpeak']:.4f}",
    )
    _check(
        "peak_at_actual_hour_err_onpeak == 10",
        abs(s["peak_at_actual_hour_err_onpeak"] - 10) < 1e-9,
        f"got {s['peak_at_actual_hour_err_onpeak']:.4f}",
    )
    _check(
        "valley_height_err_onpeak == 10",
        abs(s["valley_height_err_onpeak"] - 10) < 1e-9,
        f"got {s['valley_height_err_onpeak']:.4f}",
    )
    _check(
        "peak_window_mae_onpeak == 10",
        abs(s["peak_window_mae_onpeak"] - 10) < 1e-9,
        f"got {s['peak_window_mae_onpeak']:.4f}",
    )
    _check(
        "peak_outside_onpeak == 0.0  (HE20 inside on-peak)",
        s["peak_outside_onpeak"] == 0.0,
        f"got {s['peak_outside_onpeak']}",
    )

    print("\n(ob) amplitude flatten - forecast = 0.5*actual + 0.5*mean(actual)")
    flat = 0.5 * actual + 0.5 * actual.mean()
    s = evaluate_shape_onpeak(actual, flat)
    _check(
        "variogram_score_p05_onpeak > 0",
        s["variogram_score_p05_onpeak"] > 0.0,
        f"got {s['variogram_score_p05_onpeak']:.4f}",
    )
    _check(
        "block_mean_err < 0  (compressed amplitude lowers slice mean)",
        s["block_mean_err"] < 0.0,
        f"got {s['block_mean_err']:.4f}",
    )

    print("\n(oc) within-block time shift +2h on synthetic profile")
    synth = np.full(24, 20.0)
    synth[7:23] = np.array(
        [
            25,
            28,
            32,
            36,
            40,
            44,
            48,
            52,
            50,
            46,
            42,
            38,
            34,
            30,
            26,
            24,
        ],
        dtype=float,
    )
    shifted = synth.copy()
    shifted[7:23] = np.roll(synth[7:23], 2)
    s = evaluate_shape_onpeak(synth, shifted)
    _check(
        "time_of_peak_err_onpeak == 2",
        s["time_of_peak_err_onpeak"] == 2.0,
        f"got {s['time_of_peak_err_onpeak']}",
    )
    _check(
        "block_mean_err ~= 0  (mean preserved by within-window shift)",
        abs(s["block_mean_err"]) < 1e-9,
        f"got {s['block_mean_err']:.4e}",
    )
    _check(
        "variogram_score_p05_onpeak > 0",
        s["variogram_score_p05_onpeak"] > 0.0,
        f"got {s['variogram_score_p05_onpeak']:.4f}",
    )

    print("\n(od) canonical peak at index 19 (HE20) is inside on-peak")
    s = evaluate_shape_onpeak(actual, actual)
    _check(
        "peak_outside_onpeak == 0.0",
        s["peak_outside_onpeak"] == 0.0,
        f"got {s['peak_outside_onpeak']}",
    )

    print("\n(oe) morning-peak profile with argmax at index 5 (HE6)")
    morning = actual.copy()
    morning[5] = 200.0
    s = evaluate_shape_onpeak(morning, morning)
    _check(
        "peak_outside_onpeak == 1.0",
        s["peak_outside_onpeak"] == 1.0,
        f"got {s['peak_outside_onpeak']}",
    )

    print()
    if failures:
        print(f"FAILED: {len(failures)} smoke test(s): {', '.join(failures)}")
        _sys.exit(1)
    print("OK: all smoke tests passed.")
