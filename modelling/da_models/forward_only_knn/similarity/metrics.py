"""Distance and weight utilities for forward-only KNN."""
from __future__ import annotations

import numpy as np


def nan_aware_euclidean(a: np.ndarray, b: np.ndarray) -> tuple[float, int]:
    """Euclidean distance over dimensions where both arrays are non-NaN.

    Returns:
        (distance, n_valid_dims). Distance is NaN when n_valid_dims is 0.
    """
    mask = (~np.isnan(a)) & (~np.isnan(b))
    n_valid = int(mask.sum())
    if n_valid == 0:
        return np.nan, 0
    diff = a[mask] - b[mask]
    return float(np.sqrt(np.sum(diff**2))), n_valid


def fit_pool_zscore(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Pool-only z-score parameters with NaN-safe handling."""
    values = np.asarray(values, dtype=float)
    if values.ndim != 2:
        raise ValueError("values must be a 2-D array.")

    valid = ~np.isnan(values)
    counts = valid.sum(axis=0).astype(float)

    safe_values = np.where(valid, values, 0.0)
    means = safe_values.sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    means = np.where(counts == 0.0, 0.0, means)

    centered = np.where(valid, values - means, 0.0)
    variances = (centered**2).sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    stds = np.sqrt(variances)
    stds = np.where((stds == 0.0) | (counts == 0.0) | np.isnan(stds), 1.0, stds)
    return means, stds


def apply_zscore(values: np.ndarray, means: np.ndarray, stds: np.ndarray) -> np.ndarray:
    """Apply pre-fit z-score transform."""
    return (values - means) / stds


def compute_analog_weights(
    distances: np.ndarray,
    method: str = "inverse_distance",
    temperature: float = 1.0,
) -> np.ndarray:
    """Convert distance array to normalized analog weights."""
    if len(distances) == 0:
        return np.array([])

    if method == "softmax":
        scaled = -distances / max(temperature, 1e-8)
        scaled -= scaled.max()
        weights = np.exp(scaled)
    elif method == "rank":
        ranks = np.argsort(np.argsort(distances))
        weights = (len(distances) - ranks).astype(float)
    elif method == "uniform":
        weights = np.ones(len(distances), dtype=float)
    else:
        weights = 1.0 / np.square(np.maximum(distances, 1e-8))

    total = float(weights.sum())
    if total <= 0:
        return np.ones(len(distances), dtype=float) / float(len(distances))
    return weights / total
