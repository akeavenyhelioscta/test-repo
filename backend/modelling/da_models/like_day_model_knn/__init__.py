"""KNN like-day forecaster — per-hour scalar variant.

Holds the per-hour scalar implementation:

  - long-format pool (one row per (date, hour_ending))
  - scalar per-target-HE matching (no window)
  - sum-Euclidean over valid z-scored dims (no /n_valid)
  - linear pre-selection age penalty, days-based half-life
  - inverse-distance² post-selection weighting
  - joint MC quantile bands for OnPeak/OffPeak/Flat aggregates

Forecast subpackage: ``pjm_rto_hourly/``.

Cross-family imports flow forward only: this package may import from
``backend.modelling.da_models.common`` only.
"""
