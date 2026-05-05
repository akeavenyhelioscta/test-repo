"""Naive baseline forecasters for PJM day-ahead LMP at the configured hub.

Two variants:

  - EPF naive (Lago/Nogales/Conejo): DOW-conditional persistence
    (Mon/Sat/Sun take d-7; Tue-Fri take d-1).
  - d-7: pure same-hour-last-week persistence.

Both are point-forecast only, deterministic, and reuse the like-day
model's output table / metrics infrastructure for apples-to-apples
comparison.
"""
