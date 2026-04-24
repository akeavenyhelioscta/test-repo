"""Shared configuration values for all model families."""

from __future__ import annotations

import os
from pathlib import Path

MODELLING_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE_DIR = MODELLING_ROOT / "data" / "cache"
CACHE_DIR = Path(os.getenv("DA_MODELS_CACHE_DIR", str(DEFAULT_CACHE_DIR))).expanduser()

HOURS = tuple(range(1, 25))
HUB = "EASTERN HUB"
LOAD_REGION = "RTO"
LMP_LABEL_COLUMNS = ("date", "hour_ending", "region", "lmp")
QUANTILES = (0.1, 0.5, 0.9)

DOW_GROUPS = {
    0: "weekday",  # Monday
    1: "weekday",  # Tuesday
    2: "weekday",  # Wednesday
    3: "weekday",  # Thursday
    4: "weekday",  # Friday
    5: "weekend",  # Saturday
    6: "weekend",  # Sunday
}
