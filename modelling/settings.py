"""Non-secret configuration for modelling runs."""
from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent

# Load .env so env vars are populated regardless of which file is imported first.
_env_file = BASE_DIR / ".env"
if _env_file.exists():
    load_dotenv(dotenv_path=_env_file, override=False)

CACHE_DIR: Path = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Market date used when selecting which cache date to download (PJM = ET).
MARKET_TZ = ZoneInfo("America/New_York")

# Azure Blob (model cache) — non-secret config.
MODEL_CACHE_BLOB_ENABLED = os.getenv("MODEL_CACHE_BLOB_ENABLED", "false").lower() in (
    "true",
    "1",
    "yes",
)
MODEL_CACHE_BLOB_CONTAINER = os.getenv("MODEL_CACHE_BLOB_CONTAINER")
MODEL_CACHE_BLOB_PREFIX = os.getenv("MODEL_CACHE_BLOB_PREFIX", "da-models-cache").strip("/")
MODEL_CACHE_DATE = os.getenv("MODEL_CACHE_DATE")
