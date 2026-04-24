"""Environment and logging bootstrap for forward-only KNN."""
from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

CONFIG_DIR = Path(__file__).parent.parent
load_dotenv(dotenv_path=CONFIG_DIR / ".env", override=False)

logging.basicConfig(level=logging.INFO)

AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")
AZURE_POSTGRESQL_DB_NAME = os.getenv("AZURE_POSTGRESQL_DB_NAME")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")
