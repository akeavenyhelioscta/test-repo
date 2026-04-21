import os
from dotenv import load_dotenv
from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger().handlers[0].setLevel(logging.INFO)

# ────── Environment selection ──────
# Set BACKEND_ENV=dev to target the test database, defaults to prod
#
# Loading order:
#   1. .env.shared   — API keys, SFTP creds, Slack, etc. (everything except DB)
#   2. .env.{env}    — database credentials (prod or dev)
#   3. .env          — optional local overrides
CONFIG_DIR = Path(__file__).parent
BACKEND_ENV = os.environ.get("BACKEND_ENV", "prod")

shared_env = CONFIG_DIR / ".env.shared"
env_file = CONFIG_DIR / f".env.{BACKEND_ENV}"

if not shared_env.exists():
    raise FileNotFoundError(f"Shared env file not found: {shared_env}")
if not env_file.exists():
    raise FileNotFoundError(f"Environment file not found: {env_file}")

logging.info(f"BACKEND_ENV={BACKEND_ENV}, loading {shared_env} + {env_file}")
load_dotenv(dotenv_path=shared_env, override=False)
load_dotenv(dotenv_path=env_file, override=True)

# Optional local overrides (backend/.env, gitignored)
local_env = CONFIG_DIR / ".env"
if local_env.exists():
    load_dotenv(dotenv_path=local_env, override=True)


# ────── Azure PostgreSQL ──────
AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")

# ────── Azure SQL Server ──────
AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_USER = os.getenv("AZURE_SQL_USER")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE", "GenscapeDataFeed")

# ────── Azure Access Tokens for Outlook Emails ──────
AZURE_OUTLOOK_CLIENT_ID = os.getenv("AZURE_OUTLOOK_CLIENT_ID")
AZURE_OUTLOOK_TENANT_ID = os.getenv("AZURE_OUTLOOK_TENANT_ID")
AZURE_OUTLOOK_CLIENT_SECRET = os.getenv("AZURE_OUTLOOK_CLIENT_SECRET")

# ────── Slack ──────
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_DEFAULT_GROUP_ID = os.getenv("SLACK_DEFAULT_GROUP_ID")
SLACK_DEFAULT_CHANNEL_NAME = os.getenv("SLACK_DEFAULT_CHANNEL_NAME")
SLACK_DEFAULT_WEBHOOK_URL = os.getenv("SLACK_DEFAULT_WEBHOOK_URL")
SLACK_CHANNEL_NAME = os.getenv("SLACK_DEFAULT_CHANNEL_NAME")

# ────── Power / Grid APIs ──────
PJM_API_KEY = os.getenv("PJM_API_KEY")
GRIDSTATUS_API_KEY = os.getenv("GRIDSTATUS_API_KEY")

# ────── Positions and Trades ──────

# CLEAR_STREET
CLEAR_STREET_SFTP_HOST = os.getenv("CLEAR_STREET_SFTP_HOST")
CLEAR_STREET_SFTP_USER = os.getenv("CLEAR_STREET_SFTP_USER")
CLEAR_STREET_SSH_KEY_CONTENT = os.getenv("CLEAR_STREET_SSH_KEY_CONTENT")
CLEAR_STREET_SFTP_PORT = int(os.getenv("CLEAR_STREET_SFTP_PORT"))
CLEAR_STREET_SFTP_REMOTE_DIR = r'/'

# MUFG
MUFG_SFTP_HOST = os.getenv("MUFG_SFTP_HOST")
MUFG_SFTP_USER = os.getenv("MUFG_SFTP_USER")
MUFG_SFTP_PASSWORD = os.getenv("MUFG_SFTP_PASSWORD")
MUFG_SFTP_PORT = int(os.getenv("MUFG_SFTP_PORT"))
MUFG_SFTP_REMOTE_DIR = r'/'

# MAREX
MAREX_SFTP_HOST = os.getenv("MAREX_SFTP_HOST")
MAREX_SFTP_USER = os.getenv("MAREX_SFTP_USER")
MAREX_SFTP_PASSWORD = os.getenv("MAREX_SFTP_PASSWORD")
MAREX_SFTP_PORT = int(os.getenv("MAREX_SFTP_PORT"))
MAREX_SFTP_REMOTE_DIR = r'/'

# NAV
NAV_SFTP_HOST = os.getenv("NAV_SFTP_HOST")
NAV_SFTP_USER = os.getenv("NAV_SFTP_USER")
NAV_SFTP_PASSWORD = os.getenv("NAV_SFTP_PASSWORD")
NAV_SFTP_PORT = int(os.getenv("NAV_SFTP_PORT"))
NAV_SFTP_REMOTE_DIR = r'/'


# ────── POWER──────
# GRIDSTATUS CREDENTIALS
GRIDSTATUS_API_KEY = os.getenv("GRIDSTATUS_API_KEY")

# PJM CREDENTIALS
PJM_API_KEY = os.getenv("PJM_API_KEY")

# ERCOT CREDENTIALS
ERCOT_USERNAME = os.getenv("ERCOT_USERNAME")
ERCOT_PASSCODE = os.getenv("ERCOT_PASSCODE")
ERCOT_API_KEY = os.getenv("ERCOT_API_KEY")

# ISONE CREDENTIALS
ISONE_BASE_URL = os.getenv("ISONE_BASE_URL")
ISONE_ACCOUNT = os.getenv("ISONE_ACCOUNT")
ISONE_PASSWORD = os.getenv("ISONE_PASSWORD")


# ────── WSI ──────
WSI_TRADER_USERNAME = os.getenv("WSI_TRADER_USERNAME")
WSI_TRADER_NAME = os.getenv("WSI_TRADER_NAME")
WSI_TRADER_PASSWORD = os.getenv("WSI_TRADER_PASSWORD")

# ────── EIA ──────
EIA_API_KEY = os.getenv("EIA_API_KEY")

# ────── GENSCAPE ──────
GEN_API_KEY = os.getenv("GEN_API_KEY")

# ────── ENERGY ASPECTS ──────
ENERGY_ASPECTS_API_KEY = os.getenv("ENERGY_ASPECTS_API_KEY")

# ────── Azure Blob Storage ──────
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

# ────── METEOLOGICA ──────
# Lower 48 (US48 aggregate) account
XTRADERS_API_USERNAME_L48 = os.getenv("XTRADERS_API_USERNAME_L48")
XTRADERS_API_PASSWORD_L48 = os.getenv("XTRADERS_API_PASSWORD_L48")

# ISO-level account (PJM, ERCOT, MISO, etc.)
XTRADERS_API_USERNAME_ISO = os.getenv("XTRADERS_API_USERNAME_ISO")
XTRADERS_API_PASSWORD_ISO = os.getenv("XTRADERS_API_PASSWORD_ISO")
