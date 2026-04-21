import os
from pathlib import Path

from backend import secrets  # noqa: F401 — ensures env vars are loaded

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger().handlers[0].setLevel(logging.INFO)

SENDER_EMAIL_ADDRESS: str = "admin@HeliosCTA.com"
RECIPIENT_EMAIL_ADDRESSES: list[str] = ["Aidan.Keaveny@HeliosCTA.com", "kapil.saxena@helioscta.com", "edi.lacic@helioscta.com", "HeliosCTA@navfundservices.com"]
TEST_RECIPIENT_EMAIL_ADDRESSES: list[str] = ["Aidan.Keaveny@HeliosCTA.com"]

### ===================================
### ===================================

# Get the directory where this config file lives
CONFIG_DIR = Path(__file__).parent
logging.info(f"CONFIG_DIR: {CONFIG_DIR}")

### ===================================
### ===================================

# NOTE: TASKS
BASE_DIR = Path(__file__).parent
SFTP_DIR: Path = BASE_DIR / "sftp_files"
SQL_DIR: Path = BASE_DIR / "sql"

# ===================================
# TRADES
# ===================================

# CLEAR_STREET
CLEAR_STREET_SFTP_DIR: Path = SFTP_DIR / "trades" / "clear_street_trades"
CLEAR_STREET_EOD_TRADES_SFTP_DIR: Path = CLEAR_STREET_SFTP_DIR / "end_of_day_trades"
CLEAR_STREET_INTRADAY_TRADES_SFTP_DIR: Path = CLEAR_STREET_SFTP_DIR / "intraday_trades"

for directory in [CLEAR_STREET_EOD_TRADES_SFTP_DIR, CLEAR_STREET_INTRADAY_TRADES_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)

# MUFG
MUFG_SFTP_DIR: Path = SFTP_DIR / "trades" / "mufg_clear_street_trades"
MUFG_EOD_TRADES_SFTP_DIR: Path = MUFG_SFTP_DIR / "end_of_day_trades"
MUFG_INTRADAY_TRADES_SFTP_DIR: Path = MUFG_SFTP_DIR / "intraday_trades"

for directory in [MUFG_EOD_TRADES_SFTP_DIR, MUFG_INTRADAY_TRADES_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)

# MAREX
MAREX_SFTP_DIR: Path = SFTP_DIR / "trades" / "marex_trades"
MAREX_EOD_TRADES_SFTP_DIR: Path = MAREX_SFTP_DIR / "helios_allocated_trades"
MAREX_PRELIM_TRADES_SFTP_DIR: Path = MAREX_SFTP_DIR / "prelim"

for directory in [MAREX_EOD_TRADES_SFTP_DIR, MAREX_PRELIM_TRADES_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)

# ===================================
# TRADE BREAKS
# ===================================

# NAV
NAV_SFTP_DIR: Path = SFTP_DIR / "nav_trade_breaks"
NAV_TRADE_BREAKS_SFTP_DIR: Path = NAV_SFTP_DIR / "trade_breaks_detail_report"

for directory in [NAV_TRADE_BREAKS_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)

# ===================================
# POSITIONS
# ===================================

# NAV_POSITIONS
NAV_POSITIONS_SFTP_DIR: Path = SFTP_DIR / 'positions' / 'nav_positions'
NAV_POSITIONS_AGR_SFTP_DIR: Path = NAV_POSITIONS_SFTP_DIR / "agr"
NAV_POSITIONS_MOROSS_SFTP_DIR: Path = NAV_POSITIONS_SFTP_DIR / "moross"
NAV_POSITIONS_PNT_SFTP_DIR: Path = NAV_POSITIONS_SFTP_DIR / "pnt"
NAV_POSITIONS_TITAN_SFTP_DIR: Path = NAV_POSITIONS_SFTP_DIR / "titan"

for directory in [NAV_POSITIONS_AGR_SFTP_DIR, NAV_POSITIONS_MOROSS_SFTP_DIR, NAV_POSITIONS_PNT_SFTP_DIR, NAV_POSITIONS_TITAN_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)

# MAREX_POSITIONS
MAREX_POSITIONS_SFTP_DIR: Path = SFTP_DIR / 'positions' / "marex_positions"
MAREX_POSITIONS_PFDF_POS_SFTP_DIR: Path = MAREX_POSITIONS_SFTP_DIR / "pfdf_pos"

for directory in [MAREX_POSITIONS_PFDF_POS_SFTP_DIR]:
    os.makedirs(directory, exist_ok=True)