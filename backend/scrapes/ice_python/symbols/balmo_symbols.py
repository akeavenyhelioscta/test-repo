"""
ICE BALMO (balance-of-month) symbol registry.

Each entry has:
    symbol        – the ICE symbol code passed to get_timeseries
    description   – human-readable label for logging and audits
    product_type  – "gas"
    contract_type – "balmo"
    region        – geographic region grouping

Usage:
    from backend.scrapes.ice_python.symbols.balmo_symbols import get_balmo_symbols
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Symbol registry
# ---------------------------------------------------------------------------
# Source: backend/scrapes/ice_python/balmo/balmo_v1_2025_dec_16.py
# NOTE: NG Swing GDD Futures
# ---------------------------------------------------------------------------

BALMO_SYMBOLS: list[dict] = [
    # -- Henry Hub ---------------------------------------------------------------
    {
        "symbol": "HHD B0-IUS",
        "description": "Henry Hub BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "louisiana",
    },
    # -- Southeast ---------------------------------------------------------------
    {
        "symbol": "TRW B0-IUS",
        "description": "Transco Station 85 BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    {
        "symbol": "FTS B0-IUS",
        "description": "FGT Zone 3 BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    {
        "symbol": "CGR B0-IUS",
        "description": "Columbia Gulf (Mainline) BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    {
        "symbol": "APS B0-IUS",
        "description": "ANR SE-T BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    {
        "symbol": "CVK B0-IUS",
        "description": "Pine Prairie BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    {
        "symbol": "CVP B0-IUS",
        "description": "Tetco WLA BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southeast",
    },
    # -- East Texas --------------------------------------------------------------
    {
        "symbol": "UCS B0-IUS",
        "description": "Houston Ship Channel BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "east_texas",
    },
    {
        "symbol": "WAS B0-IUS",
        "description": "Waha BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "east_texas",
    },
    {
        "symbol": "NTS B0-IUS",
        "description": "NGPL TX/OK BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "east_texas",
    },
    # -- Northeast ---------------------------------------------------------------
    {
        "symbol": "ALS B0-IUS",
        "description": "Algonquin Citygates BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "TSS B0-IUS",
        "description": "Tetco M3 BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "DKS B0-IUS",
        "description": "Transco Zone 5 BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "T5C B0-IUS",
        "description": "Transco Zone 5 South BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "IZS B0-IUS",
        "description": "Iroquois Zone 2 BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "ZSS B0-IUS",
        "description": "Transco Zone 6 NY BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    {
        "symbol": "DSS B0-IUS",
        "description": "Dominion South (Eastern Gas-South) BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "northeast",
    },
    # -- Southwest ---------------------------------------------------------------
    {
        "symbol": "SCS B0-IUS",
        "description": "SoCal Citygate BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southwest",
    },
    {
        "symbol": "PIG B0-IUS",
        "description": "PG&E Citygate BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "southwest",
    },
    # -- Rockies/Northwest -------------------------------------------------------
    {
        "symbol": "CRS B0-IUS",
        "description": "CIG Mainline BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "rockies_northwest",
    },
    # -- Midwest -----------------------------------------------------------------
    {
        "symbol": "MTS B0-IUS",
        "description": "NGPL Midcontinent BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "midwest",
    },
    {
        "symbol": "NMS B0-IUS",
        "description": "MichCon BALMO",
        "product_type": "gas",
        "contract_type": "balmo",
        "region": "midwest",
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_balmo_symbols() -> list[dict]:
    """Return all active BALMO symbol entries."""
    return BALMO_SYMBOLS


def get_balmo_symbol_codes(symbol_entries: list[dict] | None = None) -> list[str]:
    """Return just the symbol strings for API calls."""
    entries = symbol_entries or BALMO_SYMBOLS
    return [entry["symbol"] for entry in entries]


def get_balmo_symbol_map() -> dict[str, dict]:
    """Return BALMO symbols keyed by ICE symbol code."""
    return {entry["symbol"]: entry for entry in BALMO_SYMBOLS}


def log_all_symbols(symbol_entries: list[dict] | None = None) -> None:
    """Log all configured BALMO symbols."""
    entries = symbol_entries or get_balmo_symbols()
    logger.info("Configured %s BALMO symbols", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | %s | %s",
            entry["symbol"],
            entry["description"],
            entry["region"],
            entry["contract_type"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
