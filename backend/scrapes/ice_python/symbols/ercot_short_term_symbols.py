"""
ERCOT ICE short-term symbol registry.

Add or remove ERCOT symbols here.  The intraday-quotes pipeline reads this
list at runtime, so changes take effect on the next scheduled run with no
pipeline code changes required.

Each entry must have at minimum:
    symbol        – the ICE symbol code passed to get_quotes / get_timesales
    description   – human-readable label for logging and audits
    product_type  – "power"
    contract_type – "daily", "weekly", etc.

Usage:
    from backend.scrapes.ice_python.symbols.ercot_short_term_symbols import get_ercot_symbols
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Symbol registry
# ---------------------------------------------------------------------------
# Source: ICE XL screen — ERCOT North 345KV Hub products
#
#   ERA  = ERCOT North 345KV Hub RT Peak (16 MWh)
#   END  = ERCOT North 345KV Hub RT Peak
#   NED  = ERCOT North 345KV Hub RT Off-Peak
#   NDA  = ERCOT North 345KV Hub DA Peak
# ---------------------------------------------------------------------------

ERCOT_SYMBOLS: list[dict] = [
    # -- ERA: ERCOT North RT Peak (16 MWh) ----------------------------------
    {
        "symbol": "ERA D1-IUS",
        "description": "ERCOT North RT Peak (16 MWh) Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "ERA W0-IUS",
        "description": "ERCOT North RT Peak (16 MWh) Bal Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "ERA W1-IUS",
        "description": "ERCOT North RT Peak (16 MWh) Next Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    # -- END: ERCOT North RT Peak -------------------------------------------
    {
        "symbol": "END D1-IUS",
        "description": "ERCOT North RT Peak Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "END W0-IUS",
        "description": "ERCOT North RT Peak Bal Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "END W1-IUS",
        "description": "ERCOT North RT Peak Next Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    # -- NED: ERCOT North RT Off-Peak ---------------------------------------
    {
        "symbol": "NED D1-IUS",
        "description": "ERCOT North RT Off-Peak Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "NED W0-IUS",
        "description": "ERCOT North RT Off-Peak Bal Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "NED W1-IUS",
        "description": "ERCOT North RT Off-Peak Next Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "NED W2-IUS",
        "description": "ERCOT North RT Off-Peak 2nd Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    # -- NDA: ERCOT North DA Peak -------------------------------------------
    {
        "symbol": "NDA D1-IUS",
        "description": "ERCOT North DA Peak Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "NDA W0-IUS",
        "description": "ERCOT North DA Peak Bal Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_ercot_symbols() -> list[dict]:
    """Return all active ERCOT symbol entries."""
    return ERCOT_SYMBOLS


def get_ercot_symbol_codes(symbol_entries: list[dict] | None = None) -> list[str]:
    """Return just the symbol strings for API calls."""
    entries = symbol_entries or ERCOT_SYMBOLS
    return [entry["symbol"] for entry in entries]


def get_ercot_symbol_map() -> dict[str, dict]:
    """Return ERCOT symbols keyed by ICE symbol code."""
    return {entry["symbol"]: entry for entry in ERCOT_SYMBOLS}


def resolve_ercot_symbol_entries(symbols: list[str] | None = None) -> list[dict]:
    """Resolve an optional user-selected symbol list against the registry.

    Args:
        symbols: Optional list of ICE symbol codes. If omitted, all configured
            ERCOT symbols are returned.

    Returns:
        ERCOT symbol registry entries in the requested order.

    Raises:
        ValueError: If an explicit symbol list contains blanks or unknown codes.
    """
    if symbols is None:
        return list(ERCOT_SYMBOLS)

    normalized_symbols = [symbol.strip() for symbol in symbols if symbol and symbol.strip()]
    if not normalized_symbols:
        raise ValueError("No valid ERCOT symbol codes were provided.")

    symbol_map = get_ercot_symbol_map()
    unknown_symbols = sorted(set(normalized_symbols) - set(symbol_map))
    if unknown_symbols:
        raise ValueError(
            "Unknown ERCOT ICE symbols: "
            f"{unknown_symbols}. Valid symbols must come from "
            "ercot_short_term_symbols.py."
        )

    unique_symbols = list(dict.fromkeys(normalized_symbols))
    return [symbol_map[symbol] for symbol in unique_symbols]


def log_all_symbols(symbol_entries: list[dict] | None = None) -> None:
    """Log all configured ERCOT short-term symbols."""
    entries = symbol_entries or get_ercot_symbols()
    logger.info("Configured %s ERCOT short-term symbols", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | %s | %s",
            entry["symbol"],
            entry["description"],
            entry["product_type"],
            entry["contract_type"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
