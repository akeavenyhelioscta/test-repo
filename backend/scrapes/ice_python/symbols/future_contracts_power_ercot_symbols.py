"""
ICE ERCOT power futures product registry.

Each entry has:
    product       – the ICE product prefix (combined with strip+year to form symbols)
    description   – human-readable label for logging and audits
    product_type  – "power"
    contract_type – "futures"
    region        – "ercot"

Symbols are built at runtime:  ``{product} {strip}{YY}-IUS``
    e.g. ``ERN H26-IUS`` = ERCOT North 345 kV Hub RT Peak, March 2026

Usage:
    from backend.scrapes.ice_python.symbols.future_contracts_power_ercot_symbols import (
        get_ercot_power_futures_products,
        build_ice_symbol,
    )
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Month-to-strip-letter mapping (ICE convention)
# ---------------------------------------------------------------------------

STRIP_MAPPING: dict[int, str] = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


# ---------------------------------------------------------------------------
# Product registry
# ---------------------------------------------------------------------------
# Source: backend/scrapes/ice_python/future_contracts/future_contracts_v1_2025_dec_16.py
# ---------------------------------------------------------------------------

ERCOT_POWER_FUTURES_PRODUCTS: list[dict] = [
    {
        "product": "ERN",
        "description": "ERCOT North 345 kV Hub RT Peak",
        "product_type": "power",
        "contract_type": "futures",
        "region": "ercot",
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_ercot_power_futures_products() -> list[dict]:
    """Return all active ERCOT power futures product entries."""
    return ERCOT_POWER_FUTURES_PRODUCTS


def get_ercot_power_futures_product_codes(
    product_entries: list[dict] | None = None,
) -> list[str]:
    """Return just the product prefix strings."""
    entries = product_entries or ERCOT_POWER_FUTURES_PRODUCTS
    return [entry["product"] for entry in entries]


def get_ercot_power_futures_product_map() -> dict[str, dict]:
    """Return ERCOT power futures products keyed by ICE product prefix."""
    return {entry["product"]: entry for entry in ERCOT_POWER_FUTURES_PRODUCTS}


def build_ice_symbol(
    product: str,
    strip: str,
    contract_year: int,
    suffix: str = "-IUS",
) -> str:
    """Build a full ICE symbol from product prefix, strip letter, and year.

    Example: build_ice_symbol("ERN", "H", 2026) -> "ERN H26-IUS"
    """
    return f"{product} {strip}{str(contract_year)[-2:]}{suffix}"


def log_all_symbols(product_entries: list[dict] | None = None) -> None:
    """Log all configured ERCOT power futures products."""
    entries = product_entries or get_ercot_power_futures_products()
    logger.info("Configured %s ERCOT power futures products", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | %s",
            entry["product"],
            entry["description"],
            entry["region"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
