"""
ICE gas futures product registry.

Each entry has:
    product       – the ICE product prefix (combined with strip+year to form symbols)
    description   – human-readable label for logging and audits
    product_type  – "gas"
    contract_type – "futures"
    region        – geographic region grouping

Symbols are built at runtime:  ``{product} {strip}{YY}-IUS``
    e.g. ``HNG H26-IUS`` = Henry Hub, March 2026

Usage:
    from backend.scrapes.ice_python.symbols.future_contracts_gas_symbols import (
        get_gas_futures_products,
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

GAS_FUTURES_PRODUCTS: list[dict] = [
    # -- Henry Hub ---------------------------------------------------------------
    {
        "product": "HNG",
        "description": "Henry Hub Natural Gas",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "louisiana",
    },
    # -- Southeast ---------------------------------------------------------------
    {
        "product": "TRZ",
        "description": "Transco Station 85 Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southeast",
    },
    {
        "product": "CGB",
        "description": "Columbia Gulf Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southeast",
    },
    {
        "product": "CGM",
        "description": "ANR SE-T Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southeast",
    },
    {
        "product": "TWB",
        "description": "Tetco WLA Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southeast",
    },
    # -- East Texas --------------------------------------------------------------
    {
        "product": "HXS",
        "description": "Houston Ship Channel Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "east_texas",
    },
    {
        "product": "WAH",
        "description": "Waha Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "east_texas",
    },
    {
        "product": "NTO",
        "description": "NGPL TX/OK Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "east_texas",
    },
    # -- Northeast ---------------------------------------------------------------
    {
        "product": "ALQ",
        "description": "Algonquin Citygates Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    {
        "product": "TMT",
        "description": "Tetco M3 Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    {
        "product": "T5B",
        "description": "Transco Zone 5 South Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    {
        "product": "IZB",
        "description": "Iroquois Zone 2 Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    {
        "product": "TZS",
        "description": "Transco Zone 6 NY Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    {
        "product": "DOM",
        "description": "Dominion South Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "northeast",
    },
    # -- Southwest ---------------------------------------------------------------
    {
        "product": "SCB",
        "description": "SoCal Citygate Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southwest",
    },
    {
        "product": "PGE",
        "description": "PG&E Citygate Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "southwest",
    },
    # -- Rockies/Northwest -------------------------------------------------------
    {
        "product": "CRI",
        "description": "CIG Mainline Basis",
        "product_type": "gas",
        "contract_type": "futures",
        "region": "rockies_northwest",
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_gas_futures_products() -> list[dict]:
    """Return all active gas futures product entries."""
    return GAS_FUTURES_PRODUCTS


def get_gas_futures_product_codes(product_entries: list[dict] | None = None) -> list[str]:
    """Return just the product prefix strings."""
    entries = product_entries or GAS_FUTURES_PRODUCTS
    return [entry["product"] for entry in entries]


def get_gas_futures_product_map() -> dict[str, dict]:
    """Return gas futures products keyed by ICE product prefix."""
    return {entry["product"]: entry for entry in GAS_FUTURES_PRODUCTS}


def build_ice_symbol(
    product: str,
    strip: str,
    contract_year: int,
    suffix: str = "-IUS",
) -> str:
    """Build a full ICE symbol from product prefix, strip letter, and year.

    Example: build_ice_symbol("HNG", "H", 2026) -> "HNG H26-IUS"
    """
    return f"{product} {strip}{str(contract_year)[-2:]}{suffix}"


def log_all_symbols(product_entries: list[dict] | None = None) -> None:
    """Log all configured gas futures products."""
    entries = product_entries or get_gas_futures_products()
    logger.info("Configured %s gas futures products", len(entries))
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
