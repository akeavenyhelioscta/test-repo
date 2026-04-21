"""
ICE options product registry.

Each entry defines a product to pull options for, along with discovery parameters.
Options symbols are discovered at runtime via ``ice.get_autolist()``.

Symbol conventions (from ICE XL Python Guide, Section 4.1):
    ***{product}-IUS              -> all expirations
    ***{product}-IUS {expiry}     -> all strikes for one expiration
    ***{product}-IUS {expiry} ATM:{n}  -> n strikes around the money

Discovered option symbols follow the format:
    {product} {expiry}C{strike}-IUS  (call)
    {product} {expiry}P{strike}-IUS  (put)
    e.g. HNG J26C3-IUS  = Henry Hub April 2026 Call @ $3.00
"""

from __future__ import annotations

import logging

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Month-to-strip-letter mapping (same as futures)
# ---------------------------------------------------------------------------

STRIP_MAPPING: dict[int, str] = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


# ---------------------------------------------------------------------------
# Product registry
# ---------------------------------------------------------------------------

OPTIONS_PRODUCTS: list[dict] = [
    # -- Henry Hub ---------------------------------------------------------------
    {
        "product": "HNG",
        "description": "Henry Hub Natural Gas Options",
        "product_type": "gas",
        "contract_type": "options",
        "region": "louisiana",
        "exchange_suffix": "-IUS",
        "atm_range": 10,
    },
    # -- PJM Power ---------------------------------------------------------------
    {
        "product": "PMI",
        "description": "PJM Western Hub RT Peak Options",
        "product_type": "power",
        "contract_type": "options",
        "region": "pjm",
        "exchange_suffix": "-IUS",
        "atm_range": 10,
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_options_products() -> list[dict]:
    """Return all active options product entries."""
    return OPTIONS_PRODUCTS


def get_options_product_codes(product_entries: list[dict] | None = None) -> list[str]:
    """Return just the product prefix strings."""
    entries = product_entries or OPTIONS_PRODUCTS
    return [entry["product"] for entry in entries]


def get_options_product_map() -> dict[str, dict]:
    """Return options products keyed by ICE product prefix."""
    return {entry["product"]: entry for entry in OPTIONS_PRODUCTS}


def log_all_products(product_entries: list[dict] | None = None) -> None:
    """Log all configured options products."""
    entries = product_entries or get_options_products()
    _logger.info("Configured %s options products", len(entries))
    for entry in entries:
        _logger.info(
            "%s | %s | %s | ATM:%s",
            entry["product"],
            entry["description"],
            entry["region"],
            entry["atm_range"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_products()


if __name__ == "__main__":
    _main()
