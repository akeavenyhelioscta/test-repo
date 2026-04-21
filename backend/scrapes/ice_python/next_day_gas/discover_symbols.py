"""
Discover all available ICE next-day physical gas symbols.

Uses ice.get_autolist() to enumerate available D1-IPG (next-day, NG Firm Physical)
symbols, then attempts to match them against target gas hubs.

Requires ICE XL to be running locally.

Usage:
    python -m backend.scrapes.ice_python.next_day_gas.discover_symbols
"""

from __future__ import annotations

from backend.scrapes.ice_python import utils


# Hubs we're looking for (not yet in next_day_gas_symbols.py)
TARGET_HUBS = [
    "Columbia TCO Pool",
    "Northern Ventura",
    "Tetco M2",
    "Transco Z5 non-WGL",
    "Tennessee Z4",
    "Transco Leidy",
    "Chicago CityGate",
]


def main() -> None:
    ice = utils.get_icepython_module()

    # -----------------------------------------------------------------------
    # Method 1: autolist with wildcard — discover all next-day physical gas
    # -----------------------------------------------------------------------
    print("=" * 80)
    print("METHOD 1: get_autolist('***D1-IPG')")
    print("=" * 80)

    try:
        results = ice.get_autolist("***D1-IPG")
        if results:
            print(f"\nFound {len(results)} next-day physical gas symbols:\n")
            for symbol in sorted(results):
                print(f"  {symbol}")
        else:
            print("No results — try Method 2")
    except Exception as exc:
        print(f"autolist failed: {exc}")
        print("Trying alternative patterns...\n")

    # -----------------------------------------------------------------------
    # Method 2: try broader patterns if Method 1 is empty
    # -----------------------------------------------------------------------
    alt_patterns = [
        "***-IPG",           # all physical gas products
        "*** D1-IPG",        # with space before D1
        "***IPG",            # no leading dash
    ]
    for pattern in alt_patterns:
        print(f"\n{'=' * 80}")
        print(f"TRYING: get_autolist('{pattern}')")
        print("=" * 80)
        try:
            results = ice.get_autolist(pattern)
            if results:
                print(f"Found {len(results)} results:")
                for symbol in sorted(results):
                    print(f"  {symbol}")
            else:
                print("  (no results)")
        except Exception as exc:
            print(f"  Failed: {exc}")

    # -----------------------------------------------------------------------
    # Method 3: probe specific candidate symbols with get_timeseries
    # ICE codes are 3-char. Try known patterns for each missing hub.
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 80}")
    print("METHOD 3: Probe candidate symbols with get_timeseries")
    print("=" * 80)

    # Candidate codes based on ICE naming conventions
    candidates = {
        "Columbia TCO Pool":    ["XHO", "YB7", "X5T", "XCT", "YCT", "XTC"],
        "Northern Ventura":     ["XHI", "YNV", "XNV", "XVN", "YVN", "XNR"],
        "Tetco M2":             ["XUI", "XTM", "YTM", "XM2", "XZQ", "YZR"],
        "Transco Z5 non-WGL":  ["Z2Y", "XZ5", "YZ5", "X5N", "Y5N", "XTZ"],
        "Tennessee Z4":         ["YBJ", "XTN", "YTN", "XT4", "YT4", "XTF"],
        "Transco Leidy":        ["YFI", "Y66", "XLD", "YLD", "XLY", "YLY"],
        "Chicago CityGate":     ["XFF", "XCG", "YCG", "XCH", "YCH", "XDG"],
    }

    from datetime import datetime, timedelta

    end = datetime.now()
    start = end - timedelta(days=7)

    for hub, codes in candidates.items():
        print(f"\n--- {hub} ---")
        for code in codes:
            symbol = f"{code} D1-IPG"
            try:
                data = ice.get_timeseries(
                    symbol,
                    "VWAP Close",
                    granularity="D",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )
                if data and len(data) > 1:
                    print(f"  FOUND: {symbol}  ({len(data) - 1} rows)")
                else:
                    print(f"         {symbol}  (no data)")
            except Exception as exc:
                print(f"         {symbol}  (error: {exc})")


if __name__ == "__main__":
    main()
