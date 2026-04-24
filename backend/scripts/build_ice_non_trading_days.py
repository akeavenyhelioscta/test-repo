"""Build the ICE U.S. physical next-day gas non-trading days seed.

One-shot. Downloads the current ICE calendar PDF once for audit, then
generates non-trading days across a year range via the ``holidays`` library
+ Good Friday + Day After Thanksgiving (per ICE's calendar).

Cross-reference: https://www.ice.com/publicdocs/support/phys_gas_calendar.pdf

Usage:
    python backend/scripts/build_ice_non_trading_days.py
    python backend/scripts/build_ice_non_trading_days.py --start-year 2020 --end-year 2030

Outputs:
    backend/dbt/dbt_azure_postgresql/seeds/ice_us_physical_gas_non_trading_days.csv
    backend/scripts/_ice_phys_gas_calendar.pdf   (archived PDF, for audit)
"""
from __future__ import annotations

import argparse
import csv
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import holidays

ICE_CALENDAR_URL = "https://www.ice.com/publicdocs/support/phys_gas_calendar.pdf"

# Subset of US federal holidays that ICE observes as non-trading days for the
# physical next-day gas market. Verified against ICE's 2026 calendar PDF.
ICE_OBSERVED_FEDERAL = {
    "New Year's Day",
    "Martin Luther King Jr. Day",
    "Washington's Birthday",          # aka Presidents Day
    "Memorial Day",
    "Juneteenth National Independence Day",
    "Independence Day",
    "Labor Day",
    "Columbus Day",
    "Veterans Day",
    "Thanksgiving Day",
    "Christmas Day",
}


def _easter_sunday(year: int) -> date:
    """Meeus/Jones/Butcher Gregorian Easter algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def day_after_thanksgiving(year: int) -> date:
    nov1 = date(year, 11, 1)
    # November 1 to first Thursday (weekday() Thu = 3).
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thu + timedelta(days=21)  # fourth Thursday
    return thanksgiving + timedelta(days=1)


def non_trading_days(year: int) -> list[tuple[date, str]]:
    us = holidays.US(years=year, observed=True)
    rows: list[tuple[date, str]] = []
    for d, name in sorted(us.items()):
        clean_name = name.replace(" (observed)", "")
        if clean_name in ICE_OBSERVED_FEDERAL:
            rows.append((d, clean_name))
    rows.append((good_friday(year), "Good Friday"))
    rows.append((day_after_thanksgiving(year), "Day After Thanksgiving"))

    # Collapse accidental duplicates (e.g., Juneteenth observed on adjacent day).
    seen: dict[date, str] = {}
    for d, name in sorted(rows):
        seen.setdefault(d, name)
    return sorted(seen.items())


def archive_pdf(dest: Path) -> None:
    # ICE returns 403 for the default urllib UA — identify as a browser.
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(
        ICE_CALENDAR_URL,
        headers={"User-Agent": "Mozilla/5.0 (build_ice_non_trading_days.py)"},
    )
    with urllib.request.urlopen(req) as resp, dest.open("wb") as out:
        out.write(resp.read())


def write_seed(rows: list[tuple[date, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "name"])
        for d, name in rows:
            writer.writerow([d.isoformat(), name])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2030)
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "dbt"
        / "dbt_azure_postgresql"
        / "seeds"
        / "ice_us_physical_gas_non_trading_days.csv",
    )
    parser.add_argument(
        "--pdf-path",
        type=Path,
        default=Path(__file__).resolve().parent / "_ice_phys_gas_calendar.pdf",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip re-downloading the ICE PDF (useful when offline).",
    )
    args = parser.parse_args()

    if not args.skip_download:
        print(f"Downloading {ICE_CALENDAR_URL} -> {args.pdf_path}")
        archive_pdf(args.pdf_path)

    rows: list[tuple[date, str]] = []
    for year in range(args.start_year, args.end_year + 1):
        rows.extend(non_trading_days(year))

    write_seed(rows, args.seed_path)
    print(f"Wrote {len(rows)} non-trading days ({args.start_year}-{args.end_year}) -> {args.seed_path}")


if __name__ == "__main__":
    main()
