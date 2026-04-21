"""
Shared utilities for gas EBB scrapers.
"""

import re
from datetime import datetime
from typing import Optional


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def clean_text(element) -> str:
    """Extract text from a BeautifulSoup element, collapsing whitespace."""
    text = element.get_text(separator=" ", strip=True)
    return " ".join(text.split())


def parse_datetime_str(
    dt_str: str,
    formats: list[str],
) -> Optional[str]:
    """Try parsing a datetime string with multiple formats.

    Returns an ISO-8601 string on success, or the original string if
    no format matches (so the raw value is never lost).
    """
    if not dt_str or not dt_str.strip():
        return ""

    dt_str = dt_str.strip()

    for fmt in formats:
        try:
            parsed = datetime.strptime(dt_str, fmt)
            return parsed.isoformat()
        except ValueError:
            continue

    # Fallback: return original string so downstream can still use it
    return dt_str


def extract_numeric_id(text: str) -> Optional[str]:
    """Extract a numeric notice identifier from text, or return None."""
    match = re.search(r"\d+", text.strip())
    return match.group(0) if match else None
