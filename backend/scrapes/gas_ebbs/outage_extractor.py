"""
Structured outage extraction from notice subjects and detail text.

Pure-function regex module (same pattern as notice_classifier.py).
Extracts capacity, date ranges, locations, and receipt/delivery points
from notice text to populate planned_outages records.
"""

import re
from datetime import datetime
from typing import Optional


# ── Junk detection ────────────────────────────────────────────────────────
# Many detail_text values are empty templates scraped from pages that
# didn't load properly. We skip these to avoid false-positive matches.

_JUNK_MARKERS = [
    "Notice Detail PDF TEXT Return To Results",  # Empty NAESB template (299 records)
    "Columbia Pipeline Group ANR ANRSC BISON BLGSC",  # TC Energy nav bar (231 records)
    "PipeRiv is not responsible for any incorrect information",  # PipeRiv disclaimer (325 records)
    "1Line Portal",  # Williams/Transco portal boilerplate (13 records)
    "1Line relies upon customer input",  # Williams portal variant
]


def _is_junk(text: str) -> bool:
    """Return True if text is a known junk/template page."""
    for marker in _JUNK_MARKERS:
        if marker in text:
            return True
    return False


# ── Capacity patterns ──────────────────────────────────────────────────────

# Pattern 1: Standard unit suffixed values (e.g. "1,151,000 Dth/d")
_CAPACITY_RE = re.compile(
    r"(\d[\d,]*\.?\d*)\s*"
    r"(MMcf/?d(?:ay)?|MDth/?d(?:ay)?|Bcf/?d(?:ay)?|Dth/?d(?:ay)?|"
    r"MMBtu/?d(?:ay)?|Mcf/?d(?:ay)?|dekatherms?(?:\s*(?:per|/)\s*day)?|DTH)",
    re.IGNORECASE,
)

# Pattern 2: "reduced from X Dth/d to Y Dth/d" -- capture the reduced (to) value
_REDUCED_CAPACITY_RE = re.compile(
    r"reduced\s+(?:from\s+(?:approximately\s+)?[\d,]+\s*\w+/?(?:d(?:ay)?)?\s+)?to\s+(?:approximately\s+)?(\d[\d,]*\.?\d*)\s*"
    r"(Dth/?d(?:ay)?|MMcf/?d(?:ay)?|Bcf/?d(?:ay)?|MMBtu/?d(?:ay)?|Mcf/?d(?:ay)?)",
    re.IGNORECASE,
)

# Pattern 3: "Operational Capacity (Dth)" followed by number values
# e.g. "Reduced Operational Capacity (Dth) Carlton South 811 Receipt 306,926 260,000"
# Require at least a 4-digit number to skip allocation group numbers like "811"
_OP_CAPACITY_RE = re.compile(
    r"(?:Reduced\s+)?Operational\s+Capacity\s*\(Dth\).*?(\d{1,3},\d{3}[\d,]*|\d{4,})",
    re.IGNORECASE,
)

# Pattern 4: "capacity thru ... will be reduced from X to Y"
_CAPACITY_THRU_RE = re.compile(
    r"capacity\s+(?:thru|through)\s+.*?reduced\s+from\s+(?:approximately\s+)?(\d[\d,]*\.?\d*)\s*(Dth/?d(?:ay)?|MMcf/?d(?:ay)?)\s+to\s+(?:approximately\s+)?(\d[\d,]*\.?\d*)\s*(Dth/?d(?:ay)?|MMcf/?d(?:ay)?)",
    re.IGNORECASE,
)

# Conversion factors to Bcf/d
_UNIT_TO_BCFD = {
    "bcf/d": 1.0,
    "bcfd": 1.0,
    "bcf/day": 1.0,
    "bcfday": 1.0,
    "mmcf/d": 0.001,
    "mmcfd": 0.001,
    "mmcf/day": 0.001,
    "mmcfday": 0.001,
    "mdth/d": 0.001,
    "mdthd": 0.001,
    "mdth/day": 0.001,
    "mdthday": 0.001,
    "mcf/d": 0.000001,
    "mcfd": 0.000001,
    "mcf/day": 0.000001,
    "mcfday": 0.000001,
    "mmdth/d": 1.0,
    "dth/d": 0.000001,
    "dthd": 0.000001,
    "dth/day": 0.000001,
    "dthday": 0.000001,
    "dth": 0.000001,  # bare Dth (per-day implied in context)
    "mmbtu/d": 0.000001,
    "mmbtud": 0.000001,
    "mmbtu/day": 0.000001,
    "mmbtuday": 0.000001,
    "dekatherms": 0.000001,
    "dekatherm": 0.000001,
    "dekatherms per day": 0.000001,
    "dekatherm per day": 0.000001,
    "dekatherms/day": 0.000001,
}


def _normalize_to_bcfd(value: float, unit: str) -> float:
    """Convert a capacity value to Bcf/d."""
    key = unit.lower().strip()
    # Try exact match first
    factor = _UNIT_TO_BCFD.get(key)
    if factor is None:
        # Strip slashes and try again
        stripped = key.replace("/", "").replace(" ", "")
        factor = _UNIT_TO_BCFD.get(stripped, 0.000001)
    return round(value * factor, 6)


# ── Date patterns ──────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%m-%d-%y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%b %d %Y",
    "%Y-%m-%d",
    "%Y%m%d",
]

# MM/DD/YYYY or MM-DD-YYYY date range with separator
_DATE_RANGE_RE = re.compile(
    r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*[-\u2013\u2014]+\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# "through" spelled out between dates: "01/15/2026 through 03/31/2026"
_DATE_RANGE_THROUGH_RE = re.compile(
    r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+through\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# Named date ranges (e.g. "March 15, 2026 through April 1, 2026")
_NAMED_DATE_RANGE_RE = re.compile(
    r"(\w+\s+\d{1,2},?\s*\d{4})\s*[-\u2013\u2014]+\s*(\w+\s+\d{1,2},?\s*\d{4})",
    re.IGNORECASE,
)

# Named date range with "through": "February 18, 2026, through Friday, February 20, 2026"
_NAMED_THROUGH_RE = re.compile(
    r"(\w+\s+\d{1,2},?\s*\d{4}),?\s*(?:through|thru)\s+(?:\w+,?\s+)?(\w+\s+\d{1,2},?\s*\d{4})",
    re.IGNORECASE,
)

# "Month YYYY through Month YYYY" (month-level ranges)
_MONTH_RANGE_RE = re.compile(
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\s+through\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    re.IGNORECASE,
)

# Effective/starting date keywords
_EFFECTIVE_DATE_RE = re.compile(
    r"(?:effective|beginning|starting|start(?:ing)?\s*date|eff)\s*[:\s,]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# End date keywords
_END_DATE_RE = re.compile(
    r"(?:end(?:ing)?|expir(?:es|ation)|conclud(?:es|ing)|through)\s*[:\s,]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# "for Gas Day Month DD, YYYY" (very common in body text)
_GAS_DAY_NAMED_RE = re.compile(
    r"(?:for\s+)?Gas\s+Day(?:s?)?\s+(?:\w+,?\s+)?(\w+\s+\d{1,2},?\s*\d{4})",
    re.IGNORECASE,
)

# "for Gas Day(s): MM/DD/YYYY - MM/DD/YYYY" (NNG structured format)
_GAS_DAYS_RANGE_RE = re.compile(
    r"For\s+Gas\s+Day\(?s?\)?[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*[-\u2013\u2014]\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# "Conditions for M/D/YYYY" or "Conditions for MM/DD/YYYY" in subject
_CONDITIONS_FOR_DATE_RE = re.compile(
    r"[Cc]onditions\s+for\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
)

# "RESTRICTIONS FOR MM-DD-YYYY" in subject
_RESTRICTIONS_FOR_DATE_RE = re.compile(
    r"RESTRICTIONS\s+FOR\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# "for gas day MM/DD/YYYY" in subject
_FOR_GAS_DAY_DATE_RE = re.compile(
    r"for\s+gas\s+day\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# "OUTAGE IMPACT REPORT - M-D-YY" in subject
_OUTAGE_REPORT_DATE_RE = re.compile(
    r"OUTAGE\s+IMPACT\s+REPORT\s*[-\u2013\u2014]+\s*(\d{1,2}-\d{1,2}-\d{2,4})",
    re.IGNORECASE,
)

# Structured metadata date: "Notice Effective Date: MM/DD/YYYY" (Enbridge format)
_NOTICE_EFF_DATE_RE = re.compile(
    r"Notice\s+Effective\s+Date(?:/Time)?[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

_NOTICE_END_DATE_RE = re.compile(
    r"Notice\s+End\s+Date(?:/Time)?[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.IGNORECASE,
)

# Structured metadata date ISO: "Notice Effective Date/Time: 2026-02-20 09:41:11.0"
_NOTICE_EFF_DATE_ISO_RE = re.compile(
    r"Notice\s+Effective\s+Date/Time[:\s]+(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)

_NOTICE_END_DATE_ISO_RE = re.compile(
    r"Notice\s+End\s+Date/Time[:\s]+(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)

# YYYYMMDD in subject (e.g. "Critical, Force Majeure, 20260313, ...")
_YYYYMMDD_RE = re.compile(r"\b(20\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))\b")

# Enbridge TSP Info Post: "Critical notice MM/DD/YYYY HH:MM:SS AM/PM MM/DD/YYYY HH:MM:SS AM/PM"
# Captures the effective date (first) and end date (second)
_ENBRIDGE_CRITICAL_RE = re.compile(
    r"Critical\s+notice\s+(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}:\d{2}\s+[AP]M\s+(\d{2}/\d{2}/\d{4})",
    re.IGNORECASE,
)

# "effective ... Month DD, YYYY" in body text
_EFFECTIVE_NAMED_DATE_RE = re.compile(
    r"effective\s+.{0,60}?(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*\d{4}",
    re.IGNORECASE,
)
# Capture just the date portion from the above match
_EFFECTIVE_NAMED_DATE_CAPTURE_RE = re.compile(
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*\d{4})",
    re.IGNORECASE,
)

# "for M/D/YYYY" or "for MM/DD/YYYY" (short subject pattern: "EFF 3/17")
_FOR_SHORT_DATE_RE = re.compile(
    r"(?:for|EFF)\s+(\d{1,2}/\d{1,2}(?:/\d{2,4})?)",
    re.IGNORECASE,
)


def _parse_date(text: str) -> Optional[str]:
    """Try to parse a date string into YYYY-MM-DD format."""
    text = text.strip().rstrip(".,;:")
    # Remove day names that may precede the date
    text = re.sub(r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
                  "", text, flags=re.IGNORECASE)
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_month_to_first(text: str) -> Optional[str]:
    """Parse 'Month YYYY' to YYYY-MM-01."""
    text = text.strip()
    for fmt in ["%B %Y", "%b %Y"]:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-01")
        except ValueError:
            continue
    return None


# ── Location patterns ──────────────────────────────────────────────────────

_LOCATION_PATTERNS = [
    # Named compressor stations: "Alexander City Compressor Station", "Vidor Compressor Station"
    # Match 1-4 capitalized words before "Compressor Station"
    re.compile(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\s+Compressor\s+Station"),
    # Numbered stations: "Station 60", "Station 115"
    re.compile(r"Station\s+\d+\w*", re.IGNORECASE),
    # Named segments: "Willow Run Segment", "Belle River Segment"
    re.compile(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\s+Segment(?:'s)?"),
    # Numbered segments: "Segment 170", "Seg 170"
    re.compile(r"[Ss]eg(?:ment)?\s*\d+", re.IGNORECASE),
    # Allocation groups: "Carlton South (Group 811)", "Field to Demarc (Group 831)"
    re.compile(r"\w+(?:\s+\w+)*\s*\(Group\s+\d+\)", re.IGNORECASE),
    # Mile markers
    re.compile(r"[Mm]ile\s*(?:[Pp]ost|[Mm]arker)\s*\d+(?:\.\d+)?"),
    # Named laterals: "Southeast Louisiana Lateral A"
    re.compile(r"(?:\w+(?:\s+\w+)*)\s+[Ll]ateral\s+\w+"),
    # Zones: "Zone M2-24", "Market Area Zones M2-24, M2-30 and M3"
    re.compile(r"(?:Zone|Area|Region)\s+[\w-]+(?:\s*,\s*[\w-]+)*", re.IGNORECASE),
    # Meter point IDs: "MR40832 - WHISTLER PIPELINE", "45813 - NGPL"
    re.compile(r"MR?\d{4,6}\s*[-\u2013]\s*[\w\s]+(?=[\.,;)]|$)", re.IGNORECASE),
    # POI references: "POI 61667", "(POI 58676)"
    re.compile(r"POI\s+\d+", re.IGNORECASE),
    # Named locations with state: "near Willow River, Minnesota"
    re.compile(
        r"(?:near|in|at|between)\s+[\w\s]+,\s*(?:Alabama|Alaska|Arizona|Arkansas|"
        r"California|Colorado|Connecticut|Delaware|Florida|Georgia|Idaho|Illinois|"
        r"Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|"
        r"Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|"
        r"New\s+Hampshire|New\s+Jersey|New\s+Mexico|New\s+York|North\s+Carolina|"
        r"North\s+Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode\s+Island|"
        r"South\s+Carolina|South\s+Dakota|Tennessee|Texas|Utah|Vermont|Virginia|"
        r"Washington|West\s+Virginia|Wisconsin|Wyoming)",
        re.IGNORECASE,
    ),
    # "points north/south of X"
    re.compile(r"points\s+(?:north|south|east|west)\s+of\s+\w+", re.IGNORECASE),
    # Pipeline/loop names in subjects: "Caldwell Loop", "Mainline C"
    re.compile(r"(?:Mainline|Loop)\s+[A-Z]\b", re.IGNORECASE),
]

# Location from structured "Location:" field in NNG-style text
_LOCATION_FIELD_RE = re.compile(
    r"Location:\s*([A-Z][^\n]{2,80}?)(?:\s+Critical:|\s+Project Description:|\s+Reduced\s+|\s+Notice\s+|\s+Normal\s+|\s+Capacity\s+|\s+Direction\s+|\s*$)",
    re.IGNORECASE,
)

_RECEIPT_RE = re.compile(
    r"receipt\s*(?:point|location|nomination)s?\s*(?:from\s+)?[:\s]*([^\n;.]{3,80})",
    re.IGNORECASE,
)

_DELIVERY_RE = re.compile(
    r"deliver(?:y|ing)\s*(?:point|location)s?\s*(?:to\s+)?[:\s]*([^\n;.]{3,80})",
    re.IGNORECASE,
)

# "sourced upstream/downstream of X Compressor Station"
_UPSTREAM_RE = re.compile(
    r"(?:upstream|downstream|sourced\s+(?:upstream|downstream))\s+of\s+(?:the\s+)?([^\n;.]{3,120}?(?:Compressor\s+Station|Station\s+\w+|Segment)?)(?:\.\s|\s+for\s|\s+No\s|\s+will\s|\s*$)",
    re.IGNORECASE,
)

# "north/south of X for delivery"
_DIRECTIONAL_POINTS_RE = re.compile(
    r"(?:from|sourced\s+from)\s+points\s+(north|south|east|west)\s+of\s+(\w+)\s+for\s+delivery\s+to\s+points\s+(north|south|east|west)\s+of\s+(\w+)",
    re.IGNORECASE,
)


# ── Main extractor ─────────────────────────────────────────────────────────

def extract_outage(subject: str, detail_text: str = "") -> dict:
    """Extract structured outage data from notice subject and detail text.

    Returns dict with keys:
        capacity_value, capacity_unit, capacity_bcfd,
        gas_day_start, gas_day_end,
        location, affected_locations,
        receipt_points, delivery_points
    """
    result = {
        "capacity_value": None,
        "capacity_unit": "",
        "capacity_bcfd": None,
        "gas_day_start": "",
        "gas_day_end": "",
        "location": "",
        "affected_locations": "",
        "receipt_points": "",
        "delivery_points": "",
    }

    # Clean detail_text: skip junk templates
    clean_detail = detail_text.strip() if detail_text else ""
    if clean_detail and _is_junk(clean_detail):
        clean_detail = ""

    combined = f"{subject} {clean_detail}".strip()
    if not combined:
        return result

    # ── Capacity extraction ──
    _extract_capacity(combined, result)

    # ── Date range extraction ──
    _extract_dates(subject, clean_detail, combined, result)

    # ── Location extraction ──
    _extract_locations(subject, clean_detail, combined, result)

    # ── Receipt / delivery points ──
    _extract_receipt_delivery(combined, result)

    return result


def _extract_capacity(combined: str, result: dict) -> None:
    """Extract capacity from combined text."""
    # Strategy 1: "reduced from X to Y" pattern (captures the outage-relevant value)
    reduced_match = _REDUCED_CAPACITY_RE.search(combined)
    if reduced_match:
        raw_val = reduced_match.group(1).replace(",", "")
        try:
            value = float(raw_val)
            unit = reduced_match.group(2)
            result["capacity_value"] = value
            result["capacity_unit"] = unit
            result["capacity_bcfd"] = _normalize_to_bcfd(value, unit)
            return
        except (ValueError, OverflowError):
            pass

    # Strategy 2: "capacity thru ... reduced from X to Y" pattern
    thru_match = _CAPACITY_THRU_RE.search(combined)
    if thru_match:
        raw_val = thru_match.group(3).replace(",", "")  # the "to" value
        try:
            value = float(raw_val)
            unit = thru_match.group(4)
            result["capacity_value"] = value
            result["capacity_unit"] = unit
            result["capacity_bcfd"] = _normalize_to_bcfd(value, unit)
            return
        except (ValueError, OverflowError):
            pass

    # Strategy 3: Standard capacity with unit
    cap_match = _CAPACITY_RE.search(combined)
    if cap_match:
        raw_val = cap_match.group(1).replace(",", "")
        try:
            value = float(raw_val)
            unit = cap_match.group(2)
            # Skip very small threshold values (e.g. "500 DTH" penalty thresholds)
            # unless it's the only match
            if value < 1000 and unit.upper() in ("DTH", "DTH/D"):
                # Look for a larger value
                for m in _CAPACITY_RE.finditer(combined):
                    v = float(m.group(1).replace(",", ""))
                    if v >= 1000:
                        value = v
                        unit = m.group(2)
                        break
            result["capacity_value"] = value
            result["capacity_unit"] = unit
            result["capacity_bcfd"] = _normalize_to_bcfd(value, unit)
            return
        except (ValueError, OverflowError):
            pass

    # Strategy 4: "Operational Capacity (Dth)" header followed by numbers
    op_match = _OP_CAPACITY_RE.search(combined)
    if op_match:
        raw_val = op_match.group(1).replace(",", "")
        try:
            value = float(raw_val)
            unit = "Dth"
            result["capacity_value"] = value
            result["capacity_unit"] = unit
            result["capacity_bcfd"] = _normalize_to_bcfd(value, unit)
            return
        except (ValueError, OverflowError):
            pass


def _extract_dates(subject: str, detail: str, combined: str, result: dict) -> None:
    """Extract gas day start/end dates from subject and detail text."""

    # ── Priority 1: Subject line dates (most reliable) ──

    # "Conditions for M/D/YYYY" or "Conditions for MM/DD/YYYY"
    cond_match = _CONDITIONS_FOR_DATE_RE.search(subject)
    if cond_match:
        parsed = _parse_date(cond_match.group(1))
        if parsed:
            result["gas_day_start"] = parsed
            result["gas_day_end"] = parsed
            return

    # "RESTRICTIONS FOR MM-DD-YYYY"
    restr_match = _RESTRICTIONS_FOR_DATE_RE.search(subject)
    if restr_match:
        parsed = _parse_date(restr_match.group(1))
        if parsed:
            result["gas_day_start"] = parsed
            result["gas_day_end"] = parsed
            return

    # "for gas day MM/DD/YYYY"
    fgd_match = _FOR_GAS_DAY_DATE_RE.search(subject)
    if fgd_match:
        parsed = _parse_date(fgd_match.group(1))
        if parsed:
            result["gas_day_start"] = parsed
            result["gas_day_end"] = parsed
            return

    # "OUTAGE IMPACT REPORT - M-D-YY"
    oir_match = _OUTAGE_REPORT_DATE_RE.search(subject)
    if oir_match:
        parsed = _parse_date(oir_match.group(1))
        if parsed:
            result["gas_day_start"] = parsed
            result["gas_day_end"] = parsed
            return

    # "EFF 3/17" or short date in subject -- only use if year context available
    eff_short_match = _FOR_SHORT_DATE_RE.search(subject)
    if eff_short_match:
        raw = eff_short_match.group(1)
        if "/" in raw and raw.count("/") == 1:
            # Need to add year -- guess from context or current year
            pass  # Skip short dates without year to avoid ambiguity
        else:
            parsed = _parse_date(raw)
            if parsed:
                result["gas_day_start"] = parsed

    # YYYYMMDD in subject (e.g. "Critical, Force Majeure, 20260313")
    yyyymmdd_match = _YYYYMMDD_RE.search(subject)
    if yyyymmdd_match and not result["gas_day_start"]:
        parsed = _parse_date(yyyymmdd_match.group(1))
        if parsed:
            result["gas_day_start"] = parsed

    # ── Priority 2: Structured header dates in detail text ──

    if not result["gas_day_start"] and detail:
        # "For Gas Day(s): MM/DD/YYYY - MM/DD/YYYY" (NNG format)
        gas_days_match = _GAS_DAYS_RANGE_RE.search(detail)
        if gas_days_match:
            start = _parse_date(gas_days_match.group(1))
            end = _parse_date(gas_days_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    # Enbridge TSP Info Post: "Critical notice MM/DD/YYYY HH:MM:SS AM/PM MM/DD/YYYY"
    if not result["gas_day_start"] and detail:
        enbridge_match = _ENBRIDGE_CRITICAL_RE.search(detail)
        if enbridge_match:
            start = _parse_date(enbridge_match.group(1))
            end = _parse_date(enbridge_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    if not result["gas_day_start"] and detail:
        # "Notice Effective Date: MM/DD/YYYY" or "Notice Effective Date/Time: MM/DD/YYYY"
        ne_match = _NOTICE_EFF_DATE_RE.search(detail)
        if ne_match:
            parsed = _parse_date(ne_match.group(1))
            if parsed:
                result["gas_day_start"] = parsed

        # ISO format: "Notice Effective Date/Time: 2026-02-20 09:41:11.0"
        if not result["gas_day_start"]:
            ne_iso_match = _NOTICE_EFF_DATE_ISO_RE.search(detail)
            if ne_iso_match:
                parsed = _parse_date(ne_iso_match.group(1))
                if parsed:
                    result["gas_day_start"] = parsed

    if not result["gas_day_end"] and detail:
        ned_match = _NOTICE_END_DATE_RE.search(detail)
        if ned_match:
            parsed = _parse_date(ned_match.group(1))
            if parsed:
                result["gas_day_end"] = parsed

        if not result["gas_day_end"]:
            ned_iso_match = _NOTICE_END_DATE_ISO_RE.search(detail)
            if ned_iso_match:
                parsed = _parse_date(ned_iso_match.group(1))
                if parsed:
                    result["gas_day_end"] = parsed

    # ── Priority 3: Body text date patterns ──

    if not result["gas_day_start"]:
        # "for Gas Day March 17, 2026"
        gdn_match = _GAS_DAY_NAMED_RE.search(combined)
        if gdn_match:
            parsed = _parse_date(gdn_match.group(1))
            if parsed:
                result["gas_day_start"] = parsed
                if not result["gas_day_end"]:
                    result["gas_day_end"] = parsed

    if not result["gas_day_start"]:
        # "MM/DD/YYYY through MM/DD/YYYY"
        dt_match = _DATE_RANGE_THROUGH_RE.search(combined)
        if dt_match:
            start = _parse_date(dt_match.group(1))
            end = _parse_date(dt_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    if not result["gas_day_start"]:
        # Explicit date ranges with dash/em-dash: "01/15/2026 - 03/31/2026"
        range_match = _DATE_RANGE_RE.search(combined)
        if range_match:
            start = _parse_date(range_match.group(1))
            end = _parse_date(range_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    if not result["gas_day_start"]:
        # Named date ranges: "March 15, 2026 through April 1, 2026"
        named_thru = _NAMED_THROUGH_RE.search(combined)
        if named_thru:
            start = _parse_date(named_thru.group(1))
            end = _parse_date(named_thru.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    if not result["gas_day_start"]:
        named_match = _NAMED_DATE_RANGE_RE.search(combined)
        if named_match:
            start = _parse_date(named_match.group(1))
            end = _parse_date(named_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    # ── Priority 4: Month-level ranges ──
    if not result["gas_day_start"]:
        month_match = _MONTH_RANGE_RE.search(combined)
        if month_match:
            start = _parse_month_to_first(month_match.group(1))
            end = _parse_month_to_first(month_match.group(2))
            if start:
                result["gas_day_start"] = start
            if end:
                result["gas_day_end"] = end

    # ── Priority 5: "effective ... Month DD, YYYY" in body text ──
    if not result["gas_day_start"]:
        eff_named = _EFFECTIVE_NAMED_DATE_RE.search(combined)
        if eff_named:
            date_capture = _EFFECTIVE_NAMED_DATE_CAPTURE_RE.search(eff_named.group(0))
            if date_capture:
                parsed = _parse_date(date_capture.group(1))
                if parsed:
                    result["gas_day_start"] = parsed

    # ── Priority 6: Keyword-based effective/end dates ──
    if not result["gas_day_start"]:
        eff_match = _EFFECTIVE_DATE_RE.search(combined)
        if eff_match:
            parsed = _parse_date(eff_match.group(1))
            if parsed:
                result["gas_day_start"] = parsed

    if not result["gas_day_end"]:
        end_match = _END_DATE_RE.search(combined)
        if end_match:
            parsed = _parse_date(end_match.group(1))
            if parsed:
                result["gas_day_end"] = parsed


def _extract_locations(subject: str, detail: str, combined: str, result: dict) -> None:
    """Extract locations from text."""
    locations = []

    # Priority 1: Structured "Location:" field in NNG-style text
    if detail:
        for loc_match in _LOCATION_FIELD_RE.finditer(detail):
            loc = loc_match.group(1).strip()
            if loc and len(loc) > 3 and loc not in locations:
                locations.append(loc)

    # Priority 2: Directional point descriptions (e.g. "north of Westbrook for delivery south of Westbrook")
    dir_match = _DIRECTIONAL_POINTS_RE.search(combined)
    if dir_match:
        loc = f"points {dir_match.group(1)} of {dir_match.group(2)} to points {dir_match.group(3)} of {dir_match.group(4)}"
        if loc not in locations:
            locations.append(loc)

    # Priority 3: Upstream/downstream references
    for um in _UPSTREAM_RE.finditer(combined):
        loc = um.group(1).strip()
        if loc and len(loc) > 3 and loc not in locations:
            locations.append(loc)

    # Priority 4: Standard location patterns
    for pattern in _LOCATION_PATTERNS:
        for m in pattern.finditer(combined):
            loc = m.group(0).strip()
            # Skip overly generic or too-short matches
            if len(loc) < 4:
                continue
            # Avoid duplicates and substrings
            is_dup = False
            for existing in locations:
                if loc.lower() in existing.lower() or existing.lower() in loc.lower():
                    is_dup = True
                    break
            if not is_dup:
                locations.append(loc)

    if locations:
        result["location"] = locations[0]
        result["affected_locations"] = "; ".join(locations[:10])  # cap at 10


def _extract_receipt_delivery(combined: str, result: dict) -> None:
    """Extract receipt and delivery points."""
    receipt_match = _RECEIPT_RE.search(combined)
    if receipt_match:
        val = receipt_match.group(1).strip()
        # Clean up trailing boilerplate
        val = re.sub(r"\s*(?:No increases|Please contact|Customers are).*$", "", val,
                     flags=re.IGNORECASE)
        if val:
            result["receipt_points"] = val

    delivery_match = _DELIVERY_RE.search(combined)
    if delivery_match:
        val = delivery_match.group(1).strip()
        val = re.sub(r"\s*(?:No increases|Please contact|Customers are).*$", "", val,
                     flags=re.IGNORECASE)
        if val:
            result["delivery_points"] = val
