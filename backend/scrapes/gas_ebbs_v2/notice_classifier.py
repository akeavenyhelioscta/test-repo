"""
Notice classification engine for gas EBB critical notices.

Uses ordered regex rules adapted from the Synmax reference
(.refactor/synmax/pipeline_notices_with_analysis.py).
First matching rule wins — order matters.

Categories (6 + other):
    force_majeure      (5)  — FM declarations
    ofo                (4)  — operational flow orders, unauthorized flows
    critical_alert     (4)  — weather alerts, emergencies, incidents
    capacity_reduction (4)  — constraints, curtailments, market restrictions
    operations_advisory(3)  — line pack, system conditions, advisories
    maintenance        (3)  — outages, repairs, inspections, pigging
    other              (1)  — informational (scheduling, billing, regulatory)
"""

import re
from typing import Tuple


CLASSIFICATION_RULES: list[Tuple[str, str, int]] = [
    # (category, regex_pattern, default_severity 1-5)
    # Order matters — first match wins.

    (
        "force_majeure",
        r"force\s*majeure|fmj|fm\s*(event|declaration|notice)",
        5,
    ),
    (
        "ofo",
        (
            r"ofo|operational\s*flow\s*order|critical\s*day|overage\s*alert"
            r"|underperformance|action\s*alert|system\s*overrun"
            r"|imbalance\s*order|penalty\s*factor"
            r"|unauthorized\s*(receipt|deliver)"
            r"|hourly\s*takes\s*advisory"
        ),
        4,
    ),
    (
        "critical_alert",
        (
            r"critical|operational\s*alert|emergency|leak\s*investigation"
            r"|rupture|incident|interruption|shut.in|blow.down"
            r"|weather\s*alert|high\s*wind|winter\s*weather"
            r"|cold\s*weather|ice\s*storm"
        ),
        4,
    ),
    (
        "capacity_reduction",
        (
            r"capacity\s*(reduction|constraint)|restriction"
            r"|reduced\s*capacity|curtailment|capacity\s*posting"
            r"|pipeline\s*conditions|storage\s*conditions"
            r"|capacity\s*impact|pack\s*declaration"
            r"|market.*constraint|production.*constraint"
            r"|limited.*flexibility"
        ),
        4,
    ),
    (
        "operations_advisory",
        (
            r"operations\s*advisory|system\s*operating\s*condition"
            r"|line\s*pack|location\s*performance"
        ),
        3,
    ),
    (
        "maintenance",
        (
            r"maintenance|planned.*outage|unplanned.*outage|repair"
            r"|construction|compressor\s*(station\s*)?work|pig\s*run"
            r"|hydro\s*test|inspection|turnaround|tie.in|dig\s*program"
            r"|outage|pigging"
        ),
        3,
    ),
]


def classify(subject: str) -> Tuple[str, int]:
    """Classify a notice by its subject text.

    Returns (category, severity) tuple. Falls back to ("other", 1)
    if no rule matches.
    """
    if not subject:
        return ("other", 1)

    subject_lower = subject.lower()
    for category, pattern, severity in CLASSIFICATION_RULES:
        if re.search(pattern, subject_lower):
            return (category, severity)

    return ("other", 1)
