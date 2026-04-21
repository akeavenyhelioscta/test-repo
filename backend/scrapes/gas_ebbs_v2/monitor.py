"""
Gas EBB v2 scraper health monitor.

Queries logging.pipeline_runs to report scraper health status.

Usage:
    python monitor.py                # print health report
    python monitor.py --hours 6      # custom lookback window
    python monitor.py --failures     # show only failures
    python monitor.py --alert        # send Slack alert for DEAD/DEGRADED pipelines
"""

import logging
import sys
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend import secrets  # noqa: F401
from backend.utils import azure_postgresql_utils as azure_postgresql


def get_pipeline_health(hours: int = 24) -> list[dict]:
    """Query pipeline_runs and return health status per pipeline.

    Note: pipeline_runs logging table remains in PostgreSQL
    regardless of where outage data is written.
    """
    df = azure_postgresql.pull_from_db(f"""
        WITH recent AS (
            SELECT pipeline_name, status, created_at
            FROM logging.pipeline_runs
            WHERE source = 'gas_ebbs_v2'
              AND event_type IN ('RUN_SUCCESS', 'RUN_FAILURE')
              AND created_at >= now() - interval '{hours} hours'
        )
        SELECT
            pipeline_name,
            COUNT(*) FILTER (WHERE status = 'success') as successes,
            COUNT(*) FILTER (WHERE status = 'failure') as failures,
            MAX(created_at) FILTER (WHERE status = 'success') as last_success,
            MAX(created_at) FILTER (WHERE status = 'failure') as last_failure
        FROM recent
        GROUP BY pipeline_name
        ORDER BY pipeline_name
    """)

    results = []
    for _, row in df.iterrows():
        s = int(row.successes)
        f = int(row.failures)
        if f > 0 and s == 0:
            status = "DEAD"
        elif f > s:
            status = "DEGRADED"
        elif f > 0:
            status = "FLAKY"
        else:
            status = "HEALTHY"
        results.append({
            "pipeline": row.pipeline_name,
            "successes": s,
            "failures": f,
            "status": status,
            "last_success": row.last_success,
            "last_failure": row.last_failure,
        })
    return results


def print_health_report(hours: int = 24, failures_only: bool = False):
    """Print a formatted health report."""
    results = get_pipeline_health(hours)

    if failures_only:
        results = [r for r in results if r["status"] != "HEALTHY"]

    dead = [r for r in results if r["status"] == "DEAD"]
    degraded = [r for r in results if r["status"] == "DEGRADED"]
    flaky = [r for r in results if r["status"] == "FLAKY"]
    healthy = [r for r in results if r["status"] == "HEALTHY"]

    print(f"\n=== Gas EBB v2 Scraper Health (last {hours}h) ===\n")
    print(f"  {len(healthy)} healthy, {len(flaky)} flaky, "
          f"{len(degraded)} degraded, {len(dead)} dead "
          f"(of {len(results)} total)\n")

    for label, group in [("DEAD", dead), ("DEGRADED", degraded), ("FLAKY", flaky)]:
        if not group:
            continue
        print(f"  --- {label} ---")
        for r in group:
            print(f"    {r['pipeline']:45s} ok={r['successes']:3d} fail={r['failures']:3d}")
        print()

    if not failures_only and healthy:
        print(f"  --- HEALTHY ({len(healthy)}) ---")
        for r in healthy:
            print(f"    {r['pipeline']:45s} ok={r['successes']:3d}")
        print()


def send_alert(hours: int = 24) -> bool:
    """Send a Slack alert if any pipelines are DEAD or DEGRADED."""
    webhook_url = getattr(secrets, "SLACK_DEFAULT_WEBHOOK_URL", None)
    if not webhook_url:
        logging.warning("SLACK_DEFAULT_WEBHOOK_URL not set — skipping alert")
        return False

    results = get_pipeline_health(hours)
    dead = [r for r in results if r["status"] == "DEAD"]
    degraded = [r for r in results if r["status"] == "DEGRADED"]

    if not dead and not degraded:
        return False

    lines = [f"*Gas EBB v2 Scraper Alert* (last {hours}h)"]
    if dead:
        lines.append(f"\n:red_circle: *DEAD ({len(dead)})*")
        for r in dead:
            lines.append(f"  `{r['pipeline']}` — 0 successes, {r['failures']} failures")
    if degraded:
        lines.append(f"\n:large_orange_circle: *DEGRADED ({len(degraded)})*")
        for r in degraded:
            lines.append(
                f"  `{r['pipeline']}` — {r['successes']} ok, {r['failures']} fail"
            )

    payload = {"text": "\n".join(lines)}
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"Alert sent ({len(dead)} dead, {len(degraded)} degraded)")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Slack alert: {e}")
        return False


def main():
    hours = 24
    failures_only = False
    alert = False

    args = sys.argv[1:]
    if "--hours" in args:
        idx = args.index("--hours")
        hours = int(args[idx + 1])
    if "--failures" in args:
        failures_only = True
    if "--alert" in args:
        alert = True

    print_health_report(hours=hours, failures_only=failures_only)

    if alert:
        send_alert(hours=hours)


if __name__ == "__main__":
    main()
