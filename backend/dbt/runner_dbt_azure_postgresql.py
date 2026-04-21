"""
Runner for dbt on Azure PostgreSQL.

Executes ``dbt run`` with:
- PostgreSQL advisory lock to prevent concurrent runs
- Structured logging via PipelineRunLogger
- Configurable timeout with process-tree kill
- Retry/backoff for retryable failures
- Optional --select pass-through and --dry-run mode

Usage:
    python runner_dbt_azure_postgresql.py                              # full dbt run
    python runner_dbt_azure_postgresql.py --select tag:pjm             # selective run
    python runner_dbt_azure_postgresql.py --dry-run                    # log command without executing
    python runner_dbt_azure_postgresql.py --timeout 600                # custom timeout (seconds)
    python runner_dbt_azure_postgresql.py --max-attempts 5             # retry budget
"""

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils import (  # noqa: E402
    logging_utils,
    pipeline_run_logger,
)
import dbt_utils  # noqa: E402  (colocated in same directory)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PIPELINE_NAME = "dbt_run"
LOGGING_SOURCE = "dbt"
LOGGING_TARGET_TABLE = "dbt.*"
LOGGING_OPERATION_TYPE = "consume"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "dbt,azure_postgresql"

DEFAULT_TIMEOUT_SECONDS = 1800  # 30 minutes
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 30


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dbt with advisory lock protection")
    parser.add_argument(
        "--select",
        type=str,
        default=None,
        help="dbt --select argument (e.g. 'tag:pjm')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the dbt command without executing it",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Timeout in seconds (default {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=f"Total dbt attempts for retryable failures (default {DEFAULT_MAX_ATTEMPTS})",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=int,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        help=(
            "Backoff multiplier between retries in seconds "
            f"(default {DEFAULT_RETRY_BACKOFF_SECONDS})"
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    logger = logging_utils.init_logging(
        name=PIPELINE_NAME,
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )

    pipeline_name = PIPELINE_NAME
    if args.select:
        pipeline_name = f"{PIPELINE_NAME}_{args.select.replace(':', '_')}"

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=pipeline_name,
        source=LOGGING_SOURCE,
        priority=LOGGING_PRIORITY,
        tags=LOGGING_TAGS,
        log_file_path=logger.log_file_path,
        target_table=LOGGING_TARGET_TABLE,
        operation_type=LOGGING_OPERATION_TYPE,
    )

    conn = None
    try:
        conn = dbt_utils.get_pg_connection()
        lock_acquired = dbt_utils.acquire_advisory_lock(conn)

        if not lock_acquired:
            # Check how long the lock holder has been running — surface stale locks
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT pid, state, query_start, now() - query_start AS duration
                        FROM pg_stat_activity
                        WHERE pid IN (
                            SELECT pid FROM pg_locks
                            WHERE locktype = 'advisory'
                            AND objid = hashtext('dbt_run')
                            AND granted = true
                        )
                    """)
                    holders = cur.fetchall()
                    if holders:
                        for pid, state, query_start, duration in holders:
                            logger.warning(
                                "Lock held by PID %s (state=%s, since=%s, duration=%s)",
                                pid, state, query_start, duration,
                            )
            except Exception as exc:
                logger.warning("Could not inspect lock holder: %s", exc)

            logger.info("Advisory lock held by another session; skipping this run")
            run.start()
            run.log_warning("RUN_SKIPPED: advisory lock held by another session")
            return

        run.start()
        logger.info(
            f"Starting dbt run "
            f"(select={args.select or 'all'}, timeout={args.timeout}s, "
            f"dry_run={args.dry_run}, max_attempts={args.max_attempts}, "
            f"retry_backoff_seconds={args.retry_backoff_seconds})"
        )

        result = dbt_utils.run_dbt(
            select=args.select,
            timeout_seconds=args.timeout,
            dry_run=args.dry_run,
            max_attempts=args.max_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
        )

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                logger.info(f"[dbt] {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                logger.warning(f"[dbt stderr] {line}")

        if result.exit_code == 0:
            logger.info(f"dbt run completed successfully in {result.attempts} attempt(s)")
            metadata = {
                "dbt_exit_code": result.exit_code,
                "select": args.select,
                "attempts": result.attempts,
            }
            if result.model_summary:
                metadata["models_pass"] = result.model_summary.get("pass", 0)
                metadata["models_error"] = result.model_summary.get("error", 0)
                metadata["models_warn"] = result.model_summary.get("warn", 0)
                metadata["models_skip"] = result.model_summary.get("skip", 0)
                if result.model_summary.get("errors"):
                    metadata["model_errors"] = result.model_summary["errors"]

            if result.model_summary.get("error", 0) > 0:
                error = RuntimeError(
                    f"dbt exited 0 but {result.model_summary['error']} model(s) errored: "
                    f"{[e['model'] for e in result.model_summary.get('errors', [])]}"
                )
                logger.error(str(error))
                run.failure(error=error, metadata=metadata)
                sys.exit(1)

            run.success(metadata=metadata)

        elif result.timed_out or result.exit_code == -1:
            error = TimeoutError(
                f"dbt run timed out after {args.timeout}s "
                f"(attempts={result.attempts}/{args.max_attempts})"
            )
            logger.error(str(error))
            run.failure(
                error=error,
                metadata={
                    "event": "RUN_TIMEOUT",
                    "timeout_seconds": args.timeout,
                    "attempts": result.attempts,
                    "max_attempts": args.max_attempts,
                    "select": args.select,
                },
            )
            sys.exit(1)

        else:
            error = RuntimeError(
                f"dbt run failed with exit code {result.exit_code} "
                f"after {result.attempts} attempt(s). "
                f"stderr: {result.stderr[:2000] if result.stderr else '(empty)'}"
            )
            logger.error(str(error))
            run.failure(
                error=error,
                metadata={
                    "dbt_exit_code": result.exit_code,
                    "select": args.select,
                    "attempts": result.attempts,
                },
            )
            sys.exit(1)

    except Exception as exc:
        logger.error(f"Unexpected error: {exc}")
        run.failure(error=exc)
        sys.exit(1)

    finally:
        if conn:
            try:
                dbt_utils.release_advisory_lock(conn)
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
