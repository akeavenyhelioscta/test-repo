"""
Utilities for running dbt with advisory lock protection and subprocess management.

Provides:
- PostgreSQL advisory lock acquire/release (crash-safe, auto-released on disconnect)
- dbt subprocess execution with timeout and Windows process-tree kill
"""

from dataclasses import dataclass, field
import json
import logging
import os
import platform
import shutil
import subprocess
import tempfile
import time

import psycopg2

from backend import secrets


logger = logging.getLogger(__name__)

# Advisory lock key — consistent hash so any caller uses the same lock
ADVISORY_LOCK_KEY = "dbt_run"

# Default dbt project path (relative to repo root)
DBT_PROJECT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "dbt", "dbt_azure_postgresql"
)

RETRYABLE_ERROR_PATTERNS = (
    "could not connect to server",
    "connection refused",
    "connection reset by peer",
    "connection timed out",
    "server closed the connection unexpectedly",
    "timeout expired",
    "temporary failure in name resolution",
    "could not translate host name",
    "too many connections",
    "terminating connection due to administrator command",
    "deadlock detected",
    "could not serialize access due to concurrent update",
)


@dataclass
class DbtRunResult:
    exit_code: int
    stdout: str
    stderr: str
    attempts: int
    timed_out: bool = False
    model_summary: dict = field(default_factory=dict)  # {"pass": N, "error": N, "warn": N, "skip": N, "errors": [...]}


def _parse_run_results(project_dir: str) -> dict:
    """Parse dbt run_results.json to extract per-model pass/fail summary."""
    results_path = os.path.join(project_dir, "target", "run_results.json")
    try:
        with open(results_path, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not parse run_results.json: %s", exc)
        return {}

    results = data.get("results", [])
    summary = {"pass": 0, "error": 0, "warn": 0, "skip": 0, "errors": []}
    for r in results:
        status = r.get("status", "unknown")
        if status == "success":
            summary["pass"] += 1
        elif status == "error":
            summary["error"] += 1
            summary["errors"].append({
                "model": r.get("unique_id", "unknown"),
                "message": r.get("message", "")[:500],
            })
        elif status == "skipped":
            summary["skip"] += 1
        else:
            summary["warn"] += 1
    return summary


# ---------------------------------------------------------------------------
# Advisory lock
# ---------------------------------------------------------------------------

def get_pg_connection(
    max_attempts: int = 3,
    retry_backoff_seconds: int = 5,
) -> psycopg2.extensions.connection:
    """Open a connection to Azure PostgreSQL for advisory lock management."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                host=secrets.AZURE_POSTGRESQL_DB_HOST,
                user=secrets.AZURE_POSTGRESQL_DB_USER,
                password=secrets.AZURE_POSTGRESQL_DB_PASSWORD,
                port=secrets.AZURE_POSTGRESQL_DB_PORT,
                dbname="helioscta",
                connect_timeout=10,
            )
            conn.autocommit = True
            return conn
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break

            wait_seconds = retry_backoff_seconds * attempt
            logger.warning(
                "PostgreSQL connection attempt %s/%s failed: %s. Retrying in %ss",
                attempt,
                max_attempts,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise last_error


def acquire_advisory_lock(conn: psycopg2.extensions.connection) -> bool:
    """Try to acquire a PostgreSQL session-level advisory lock.

    Returns True if acquired, False if another session holds it.
    The lock is automatically released when the connection closes (crash-safe).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))", (ADVISORY_LOCK_KEY,)
        )
        result = cur.fetchone()
        return result[0] if result else False


def release_advisory_lock(conn: psycopg2.extensions.connection) -> None:
    """Explicitly release the advisory lock (also released on disconnect)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))", (ADVISORY_LOCK_KEY,)
            )
    except Exception as e:
        logger.warning(f"Failed to release advisory lock: {e}")


# ---------------------------------------------------------------------------
# dbt subprocess
# ---------------------------------------------------------------------------

def _kill_process_tree(pid: int) -> None:
    """Kill a process and all its children. Windows-safe via taskkill /T."""
    try:
        if platform.system() == "Windows":
            result = subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(pid)],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning(
                    "taskkill exited %s for PID %s: %s",
                    result.returncode, pid, result.stderr.strip(),
                )
            else:
                logger.info("Killed process tree for PID %s", pid)
        else:
            import signal

            os.killpg(os.getpgid(pid), signal.SIGKILL)
            logger.info("Killed process group for PID %s", pid)
    except Exception as exc:
        logger.error("Failed to kill process tree for PID %s: %s", pid, exc)


def _build_env() -> dict:
    """Build subprocess env with telemetry disabled for stability/compliance."""
    env = os.environ.copy()
    env.setdefault("DO_NOT_TRACK", "1")
    env.setdefault("DBT_DO_NOT_TRACK", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _is_retryable_failure(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    return any(pattern in combined for pattern in RETRYABLE_ERROR_PATTERNS)


def _read_text_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as file_obj:
            return file_obj.read()
    except Exception:
        return ""


def _preflight_check(project_dir: str) -> None:
    """Run ``dbt debug`` to verify profile, deps, and database connectivity.

    Fails fast with a clear error before committing to a full ``dbt run``.
    Times out after 60 seconds — if the database is unreachable, we know
    immediately rather than wasting a 30-minute attempt.
    """
    cmd = [
        "dbt", "debug",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
    ]
    logger.info("Preflight: running dbt debug")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=project_dir,
            env=_build_env(),
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Preflight dbt debug failed (exit {result.returncode}): "
                f"{result.stdout[-1000:]}\n{result.stderr[-1000:]}"
            )
        logger.info("Preflight: dbt debug passed")
    except subprocess.TimeoutExpired:
        raise RuntimeError("Preflight dbt debug timed out after 60s — database may be unreachable")


def _run_dbt_seed(project_dir: str) -> None:
    """Run ``dbt seed`` to load/update seed CSVs into the database.

    Fast and idempotent — safe to run before every ``dbt run``.
    Times out after 120 seconds.
    """
    cmd = [
        "dbt", "seed",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
    ]
    logger.info("Running dbt seed")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=project_dir,
            env=_build_env(),
        )
        if result.returncode != 0:
            logger.warning(
                "dbt seed failed (exit %s): %s",
                result.returncode,
                result.stderr[-500:] if result.stderr else result.stdout[-500:],
            )
        else:
            logger.info("dbt seed completed successfully")
    except subprocess.TimeoutExpired:
        logger.warning("dbt seed timed out after 120s — continuing with dbt run")


def run_dbt(
    project_dir: str | None = None,
    select: str | None = None,
    timeout_seconds: int = 1800,
    dry_run: bool = False,
    max_attempts: int = 3,
    retry_backoff_seconds: int = 30,
) -> DbtRunResult:
    """Run ``dbt run`` as a subprocess.

    Args:
        project_dir: Path to the dbt project. Defaults to the repo's dbt project.
        select: Optional dbt --select argument (e.g. "tag:pjm").
        timeout_seconds: Hard kill after this many seconds (default 30 min).
        dry_run: If True, log the command but don't execute.
        max_attempts: Total attempts for retryable failures (default 3).
        retry_backoff_seconds: Backoff multiplier between attempts.

    Returns:
        ``DbtRunResult`` with exit code, output, and attempt metadata.
    """
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be >= 0")

    project_dir = project_dir or os.path.abspath(DBT_PROJECT_DIR)
    project_dir = os.path.abspath(project_dir)

    if not os.path.isdir(project_dir):
        raise FileNotFoundError(f"dbt project directory not found: {project_dir}")
    if not os.path.isfile(os.path.join(project_dir, "dbt_project.yml")):
        raise FileNotFoundError(f"dbt_project.yml not found in: {project_dir}")
    if shutil.which("dbt") is None:
        raise FileNotFoundError("dbt executable not found in PATH")

    # Preflight: fast connectivity check before committing to a full dbt run
    _preflight_check(project_dir)

    # Seed: load/update seed CSVs before running models
    _run_dbt_seed(project_dir)

    cmd = ["dbt", "run", "--project-dir", project_dir, "--profiles-dir", project_dir]
    if select:
        cmd.extend(["--select", select])

    logger.info(f"dbt command: {' '.join(cmd)} (cwd={project_dir})")

    if dry_run:
        logger.info("[DRY RUN] Skipping dbt execution")
        return DbtRunResult(
            exit_code=0,
            stdout="[dry run] no output",
            stderr="",
            attempts=1,
            timed_out=False,
        )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    for attempt in range(1, max_attempts + 1):
        logger.info("Starting dbt attempt %s/%s", attempt, max_attempts)

        stdout_fd, stdout_path = tempfile.mkstemp(prefix="dbt_stdout_", suffix=".log")
        stderr_fd, stderr_path = tempfile.mkstemp(prefix="dbt_stderr_", suffix=".log")
        timed_out = False

        try:
            with (
                os.fdopen(stdout_fd, "w", encoding="utf-8", buffering=1) as stdout_file,
                os.fdopen(stderr_fd, "w", encoding="utf-8", buffering=1) as stderr_file,
            ):
                popen_kwargs = {
                    "stdout": stdout_file,
                    "stderr": stderr_file,
                    "text": True,
                    "cwd": project_dir,
                    "env": _build_env(),
                }

                if platform.system() == "Windows":
                    creationflags = (
                        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                        | getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    )
                    if creationflags:
                        popen_kwargs["creationflags"] = creationflags
                else:
                    popen_kwargs["start_new_session"] = True

                proc = subprocess.Popen(cmd, **popen_kwargs)
                try:
                    proc.wait(timeout=timeout_seconds)
                    exit_code = proc.returncode if proc.returncode is not None else 1
                except subprocess.TimeoutExpired:
                    timed_out = True
                    logger.error(
                        "dbt attempt %s/%s timed out after %ss. Killing process tree.",
                        attempt,
                        max_attempts,
                        timeout_seconds,
                    )
                    _kill_process_tree(proc.pid)
                    try:
                        proc.wait(timeout=10)
                    except Exception:
                        pass
                    exit_code = -1

            stdout_text = _read_text_file(stdout_path).strip()
            stderr_text = _read_text_file(stderr_path).strip()
        finally:
            for path in (stdout_path, stderr_path):
                try:
                    os.remove(path)
                except Exception:
                    pass

        if stdout_text:
            stdout_chunks.append(f"[attempt {attempt}/{max_attempts}]")
            stdout_chunks.append(stdout_text)
        if stderr_text:
            stderr_chunks.append(f"[attempt {attempt}/{max_attempts}]")
            stderr_chunks.append(stderr_text)

        retryable = timed_out or _is_retryable_failure(stdout_text, stderr_text)
        if exit_code == 0:
            if attempt > 1:
                logger.warning("dbt succeeded on attempt %s/%s", attempt, max_attempts)
            model_summary = _parse_run_results(project_dir)
            if model_summary.get("error", 0) > 0:
                logger.warning(
                    "dbt exited 0 but %d model(s) errored: %s",
                    model_summary["error"],
                    [e["model"] for e in model_summary.get("errors", [])],
                )
            return DbtRunResult(
                exit_code=0,
                stdout="\n".join(stdout_chunks),
                stderr="\n".join(stderr_chunks),
                attempts=attempt,
                timed_out=False,
                model_summary=model_summary,
            )

        if attempt < max_attempts and retryable:
            wait_seconds = retry_backoff_seconds * attempt
            logger.warning(
                "dbt attempt %s/%s failed (exit=%s). Retrying in %ss",
                attempt,
                max_attempts,
                exit_code,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        return DbtRunResult(
            exit_code=exit_code,
            stdout="\n".join(stdout_chunks),
            stderr="\n".join(stderr_chunks),
            attempts=attempt,
            timed_out=timed_out,
        )

    return DbtRunResult(
        exit_code=1,
        stdout="\n".join(stdout_chunks),
        stderr="\n".join(stderr_chunks),
        attempts=max_attempts,
        timed_out=False,
    )
