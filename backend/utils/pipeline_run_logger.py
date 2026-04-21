import json
import logging
import socket
import time
import uuid
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from backend.utils import (
    azure_postgresql_utils,
    file_utils,
)

"""
Pipeline run logging system.

Tracks the full lifecycle of every pipeline run in `logging.pipeline_runs`.
Replaces the old `db_error_handler.py` which only logged errors and overwrote
repeated identical errors rather than appending.

Usage:
    from backend.utils import pipeline_run_logger

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="my_pipeline",
        source="helioscta_api_scrapes",
        priority="high",
        tags="trades,clear_street",
        log_file_path=logger.log_file_path,
    )
    run.start()
    ...
    run.success(rows_processed=100, files_processed=1)

Or as a context manager:
    with pipeline_run_logger.PipelineRunLogger(...) as run:
        ...
        run.log_rows_processed(100)
"""

# Schema / table constants
SCHEMA = "logging"
TABLE = "pipeline_runs"

# Column definitions for logging.pipeline_runs
COLUMNS = [
    "run_id",
    "pipeline_name",
    "event_type",
    "event_timestamp",
    "duration_seconds",
    "status",
    "error_type",
    "error_message",
    "log_file_content",
    "rows_processed",
    "files_processed",
    "source",
    "priority",
    "tags",
    "hostname",
    "notification_channel",
    "notification_recipient",
    "metadata",
    "target_table",
    "operation_type",
]

DATA_TYPES = [
    "VARCHAR",       # run_id
    "VARCHAR",       # pipeline_name
    "VARCHAR",       # event_type
    "TIMESTAMP",     # event_timestamp (MST, no tz conversion)
    "FLOAT",         # duration_seconds
    "VARCHAR",       # status
    "VARCHAR",       # error_type
    "TEXT",          # error_message
    "TEXT",          # log_file_content
    "INTEGER",       # rows_processed
    "INTEGER",       # files_processed
    "VARCHAR",       # source
    "VARCHAR",       # priority
    "VARCHAR",       # tags
    "VARCHAR",       # hostname
    "VARCHAR",       # notification_channel
    "VARCHAR",       # notification_recipient
    "TEXT",          # metadata
    "VARCHAR",       # target_table
    "VARCHAR",       # operation_type
]

PRIMARY_KEY = ["run_id", "event_type", "event_timestamp"]

VALID_OPERATION_TYPES = {"upsert", "consume"}


class PipelineRunLogger:
    """Tracks the full lifecycle of a pipeline run.

    Each event (start, success, failure, notification, stage) is appended as a
    separate row in ``logging.pipeline_runs``.

    ``operation_type`` and ``target_table`` work together:
        - ``"upsert"``  — ``target_table`` is the DB table being written to.
        - ``"consume"`` — ``target_table`` is the DB table being read from.
    """

    def __init__(
        self,
        pipeline_name: str,
        source: str = "",
        priority: str = "medium",
        tags: str = "",
        log_file_path: Optional[Union[str, Path]] = None,
        target_table: str = "",
        operation_type: str = "",
    ):
        self.run_id: str = str(uuid.uuid4())
        self.pipeline_name: str = pipeline_name
        self.source: str = source
        self.priority: str = priority
        self.tags: str = tags
        self.log_file_path: Optional[Union[str, Path]] = log_file_path
        if operation_type and operation_type not in VALID_OPERATION_TYPES:
            raise ValueError(
                f"Invalid operation_type '{operation_type}'. "
                f"Must be one of: {VALID_OPERATION_TYPES}"
            )
        if operation_type and not target_table:
            raise ValueError(
                f"target_table is required when operation_type is set. "
                f"For 'upsert': the table being written to. "
                f"For 'consume': the table being read from."
            )
        self.target_table: str = target_table
        self.operation_type: str = operation_type
        self.hostname: str = socket.gethostname()

        self._start_time: Optional[float] = None
        self._rows_processed: int = 0
        self._files_processed: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Record the start time (no DB write — only finished runs are logged)."""
        self._start_time = time.time()

    def success(
        self,
        rows_processed: Optional[int] = None,
        files_processed: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Log a RUN_SUCCESS event."""
        if rows_processed is not None:
            self._rows_processed = rows_processed
        if files_processed is not None:
            self._files_processed = files_processed

        self._write_event(
            event_type="RUN_SUCCESS",
            status="success",
            duration_seconds=self._elapsed(),
            rows_processed=self._rows_processed,
            files_processed=self._files_processed,
            metadata=metadata,
        )

    def failure(
        self,
        error: Optional[Exception] = None,
        log_file_path: Optional[Union[str, Path]] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Log a RUN_FAILURE event."""
        log_path = log_file_path or self.log_file_path
        log_content = self._read_log_file(log_path)

        error_type = ""
        error_message = ""
        if error is not None:
            error_type = type(error).__name__
            error_message = f"{error_type}: {error}"

        self._write_event(
            event_type="RUN_FAILURE",
            status="failure",
            duration_seconds=self._elapsed(),
            error_type=error_type,
            error_message=error_message,
            log_file_content=log_content,
            rows_processed=self._rows_processed,
            files_processed=self._files_processed,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Notification tracking
    # ------------------------------------------------------------------

    def log_notification(
        self,
        channel: str,
        recipient: str,
        metadata: Optional[dict] = None,
    ) -> None:
        """Log a SLACK_SENT or EMAIL_SENT event."""
        event_type = "SLACK_SENT" if channel.lower() == "slack" else "EMAIL_SENT"
        self._write_event(
            event_type=event_type,
            status="sent",
            notification_channel=channel,
            notification_recipient=recipient,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Stage / metric tracking
    # ------------------------------------------------------------------

    def log_stage(
        self,
        stage_name: str,
        rows: int = 0,
        files: int = 0,
        duration_seconds: float = 0.0,
        metadata: Optional[dict] = None,
    ) -> None:
        """Log a STAGE_END event."""
        extra_meta = {"stage_name": stage_name}
        if metadata:
            extra_meta.update(metadata)

        self._write_event(
            event_type="STAGE_END",
            status="success",
            duration_seconds=duration_seconds,
            rows_processed=rows,
            files_processed=files,
            metadata=extra_meta,
        )

    def log_warning(
        self,
        message: str,
        metadata: Optional[dict] = None,
    ) -> None:
        """Log a WARNING event."""
        self._write_event(
            event_type="WARNING",
            status="warning",
            error_message=message,
            metadata=metadata,
        )

    def log_rows_processed(self, count: int) -> None:
        """Accumulate row count."""
        self._rows_processed += count

    def log_files_processed(self, count: int) -> None:
        """Accumulate file count."""
        self._files_processed += count

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipelineRunLogger":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            self.failure(error=exc_val)
        else:
            self.success()
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _elapsed(self) -> float:
        if self._start_time is None:
            return 0.0
        return round(time.time() - self._start_time, 3)

    @staticmethod
    def _read_log_file(log_file_path: Optional[Union[str, Path]]) -> str:
        if not log_file_path:
            return ""
        path = Path(log_file_path)
        if path.exists():
            try:
                return path.read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""

    def _write_event(
        self,
        event_type: str,
        status: str,
        duration_seconds: float = 0.0,
        error_type: str = "",
        error_message: str = "",
        log_file_content: str = "",
        rows_processed: int = 0,
        files_processed: int = 0,
        notification_channel: str = "",
        notification_recipient: str = "",
        metadata: Optional[dict] = None,
    ) -> None:
        """Build a single-row DataFrame and upsert to logging.pipeline_runs.

        Pre-fills every field so that ``azure_postgresql_utils.upsert_to_azure_postgresql``
        never encounters ``NaN`` values (its ``fillna(0)`` would coerce text columns to "0").
        """
        metadata_str = json.dumps(metadata) if metadata else ""
        event_timestamp = file_utils.get_mst_timestamp().replace(tzinfo=None)

        row = {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "duration_seconds": duration_seconds,
            "status": status,
            "error_type": error_type,
            "error_message": error_message,
            "log_file_content": log_file_content,
            "rows_processed": rows_processed,
            "files_processed": files_processed,
            "source": self.source,
            "priority": self.priority,
            "tags": self.tags,
            "hostname": self.hostname,
            "notification_channel": notification_channel,
            "notification_recipient": notification_recipient,
            "metadata": metadata_str,
            "target_table": self.target_table,
            "operation_type": self.operation_type,
        }

        df = pd.DataFrame([row])

        try:
            azure_postgresql_utils.upsert_to_azure_postgresql(
                schema=SCHEMA,
                table_name=TABLE,
                df=df,
                columns=COLUMNS,
                primary_key=PRIMARY_KEY,
                data_types=DATA_TYPES,
            )
            logging.info(
                f"[pipeline_run_logger] {event_type} logged for {self.pipeline_name} "
                f"(run_id={self.run_id})"
            )
        except Exception as e:
            logging.error(f"[pipeline_run_logger] Failed to log {event_type}: {e}")


# ------------------------------------------------------------------
# Backward-compatible wrapper (drop-in for db_error_handler.upsert_error_log)
# ------------------------------------------------------------------


def upsert_error_log(
    pipeline_name: str,
    error: Exception,
    log_file_path: Optional[Union[str, Path]] = None,
    priority: str = "medium",
    source: str = "api_scrapes",
    tag: str = "general",
    schema: str = SCHEMA,
    table: str = TABLE,
    target_table: str = "",
    operation_type: str = "",
) -> None:
    """Backward-compatible shim that mirrors ``db_error_handler.upsert_error_log``.

    Creates a one-shot ``PipelineRunLogger`` and logs a RUN_FAILURE event.
    """
    run = PipelineRunLogger(
        pipeline_name=pipeline_name,
        source=source,
        priority=priority,
        tags=tag,
        log_file_path=log_file_path,
        target_table=target_table,
        operation_type=operation_type,
    )
    run.failure(error=error, log_file_path=log_file_path)
