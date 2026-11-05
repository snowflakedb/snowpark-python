#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from snowflake.snowpark.jobs._utils import (
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    fetch_event_logs,
    sql_str,
    unquote_ident,
)

_POLL_INTERVAL_SECONDS = 2.0

# Hint appended to the error of a job that hit the owner's rights restrictions.
_OWNER_RIGHTS_MARKERS = ("owner's rights context", "owner's rights procedure")
_OWNER_RIGHTS_HINT = (
    "Hint: this operation is not allowed in an owner's rights stored procedure. "
    'Deploy with StoredProcedure(execute_as="caller") to run with caller\'s rights.'
)


class JobStatus(str, Enum):
    """Normalized status of a job."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED)


class JobNotSupportedError(RuntimeError):
    """Raised when a job handle does not support an operation (for example
    ``result()`` of a code bundle run)."""


class Job(ABC):
    """Common interface of all job handles.

    Attributes:
        id: Stable identifier usable with ``session.jobs.get(...)``.
        target: ``"stored_procedure"`` or ``"code_bundle"``.
        query_id: The id of the query that runs the job.
    """

    id: str
    target: str
    query_id: str

    @property
    @abstractmethod
    def raw_status(self) -> str:
        """The unmapped status reported by Snowflake."""

    @abstractmethod
    def status(self) -> JobStatus:
        """Returns the current normalized status."""

    def is_done(self) -> bool:
        return self.status().is_terminal

    def wait(
        self,
        timeout: Optional[float] = None,
        poll_interval: float = _POLL_INTERVAL_SECONDS,
    ) -> JobStatus:
        """Polls until the job reaches a terminal status and returns it.

        Raises:
            TimeoutError: If ``timeout`` seconds elapse first.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            status = self.status()
            if status.is_terminal:
                return status
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Job {self.id} did not finish within {timeout} seconds "
                    f"(status: {status.value})"
                )
            time.sleep(poll_interval)

    @abstractmethod
    def result(self, timeout: Optional[float] = None) -> Any:
        """Waits for the job and returns its result."""

    @abstractmethod
    def error(self) -> Optional[str]:
        """The error message of a failed job, otherwise ``None``."""

    @abstractmethod
    def logs(self, limit: int = -1) -> str:
        """Log records of the job (all of them, or the most recent ``limit``).

        Records come from the account event table, which ingests with a delay of
        seconds to minutes, so a job that just finished may show no logs yet.
        """

    @abstractmethod
    def cancel(self) -> None:
        """Cancels the job."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(id={self.id!r})"


def _cancel_query(session, query_id: str) -> None:
    session.sql(f"SELECT SYSTEM$CANCEL_QUERY({sql_str(query_id)})").collect()


def _map_query_status(name: str) -> JobStatus:
    if name == "SUCCESS":
        return JobStatus.SUCCEEDED
    if name in ("FAILED_WITH_ERROR", "FAILED_WITH_INCIDENT"):
        return JobStatus.FAILED
    if name in ("ABORTING", "ABORTED"):
        return JobStatus.CANCELLED
    if name in (
        "QUEUED",
        "RESUMING_WAREHOUSE",
        "QUEUED_REPARING_WAREHOUSE",
        "BLOCKED",
        "NO_DATA",
    ):
        return JobStatus.PENDING
    return JobStatus.RUNNING


def _query_status_name(session, query_id: str) -> str:
    return session._conn._conn.get_query_status(query_id).name


def _query_error(session, query_id: str) -> Optional[str]:
    try:
        session._conn._conn.get_query_status_throw_if_error(query_id)
    except Exception as ex:
        msg = getattr(ex, "msg", None) or str(ex)
        return str(msg)
    return None


class QueryJob(Job):
    """A job that is a plain query, such as a stored procedure call."""

    def __init__(self, session, async_job, target: str = STORED_PROCEDURE_KIND) -> None:
        self._session = session
        self._async = async_job
        self.query_id = async_job.query_id
        self.target = target
        self.id = f"query:{self.query_id}"

    @property
    def raw_status(self) -> str:
        return _query_status_name(self._session, self.query_id)

    def status(self) -> JobStatus:
        return _map_query_status(self.raw_status)

    def result(self, timeout: Optional[float] = None) -> Any:
        """Returns the procedure's return value (deserialized from JSON).

        Raises the underlying SQL exception if the job failed.
        """
        self.wait(timeout)
        rows = self._async.result("row")
        if not rows:
            return None
        value = rows[0][0]
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value

    def error(self) -> Optional[str]:
        if self.status() != JobStatus.FAILED:
            return None
        msg = _query_error(self._session, self.query_id) or "Query failed"
        if any(m in msg.lower() for m in _OWNER_RIGHTS_MARKERS):
            msg = f"{msg}\n{_OWNER_RIGHTS_HINT}"
        return msg

    def logs(self, limit: int = -1) -> str:
        return fetch_event_logs(self._session, self.query_id, limit)

    def cancel(self) -> None:
        _cancel_query(self._session, self.query_id)


_BUNDLE_STATUS = {
    "PENDING": JobStatus.PENDING,
    "RUNNING": JobStatus.RUNNING,
    "DONE": JobStatus.SUCCEEDED,
    "FAILED": JobStatus.FAILED,
    "CANCELLED": JobStatus.CANCELLED,
}


class BundleRun(Job):
    """A code bundle execution."""

    def __init__(
        self,
        session,
        query_id: str,
        compute_type: Optional[str] = None,
        temp_bundle: Optional[str] = None,
        execution_name: Optional[str] = None,
        row: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._session = session
        self.query_id = query_id
        self.target = CODE_BUNDLE_KIND
        self.id = f"bundle:{query_id}"
        self.compute_type = compute_type
        self.execution_name = execution_name
        self._temp_bundle = temp_bundle
        self._row = row

    def _history_row(self) -> Optional[Dict[str, Any]]:
        rows = self._session.sql(
            "SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
            f"QUERY_ID => {sql_str(self.query_id)}))"
        ).collect()
        if not rows:
            return None
        self._row = rows[0].as_dict()
        if self.execution_name is None:
            self.execution_name = self._row.get("EXECUTION_NAME")
        return self._row

    @property
    def raw_status(self) -> str:
        row = self._history_row()
        if row is not None:
            return str(row["STATUS"])
        # No history row yet: either it was not recorded yet, or the statement failed
        # before the run started.
        name = _query_status_name(self._session, self.query_id)
        if name in ("FAILED_WITH_ERROR", "FAILED_WITH_INCIDENT"):
            return "FAILED"
        if name in ("ABORTING", "ABORTED"):
            return "CANCELLED"
        return "PENDING"

    def status(self) -> JobStatus:
        status = _BUNDLE_STATUS.get(self.raw_status.upper(), JobStatus.RUNNING)
        if status.is_terminal:
            self.cleanup()
        return status

    def result(self, timeout: Optional[float] = None) -> Any:
        raise JobNotSupportedError(
            "EXECUTE CODE BUNDLE does not return a value; use wait() and status(), "
            "or write results to a table or stage."
        )

    def error(self) -> Optional[str]:
        if self.status() != JobStatus.FAILED:
            return None
        row = self._row or self._history_row()
        if row is not None and row.get("ERROR_MESSAGE"):
            return str(row["ERROR_MESSAGE"])
        return _query_error(self._session, self.query_id) or "Code bundle run failed"

    def logs(self, limit: int = -1) -> str:
        row = self._row or self._history_row()
        compute_type = self.compute_type
        if compute_type is None and row is not None:
            compute_type = (row.get("COMPUTE_TYPE") or "").lower() or None
        if compute_type == "compute_pool":
            raise JobNotSupportedError(
                "Logs of code bundle runs on a compute pool are not retrievable."
            )
        return fetch_event_logs(self._session, self.query_id, limit)

    def cancel(self) -> None:
        _cancel_query(self._session, self.query_id)

    def cleanup(self) -> None:
        """Drops the temporary code bundle created for this run, if any."""
        bundle, self._temp_bundle = self._temp_bundle, None
        if bundle:
            try:
                self._session.sql(f"DROP CODE BUNDLE IF EXISTS {bundle}").collect()
            except Exception:  # best effort
                self._temp_bundle = bundle


_TASK_STATUS = {
    "SCHEDULED": JobStatus.PENDING,
    "EXECUTING": JobStatus.RUNNING,
    "SUCCEEDED": JobStatus.SUCCEEDED,
    "FAILED": JobStatus.FAILED,
    "FAILED_AND_AUTO_SUSPENDED": JobStatus.FAILED,
    "CANCELLED": JobStatus.CANCELLED,
    "SKIPPED": JobStatus.CANCELLED,
}


def task_history_sql(task_fqn: str, limit: int, query_id: Optional[str] = None) -> str:
    from snowflake.snowpark.jobs._utils import split_fqn

    db, schema, name = split_fqn(task_fqn)
    sql = (
        f"SELECT * FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY("
        f"TASK_NAME => {sql_str(unquote_ident(name))}, RESULT_LIMIT => {int(limit)})) "
        f"WHERE SCHEMA_NAME = {sql_str(unquote_ident(schema))}"
    )
    if query_id:
        sql += f" AND QUERY_ID = {sql_str(query_id)}"
    return sql + " ORDER BY SCHEDULED_TIME DESC"


class TaskRun(Job):
    """One execution of a scheduled task."""

    def __init__(
        self,
        session,
        task_fqn: str,
        query_id: str,
        row: Optional[Dict[str, Any]] = None,
        target: Optional[str] = None,
    ) -> None:
        self._session = session
        self.task_fqn = task_fqn
        self.query_id = query_id
        self.id = f"task:{task_fqn}/{query_id}"
        self._row = row
        self._target = target

    @property
    def target(self) -> str:  # type: ignore[override]
        if self._target is None:
            text = (
                (self._row or self._refresh() or {}).get("QUERY_TEXT") or ""
            ).lstrip()
            self._target = (
                CODE_BUNDLE_KIND
                if text.upper().startswith("EXECUTE CODE BUNDLE")
                else STORED_PROCEDURE_KIND
            )
        return self._target

    def _refresh(self) -> Optional[Dict[str, Any]]:
        rows = self._session.sql(
            task_history_sql(self.task_fqn, 1000, self.query_id)
        ).collect()
        if rows:
            self._row = rows[0].as_dict()
        return self._row

    @property
    def raw_status(self) -> str:
        row = self._refresh()
        return str(row["STATE"]) if row else "SCHEDULED"

    def status(self) -> JobStatus:
        return _TASK_STATUS.get(self.raw_status.upper(), JobStatus.RUNNING)

    def child(self) -> Job:
        """The handle of the query the task executed."""
        if self.target == CODE_BUNDLE_KIND:
            return BundleRun(self._session, self.query_id)
        return QueryJob(
            self._session,
            self._session.create_async_job(self.query_id),
            STORED_PROCEDURE_KIND,
        )

    def result(self, timeout: Optional[float] = None) -> Any:
        self.wait(timeout)
        return self.child().result()

    def error(self) -> Optional[str]:
        if self.status() != JobStatus.FAILED:
            return None
        row = self._row or {}
        msg = row.get("ERROR_MESSAGE")
        if msg:
            msg = str(msg)
            if any(m in msg.lower() for m in _OWNER_RIGHTS_MARKERS):
                msg = f"{msg}\n{_OWNER_RIGHTS_HINT}"
            return msg
        return self.child().error()

    def logs(self, limit: int = -1) -> str:
        return self.child().logs(limit)

    def cancel(self) -> None:
        _cancel_query(self._session, self.query_id)


def make_task_runs(
    session, task_fqn: str, rows: List[Any], target: Optional[str] = None
) -> List[TaskRun]:
    runs = []
    for r in rows:
        d = r.as_dict()
        if d.get("QUERY_ID"):
            runs.append(TaskRun(session, task_fqn, d["QUERY_ID"], d, target))
    return runs
