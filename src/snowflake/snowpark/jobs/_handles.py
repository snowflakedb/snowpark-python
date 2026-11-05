#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import json
import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from snowflake.snowpark.jobs._utils import (
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    TEMP_BUNDLE_RE,
    fetch_event_logs,
    run_sql,
    sql_str,
    unquote_ident,
    validate_fqn,
    validate_query_id,
)

_logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 2.0

# How long a handle tolerates that Snowflake knows nothing about its job (an unknown
# query id, no history row) before status()/wait() raise JobNotFoundError. The clock
# starts at the handle's first status check; a job just submitted needs a moment to show.
MISSING_JOB_GRACE_SECONDS = 120

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


class JobNotFoundError(ValueError):
    """Raised when Snowflake keeps reporting that a job does not exist, for example
    for a mistyped query id."""


_warned_status_names = set()


def _lookup_status(mapping: Dict[str, "JobStatus"], kind: str, name: str):
    """Maps a Snowflake status name. Unknown names count as running and are logged
    once, so that new server-side states stay visible without breaking ``wait()``."""
    status = mapping.get(name.upper())
    if status is None:
        if (kind, name) not in _warned_status_names:
            _warned_status_names.add((kind, name))
            _logger.warning(
                "Unknown %s status %r: treating it as RUNNING. The job handle may need "
                "an update.",
                kind,
                name,
            )
        return JobStatus.RUNNING
    return status


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

    _first_status_check: Optional[float] = None
    _seen_existing = False

    def _note_existence(self, exists: bool) -> None:
        """Raises ``JobNotFoundError`` if the job has been missing for too long."""
        now = time.monotonic()
        if self._first_status_check is None:
            self._first_status_check = now
        if exists:
            self._seen_existing = True
        elif (
            not self._seen_existing
            and now - self._first_status_check > MISSING_JOB_GRACE_SECONDS
        ):
            raise JobNotFoundError(
                f"{self.id}: Snowflake does not know this job (checked for more than "
                f"{MISSING_JOB_GRACE_SECONDS} seconds). Is the id correct?"
            )

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

        A job lasts only as long as the session that submitted it (closing that session
        cancels runs that are still going), so wait for it before closing the session.
        A handle re-attached with ``session.jobs.get()`` in another process works while
        the submitting session is still open.

        Raises:
            TimeoutError: If ``timeout`` seconds elapse first.
            JobNotFoundError: If Snowflake keeps reporting that the job does not exist
                for more than ``MISSING_JOB_GRACE_SECONDS`` (120) seconds.
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
    run_sql(session, f"SELECT SYSTEM$CANCEL_QUERY({sql_str(query_id)})")


# names of snowflake.connector.cursor.QueryStatus, plus INCIDENT used by QUERY_HISTORY
_QUERY_STATUS = {
    "SUCCESS": JobStatus.SUCCEEDED,
    "FAILED_WITH_ERROR": JobStatus.FAILED,
    "FAILED_WITH_INCIDENT": JobStatus.FAILED,
    "INCIDENT": JobStatus.FAILED,
    # the connection to the query was lost: the outcome is not known
    "DISCONNECTED": JobStatus.FAILED,
    "ABORTING": JobStatus.CANCELLED,
    "ABORTED": JobStatus.CANCELLED,
    "RUNNING": JobStatus.RUNNING,
    "RESTARTED": JobStatus.RUNNING,
    "QUEUED": JobStatus.PENDING,
    "RESUMING_WAREHOUSE": JobStatus.PENDING,
    "QUEUED_REPARING_WAREHOUSE": JobStatus.PENDING,
    "BLOCKED": JobStatus.PENDING,
    "NO_DATA": JobStatus.PENDING,
}


def _map_query_status(name: str) -> JobStatus:
    return _lookup_status(_QUERY_STATUS, "query", name)


def _query_status_name(session, query_id: str) -> str:
    return session._conn._conn.get_query_status(query_id).name


def _query_error(session, query_id: str) -> Optional[str]:
    try:
        session._conn._conn.get_query_status_throw_if_error(query_id)
    except Exception as ex:
        msg = getattr(ex, "msg", None) or str(ex)
        return str(msg)
    return None


_STATEMENT_NOT_FOUND = 709  # "Statement <id> not found" (000709)

_SCHEDULED_RESULT_HINT = (
    "The return value of this scheduled run cannot be read (the statement result is "
    "not available). The usual cause is a procedure deployed with execute_as='owner': "
    "the return value of a scheduled run can only be read for caller's-rights "
    "procedures. Deploy with StoredProcedure(execute_as='caller'), or have the "
    "procedure write its result to a table."
)


def _is_statement_not_found(ex: Exception) -> bool:
    for attr in ("errno", "sql_error_code"):
        try:
            if int(getattr(ex, attr, None)) == _STATEMENT_NOT_FOUND:
                return True
        except (TypeError, ValueError):
            pass
    return False


class QueryJob(Job):
    """A job that is a plain query, such as a stored procedure call."""

    def __init__(
        self,
        session,
        async_job,
        target: str = STORED_PROCEDURE_KIND,
        scheduled: bool = False,
        started_at: Optional[datetime.datetime] = None,
    ) -> None:
        self._session = session
        # when the query started, to bound the event table scan of logs()
        self._started_at = started_at
        # the query was run by a task: its result may not be readable (see result())
        self._scheduled = scheduled
        self._async = async_job
        self.query_id = async_job.query_id
        self.target = target
        self.id = f"query:{self.query_id}"

    @property
    def raw_status(self) -> str:
        name = _query_status_name(self._session, self.query_id)
        self._note_existence(name != "NO_DATA")
        return name

    def status(self) -> JobStatus:
        return _map_query_status(self.raw_status)

    def result(self, timeout: Optional[float] = None) -> Any:
        """Returns the procedure's return value (deserialized from JSON).

        Raises the underlying SQL exception if the job failed. For the child of a task
        run, raises :class:`JobNotSupportedError` when the statement result is not available,
        which is usually the case for procedures deployed with ``execute_as="owner"``.
        """
        self.wait(timeout)
        try:
            rows = self._async.result("row")
        except Exception as ex:
            if self._scheduled and _is_statement_not_found(ex):
                raise JobNotSupportedError(_SCHEDULED_RESULT_HINT) from ex
            raise
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

    def _start_time(self) -> Optional[datetime.datetime]:
        if self._started_at is None:
            try:
                rows = run_sql(
                    self._session,
                    "SELECT START_TIME FROM TABLE(INFORMATION_SCHEMA."
                    "QUERY_HISTORY_BY_USER(RESULT_LIMIT => 10000)) "
                    f"WHERE QUERY_ID = {sql_str(self.query_id)}",
                )
                if rows:
                    self._started_at = rows[0][0]
            except Exception:  # best effort: the scan falls back to the last 7 days
                pass
        return self._started_at

    def logs(self, limit: int = -1) -> str:
        return fetch_event_logs(self._session, self.query_id, limit, self._start_time())

    def cancel(self) -> None:
        _cancel_query(self._session, self.query_id)


_BUNDLE_STATUS = {
    "PENDING": JobStatus.PENDING,
    "RUNNING": JobStatus.RUNNING,
    "DONE": JobStatus.SUCCEEDED,
    "FAILED": JobStatus.FAILED,
    "CANCELLED": JobStatus.CANCELLED,
    # a cancelled run on a compute pool ends as DELETED (its job service is removed)
    "DELETED": JobStatus.CANCELLED,
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
        self._temp_checked = temp_bundle is not None
        self._row = row

    def _history_row(self) -> Optional[Dict[str, Any]]:
        rows = run_sql(
            self._session,
            "SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
            f"QUERY_ID => {sql_str(self.query_id)}))",
        )
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
            self._note_existence(True)
            return str(row["STATUS"])
        # No history row: it may not be recorded yet, or the statement failed before the
        # run started, or the id is not a code bundle run at all.
        name = _query_status_name(self._session, self.query_id)
        if name in ("FAILED_WITH_ERROR", "FAILED_WITH_INCIDENT"):
            self._note_existence(True)
            return "FAILED"
        if name in ("ABORTING", "ABORTED"):
            self._note_existence(True)
            return "CANCELLED"
        # an EXECUTE that is still running is pending; a finished or unknown query
        # without a history row is not a code bundle run
        self._note_existence(name not in ("NO_DATA", "SUCCESS"))
        return "PENDING"

    def status(self) -> JobStatus:
        status = _lookup_status(_BUNDLE_STATUS, "code bundle", self.raw_status)
        if status.is_terminal:
            self._adopt_temp_bundle()
            self.cleanup()
        return status

    def _adopt_temp_bundle(self) -> None:
        """A re-attached handle also drops the temporary bundle of a pool run."""
        if self._temp_bundle is not None or self._temp_checked:
            return
        self._temp_checked = True
        row = self._row or {}
        name = str(row.get("CODE_BUNDLE_NAME") or "")
        if (
            TEMP_BUNDLE_RE.match(name)
            and row.get("DATABASE_NAME")
            and row.get("SCHEMA_NAME")
        ):
            self._temp_bundle = ".".join(
                '"' + str(part).replace('"', '""') + '"'
                for part in (row["DATABASE_NAME"], row["SCHEMA_NAME"], name)
            )

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
        started = (row or {}).get("START_TIME")
        return fetch_event_logs(self._session, self.query_id, limit, started)

    def cancel(self) -> None:
        _cancel_query(self._session, self.query_id)

    def cleanup(self) -> None:
        """Drops the temporary code bundle created for this run, if any.

        Call it only once the run has finished: dropping the bundle while a pool run is
        still starting breaks the run.
        """
        bundle, self._temp_bundle = self._temp_bundle, None
        if bundle:
            try:
                run_sql(self._session, f"DROP CODE BUNDLE IF EXISTS {bundle}")
                manager = getattr(self._session, "_jobs", None)
                if manager is not None:
                    manager._temp_bundles.discard(bundle)
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

    db, schema, name = split_fqn(validate_fqn(task_fqn, "task name"))
    sql = (
        f"SELECT * FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY("
        f"TASK_NAME => {sql_str(unquote_ident(name))}, RESULT_LIMIT => {int(limit)})) "
        f"WHERE SCHEMA_NAME = {sql_str(unquote_ident(schema))}"
    )
    if query_id:
        sql += f" AND QUERY_ID = {sql_str(validate_query_id(query_id))}"
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
        self.task_fqn = validate_fqn(task_fqn, "task name")
        self.query_id = validate_query_id(query_id)
        self.id = f"task:{task_fqn}/{self.query_id}"
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
        rows = run_sql(
            self._session, task_history_sql(self.task_fqn, 1000, self.query_id)
        )
        if rows:
            self._row = rows[0].as_dict()
        return self._row

    @property
    def raw_status(self) -> str:
        row = self._refresh()
        self._note_existence(row is not None)
        return str(row["STATE"]) if row else "SCHEDULED"

    def status(self) -> JobStatus:
        return _lookup_status(_TASK_STATUS, "task", self.raw_status)

    def child(self) -> Job:
        """The handle of the query the task executed."""
        if self.target == CODE_BUNDLE_KIND:
            return BundleRun(self._session, self.query_id)
        row = self._row or {}
        return QueryJob(
            self._session,
            self._session.create_async_job(self.query_id),
            STORED_PROCEDURE_KIND,
            scheduled=True,
            started_at=row.get("QUERY_START_TIME") or row.get("SCHEDULED_TIME"),
        )

    def result(self, timeout: Optional[float] = None) -> Any:
        """Waits for the run and returns the return value of its procedure.

        Limitation: the return value of a scheduled run can only be read for
        caller's-rights procedures. For ``execute_as="owner"`` procedures, this
        usually raises :class:`JobNotSupportedError` (also whenever the statement
        result is not available); the run's status is still available. Deploy
        with ``StoredProcedure(execute_as="caller")``, or have the procedure write its
        result to a table. Code bundle runs have no return value.
        """
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
