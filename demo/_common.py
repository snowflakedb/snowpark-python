"""Shared setup for the session.jobs samples: a session, a scratch schema, a stage."""

import contextlib
import os
import time
import uuid

from snowflake.snowpark import Session
from snowflake.snowpark.jobs import JobNotSupportedError


def say(*args):
    print(*args, flush=True)  # noqa: T201 - the samples are meant to print


@contextlib.contextmanager
def demo_session():
    """Yields ``(session, stage)`` inside a scratch schema that is dropped afterwards.

    Everything the samples create (procedures, code bundles, tasks, stage) lives in that
    schema, so dropping it is the whole clean-up and the samples can be re-run freely.
    """
    name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "default")
    session = Session.builder.config("connection_name", name).create()
    if not session.get_current_warehouse():
        session.close()
        raise SystemExit(
            "The samples need a current warehouse: set one in your connection "
            "configuration or run session.use_warehouse(...)."
        )
    database = session.get_current_database()
    schema = f"JOBS_DEMO_{uuid.uuid4().hex[:8].upper()}"
    session.sql(f"CREATE SCHEMA {database}.{schema}").collect()
    try:
        session.sql(f"USE SCHEMA {database}.{schema}").collect()
        session.sql("CREATE STAGE DEMO_STAGE").collect()
        say(f"[setup] scratch schema {database}.{schema}")
        yield session, f"@{database}.{schema}.DEMO_STAGE"
    finally:
        session.sql(f"DROP SCHEMA IF EXISTS {database}.{schema}").collect()
        say(f"[cleanup] dropped {database}.{schema}")
        session.close()


def show_logs(job, tries=12):
    """Prints a job's logs. The event table ingests with a delay, so poll for a while."""
    try:
        for _ in range(tries):
            logs = job.logs()
            if logs:
                say(logs)
                return
            time.sleep(5)
        say("(no log records yet: the event table ingests with a delay)")
    except JobNotSupportedError as ex:
        say(f"(logs not available: {ex})")
