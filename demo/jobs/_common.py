"""Shared setup for the session.jobs samples: a session, a scratch schema, a stage."""

import contextlib
import datetime
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


def qualified(session, name):
    """Fully qualified name in the scratch schema. Server-side programs get these as
    string arguments, because they must not rely on the current schema."""
    return f"{session.get_current_database()}.{session.get_current_schema()}.{name}"


def seed_sales(session):
    """Creates SALES(REGION, PRODUCT, AMOUNT, SOLD_ON) with 60 deterministic rows."""
    regions = ["EMEA", "AMER", "APAC"]
    products = ["ALPHA", "BETA", "GAMMA", "DELTA"]
    start = datetime.date(2025, 1, 1)
    rows = [
        (
            regions[i % 3],
            products[i % 4],
            100 + (i * 37) % 250,
            start + datetime.timedelta(days=i),
        )
        for i in range(60)
    ]
    table = qualified(session, "SALES")
    session.create_dataframe(
        rows, schema=["REGION", "PRODUCT", "AMOUNT", "SOLD_ON"]
    ).write.save_as_table(table, mode="overwrite")
    return table


def seed_customers(session):
    """Creates CUSTOMERS_RAW(ID, NAME, EMAIL) with messy values: stray whitespace, mixed
    case, duplicates and nulls."""
    rows = [
        (1, "  ada lovelace ", "ADA@Example.com "),
        (2, "Grace Hopper", "grace@example.com"),
        (3, "grace hopper  ", " Grace@EXAMPLE.com"),
        (4, "Alan Turing", None),
        (5, None, "nobody@example.org"),
        (6, " linus torvalds", "LINUS@kernel.org"),
        (7, "Margaret Hamilton ", "margaret@nasa.gov"),
        (8, "margaret hamilton", "MARGARET@nasa.gov "),
        (9, "Dennis Ritchie", "dmr@bell-labs.com"),
        (10, "  ken thompson", "ken@bell-labs.com "),
    ]
    table = qualified(session, "CUSTOMERS_RAW")
    session.create_dataframe(rows, schema=["ID", "NAME", "EMAIL"]).write.save_as_table(
        table, mode="overwrite"
    )
    return table


def show_table(session, table, order_by, limit=None):
    """Prints a table from the client, to show what the server-side program wrote."""
    rows = session.table(table).sort(order_by)
    for row in (rows.limit(limit) if limit else rows).collect():
        say("  ", tuple(row))
