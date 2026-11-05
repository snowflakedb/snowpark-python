#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import contextlib
import datetime
import re
import uuid
from typing import Any, List, Optional, Tuple

from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.utils import (
    escape_quotes_and_backslashes,
    split_snowflake_identifier_with_dot,
    validate_object_name,
)

# Appended to the CALL statements of procedure jobs so that ``jobs.list()`` can find them.
JOB_MARKER = "/* snowpark.jobs */"
BUNDLE_EXECUTION_PREFIX = "SNOWPARK_JOB_"
# Temporary code bundles created for runs on a compute pool. The prefix is reserved:
# cleanup drops bundles with names of exactly this shape.
TEMP_BUNDLE_PREFIX = "SNOWPARK_JOB_TMP_"
TEMP_BUNDLE_RE = re.compile(r"^SNOWPARK_JOB_TMP_[0-9A-F]{12}$")

STORED_PROCEDURE_KIND = "stored_procedure"
CODE_BUNDLE_KIND = "code_bundle"


def run_sql(session, statement: str) -> List[Any]:
    """Runs ``statement`` and returns its rows, without emitting AST."""
    return session.sql(statement, _emit_ast=False).collect(_emit_ast=False)


def run_sql_nowait(session, statement: str):
    """Starts ``statement`` without waiting and returns the ``AsyncJob``."""
    return session.sql(statement, _emit_ast=False).collect_nowait(_emit_ast=False)


_QUERY_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def validate_query_id(query_id: Any) -> str:
    """Returns the lower-case ``query_id`` or raises ``ValueError`` if it is not a UUID."""
    if not isinstance(query_id, str) or not _QUERY_ID_RE.fullmatch(query_id):
        raise ValueError(f"Invalid query id (expected a UUID): {query_id!r}")
    return query_id.lower()


def validate_fqn(name: Any, what: str = "name") -> str:
    """Raises ``ValueError`` unless ``name`` is a valid ``db.schema.object`` name."""
    name = validate_name(name, what)
    if len(split_name(name)) != 3:
        raise ValueError(f"Invalid {what}: {name!r} (expected db.schema.name)")
    return name


def to_epoch(value: Any) -> float:
    """Seconds since the epoch of a datetime (naive means UTC); 0 for ``None``."""
    if not isinstance(value, datetime.datetime):
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.timestamp()


def sql_str(value: str) -> str:
    """Renders ``value`` as a single-quoted SQL string literal."""
    return f"'{escape_quotes_and_backslashes(str(value))}'"


def random_suffix(n: int = 8) -> str:
    return uuid.uuid4().hex[:n].upper()


def identifier_safe(text: str, max_len: int = 40) -> str:
    """Uppercase, identifier-safe rendering of ``text``."""
    out = re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_").upper()
    if not out or out[0].isdigit():
        out = "J_" + out
    return out[:max_len]


def split_name(name: str) -> List[str]:
    return split_snowflake_identifier_with_dot(name.strip())


def validate_name(name: str, what: str = "name") -> str:
    """Raises ``ValueError`` unless ``name`` is a valid (possibly qualified) identifier."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Invalid {what}: {name!r}")
    try:
        validate_object_name(name.strip())
    except Exception as ex:
        raise ValueError(f"Invalid {what}: {name!r}") from ex
    return name.strip()


def qualify(session, name: str) -> str:
    """Returns ``db.schema.object`` for a possibly partially qualified ``name``."""
    name = validate_name(name)
    parts = split_name(name)
    if len(parts) > 3 or not parts or not all(parts):
        raise ValueError(f"Invalid object name: {name!r}")
    if len(parts) == 3:
        return ".".join(parts)
    schema = session.get_current_schema()
    database = session.get_current_database()
    if len(parts) == 2:
        if not database:
            raise ValueError(f"No current database to qualify {name!r}")
        return ".".join([database, *parts])
    if not database or not schema:
        raise ValueError(f"No current database/schema to qualify {name!r}")
    return ".".join([database, schema, parts[0]])


def split_fqn(fqn: str) -> Tuple[str, str, str]:
    parts = split_name(fqn)
    if len(parts) != 3:
        raise ValueError(f"Expected a fully qualified name, got {fqn!r}")
    return parts[0], parts[1], parts[2]


def unquote_ident(ident: str) -> str:
    if len(ident) >= 2 and ident[0] == '"' and ident[-1] == '"':
        return ident[1:-1].replace('""', '"')
    return ident.upper()


def require_warehouse(session, warehouse: Optional[str] = None) -> str:
    """The warehouse a job runs on: ``warehouse`` or else the session's current one."""
    wh = warehouse or session.get_current_warehouse()
    if not wh:
        raise ValueError(
            "The session has no current warehouse. Jobs run on the session's current "
            "warehouse unless the target sets warehouse=...: call "
            "session.use_warehouse(...) first."
        )
    return wh


def validate_warehouse(warehouse: Optional[str]) -> Optional[str]:
    if warehouse is None:
        return None
    warehouse = validate_name(warehouse, "warehouse")
    if len(split_name(warehouse)) != 1:
        raise ValueError(f"Invalid warehouse: {warehouse!r}")
    return warehouse


@contextlib.contextmanager
def using_warehouse(session, warehouse: Optional[str]):
    """Makes ``warehouse`` the session's current warehouse for the duration of the block.

    The previous warehouse is restored afterwards. If the session had none, the
    warehouse stays set, because a session cannot go back to having no warehouse.
    The switch is session-wide and visible to other threads using the session.
    """
    if not warehouse:
        yield
        return
    with session._lock:
        current = session.get_current_warehouse()
        if current and unquote_ident(current) == unquote_ident(warehouse):
            yield
            return
        session.use_warehouse(warehouse)
        try:
            yield
        finally:
            if current:
                session.use_warehouse(current)


def send_telemetry(session, verb: str, target_kind: str) -> None:
    try:
        session._conn._telemetry_client.send_function_usage_telemetry(
            f"JobManager.{verb}",
            TelemetryField.FUNC_CAT_USAGE.value,
            api_calls=[f"target:{target_kind}"],
        )
    except Exception:  # pragma: no cover - telemetry must never break the API
        pass


def get_event_table(session) -> Optional[str]:
    rows = run_sql(session, "SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT")
    if rows:
        value = rows[0]["value"]
        if value:
            return value
    return None


def fetch_event_logs(
    session,
    query_id: str,
    limit: int = -1,
    since: Optional[datetime.datetime] = None,
) -> str:
    """Formats the LOG records of the event table that belong to ``query_id``.

    A positive ``limit`` keeps the most recent ``limit`` records. The scan is bounded
    by ``since`` (minus 5 minutes) when it is known, otherwise by the last 7 days.
    """
    table = get_event_table(session)
    if not table:
        raise RuntimeError("No account event table is configured (EVENT_TABLE).")
    if since is not None:
        if since.tzinfo is None:
            since = since.replace(tzinfo=datetime.timezone.utc)
        start = since.astimezone(datetime.timezone.utc) - datetime.timedelta(minutes=5)
        lower = f"TIMESTAMP >= {sql_str(start.strftime('%Y-%m-%d %H:%M:%S'))}::TIMESTAMP_NTZ"
    else:
        lower = "TIMESTAMP >= DATEADD(day, -7, SYSDATE())"
    sql = (
        "SELECT TIMESTAMP, RECORD['severity_text']::STRING, VALUE::STRING "
        f"FROM {table} WHERE RECORD_TYPE = 'LOG' AND {lower} "
        f"AND RESOURCE_ATTRIBUTES['snow.query.id']::STRING = {sql_str(query_id)} "
    )
    if limit is not None and limit >= 0:
        sql += f"ORDER BY TIMESTAMP DESC LIMIT {int(limit)}"
    else:
        sql += "ORDER BY TIMESTAMP"
    rows = run_sql(session, sql)
    if limit is not None and limit >= 0:
        rows = list(reversed(rows))
    return "\n".join(f"{r[0]} {r[1]} {r[2]}" for r in rows)
