#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import re
from unittest.mock import Mock, patch
from snowflake.snowpark._internal.data_source.drivers.base_driver import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.jdbc import JDBC
from snowflake.snowpark._internal.data_source.utils import (
    DBMS_TYPE,
    resolve_udtf_packages,
)
from snowflake.snowpark.context import (
    _ANACONDA_SHARED_REPOSITORY,
    _PYPI_SHARED_REPOSITORY,
)
from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark.types import StructType, StructField, StringType


@pytest.mark.parametrize(
    "cursor_fails,conn_fails",
    [
        (True, False),  # cursor.close() fails
        (False, True),  # connection.close() fails
        (True, True),  # both fail
    ],
)
def test_close_error_handling(cursor_fails, conn_fails):
    """Test that errors during cursor/connection close are handled gracefully."""
    # Setup mock driver
    mock_create_connection = Mock()
    driver = BaseDriver(mock_create_connection, DBMS_TYPE.UNKNOWN)

    # Setup mocks
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_create_connection.return_value = mock_conn

    # Configure failures

    if conn_fails:
        mock_conn.close.side_effect = Exception("Connection close failed")
    if cursor_fails:
        mock_cursor.close.side_effect = Exception("Cursor close failed")

    # Mock schema inference to succeed
    expected_schema = StructType([StructField("test_col", StringType())])
    driver.infer_schema_from_description = Mock(return_value=expected_schema)

    # Test - should succeed despite close errors and log the failure
    with patch(
        "snowflake.snowpark._internal.data_source.drivers.base_driver.logger"
    ) as mock_logger:
        result = driver.infer_schema_from_description_with_error_control(
            "test_table", False, "mock_query_input_alias"
        )

        assert result == expected_schema

        # Use regex to match the log message flexibly
        mock_logger.debug.assert_called()
        args, kwargs = mock_logger.debug.call_args
        assert re.search(r"Failed to close", args[0])


@pytest.mark.parametrize(
    "cursor_fails,conn_fails",
    [
        (True, False),  # cursor.close() fails
        (False, True),  # connection.close() fails
        (True, True),  # both fail
    ],
)
def test_datasource_reader_close_error_handling(cursor_fails, conn_fails):
    """Test that DataSourceReader handles cursor/connection close errors gracefully."""
    # Setup mocks
    mock_create_connection = Mock()
    expected_schema = StructType([StructField("test_col", StringType())])

    # Mock driver, connection, and cursor
    mock_driver = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()

    # Configure the mock chain
    mock_driver.prepare_connection.return_value = mock_conn
    mock_driver.create_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_driver.get_server_cursor_if_supported.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("test_data",)]

    # Configure failures
    if cursor_fails:
        mock_cursor.close.side_effect = Exception("Cursor close failed")
    if conn_fails:
        mock_conn.close.side_effect = Exception("Connection close failed")

    # Create mock driver class that returns our mock driver
    mock_driver_class = Mock(return_value=mock_driver)

    # Create reader with the mock driver class
    reader = DataSourceReader(
        driver_class=mock_driver_class,
        create_connection=mock_create_connection,
        schema=expected_schema,
        dbms_type=DBMS_TYPE.UNKNOWN,
        fetch_size=0,  # Use 0 to trigger fetchall() path
        query_timeout=0,
        session_init_statement=None,
        fetch_merge_count=1,
    )

    # Test - should succeed despite close errors and log the failure
    with patch(
        "snowflake.snowpark._internal.data_source.datasource_reader.logger"
    ) as mock_logger:
        # Consume the generator to trigger execution of the finally block
        list(reader.read("SELECT * FROM test"))

        mock_logger.debug.assert_called()
        args, kwargs = mock_logger.debug.call_args
        assert re.search(r"Failed to close", args[0])


@pytest.mark.parametrize(
    "dbms_type,expected_fragment",
    [
        (DBMS_TYPE.ORACLE_DB, "oracledb"),
        (DBMS_TYPE.POSTGRES_DB, "psycopg2-binary"),
        (DBMS_TYPE.MYSQL_DB, "pymysql"),
        (DBMS_TYPE.SQLITE_DB, "snowflake-snowpark-python"),
    ],
)
def test_resolve_udtf_packages_pypi_supported(dbms_type, expected_fragment):
    """PyPI-supported DBMSes return a package list with the expected dependency."""
    packages = resolve_udtf_packages(dbms_type, _PYPI_SHARED_REPOSITORY)
    assert any(expected_fragment in p for p in packages), (
        f"Expected to find a package matching '{expected_fragment}' in " f"{packages}"
    )


@pytest.mark.parametrize(
    "dbms_type",
    [DBMS_TYPE.SQL_SERVER_DB, DBMS_TYPE.DATABRICKS_DB],
)
def test_resolve_udtf_packages_pypi_unsupported_raises(dbms_type):
    """SQL Server and Databricks have no PyPI-installable package set, so
    ``resolve_udtf_packages`` raises a ``SnowparkClientException`` pointing
    the user to the Anaconda artifact repository."""
    with pytest.raises(SnowparkClientException) as exc_info:
        resolve_udtf_packages(dbms_type, _PYPI_SHARED_REPOSITORY)

    message = str(exc_info.value)
    assert dbms_type.value in message


@pytest.mark.parametrize(
    "dbms_type,expected_fragment",
    [
        (DBMS_TYPE.ORACLE_DB, "oracledb"),
        # On Anaconda we ship plain ``psycopg2`` (the conda build bundles
        # libpq), in contrast to the PyPI variant which uses
        # ``psycopg2-binary``.
        (DBMS_TYPE.POSTGRES_DB, "psycopg2>="),
        (DBMS_TYPE.MYSQL_DB, "pymysql"),
        (DBMS_TYPE.SQLITE_DB, "snowflake-snowpark-python"),
        (DBMS_TYPE.SQL_SERVER_DB, "msodbcsql"),
        (DBMS_TYPE.DATABRICKS_DB, "databricks-sql-connector"),
    ],
)
def test_resolve_udtf_packages_anaconda(dbms_type, expected_fragment):
    """On the Anaconda repository every DBMS has a working package list,
    including SQL Server and Databricks."""
    packages = resolve_udtf_packages(dbms_type, _ANACONDA_SHARED_REPOSITORY)
    assert packages is not None
    assert any(expected_fragment in p for p in packages), (
        f"Expected to find a package matching '{expected_fragment}' in " f"{packages}"
    )


def _render_jdbc_ddls(**jdbc_kwargs):
    """Render the read UDTF DDL without a live session."""
    session = Mock()
    captured = []
    session.sql.side_effect = lambda sql, **kwargs: (captured.append(sql), Mock())[1]

    client = JDBC(
        session=session,
        url="jdbc:postgresql://host:5432/db",
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
        **jdbc_kwargs,
    )
    # Pin the schema so read() does not need to call the infer-schema UDTF.
    client.__dict__["schema"] = StructType(
        [StructField("A", StringType()), StructField("B", StringType())]
    )
    # read() registers the UDTF (the DDL we want) and then builds a DataFrame
    # from the mocked session, which fails on a Mock. The DDL has already been
    # captured by that point, so the tail of read() is irrelevant here.
    with pytest.raises(TypeError):
        client.read("SNOWPARK_TEMP_TABLE_X")
    assert captured, "expected read() to register the UDTF before failing"
    return captured[0]


def test_jdbc_read_udtf_executes_query_lazily():
    """The read UDTF must not touch the database until the returned Stream is pulled.

    Snowflake's Java UDF handler timeout (``JAVA_UDF_HANDLER_TIMEOUT_SECONDS``,
    600s by default) only measures the time until ``process()`` returns. Running
    ``executeQuery`` eagerly charges the external source's whole query time
    against that budget, so a slow but healthy source query is killed at 600s.
    """
    ddl = _render_jdbc_ddls(query_timeout=240, fetch_size=100)

    # The statement work must be emitted *after* the Stream.generate call, i.e.
    # inside the lazily evaluated supplier. Match the full call sites rather than
    # bare identifiers, which also appear in the surrounding comments.
    assert ddl.index("stmt.executeQuery(query)") > ddl.index("Stream.generate")
    assert ddl.index("this.conn.createStatement()") > ddl.index("Stream.generate")
    assert ddl.index("stmt.setQueryTimeout(") > ddl.index("Stream.generate")

    # Lambda capture requires effectively-final holders.
    assert "final Statement[] stmtRef" in ddl
    assert "final ResultSet[] rsRef" in ddl

    # The parameters must still be applied, and the timeout must precede the
    # session-init statements so they inherit it.
    assert "stmt.setQueryTimeout(240);" in ddl
    assert "stmt.setFetchSize(100);" in ddl


def _render_jdbc_infer_schema_sql(table_or_query, is_query=True):
    """Return the SQL that runs the infer-schema UDTF, without a live session."""
    session = Mock()
    captured = []

    def fake_sql(sql, **kwargs):
        captured.append(sql)
        result = Mock()
        result.collect.return_value = []
        return result

    session.sql.side_effect = fake_sql
    client = JDBC(
        session=session,
        url="jdbc:postgresql://host:5432/db",
        table_or_query=table_or_query,
        external_access_integration="EAI",
        imports=[],
        is_query=is_query,
        secret="cred",
    )
    client.schema
    # captured[0] registers the UDTF, captured[1] calls it.
    infer_sql = captured[1]
    match = re.fullmatch(r"SELECT \* FROM TABLE\((\w+)\((.*)\)\)", infer_sql, re.S)
    assert match, infer_sql
    return client, match.group(2)


def _strip_infer_schema_wrapper(infer_sql):
    """Recover the user table/query from JDBC.infer_schema_sql() output."""
    match = re.fullmatch(
        r"SELECT (?:(\w+)\.)?\* FROM (?:\((.*)\n\) \1|(.*)) WHERE 1 = 0",
        infer_sql,
        re.S,
    )
    assert match, infer_sql
    return match.group(2) if match.group(2) is not None else match.group(3)


def _decode_sql_string_literal(literal):
    """Decode a single-quoted Snowflake string literal the way Snowflake does."""
    assert literal.startswith("'") and literal.endswith("'"), literal
    body, out, i = literal[1:-1], [], 0
    while i < len(body):
        ch = body[i]
        if ch == "\\":
            nxt = body[i + 1]
            out.append({"n": "\n", "t": "\t", "\\": "\\", "'": "'"}.get(nxt, nxt))
            i += 2
        elif ch == "'":
            assert body[i + 1] == "'", f"unescaped quote in {literal}"
            out.append("'")
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM T WHERE STATUS = 'P'",
        "SELECT * FROM T WHERE NAME = 'O''Brien'",
        "SELECT * FROM T WHERE PATH LIKE 'a\\b%'",
        "SELECT *\nFROM T\nWHERE STATUS = 'P'",
        "SELECT ''''",
    ],
)
def test_jdbc_infer_schema_sql_escapes_query(query):
    """SNOW-4522827: the infer-schema UDTF must receive the user query verbatim."""
    _, literal = _render_jdbc_infer_schema_sql(query)
    assert _strip_infer_schema_wrapper(_decode_sql_string_literal(literal)) == query


@pytest.mark.parametrize(
    "table_or_query,is_query",
    [("SELECT A, B FROM T", True), ("MY_SCHEMA.MY_TABLE", False)],
)
def test_jdbc_infer_schema_sql_unchanged_without_special_chars(
    table_or_query, is_query
):
    """Queries without quotes or backslashes are not double-escaped."""
    _, literal = _render_jdbc_infer_schema_sql(table_or_query, is_query)
    assert literal.startswith("'SELECT ") and literal.endswith(" WHERE 1 = 0'")
    assert "''" not in literal
    # Only the newline before the closing parenthesis of a query is escaped.
    assert literal.count("\\") == (1 if is_query else 0)
    decoded = _decode_sql_string_literal(literal)
    assert _strip_infer_schema_wrapper(decoded) == table_or_query


def test_jdbc_read_casts_partition_argument_to_varchar():
    """The partition table can inherit DEFAULT_DDL_COLLATION; the collated column
    must be cast to plain VARCHAR to match the read UDTF's `query VARCHAR`."""
    session = Mock()
    captured = []
    session.sql.side_effect = lambda sql, **kwargs: (captured.append(sql), Mock())[1]
    client = JDBC(
        session=session,
        url="jdbc:postgresql://host:5432/db",
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
    )
    client.__dict__["schema"] = StructType([StructField("A", StringType())])
    with pytest.raises(TypeError):
        client.read("SNOWPARK_TEMP_TABLE_X")
    select = next(
        sql for sql in captured if "table(" in sql.lower() and "select result" in sql
    )
    assert "(partition::VARCHAR)) AS result" in select


@pytest.mark.parametrize(
    "query",
    ["SELECT 1 AS s", "SELECT 1 AS s;", "  SELECT 1 AS s ; \n", "SELECT 1 AS s;;"],
)
def test_jdbc_query_trailing_semicolon_is_stripped(query):
    """SNOW-4537775: a trailing ';' is a syntax error inside the wrapping subquery."""
    client = JDBC(
        session=Mock(),
        url="jdbc:postgresql://host/db",
        table_or_query=query,
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
    )
    assert client.table_or_query == "SELECT 1 AS s"
    client.raw_schema = []
    assert re.match(
        r"SELECT (\w+)\.\* FROM \(SELECT 1 AS s\n\) \1$", client.generate_select_sql()
    )


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT 1; -- note", "SELECT 1 -- note"),
        ("SELECT 1; /* note */", "SELECT 1 /* note */"),
        (
            "SELECT 1; /* outer /* inner */ outer */ -- end",
            "SELECT 1 /* outer /* inner */ outer */ -- end",
        ),
        ("SELECT 1; \n ; -- end", "SELECT 1 \n  -- end"),
        (
            "SELECT '; -- not a comment' AS s; -- end",
            "SELECT '; -- not a comment' AS s -- end",
        ),
        ('SELECT 1 AS "x; -- y"; -- end', 'SELECT 1 AS "x; -- y" -- end'),
        ("SELECT 1 AS `x; -- y`; -- end", "SELECT 1 AS `x; -- y` -- end"),
        ("SELECT 1 AS [x; -- y]; -- end", "SELECT 1 AS [x; -- y] -- end"),
        ("SELECT $tag$; -- value$tag$; -- end", "SELECT $tag$; -- value$tag$ -- end"),
        (
            "SELECT q'[it's; -- text]' FROM DUAL; -- end",
            "SELECT q'[it's; -- text]' FROM DUAL -- end",
        ),
        ("SELECT 1; SELECT 2; -- end", "SELECT 1; SELECT 2 -- end"),
        ("SELECT '/* not comment; */'", "SELECT '/* not comment; */'"),
    ],
)
def test_jdbc_semicolon_before_comments(query, expected):
    client = JDBC(
        session=Mock(),
        url="jdbc:postgresql://host/db",
        table_or_query=query,
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
    )
    assert client.table_or_query == expected


def test_jdbc_query_trailing_comment_does_not_hide_closing_paren():
    client = JDBC(
        session=Mock(),
        url="jdbc:postgresql://host/db",
        table_or_query="SELECT 1 AS s -- note",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
    )
    client.raw_schema = []
    for sql in (client.generate_select_sql(), client.infer_schema_sql()):
        assert "-- note\n) " in sql
