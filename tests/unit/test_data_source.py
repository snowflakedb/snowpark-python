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
        r"SELECT (?:(\w+)\.)?\* FROM (?:\((.*)\) \1|(.*)) WHERE 1 = 0", infer_sql, re.S
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
    """Queries without quotes, backslashes or newlines generate the same SQL as before."""
    _, literal = _render_jdbc_infer_schema_sql(table_or_query, is_query)
    # Same format as before the fix: the inference SQL wrapped in plain quotes.
    assert literal.startswith("'SELECT ") and literal.endswith(" WHERE 1 = 0'")
    assert "\\" not in literal and "''" not in literal
    assert _strip_infer_schema_wrapper(literal[1:-1]) == table_or_query


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
    "url, lower_literal",
    [
        ("jdbc:oracle:thin:@//host:1521/svc", "TIMESTAMP '2024-01-11 00:00:00'"),
        ("jdbc:postgresql://host/db", "'2024-01-11 00:00:00'"),
    ],
)
def test_jdbc_partitions_on_timestamp_column(url, lower_literal):
    """SNOW-4537771: TIMESTAMP partition columns (incl. Oracle DATE) were rejected."""
    client = JDBC(
        session=Mock(),
        url=url,
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
        column="D",
        lower_bound="2024-01-01",
        upper_bound="2024-01-31",
        num_partitions=3,
    )
    client.__dict__["schema"] = StructType(
        [StructField("D", TimestampType(TimestampTimeZone.NTZ))]
    )
    client.raw_schema = []
    partitions = client.partitions
    assert len(partitions) == 3
    assert partitions[1] == (
        f"SELECT * FROM T WHERE D >= {lower_literal} AND D < "
        + lower_literal.replace("2024-01-11", "2024-01-21")
    )


@pytest.mark.parametrize(
    "url, name, column, expected",
    [
        ("jdbc:postgresql://h/d", "MixedCase", "MixedCase", '"MixedCase"'),
        ("jdbc:postgresql://h/d", "a", '"a"', '"a"'),
        ("jdbc:mysql://h/d", "my col", '"my col"', "`my col`"),
        ("jdbc:mysql://h/d", "a`b", '"a`b"', "`a``b`"),
        ("jdbc:sqlserver://h", "a]b", '"a]b"', "[a]]b]"),
        ("jdbc:oracle:thin:@h", 'a"b', '"a""b"', '"a""b"'),
    ],
)
def test_jdbc_partition_source_identifiers(url, name, column, expected):
    client = JDBC(
        session=Mock(),
        url=url,
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
        column=column,
        lower_bound=0,
        upper_bound=10,
        num_partitions=2,
    )
    client.raw_schema = [(name, "INTEGER", "java.lang.Integer", 10, 0, True)]
    client.__dict__["schema"] = client.to_snow_type()
    assert f"WHERE {expected} < '5' OR {expected} is null" in client.partitions[0]


@pytest.mark.parametrize(
    "timezone", [TimestampTimeZone.NTZ, TimestampTimeZone.TZ, TimestampTimeZone.LTZ]
)
def test_jdbc_timestamp_partition_precision_and_offsets(timezone):
    client = JDBC(
        session=Mock(),
        url="jdbc:postgresql://h/d",
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
        column="D",
        lower_bound="2024-01-01 00:00:00.100000",
        upper_bound="2024-01-01 00:00:00.900000",
        num_partitions=2,
    )
    if timezone != TimestampTimeZone.NTZ:
        client.lower_bound += "+05:30"
        client.upper_bound += "+05:30"
    client.raw_schema = [("D", "TIMESTAMP", "java.sql.Timestamp", 0, 0, True)]
    client.__dict__["schema"] = StructType([StructField("D", TimestampType(timezone))])
    partitions = client.partitions
    assert len(partitions) == 2
    expected = (
        "2024-01-01 00:00:00.500000"
        if timezone == TimestampTimeZone.NTZ
        else "2023-12-31 18:30:00.500000+00:00"
    )
    assert expected in partitions[0]


@pytest.mark.parametrize(
    "timezone, bound",
    [
        (TimestampTimeZone.NTZ, "2024-01-01+00:00"),
        (TimestampTimeZone.TZ, "2024-01-01"),
        (TimestampTimeZone.LTZ, "2024-01-01"),
    ],
)
def test_timestamp_partition_rejects_ambiguous_offsets(timezone, bound):
    from snowflake.snowpark._internal.data_source.datasource_partitioner import (
        to_internal_value,
    )

    if timezone == TimestampTimeZone.NTZ:
        bound = "2024-01-01 00:00:00+00:00"
    with pytest.raises(ValueError, match="offset"):
        to_internal_value(bound, TimestampType(timezone))


def test_dbapi_partitioner_still_rejects_timestamp_column():
    from snowflake.snowpark._internal.data_source import DataSourcePartitioner

    with pytest.raises(ValueError, match="unsupported type"):
        DataSourcePartitioner.generate_partitions(
            "SELECT * FROM T",
            StructType([StructField("D", TimestampType())]),
            column="D",
            lower_bound="2024-01-01",
            upper_bound="2024-01-31",
            num_partitions=3,
        )
