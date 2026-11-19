#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import re
from unittest.mock import Mock, patch
from snowflake.snowpark._internal.data_source.drivers.base_driver import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.jdbc import JDBC, to_java_string_literal
from snowflake.snowpark._internal.data_source.utils import (
    DBMS_TYPE,
    resolve_udtf_packages,
)
from snowflake.snowpark.context import (
    _ANACONDA_SHARED_REPOSITORY,
    _PYPI_SHARED_REPOSITORY,
)
from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)


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
    assert ddl.index("getConnection().createStatement()") > ddl.index("Stream.generate")
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


@pytest.mark.parametrize(
    "url, expected",
    [
        ("jdbc:postgresql://u:pw@host:5432/db", "jdbc:postgresql://host:5432/db"),
        (
            "jdbc:postgresql://host:5432/db?user=u&password=pw",
            "jdbc:postgresql://host:5432/db",
        ),
        (
            "jdbc:postgresql://host:5432/db?sslmode=require&password=pw&ssl=true",
            "jdbc:postgresql://host:5432/db?sslmode=require&ssl=true",
        ),
        (
            "jdbc:mysql://host:3306/db?PASSWORD=pw&useSSL=true",
            "jdbc:mysql://host:3306/db?useSSL=true",
        ),
        (
            "jdbc:sqlserver://host:1433;databaseName=d;user=u;password=pw;encrypt=true",
            "jdbc:sqlserver://host:1433;databaseName=d;encrypt=true",
        ),
        ("jdbc:oracle:thin:u/pw@//host:1521/svc", "jdbc:oracle:thin:@//host:1521/svc"),
        ("jdbc:oracle:thin:@//host:1521/svc", "jdbc:oracle:thin:@//host:1521/svc"),
        (
            "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(HOST=h)(PORT=1521)))",
            "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(HOST=h)(PORT=1521)))",
        ),
    ],
)
def test_jdbc_secret_detector_strips_url_credentials(url, expected):
    """SNOW-4537765: the URL is embedded in the UDTF DDL, so credentials in any
    common JDBC URL form must be removed while other parameters are kept."""
    client = JDBC(
        session=Mock(),
        url=url,
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
    )
    assert client.url == expected


@pytest.mark.parametrize(
    "value, literal",
    [
        ("plain", '"plain"'),
        ('a"b', '"a\\"b"'),
        ("a\\b", '"a\\\\b"'),
        ("l1\nl2\r\tx", '"l1\\nl2\\r\\tx"'),
        ("a$$b", '"a\\u0024\\u0024b"'),
        ("\\u0022", '"\\\\u0022"'),
        ("日本", '"日本"'),
    ],
)
def test_to_java_string_literal(value, literal):
    assert to_java_string_literal(value) == literal


def test_jdbc_user_strings_are_java_escaped():
    """SNOW-4537768: url, properties and session_init_statement are written into
    Java source, so they must be escaped rather than pasted raw."""
    ddl = _render_jdbc_ddls(
        properties={"ApplicationName": 'snow"park$$'},
        session_init_statement=['SET search_path TO "public"'],
    )
    assert 'properties.put("ApplicationName", "snow\\"park\\u0024\\u0024");' in ddl
    assert 'stmt.execute("SET search_path TO \\"public\\"");' in ddl
    # Only the two DDL delimiters remain.
    assert ddl.count("$$") == 2


def test_jdbc_udtf_uses_positional_column_names():
    """SNOW-4537772: source names never appear as Java identifiers or UDTF columns."""
    session = Mock()
    captured = []
    session.sql.side_effect = lambda sql, **kwargs: (captured.append(sql), Mock())[1]
    client = JDBC(
        session=session,
        url="jdbc:postgresql://host:5432/db",
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
    )
    client.raw_schema = [
        ("my col", "INTEGER", "java.lang.Integer", 10, 0, True),
        ("class", "VARCHAR", "java.lang.String", 10, 0, True),
    ]
    client.__dict__["schema"] = client.to_snow_type()
    assert client.generate_select_sql() == "SELECT * FROM T"
    assert client.create_output_row_class() == "public String C0;\npublic String C1;\n"
    assert "row.C1 = rs.getString(2);" in client.create_output_row_java_code()


def _jdbc_reader():
    from snowflake.snowpark.dataframe_reader import DataFrameReader

    reader = DataFrameReader.__new__(DataFrameReader)
    reader._session = Mock()
    return reader


@pytest.mark.parametrize(
    "missing", ["external_access_integration", "secret", "imports"]
)
def test_jdbc_missing_udtf_config_raises_value_error(missing):
    """SNOW-4537774: a missing key used to raise a bare KeyError for imports."""
    configs = {"external_access_integration": "E", "secret": "S", "imports": ["@s/j"]}
    del configs[missing]
    with pytest.raises(ValueError, match="must be specified in udtf configs"):
        _jdbc_reader().jdbc(url="jdbc:x://h/d", udtf_configs=configs, query="q")


@pytest.mark.parametrize("num_partitions", [0, -1, 2.5, "3", True])
def test_jdbc_rejects_invalid_num_partitions(num_partitions):
    configs = {"external_access_integration": "E", "secret": "S", "imports": ["@s/j"]}
    with pytest.raises(ValueError, match="num_partitions must be a positive integer"):
        _jdbc_reader().jdbc(
            url="jdbc:x://h/d",
            udtf_configs=configs,
            query="q",
            column="ID",
            lower_bound=0,
            upper_bound=10,
            num_partitions=num_partitions,
        )


@pytest.mark.parametrize(
    "packages, expected",
    [
        (
            ["org.example:lib:1.0"],
            ["org.example:lib:1.0", "com.snowflake:snowpark:latest"],
        ),
        (["com.snowflake:snowpark:1.15.0"], ["com.snowflake:snowpark:1.15.0"]),
        (["com.snowflake:snowpark"], ["com.snowflake:snowpark"]),
    ],
)
def test_jdbc_packages_not_mutated_and_pin_respected(packages, expected):
    original = list(packages)
    client = JDBC(
        session=Mock(),
        url="jdbc:postgresql://host/db",
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
        packages=packages,
    )
    assert packages == original
    assert client.packages == expected


def test_jdbc_custom_schema_unknown_column_raises():
    session = Mock()
    client = JDBC(
        session=session,
        url="jdbc:postgresql://host/db",
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="cred",
        custom_schema="ID INTEGER, NOPE STRING",
    )
    session.sql.return_value.collect.return_value = [
        ("ID", "INTEGER", "java.lang.Integer", 10, 0, True)
    ]
    with pytest.raises(ValueError, match="NOPE"):
        client.schema


@pytest.mark.parametrize(
    "precision, scale, expected",
    [
        (0, -127, DecimalType(38, 18)),  # Oracle unconstrained NUMBER
        (126, -127, DoubleType()),  # Oracle FLOAT
        (0, 0, DecimalType(38, 18)),  # Postgres numeric without precision
        (10, 2, DecimalType(10, 2)),
        (50, 10, DecimalType(38, 10)),  # precision above 38 keeps the scale
        (5, -2, DecimalType(38, 0)),  # Oracle NUMBER(5, -2) is an integer
        (None, None, DecimalType(38, 0)),
    ],
)
def test_jdbc_numeric_type_mapping(precision, scale, expected):
    """SNOW-4537763: unconstrained numerics used to be truncated to DECIMAL(38, 0)."""
    client = JDBC(
        session=Mock(),
        url="jdbc:oracle:thin:@host:1521/db",
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
    )
    client.raw_schema = [
        ("N", "NUMERIC", "java.math.BigDecimal", precision, scale, True)
    ]
    assert client.to_snow_type().fields[0].datatype == expected


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
    assert re.match(
        r"SELECT (\w+)\.\* FROM \(SELECT 1 AS s\n\) \1$", client.generate_select_sql()
    )


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
    for sql in (client.generate_select_sql(), client.infer_schema_sql()):
        assert "-- note\n) " in sql


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


@pytest.mark.parametrize("failure", ["save", "read", "projection", "telemetry", None])
@pytest.mark.parametrize("cleanup_fails", [False, True])
@pytest.mark.parametrize("use_scoped", [False, True])
def test_jdbc_reader_cleans_up_failed_setup(failure, cleanup_fails, use_scoped):
    reader = _jdbc_reader()
    reader._session._use_scoped_temp_objects = use_scoped
    client = Mock()
    client.partitions = ["SELECT 1"]
    operations = {
        "save": reader._session.create_dataframe.return_value.write.save_as_table,
        "read": client.read,
        "projection": client.to_result_snowpark_df,
        "telemetry": reader._session._conn._telemetry_client.send_data_source_perf_telemetry,
    }
    if failure:
        operations[failure].side_effect = RuntimeError("original failure")
    if cleanup_fails:

        def sql(text, **kwargs):
            if text.startswith("DROP"):
                raise RuntimeError("cleanup failure")
            return Mock()

        reader._session.sql.side_effect = sql
    with patch("snowflake.snowpark.dataframe_reader.JDBC", return_value=client):
        if failure:
            with pytest.raises(RuntimeError, match="original failure"):
                reader.jdbc(
                    url="jdbc:postgresql://h/d",
                    query="SELECT 1",
                    udtf_configs={
                        "external_access_integration": "E",
                        "secret": "S",
                        "imports": [],
                    },
                )
            client.cleanup_read_udtf.assert_called_once()
            assert reader._session.sql.call_args.args[0].startswith(
                "DROP TABLE IF EXISTS "
            )
        else:
            reader.jdbc(
                url="jdbc:postgresql://h/d",
                query="SELECT 1",
                udtf_configs={
                    "external_access_integration": "E",
                    "secret": "S",
                    "imports": [],
                },
            )
            client.cleanup_read_udtf.assert_not_called()
            reader._session.sql.assert_called_once()
        temp_type = "SCOPED TEMPORARY" if use_scoped else "TEMPORARY"
        assert (
            reader._session.sql.call_args_list[0]
            .args[0]
            .startswith(f"CREATE {temp_type} TABLE ")
        )


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_jdbc_read_projection_failure_drops_function(cleanup_fails):
    session = Mock()
    executed = []

    def sql(text, **kwargs):
        executed.append(text)
        if text.startswith("DROP") and cleanup_fails:
            raise RuntimeError("cleanup failure")
        return Mock()

    session.sql.side_effect = sql
    client = JDBC(
        session=session,
        url="jdbc:postgresql://h/d",
        table_or_query="T",
        external_access_integration="E",
        imports=[],
        is_query=False,
        secret="S",
    )
    client.__dict__["schema"] = StructType([StructField("A", StringType())])
    with pytest.raises(TypeError):
        client.read("P")
    assert executed[-1].startswith("DROP FUNCTION IF EXISTS ")


@pytest.mark.parametrize("inference_fails", [False, True])
@pytest.mark.parametrize(
    "use_scoped, temp_type", [(True, "SCOPED TEMPORARY"), (False, "TEMPORARY")]
)
def test_jdbc_infer_schema_udtf_is_dropped(inference_fails, use_scoped, temp_type):
    """SNOW-4537773: the inference UDTF used to live until the session ended."""
    session = Mock()
    session._use_scoped_temp_objects = use_scoped
    executed = []

    def sql(text, **kwargs):
        executed.append(text)
        result = Mock()
        if text.startswith("SELECT * FROM TABLE(") and inference_fails:
            result.collect.side_effect = RuntimeError("source down")
        else:
            result.collect.return_value = [
                ("ID", "INTEGER", "java.lang.Integer", 10, 0, True)
            ]
        return result

    session.sql.side_effect = sql
    client = JDBC(
        session=session,
        url="jdbc:postgresql://host/db",
        table_or_query="T",
        external_access_integration="EAI",
        imports=[],
        is_query=False,
        secret="cred",
    )
    if inference_fails:
        with pytest.raises(RuntimeError, match="source down"):
            client.schema
    else:
        client.schema
    create, _, drop = executed
    name = re.search(rf"CREATE OR REPLACE {temp_type} FUNCTION (\w+)\(", create)
    assert name, create
    assert drop == f"DROP FUNCTION IF EXISTS {name.group(1)}(VARCHAR)"
