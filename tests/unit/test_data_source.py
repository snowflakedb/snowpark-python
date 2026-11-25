#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import re
import shutil
import subprocess
from pathlib import Path
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


@pytest.fixture(scope="module")
def compiled_jdbc_handler(tmp_path_factory):
    """Execute the generated ingestion handler against JDBC proxies, without Snowflake."""
    if not shutil.which("javac") or not shutil.which("java"):
        pytest.skip("JDK required to execute generated JDBC handlers")
    directory = tmp_path_factory.mktemp("jdbc_read")
    with patch.object(
        JDBC,
        "generate_create_connection",
        return_value="return JdbcHandlerHarness.connect();",
    ):
        ddl = _render_jdbc_ddls()
    source = ddl.split("$$")[1]
    source = re.sub(r"import com\.snowflake\.[^;]+;", "", source)
    (directory / "DataLoader.java").write_text(source)
    harness = (
        Path(__file__).parents[1]
        / "resources/test_data_source_dir/JdbcHandlerHarness.java"
    )
    shutil.copy2(harness, directory)
    result = subprocess.run(
        ["javac", str(directory / "DataLoader.java"), str(directory / harness.name)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return directory


@pytest.mark.parametrize("failure", ["", "close"])
def test_jdbc_handler_exhaustion_and_close_errors(compiled_jdbc_handler, failure):
    directory = compiled_jdbc_handler
    result = subprocess.run(
        [
            "java",
            "-cp",
            str(directory),
            "JdbcHandlerHarness",
            "exhaust",
            failure,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "scenario", ["repeat", "partial", "end", "interleaved", "interleaved-error"]
)
def test_jdbc_handler_lifecycle(compiled_jdbc_handler, scenario):
    directory = compiled_jdbc_handler
    result = subprocess.run(
        ["java", "-cp", str(directory), "JdbcHandlerHarness", scenario],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "failure",
    [
        "createStatement",
        "setQueryTimeout",
        "setFetchSize",
        "executeQuery",
        "next",
        "getString",
    ],
)
def test_jdbc_handler_error_cleanup(compiled_jdbc_handler, failure):
    directory = compiled_jdbc_handler
    result = subprocess.run(
        [
            "java",
            "-cp",
            str(directory),
            "JdbcHandlerHarness",
            "error",
            failure,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


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
    "descriptor",
    [
        '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=host)(PORT=1521))(SECURITY=(SSL_SERVER_CERT_DN="CN=host,OU=engineering (test),O=company")))',
        '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME="service@host?name")))',
    ],
)
def test_jdbc_security_preserves_quoted_descriptor_leaf(descriptor):
    url = "jdbc:oracle:thin:@" + descriptor
    assert _jdbc_security_client(url).url == url


@pytest.mark.parametrize(
    "descriptor",
    [
        '(DESCRIPTION=(PASSWORD="SYNTHETIC_CREDENTIAL"))',
        '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME="SYNTHETIC_CREDENTIAL)))',
        '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME="svc"SYNTHETIC_CREDENTIAL)))',
        "(DESCRIPTION=(HOST=host))SYNTHETIC_CREDENTIAL",
        "(DESCRIPTION=(HOST=host)",
    ],
)
def test_jdbc_security_rejects_ambiguous_descriptor(descriptor):
    session = Mock()
    with pytest.raises(ValueError) as error:
        _jdbc_security_client("jdbc:oracle:thin:@" + descriptor, session=session)
    assert "SYNTHETIC_CREDENTIAL" not in str(error.value)
    session.sql.assert_not_called()


def _jdbc_security_client(url="jdbc:postgresql://host/db", **kwargs):
    return JDBC(
        session=kwargs.pop("session", Mock()),
        url=url,
        table_or_query="SELECT 1",
        external_access_integration="EAI",
        imports=[],
        is_query=True,
        secret="DB.SCHEMA.CREDENTIAL",
        **kwargs,
    )


@pytest.mark.parametrize(
    "url,expected",
    [
        (
            "jdbc:postgresql://host/db?password=LEFT;RIGHT&sslmode=require",
            "jdbc:postgresql://host/db?sslmode=require",
        ),
        (
            "jdbc:mysql://host/db?password=LEFT;RIGHT&useSSL=true",
            "jdbc:mysql://host/db?useSSL=true",
        ),
        (
            "jdbc:mysql://host/db?%75ser=LEFT&%70assword=RIGHT&useSSL=true",
            "jdbc:mysql://host/db?useSSL=true",
        ),
        (
            "jdbc:sqlserver://host; password = {LEFT}};RIGHT&TAIL};encrypt=true",
            "jdbc:sqlserver://host;encrypt=true",
        ),
        (
            "jdbc:sqlserver://host;password=LEFT&RIGHT;encrypt=true",
            "jdbc:sqlserver://host;encrypt=true",
        ),
        (
            "jdbc:sqlserver://host;password=LEFT;password=RIGHT;encrypt=true",
            "jdbc:sqlserver://host;encrypt=true",
        ),
        (
            "jdbc:oracle:thin:u/LEFT@host:1521:SID?password=RIGHT&connectTimeout=10",
            "jdbc:oracle:thin:@host:1521:SID?connectTimeout=10",
        ),
    ],
)
def test_jdbc_security_complete_credentials(url, expected):
    client = _jdbc_security_client(url)
    assert client.url == expected
    client.secret_detector()
    assert client.url == expected


_JDBC_CREDENTIAL_ALIASES = [
    "user",
    "username",
    "password",
    "pwd",
    "token",
    "accesskey",
    "secret",
    "apikey",
    "sslpassword",
    "password1",
    "password2",
    "password3",
    "certKeyStorePass",
    "clientCertificateKeyStorePassword",
    "trustCertificateKeyStorePassword",
    "accessToken",
    "trustStorePassword",
    "keyStoreSecret",
    "clientKeyPassword",
    "keyVaultProviderClientKey",
    "AADSecurePrincipalSecret",
    "oracle.jdbc.password",
    "oracle.jdbc.user",
    "oracle.jdbc.accessToken",
    "newPassword",
    "oracle.jdbc.newPassword",
    "clientSecret",
    "oracle.jdbc.clientSecret",
    "clientCertificatePassword",
    "oracle.jdbc.clientCertificatePassword",
    "oracle.net.wallet_password",
    "websocketPassword",
    "oracle.net.websocketPassword",
    "javax.net.ssl.keyStorePassword",
    "javax.net.ssl.trustStorePassword",
    "oracle.net.ldap.ssl.keyStorePassword",
    "oracle.net.ldap.ssl.trustStorePassword",
    "oracle.net.ldap.ssl.walletPassword",
    "oracle.net.ldap.security.credentials",
    "oracle.jdbc.ons.walletpassword",
]


@pytest.mark.parametrize(
    "key",
    [
        "%70assword",
        "%75ser",
        " password ",
        "%20password%20",
        "+password+",
        "\tPaSsWoRd\t",
    ],
)
@pytest.mark.parametrize(
    "base,separator",
    [
        ("jdbc:postgresql://host/db", "?"),
        ("jdbc:mysql://host/db", "?"),
        ("jdbc:sqlserver://host", ";"),
        ("jdbc:oracle:thin:@//host/svc", "?"),
    ],
)
def test_jdbc_security_encoded_and_whitespace_url_names(key, base, separator):
    assert (
        _jdbc_security_client(base + separator + key + "=SYNTHETIC_CREDENTIAL").url
        == base
    )


@pytest.mark.parametrize("key", _JDBC_CREDENTIAL_ALIASES)
@pytest.mark.parametrize(
    "spelling",
    [
        str,
        str.upper,
        lambda key: " \t" + key + " \t",
        lambda key: "%" + format(ord(key[0]), "02x") + key[1:],
    ],
)
def test_jdbc_security_alias_properties(key, spelling):
    properties = {
        spelling(key): "SYNTHETIC_CREDENTIAL",
        "applicationName": "password-manager",
        "connectTimeout": 10,
    }
    original = properties.copy()
    client = _jdbc_security_client(properties=properties)
    assert properties == original
    assert client.properties == {
        "applicationName": "password-manager",
        "connectTimeout": 10,
    }
    assert "SYNTHETIC_CREDENTIAL" not in client.generate_create_connection()


@pytest.mark.parametrize("key", _JDBC_CREDENTIAL_ALIASES)
@pytest.mark.parametrize(
    "base,separator",
    [
        ("jdbc:postgresql://host/db", "?"),
        ("jdbc:mysql://host/db", "?"),
        ("jdbc:sqlserver://host", ";"),
        ("jdbc:oracle:thin:@//host/svc", "?"),
    ],
)
def test_jdbc_security_alias_urls(key, base, separator):
    assert (
        _jdbc_security_client(base + separator + key + "=SYNTHETIC_CREDENTIAL").url
        == base
    )


@pytest.mark.parametrize(
    "url",
    [
        "jdbc:unknown://host/db?credential=SYNTHETIC_CREDENTIAL",
        "jdbc:mysql://host/db?password=SYNTHETIC_CREDENTIAL%ZZ",
        "jdbc:postgresql://host/db?password=SYNTHETIC_CREDENTIAL%",
        "jdbc:mysql://host/db?%2570assword=SYNTHETIC_CREDENTIAL",
        "jdbc:mysql://host/db?password:SYNTHETIC_CREDENTIAL",
        "jdbc:postgresql://host/db;password=SYNTHETIC_CREDENTIAL",
        "jdbc:sqlserver://host;password={SYNTHETIC_CREDENTIAL",
        "jdbc:sqlserver://host;password={LEFT}SYNTHETIC_CREDENTIAL",
        "jdbc:sqlserver://host;password=LEFT;SYNTHETIC_CREDENTIAL",
        'jdbc:oracle:thin:@//host/svc?password="SYNTHETIC_CREDENTIAL',
        "jdbc:oracle:thin:u/LEFT@SYNTHETIC_CREDENTIAL@//host/svc",
        "jdbc:oracle:thin:@(DESCRIPTION=(PASSWORD=SYNTHETIC_CREDENTIAL))",
        None,
        123,
        b"jdbc:mysql://host/db?password=SYNTHETIC_CREDENTIAL",
    ],
)
def test_jdbc_security_rejects_before_sql(url):
    session = Mock()
    with pytest.raises(ValueError) as error:
        _jdbc_security_client(url, session=session).schema
    assert "SYNTHETIC_CREDENTIAL" not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    session.sql.assert_not_called()


@pytest.mark.parametrize(
    "properties",
    [
        [],
        "SYNTHETIC_CREDENTIAL",
        {1: "SYNTHETIC_CREDENTIAL"},
        {None: "SYNTHETIC_CREDENTIAL"},
        {b"password": "SYNTHETIC_CREDENTIAL"},
        {"%ZZ": "SYNTHETIC_CREDENTIAL"},
    ],
)
def test_jdbc_security_invalid_properties(properties):
    session = Mock()
    with pytest.raises(ValueError) as error:
        _jdbc_security_client(properties=properties, session=session)
    assert "SYNTHETIC_CREDENTIAL" not in str(error.value)
    session.sql.assert_not_called()


@pytest.mark.parametrize(
    "url",
    [
        "jdbc:mysql://(host=h,user=LEFT,password=RIGHT)/db",
        "jdbc:mysql://address=(host=h)(user=LEFT)(password=RIGHT)/db",
        "jdbc:mysql://(host=h,%75ser=LEFT,%70assword=RIGHT)/db",
        "jdbc:mysql://h1,LEFT:RIGHT@h2/db",
        "jdbc:mysql://LEFT:RIGHT@[h1,h2]/db",
        'jdbc:oracle:thin:u/"LEFT@RIGHT"@//host/svc',
        'jdbc:oracle:thin:@//host/svc?password="LEFT&RIGHT"',
    ],
)
def test_jdbc_security_complex_strip_or_reject(url):
    session = Mock()
    try:
        client = _jdbc_security_client(url, session=session)
    except ValueError as error:
        assert "LEFT" not in str(error) and "RIGHT" not in str(error)
        session.sql.assert_not_called()
    else:
        assert "LEFT" not in client.url and "RIGHT" not in client.url
        client.secret_detector()
        assert "LEFT" not in client.url and "RIGHT" not in client.url


@pytest.mark.parametrize(
    "url",
    [
        "jdbc:mysql://h1:3306,h2:3306/db?useSSL=true",
        "jdbc:postgresql://[2001:db8::1]:5432/db?ApplicationName=password-manager",
        "jdbc:mysql://host/db?applicationName=a%26b%3Bc&useSSL=true",
        "jdbc:sqlserver://host\\INSTANCE;applicationName={password-manager;safe}}value};encrypt=true",
        "jdbc:sqlserver://host;applicationName=foo&password=benign;encrypt=true",
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=svc)))",
    ],
)
def test_jdbc_security_preserves_nonsecret_url(url):
    assert _jdbc_security_client(url).url == url


@pytest.mark.parametrize(
    "value",
    [
        'quote"',
        r"path\q\t\u0022",
        "line\nnext\r\t",
        "dollar$$",
        "unicode_é",
        "control\x00\x1f",
    ],
)
def test_jdbc_security_java_literals(value):
    client = _jdbc_security_client(
        "jdbc:postgresql://host/db?ApplicationName=" + value,
        properties={"applicationName": value},
        session_init_statement=[value],
    )
    source = (
        client.generate_create_connection() + client.generate_session_init_statement()
    )
    # The same escaped literal must be used for properties and arbitrary init SQL.
    literal = source.split('properties.put("applicationName", ', 1)[1].split(");", 1)[0]
    assert "stmt.execute(" + literal + ");" in source
    assert "$$" not in source
    assert "\n" not in literal and "\r" not in literal
    # Octal escapes are valid Java and avoid the SQL dollar delimiter.
    import json

    decoded = json.loads(
        re.sub(
            r"\\([0-3][0-7]{2})",
            lambda match: json.dumps(chr(int(match[1], 8)))[1:-1],
            literal,
        )
    )
    assert decoded == value


def test_jdbc_security_generated_error_boundaries():
    client, _ = _render_jdbc_infer_schema_sql("SELECT 1")
    inference = client.session.sql.call_args_list[0].args[0]
    ingestion = _render_jdbc_ddls()
    for ddl in (inference, ingestion):
        assert "e.getMessage()" not in ddl
        assert not re.search(r"new RuntimeException\([^;]*, e\)", ddl)
        assert "Failed to create JDBC connection" in ddl
        assert "Ingestion error:" in ddl
    lazy_inference = inference.split("Stream.generate", 1)[1]
    assert "catch (Exception e)" in lazy_inference


def test_jdbc_security_provider_and_sql_boundary():
    client = _jdbc_security_client(
        properties={
            "user": "INPUT_USER",
            "password": "INPUT_PASSWORD",
            "sslpassword": "INPUT_TLS",
            "authentication": "SqlPassword",
        },
        session_init_statement=["SET password = 'EXPLICIT_SQL_LITERAL'"],
    )
    assert client.secret_sql == "SECRETS = ('cred' = DB.SCHEMA.CREDENTIAL)"
    source = client.generate_create_connection()
    assert 'properties.put("user", up.getUsername())' in source
    assert 'properties.put("password", up.getPassword())' in source
    assert 'properties.put("authentication", "SqlPassword")' in source
    assert "INPUT_" not in source
    assert "EXPLICIT_SQL_LITERAL" in client.generate_session_init_statement()


@pytest.mark.parametrize(
    "url",
    [
        "jdbc:postgresql://host/db?password=INPUT_LEFT;INPUT_RIGHT&sslmode=require",
        "jdbc:mysql://host/db?%70assword=INPUT_PASSWORD&useSSL=true",
        "jdbc:sqlserver://host;password={INPUT_LEFT}};INPUT_RIGHT};encrypt=true",
        "jdbc:oracle:thin:INPUT_USER/INPUT_PASSWORD@//host/svc",
    ],
)
def test_jdbc_security_both_registration_sinks(url):
    session = Mock()
    session.sql.return_value.collect.return_value = [
        ("A", "VARCHAR", "java.lang.String", 0, 0, True)
    ]
    client = _jdbc_security_client(
        url,
        session=session,
        properties={"sslpassword": "INPUT_TLS", "applicationName": 'benign"\\q\n$$'},
    )
    client.schema
    with patch.object(BaseDriver, "to_result_snowpark_df_udtf", return_value=None):
        client.read("PARTITIONS")
    calls = session.sql.call_args_list
    assert len(calls) == 4
    for index in (0, 2):
        ddl = calls[index].args[0]
        assert "INPUT_" not in ddl
        assert ddl.count("$$") == 2
        assert "SECRETS = ('cred' = DB.SCHEMA.CREDENTIAL)" in ddl
        assert 'properties.put("user", up.getUsername())' in ddl
        assert 'properties.put("password", up.getPassword())' in ddl
        assert r"benign\"\\q\n\044\044" in ddl


def _assert_jdbc_correction_registration(
    url, properties, expected_url, expected_properties
):
    session = Mock()
    session.sql.return_value.collect.return_value = [
        ("A", "VARCHAR", "java.lang.String", 0, 0, True)
    ]
    original = properties.copy()
    client = _jdbc_security_client(url, session=session, properties=properties)
    assert properties == original
    assert (client.url, client.properties) == (expected_url, expected_properties)
    client.secret_detector()
    assert (client.url, client.properties) == (expected_url, expected_properties)
    client.schema
    with patch.object(BaseDriver, "to_result_snowpark_df_udtf", return_value=None):
        client.read("PARTITIONS")
    assert session.sql.call_count == 4
    for index in (0, 2):
        ddl = session.sql.call_args_list[index].args[0]
        assert "CORRECTION_SECRET" not in ddl
        assert ddl.count("$$") == 2
        assert 'String url = "' + expected_url.replace("$", r"\044") + '";' in ddl
        for key, value in expected_properties.items():
            assert (
                'properties.put("' + key.replace("$", r"\044") + '", "' + value + '");'
            ) in ddl
        assert 'properties.put("user", up.getUsername())' in ddl
        assert 'properties.put("password", up.getPassword())' in ddl


@pytest.mark.parametrize(
    "key",
    [
        "oracle.jdbc.clientSecret",
        "oracle.jdbc.clientCertificatePassword",
        "oracle.jdbc.accesstoken",
        "oracle.jdbc.newPassword",
    ],
)
@pytest.mark.parametrize(
    "spelling",
    [str, str.upper, lambda key: " " + key + " ", lambda key: "%6f" + key[1:]],
)
@pytest.mark.parametrize("channel", ["url", "properties"])
def test_jdbc_correction_oracle_prefixed_aliases(key, spelling, channel):
    base = "jdbc:oracle:thin:@//host/svc"
    key = spelling(key)
    url = base + ("?" + key + "=CORRECTION_SECRET" if channel == "url" else "")
    properties = {key: "CORRECTION_SECRET"} if channel == "properties" else {}
    _assert_jdbc_correction_registration(url, properties, base, {})


@pytest.mark.parametrize("channel", ["url", "properties"])
def test_jdbc_correction_sqlserver_optional_authority(channel):
    base = "jdbc:sqlserver://"
    properties = {"password": "CORRECTION_SECRET"}
    if channel == "url":
        expected = base + ";serverName=host;databaseName=db"
        url = expected + ";password=CORRECTION_SECRET"
        retained = {}
    else:
        url = expected = base
        retained = {"serverName": "host", "databaseName": "db"}
        properties.update(retained)
    _assert_jdbc_correction_registration(url, properties, expected, retained)


@pytest.mark.parametrize(
    "base,separator,key",
    [
        ("jdbc:sqlserver://host", ";", "applicationName"),
        ("jdbc:oracle:thin:@//host/svc", "?", "v$session.program"),
    ],
)
@pytest.mark.parametrize("value", ["100%complete", "%", "%ZZ", "%26%3B%24"])
@pytest.mark.parametrize("channel", ["url", "properties"])
def test_jdbc_correction_literal_percent_values(base, separator, key, value, channel):
    # SQL Server and Oracle values are literal, not URL-percent-decoded. This
    # deliberately replaces blanket malformed-percent rejection for these drivers.
    properties = {"password": "CORRECTION_SECRET" + value}
    retained = {}
    if channel == "url":
        expected = base + separator + key + "=" + value
        delimiter = ";" if separator == ";" else "&"
        url = expected + delimiter + "password=CORRECTION_SECRET" + value
    else:
        url = expected = base
        retained = {key: value}
        properties.update(retained)
    _assert_jdbc_correction_registration(url, properties, expected, retained)


@pytest.mark.parametrize("key", ["v$session.program", "oracle.jdbc.v$session.program"])
@pytest.mark.parametrize("channel", ["url", "properties"])
def test_jdbc_correction_oracle_dollar_names(key, channel):
    base = "jdbc:oracle:thin:@//host/svc"
    retained = {key: "review"} if channel == "properties" else {}
    url = base + ("?" + key + "=review" if channel == "url" else "")
    _assert_jdbc_correction_registration(
        url, {**retained, "password": "CORRECTION_SECRET"}, url, retained
    )


@pytest.mark.parametrize(
    "query", ["ssl", "ssl&password=CORRECTION_SECRET", "password=CORRECTION_SECRET&ssl"]
)
def test_jdbc_correction_postgres_ssl_flag(query):
    base = "jdbc:postgresql://host/db"
    _assert_jdbc_correction_registration(base + "?" + query, {}, base + "?ssl", {})


@pytest.mark.parametrize(
    "base",
    [
        "jdbc:postgresql://host/db",
        "jdbc:mysql://host/db",
        "jdbc:oracle:thin:@//host/svc",
    ],
)
@pytest.mark.parametrize(
    "query",
    [
        "CORRECTION_SECRET",
        "password",
        "ssl&CORRECTION_SECRET",
        "password=LEFT&CORRECTION_SECRET",
        "ssl;CORRECTION_SECRET",
        "%73sl",
    ],
)
def test_jdbc_correction_rejects_bare_tails(base, query):
    session = Mock()
    with pytest.raises(ValueError) as error:
        _jdbc_security_client(base + "?" + query, session=session)
    assert "CORRECTION_SECRET" not in str(error.value)
    session.sql.assert_not_called()


@pytest.mark.parametrize("base", ["jdbc:mysql://host/db", "jdbc:postgresql://host/db"])
@pytest.mark.parametrize(
    "query",
    ["password=CORRECTION_SECRET%ZZ", "applicationName=%", "%ZZ=CORRECTION_SECRET"],
)
def test_jdbc_correction_preserves_percent_rejection(base, query):
    session = Mock()
    with pytest.raises(ValueError):
        _jdbc_security_client(base + "?" + query, session=session)
    session.sql.assert_not_called()


@pytest.mark.parametrize("key", _JDBC_CREDENTIAL_ALIASES)
def test_jdbc_correction_postgres_bare_secret_names_reject(key):
    session = Mock()
    with pytest.raises(ValueError):
        _jdbc_security_client("jdbc:postgresql://host/db?ssl&" + key, session=session)
    session.sql.assert_not_called()


@pytest.mark.parametrize(
    "key",
    ["oracle.jdbc.clientSecretLabel", "oracle.jdbc.clientCertificatePasswordHint"],
)
@pytest.mark.parametrize("channel", ["url", "properties"])
def test_jdbc_correction_alias_matching_is_exact(key, channel):
    base = "jdbc:oracle:thin:@//host/svc"
    retained = {key: "password-manager"} if channel == "properties" else {}
    url = base + ("?" + key + "=password-manager" if channel == "url" else "")
    _assert_jdbc_correction_registration(
        url, {**retained, "password": "CORRECTION_SECRET"}, url, retained
    )


@pytest.mark.parametrize(
    "base", ["jdbc:mysql://host/db", "jdbc:oracle:thin:@//host/svc"]
)
def test_jdbc_correction_ssl_flag_is_postgres_only(base):
    session = Mock()
    with pytest.raises(ValueError):
        _jdbc_security_client(base + "?ssl", session=session)
    session.sql.assert_not_called()
