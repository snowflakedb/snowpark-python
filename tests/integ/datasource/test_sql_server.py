#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.data_source.utils import DBMS_TYPE
from snowflake.snowpark.types import StringType

from tests.parameters import SQL_SERVER_CONNECTION_PARAMETERS
from tests.utils import IS_IN_STORED_PROC, Utils, IS_WINDOWS, IS_MACOS, RUNNING_ON_GH
from tests.resources.test_data_source_dir.test_sql_server_data import (
    SQL_SERVER_TABLE_NAME,
    EXPECTED_TEST_DATA,
    SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
    EXPECTED_UNICODE_TEST_DATA,
    SQL_SERVER_SCHEMA,
    SQL_SERVER_UNICODE_SCHEMA,
    SQL_SEVER_UNICODE_TABLE_NAME,
)
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkClientException,
)

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import pyodbc
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'pyodbc'"),
    pytest.mark.skipif(IS_IN_STORED_PROC, reason="Need External Access Integration"),
    pytest.mark.skipif(
        RUNNING_ON_GH and (IS_WINDOWS or IS_MACOS),
        reason="ODBC Driver 18 for SQL Server is not installed",
    ),
    pytest.mark.skipif(
        not RUNNING_ON_GH,
        reason="Only run on GitHub action since it has the driver and access to SQL Server",
    ),
]


def construct_input_dict(input_type, table_name):
    return {
        input_type: table_name
        if input_type == "table"
        else f"SELECT * FROM {table_name}"
    }


def verify_save_table_result(
    session, df, expected_data, expected_schema, apply_order, ignore_string_size=False
):

    if apply_order:
        df = df.order_by("ID")

    Utils.check_answer(df, expected_data)

    def verify_schemas(df, expected_schema, ignore_string_size):
        # TODO: SNOW-2362041
        # - UDTF ingestion returning StringType 128 MB (due to variant default to 128MB)
        # - parquet based ingestion returning StringType 16 MB
        # we should align the two
        for field, expected_field in zip(df.schema.fields, expected_schema.fields):
            if isinstance(field.datatype, StringType):
                assert isinstance(field.datatype, type(expected_field.datatype))
                if ignore_string_size:
                    assert (
                        field.datatype.length == expected_field.datatype.length
                        or field.datatype.length == 134217728
                    )
                else:
                    assert field.datatype.length == expected_field.datatype.length
            else:
                assert field.datatype == expected_field.datatype
            assert field.name == expected_field.name
            assert field.nullable == expected_field.nullable

    verify_schemas(df, expected_schema, ignore_string_size)
    # after the fix SNOW-2362041, we should be able to enable this assertion
    # assert df.schema == expected_schema

    table_name = Utils.random_table_name()
    # save and read
    df.write.mode("overwrite").save_as_table(table_name, table_type="temp")
    read_table = session.table(table_name)

    if apply_order:
        read_table = read_table.order_by("ID")

    Utils.check_answer(read_table, expected_data)
    verify_schemas(read_table, expected_schema, ignore_string_size)
    # after the fix SNOW-2362041, we should be able to enable this assertion
    # assert read_table.schema == expected_schema


def create_connection_sql_server():
    return pyodbc.connect(
        "DRIVER=" + SQL_SERVER_CONNECTION_PARAMETERS["DRIVER"] + ";"
        "SERVER=" + SQL_SERVER_CONNECTION_PARAMETERS["SERVER"] + ";"
        "UID=" + SQL_SERVER_CONNECTION_PARAMETERS["UID"] + ";"
        "PWD=" + SQL_SERVER_CONNECTION_PARAMETERS["PWD"] + ";"
        "TrustServerCertificate="
        + SQL_SERVER_CONNECTION_PARAMETERS["TrustServerCertificate"]
        + ";"
        "Encrypt=" + SQL_SERVER_CONNECTION_PARAMETERS["Encrypt"] + ";"
    )


@pytest.mark.parametrize(
    "input_type, table_name, expected_data, expected_schema, apply_order",
    [
        ("table", SQL_SERVER_TABLE_NAME, EXPECTED_TEST_DATA, SQL_SERVER_SCHEMA, True),
        ("query", SQL_SERVER_TABLE_NAME, EXPECTED_TEST_DATA, SQL_SERVER_SCHEMA, True),
        (
            "table",
            SQL_SEVER_UNICODE_TABLE_NAME,
            EXPECTED_UNICODE_TEST_DATA,
            SQL_SERVER_UNICODE_SCHEMA,
            False,
        ),
        (
            "query",
            SQL_SEVER_UNICODE_TABLE_NAME,
            EXPECTED_UNICODE_TEST_DATA,
            SQL_SERVER_UNICODE_SCHEMA,
            False,
        ),
    ],
)
def test_sql_server_ingestion(
    session, input_type, table_name, expected_data, expected_schema, apply_order
):
    if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
        pytest.skip("Microsoft ODBC Driver 18 for SQL Server is not installed")

    df = session.read.dbapi(
        create_connection_sql_server,
        **construct_input_dict(input_type, table_name),
    )

    verify_save_table_result(session, df, expected_data, expected_schema, apply_order)


@pytest.mark.parametrize(
    "input_type, table_name, expected_data, expected_schema, apply_order",
    [
        ("table", SQL_SERVER_TABLE_NAME, EXPECTED_TEST_DATA, SQL_SERVER_SCHEMA, True),
        ("query", SQL_SERVER_TABLE_NAME, EXPECTED_TEST_DATA, SQL_SERVER_SCHEMA, True),
        (
            "table",
            SQL_SEVER_UNICODE_TABLE_NAME,
            EXPECTED_UNICODE_TEST_DATA,
            SQL_SERVER_UNICODE_SCHEMA,
            False,
        ),
        (
            "query",
            SQL_SEVER_UNICODE_TABLE_NAME,
            EXPECTED_UNICODE_TEST_DATA,
            SQL_SERVER_UNICODE_SCHEMA,
            False,
        ),
    ],
)
def test_sql_server_udtf_ingestion(
    session, input_type, table_name, expected_data, expected_schema, apply_order
):
    local_parameters = SQL_SERVER_CONNECTION_PARAMETERS.copy()

    def local_create_connection_sql_server():
        return pyodbc.connect(
            "DRIVER=" + local_parameters["DRIVER"] + ";"
            "SERVER=" + local_parameters["SERVER"] + ";"
            "UID=" + local_parameters["UID"] + ";"
            "PWD=" + local_parameters["PWD"] + ";"
            "TrustServerCertificate=" + local_parameters["TrustServerCertificate"] + ";"
            "Encrypt=" + local_parameters["Encrypt"] + ";"
        )

    # sql server pyodbc required microsoft odbc driver installed on the machine
    df = session.read.dbapi(
        local_create_connection_sql_server,
        **construct_input_dict(input_type, table_name),
        udtf_configs=SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
    )

    verify_save_table_result(
        session,
        df,
        expected_data,
        expected_schema,
        apply_order,
        ignore_string_size=True,
    )


@pytest.mark.parametrize(
    "input_type, input_value, error_message, udtf_configs",
    [
        ("table", "NONEXISTTABLE", "Invalid object name", None),
        ("query", "SELEC ** FORM TABLE", "Incorrect syntax near", None),
        (
            "table",
            "NONEXISTTABLE",
            "Invalid object name",
            SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
        ),
        (
            "query",
            "SELEC ** FORM TABLE",
            "Incorrect syntax near",
            SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
        ),
    ],
)
def test_error_case(session, input_type, input_value, error_message, udtf_configs):
    # Use local connection function when udtf_configs is provided
    if udtf_configs:
        local_parameters = SQL_SERVER_CONNECTION_PARAMETERS.copy()

        def connection_func():
            return pyodbc.connect(
                "DRIVER=" + local_parameters["DRIVER"] + ";"
                "SERVER=" + local_parameters["SERVER"] + ";"
                "UID=" + local_parameters["UID"] + ";"
                "PWD=" + local_parameters["PWD"] + ";"
                "TrustServerCertificate="
                + local_parameters["TrustServerCertificate"]
                + ";"
                "Encrypt=" + local_parameters["Encrypt"] + ";"
            )

    else:
        connection_func = create_connection_sql_server

    # Prepare kwargs for dbapi call
    dbapi_kwargs = construct_input_dict(input_type, input_value)
    if udtf_configs:
        dbapi_kwargs["udtf_configs"] = udtf_configs

    with pytest.raises(SnowparkDataframeReaderException, match=error_message):
        session.read.dbapi(connection_func, **dbapi_kwargs)


@pytest.mark.parametrize(
    "udtf_configs",
    [
        None,
        SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
    ],
)
def test_partitions_and_predicates(session, udtf_configs):
    # Use local connection function when udtf_configs is provided
    if udtf_configs:
        local_parameters = SQL_SERVER_CONNECTION_PARAMETERS.copy()

        def connection_func():
            return pyodbc.connect(
                "DRIVER=" + local_parameters["DRIVER"] + ";"
                "SERVER=" + local_parameters["SERVER"] + ";"
                "UID=" + local_parameters["UID"] + ";"
                "PWD=" + local_parameters["PWD"] + ";"
                "TrustServerCertificate="
                + local_parameters["TrustServerCertificate"]
                + ";"
                "Encrypt=" + local_parameters["Encrypt"] + ";"
            )

    else:
        connection_func = create_connection_sql_server

    # Prepare kwargs for dbapi call
    dbapi_kwargs = {
        "table": SQL_SERVER_TABLE_NAME,
        "column": "ID",
        "num_partitions": 3,
        "upper_bound": 10,
        "lower_bound": 0,
    }
    if udtf_configs:
        dbapi_kwargs["udtf_configs"] = udtf_configs

    df = session.read.dbapi(connection_func, **dbapi_kwargs)

    # Use ignore_string_size=True for UDTF scenarios like in other tests
    verify_save_table_result(
        session,
        df,
        EXPECTED_TEST_DATA,
        SQL_SERVER_SCHEMA,
        True,
        ignore_string_size=bool(udtf_configs),
    )

    dbapi_kwargs = {
        "table": SQL_SERVER_TABLE_NAME,
        "predicates": ["ID < 6", "ID >= 6"],
    }
    if udtf_configs:
        dbapi_kwargs["udtf_configs"] = udtf_configs

    df = session.read.dbapi(connection_func, **dbapi_kwargs)

    verify_save_table_result(
        session,
        df,
        EXPECTED_TEST_DATA,
        SQL_SERVER_SCHEMA,
        True,
        ignore_string_size=bool(udtf_configs),
    )


@pytest.mark.parametrize(
    "udtf_configs",
    [
        None,
        SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
    ],
)
def test_session_init_statement(session, udtf_configs):
    # Use local connection function when udtf_configs is provided
    if udtf_configs:
        local_parameters = SQL_SERVER_CONNECTION_PARAMETERS.copy()

        def connection_func():
            return pyodbc.connect(
                "DRIVER=" + local_parameters["DRIVER"] + ";"
                "SERVER=" + local_parameters["SERVER"] + ";"
                "UID=" + local_parameters["UID"] + ";"
                "PWD=" + local_parameters["PWD"] + ";"
                "TrustServerCertificate="
                + local_parameters["TrustServerCertificate"]
                + ";"
                "Encrypt=" + local_parameters["Encrypt"] + ";"
            )

    else:
        connection_func = create_connection_sql_server

    # here we use a statement that will fail to verify the session init statement is executed
    statements = [
        "DECLARE @VAR1 INT;",
        "DECLARE @VAR2 INT;",
        "SET @VAR_NON_EXIST = 12345;",
    ]

    # Prepare kwargs for dbapi call
    dbapi_kwargs = {
        "table": SQL_SERVER_TABLE_NAME,
        "session_init_statement": statements,
    }
    if udtf_configs:
        dbapi_kwargs["udtf_configs"] = udtf_configs

    with pytest.raises(
        SnowparkClientException, match="Must declare the scalar variable"
    ):
        # TODO: 2362041, UDTF error experience is different from parquet ingestion
        # 1. UDTF needs .collect() to trigger the error while parquet ingestion triggers on .dbapi()
        # 2. error exception is different
        session.read.dbapi(connection_func, **dbapi_kwargs).collect()


def test_pyodbc_driver_class_builder():
    from snowflake.snowpark._internal.data_source.drivers.pyodbc_driver import (
        PyodbcDriver,
    )

    driver = PyodbcDriver(create_connection_sql_server, DBMS_TYPE.SQL_SERVER_DB)
    udtf_class = driver.udtf_class_builder(
        fetch_size=2,
    )
    ingestion = udtf_class()
    results = list(ingestion.process(f"SELECT * FROM {SQL_SERVER_TABLE_NAME}"))
    assert len(results) == len(EXPECTED_TEST_DATA)


@pytest.mark.parametrize(
    "udtf_configs",
    [
        None,
        {
            "external_access_integration": SQL_SERVER_TEST_EXTERNAL_ACCESS_INTEGRATION,
        },
    ],
)
def test_sql_server_with_connection_parameters(session, udtf_configs):
    """Test connection_parameters with local/default ingestion and UDTF ingestion."""

    def create_connection_with_params(connection_string=None, **kwargs):
        if kwargs.get("extra_param") != "extra_value":
            raise ValueError("extra_param should be extra_value")
        import pyodbc

        return pyodbc.connect(connection_string)

    connection_params = {
        "connection_string": SQL_SERVER_CONNECTION_PARAMETERS["connection_string"],
        "extra_param": "extra_value",  # Extra param to verify arbitrary params are passed
    }

    df = session.read.dbapi(
        create_connection_with_params,
        table=SQL_SERVER_TABLE_NAME,
        custom_schema=SQL_SERVER_SCHEMA,
        connection_parameters=connection_params,
        udtf_configs=udtf_configs,
    )
    result = df.order_by("ID").collect()
    Utils.assert_rows_count(result, len(EXPECTED_TEST_DATA))
