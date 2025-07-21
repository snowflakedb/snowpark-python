#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import math
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source.drivers.pymsql_driver import (
    PymysqlDriver,
    PymysqlTypeCode,
)
from snowflake.snowpark._internal.data_source.utils import DBMS_TYPE
from tests.resources.test_data_source_dir.test_mysql_data import (
    mysql_real_data,
    MysqlType,
    mysql_schema,
)
from tests.utils import RUNNING_ON_JENKINS, Utils
from tests.parameters import MYSQL_CONNECTION_PARAMETERS

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import pymysql  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'pymysql'"),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS, reason="cannot access external datasource from jenkins"
    ),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
]


TEST_TABLE_NAME = "ALL_TYPES_TABLE"
TEST_QUERY = "select * from ALL_TYPES_TABLE"
MYSQL_TEST_EXTERNAL_ACCESS_INTEGRATION = "snowpark_dbapi_mysql_test_integration"


def create_connection_mysql():
    import pymysql  # noqa: F811

    # TODO: SNOW-2112895 make key in connection parameters align with driver
    conn = pymysql.connect(
        user=MYSQL_CONNECTION_PARAMETERS["username"],
        password=MYSQL_CONNECTION_PARAMETERS["password"],
        host=MYSQL_CONNECTION_PARAMETERS["host"],
        database=MYSQL_CONNECTION_PARAMETERS["database"],
    )
    return conn


@pytest.mark.parametrize(
    "create_connection, table_name, query",
    [
        (
            create_connection_mysql,
            TEST_TABLE_NAME,
            None,
        ),
        (
            create_connection_mysql,
            None,
            TEST_QUERY,
        ),
        (
            create_connection_mysql,
            None,
            f"({TEST_QUERY})",
        ),
    ],
)
@pytest.mark.parametrize(
    "custom_schema",
    [
        mysql_schema,
        None,
    ],
)
def test_basic_mysql(session, create_connection, table_name, query, custom_schema):
    df = session.read.dbapi(
        create_connection, table=table_name, query=query, custom_schema=custom_schema
    )
    Utils.check_answer(df, mysql_real_data)
    assert df.schema == mysql_schema


@pytest.mark.parametrize(
    "create_connection, table_name, query, expected_result",
    [
        (
            create_connection_mysql,
            TEST_TABLE_NAME,
            None,
            mysql_real_data,
        ),
        (
            create_connection_mysql,
            None,
            TEST_QUERY,
            mysql_real_data,
        ),
    ],
)
@pytest.mark.parametrize("fetch_size", [1, 3])
def test_dbapi_batch_fetch(
    session, create_connection, table_name, query, expected_result, fetch_size, caplog
):
    with caplog.at_level(logging.DEBUG):
        df = session.read.dbapi(
            create_connection,
            table=table_name,
            query=query,
            max_workers=4,
            fetch_size=fetch_size,
        )
        # we only expect math.ceil(len(expected_result) / fetch_size) parquet files to be generated
        # for example, 5 rows, fetch size 2, we expect 3 parquet files
        assert caplog.text.count("Retrieved BytesIO parquet from queue") == math.ceil(
            len(expected_result) / fetch_size
        )
        assert df.order_by("ID").collect() == expected_result


def test_type_conversion():
    invalid_type = MysqlType("ID", "UNKNOWN", None, None, None, None, False)
    with pytest.raises(NotImplementedError, match="mysql type not supported"):
        PymysqlDriver(create_connection_mysql, DBMS_TYPE.MYSQL_DB).to_snow_type(
            [invalid_type]
        )


def test_pymysql_driver_coverage(caplog):
    mysql_driver = PymysqlDriver(create_connection_mysql, DBMS_TYPE.MYSQL_DB)
    mysql_driver.to_snow_type(
        [
            MysqlType(
                "NUMBER_COL", PymysqlTypeCode((246, Decimal)), None, None, 41, 2, True
            )
        ]
    )
    assert "Snowpark does not support column" in caplog.text


def test_unicode_column_name_mysql(session):
    df = session.read.dbapi(create_connection_mysql, table="用户資料")
    assert df.collect() == [Row(編號=1, 姓名="山田太郎", 國家="日本", 備註="これはUnicodeテストです")]


def test_double_quoted_column_name_mysql(session):
    df = session.read.dbapi(create_connection_mysql, table='"UserProfile"')
    assert df.collect() == [
        Row(
            Id=1,
            FullName="John Doe",
            Country="USA",
            Notes="This is a case-sensitive example.",
        )
    ]


@pytest.mark.parametrize(
    "data, number_of_columns, expected_result",
    [
        (
            [(1, 2.00, "aa", b"asd")],
            4,
            [int, float, str, bytes],
        ),
        (
            [],
            4,
            [str, str, str, str],
        ),
        (
            [(1, 2.00, None, b"asd")],
            4,
            [int, float, str, bytes],
        ),
        (
            [(1, 2.00, "aa", b"asd"), (1, 2.00, "aa", "asd")],
            4,
            [int, float, str, str],
        ),
    ],
)
def test_infer_type_from_data(data, number_of_columns, expected_result):
    result = PymysqlDriver.infer_type_from_data(data, number_of_columns)
    assert result == expected_result


def test_udtf_ingestion_mysql(session, caplog):
    from tests.parameters import MYSQL_CONNECTION_PARAMETERS

    def create_connection_mysql():
        import pymysql  # noqa: F811

        conn = pymysql.connect(
            user=MYSQL_CONNECTION_PARAMETERS["username"],
            password=MYSQL_CONNECTION_PARAMETERS["password"],
            host=MYSQL_CONNECTION_PARAMETERS["host"],
            database=MYSQL_CONNECTION_PARAMETERS["database"],
        )
        return conn

    df = session.read.dbapi(
        create_connection_mysql,
        table=TEST_TABLE_NAME,
        udtf_configs={
            "external_access_integration": MYSQL_TEST_EXTERNAL_ACCESS_INTEGRATION
        },
    ).order_by("ID")

    Utils.check_answer(df, mysql_real_data)

    # check that udtf is used
    assert (
        "TEMPORARY  FUNCTION  data_source_udtf_" "" in caplog.text
        and "table(data_source_udtf" in caplog.text
    )


def test_pymysql_driver_udtf_class_builder():
    """Test the UDTF class builder in PymysqlDriver using a real pymysql connection"""
    # Create the driver with the real connection function
    driver = PymysqlDriver(create_connection_mysql, DBMS_TYPE.MYSQL_DB)

    # Get the UDTF class with a small fetch size to test batching
    UDTFClass = driver.udtf_class_builder(fetch_size=2)

    # Instantiate the UDTF class
    udtf_instance = UDTFClass()

    # Test with a simple query that should return a few rows
    test_query = f"SELECT * FROM {TEST_TABLE_NAME} LIMIT 5"
    result_rows = list(udtf_instance.process(test_query))

    # Verify we got some data back (we know the test table has data from other tests)
    assert len(result_rows) > 0

    # Test with a query that returns specific columns
    test_columns_query = f"SELECT INTCOL, DOUBLECOL FROM {TEST_TABLE_NAME} LIMIT 3"
    column_result_rows = list(udtf_instance.process(test_columns_query))

    # Verify we got data with the right structure (2 columns)
    assert len(column_result_rows) > 0
    assert len(column_result_rows[0]) == 2  # Two columns
