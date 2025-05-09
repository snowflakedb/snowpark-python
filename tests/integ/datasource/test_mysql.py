#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import math
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source.drivers.pymsql_driver import PymysqlDriver
from snowflake.snowpark._internal.data_source.utils import DBMS_TYPE
from tests.resources.test_data_source_dir.test_mysql_data import (
    mysql_real_data,
    MysqlType,
)
from tests.utils import RUNNING_ON_JENKINS, Utils
from tests.parameters import MYSQL_CONNECTION_PARAMETERS

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import databricks  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'databricks'"),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS, reason="cannot access external datasource from jenkins"
    ),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
]

TEST_TABLE_NAME = "ALL_TYPES_TABLE"


def create_connection_mysql():
    import pymysql

    conn = pymysql.connect(
        user=MYSQL_CONNECTION_PARAMETERS["username"],
        password=MYSQL_CONNECTION_PARAMETERS["password"],
        host=MYSQL_CONNECTION_PARAMETERS["host"],
        database=MYSQL_CONNECTION_PARAMETERS["database"],
    )
    return conn


def test_dbapi_mysql(session):
    df = session.read.dbapi(create_connection_mysql, table=TEST_TABLE_NAME)
    Utils.check_answer(df, mysql_real_data)


@pytest.mark.parametrize(
    "create_connection, table_name, expected_result",
    [
        (
            create_connection_mysql,
            TEST_TABLE_NAME,
            mysql_real_data,
        ),
    ],
)
@pytest.mark.parametrize("fetch_size", [1, 3])
def test_dbapi_batch_fetch(
    session, create_connection, table_name, expected_result, fetch_size, caplog
):
    with caplog.at_level(logging.DEBUG):
        df = session.read.dbapi(
            create_connection, table=table_name, max_workers=4, fetch_size=fetch_size
        )
        # we only expect math.ceil(len(expected_result) / fetch_size) parquet files to be generated
        # for example, 5 rows, fetch size 2, we expect 3 parquet files
        assert caplog.text.count("Retrieved file from parquet queue") == math.ceil(
            len(expected_result) / fetch_size
        )
        assert df.order_by("ID").collect() == expected_result


def test_type_conversion():
    invalid_type = MysqlType("ID", "UNKNOWN", None, None, None, None, False)
    with pytest.raises(NotImplementedError, match="mysql type not supported"):
        PymysqlDriver(create_connection_mysql, DBMS_TYPE.MYSQL_DB).to_snow_type(
            [invalid_type]
        )


def test_oracledb_driver_coverage(caplog):
    mysql_driver = PymysqlDriver(create_connection_mysql, DBMS_TYPE.MYSQL_DB)
    mysql_driver.to_snow_type(
        [MysqlType("NUMBER_COL", (246, Decimal), None, None, 41, 2, True)]
    )
    assert "Snowpark does not support column" in caplog.text


def test_unicode_column_name_oracledb(session):
    df = session.read.dbapi(create_connection_mysql, table="用户資料")
    assert df.collect() == [Row(編號=1, 姓名="山田太郎", 國家="日本", 備註="これはUnicodeテストです")]


def test_double_quoted_column_name_oracledb(session):
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
