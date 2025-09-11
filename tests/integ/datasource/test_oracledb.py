#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import math
import sys
from collections import namedtuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source.drivers.oracledb_driver import (
    output_type_handler,
)
from snowflake.snowpark._internal.data_source.drivers import (
    OracledbDriver,
)
from snowflake.snowpark._internal.data_source.utils import (
    DBMS_TYPE,
)
from snowflake.snowpark.types import StructType, StructField, StringType
from tests.parameters import ORACLEDB_CONNECTION_PARAMETERS
from tests.resources.test_data_source_dir.test_data_source_data import (
    OracleDBType,
    oracledb_real_data,
    oracledb_real_data_small,
    oracledb_real_schema,
    oracledb_less_column_schema,
    oracledb_more_column_schema,
    oracledb_unicode_schema,
    oracledb_double_quoted_schema,
)
from tests.utils import Utils, RUNNING_ON_JENKINS

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import pandas  # noqa: F401
    import oracledb  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
    pytest.mark.skipif(
        DEPENDENCIES_PACKAGE_UNAVAILABLE,
        reason="dependency is not available",
    ),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS,
        reason="SNOW-2089683: oracledb real connection test failed on jenkins",
    ),
]


ORACLEDB_TABLE_NAME = "ALL_TYPE_TABLE"
ORACLEDB_TABLE_NAME_SMALL = "ALL_TYPE_TABLE_SMALL"
ORACLEDB_TEST_EXTERNAL_ACCESS_INTEGRATION = "snowpark_dbapi_oracledb_test_integration"


def create_connection_oracledb():
    import oracledb

    host = ORACLEDB_CONNECTION_PARAMETERS["host"]
    port = ORACLEDB_CONNECTION_PARAMETERS["port"]
    service_name = ORACLEDB_CONNECTION_PARAMETERS["service_name"]
    username = ORACLEDB_CONNECTION_PARAMETERS["username"]
    password = ORACLEDB_CONNECTION_PARAMETERS["password"]
    dsn = f"{host}:{port}/{service_name}"
    connection = oracledb.connect(user=username, password=password, dsn=dsn)
    return connection


@pytest.mark.parametrize(
    "input_type, input_value",
    [
        ("table", ORACLEDB_TABLE_NAME),
        ("query", f"SELECT * FROM {ORACLEDB_TABLE_NAME}"),
        ("query", f"(SELECT * FROM {ORACLEDB_TABLE_NAME})"),
    ],
)
@pytest.mark.parametrize(
    "custom_schema",
    [
        oracledb_real_schema,
        oracledb_less_column_schema,
        oracledb_more_column_schema,
        None,
    ],
)
def test_basic_oracledb(session, input_type, input_value, custom_schema):
    input_dict = {
        input_type: input_value,
        "max_workers": 4,
        "query_timeout": 5,
        "custom_schema": custom_schema,
    }
    df = session.read.dbapi(create_connection_oracledb, **input_dict).order_by("ID")
    assert df.collect() == oracledb_real_data
    assert df.schema == oracledb_real_schema


@pytest.mark.parametrize(
    "create_connection, table_name, expected_result",
    [
        (
            create_connection_oracledb,
            ORACLEDB_TABLE_NAME,
            oracledb_real_data,
        ),
        (
            create_connection_oracledb,
            ORACLEDB_TABLE_NAME_SMALL,
            oracledb_real_data_small,
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
        assert caplog.text.count("Retrieved BytesIO parquet from queue") == math.ceil(
            len(expected_result) / fetch_size
        )
        assert df.order_by("ID").collect() == expected_result


def test_oracledb_driver_coverage(caplog):
    oracledb_driver = OracledbDriver(create_connection_oracledb, DBMS_TYPE.ORACLE_DB)
    conn = oracledb_driver.prepare_connection(oracledb_driver.create_connection(), 0)
    assert conn.outputtypehandler == output_type_handler

    oracledb_driver.to_snow_type(
        [OracleDBType("NUMBER_COL", oracledb.DB_TYPE_NUMBER, 40, 2, True)]
    )
    assert "Snowpark does not support column" in caplog.text


@pytest.mark.udf
@pytest.mark.skipif(
    sys.version_info[:2] == (3, 13), reason="driver not supported in python 3.13"
)
def test_udtf_ingestion_oracledb(session):
    from tests.parameters import ORACLEDB_CONNECTION_PARAMETERS

    his = session.query_history()

    def create_connection_oracledb():
        import oracledb

        host = ORACLEDB_CONNECTION_PARAMETERS["host"]
        port = ORACLEDB_CONNECTION_PARAMETERS["port"]
        service_name = ORACLEDB_CONNECTION_PARAMETERS["service_name"]
        username = ORACLEDB_CONNECTION_PARAMETERS["username"]
        password = ORACLEDB_CONNECTION_PARAMETERS["password"]
        dsn = f"{host}:{port}/{service_name}"
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        return connection

    df = session.read.dbapi(
        create_connection_oracledb,
        table="ALL_TYPE_TABLE",
        udtf_configs={
            "external_access_integration": ORACLEDB_TEST_EXTERNAL_ACCESS_INTEGRATION
        },
    ).order_by("ID")

    Utils.check_answer(df, oracledb_real_data)

    # check that udtf is used
    flag = False
    for q in his.queries:
        if (
            """CREATE
TEMPORARY  FUNCTION  SNOWPARK_TEMP_FUNCTION"""
            in q.sql_text
        ):
            flag = True
    assert flag


def test_external_access_integration_not_set(session):
    with pytest.raises(
        ValueError,
        match="external_access_integration cannot be None when udtf ingestion is used.",
    ):
        session.read.dbapi(
            create_connection_oracledb, table=ORACLEDB_TABLE_NAME, udtf_configs={}
        )


@pytest.mark.parametrize(
    "custom_schema",
    [
        oracledb_unicode_schema,
        None,
    ],
)
def test_unicode_column_name_oracledb(session, custom_schema):
    df = session.read.dbapi(
        create_connection_oracledb, table='"用户資料"', custom_schema=custom_schema
    )
    assert df.collect() == [Row(編號=1, 姓名="山田太郎", 國家="日本", 備註="これはUnicodeテストです")]
    assert df.schema == oracledb_unicode_schema


@pytest.mark.parametrize(
    "custom_schema",
    [
        oracledb_double_quoted_schema,
        None,
    ],
)
def test_double_quoted_column_name_oracledb(session, custom_schema):
    df = session.read.dbapi(
        create_connection_oracledb, table='"UserProfile"', custom_schema=custom_schema
    )
    assert df.collect() == [
        Row(
            Id=1,
            FullName="John Doe",
            Country="USA",
            Notes="This is a case-sensitive example.",
        )
    ]
    assert df.schema == oracledb_double_quoted_schema


def test_unsupported_type():
    invalid_type = OracleDBType("ID", "UNKNOWN", None, None, False)
    MockDescription = namedtuple(
        "mock_description", ["name", "type_code", "precision", "scale", "null_ok"]
    )

    schema = OracledbDriver(
        create_connection_oracledb, DBMS_TYPE.ORACLE_DB
    ).to_snow_type([MockDescription("test_col", invalid_type, 0, 0, True)])
    assert schema == StructType([StructField("TEST_COL", StringType(), nullable=True)])


def test_oracledb_non_retryable_error(session):
    # input_dict = {"query": "invalid syntax", "custom_schema": oracledb_real_schema}
    # session.read.dbapi(create_connection_oracledb, **input_dict).collect()
    pass
