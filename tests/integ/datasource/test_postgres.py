#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import json

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source import DataSourcePartitioner
from snowflake.snowpark._internal.data_source.drivers import Psycopg2Driver
from snowflake.snowpark._internal.data_source.drivers.psycopg2_driver import (
    Psycopg2TypeCode,
)
from snowflake.snowpark._internal.data_source.utils import DBMS_TYPE
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    DecimalType,
    BinaryType,
    VariantType,
)
from tests.parameters import POSTGRES_CONNECTION_PARAMETERS
from tests.resources.test_data_source_dir.test_postgres_data import (
    POSTGRES_TABLE_NAME,
    EXPECTED_TEST_DATA,
    EXPECTED_TYPE,
    POSTGRES_TEST_EXTERNAL_ACCESS_INTEGRATION,
)
from tests.utils import IS_IN_STORED_PROC

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import psycopg2  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass

pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'psycopg2'"),
    pytest.mark.skipif(IS_IN_STORED_PROC, reason="Need External Access Integration"),
]


def create_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONNECTION_PARAMETERS)


@pytest.mark.parametrize(
    "input_type, input_value",
    [
        ("table", POSTGRES_TABLE_NAME),
        ("query", f"(SELECT * FROM {POSTGRES_TABLE_NAME})"),
    ],
)
def test_basic_postgres(session, input_type, input_value):
    input_dict = {
        input_type: input_value,
    }
    df = session.read.dbapi(create_postgres_connection, **input_dict)
    assert df.collect() == EXPECTED_TEST_DATA and df.schema == EXPECTED_TYPE


@pytest.mark.parametrize(
    "input_type, input_value, error_message",
    [
        ("table", "NONEXISTTABLE", "does not exist"),
        ("query", "SELEC ** FORM TABLE", "syntax error at or near"),
    ],
)
def test_error_case(session, input_type, input_value, error_message):
    input_dict = {
        input_type: input_value,
    }
    with pytest.raises(SnowparkDataframeReaderException, match=error_message):
        session.read.dbapi(create_postgres_connection, **input_dict)


def test_query_timeout(session):
    with pytest.raises(
        SnowparkDataframeReaderException,
        match=r"due to exception 'QueryCanceled\('canceling statement due to statement timeout",
    ):
        session.read.dbapi(
            create_postgres_connection,
            table=POSTGRES_TABLE_NAME,
            query_timeout=1,
            session_init_statement=["SELECT pg_sleep(5)"],
        )


def test_external_access_integration_not_set(session):
    with pytest.raises(
        ValueError,
        match="external_access_integration cannot be None when udtf ingestion is used.",
    ):
        session.read.dbapi(
            create_postgres_connection, table=POSTGRES_TABLE_NAME, udtf_configs={}
        )


def test_unicode_column_name_postgres(session):
    df = session.read.dbapi(
        create_postgres_connection, table='test_schema."用户資料"'
    ).order_by("編號")
    assert df.collect() == [Row(編號=1, 姓名="山田太郎", 國家="日本", 備註="これはUnicodeテストです")]
    assert df.columns == ['"編號"', '"姓名"', '"國家"', '"備註"']


@pytest.mark.parametrize(
    "input_type, input_value",
    [
        ("table", POSTGRES_TABLE_NAME),
        ("query", f"(SELECT * FROM {POSTGRES_TABLE_NAME})"),
    ],
)
def test_udtf_ingestion_postgres(session, input_type, input_value, caplog):
    from tests.parameters import POSTGRES_CONNECTION_PARAMETERS

    def create_connection_postgres():
        import psycopg2

        return psycopg2.connect(**POSTGRES_CONNECTION_PARAMETERS)

    input_dict = {
        input_type: input_value,
    }
    df = session.read.dbapi(
        create_connection_postgres,
        **input_dict,
        udtf_configs={
            "external_access_integration": POSTGRES_TEST_EXTERNAL_ACCESS_INTEGRATION
        },
    ).order_by("BIGSERIAL_COL")

    assert df.collect() == EXPECTED_TEST_DATA
    # assert UDTF creation and UDTF call
    assert (
        "TEMPORARY  FUNCTION  data_source_udtf_" "" in caplog.text
        and "table(data_source_udtf" in caplog.text
    )


def test_unit_udtf_ingestion_postgres(session):
    psycopg2_driver = Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB)
    udtf_ingestion_class = psycopg2_driver.udtf_class_builder()
    udtf_ingestion_instance = udtf_ingestion_class()

    dsp = DataSourcePartitioner(create_postgres_connection, POSTGRES_TABLE_NAME)
    yield_data = udtf_ingestion_instance.process(dsp.partitions[0])
    for row, expected_row in zip(yield_data, EXPECTED_TEST_DATA):
        for index, (field, value) in enumerate(zip(EXPECTED_TYPE.fields, row)):
            if isinstance(field.datatype, VariantType):
                # Convert Variant to JSON
                assert (
                    (json.loads(value) == json.loads(expected_row[index]))
                    if value
                    else True
                )
            elif isinstance(field.datatype, BinaryType):
                # Convert BinaryType to hex string
                assert (bytes.fromhex(value) == expected_row[index]) if value else True
            else:
                # Keep other types as is
                assert value == expected_row[index]


def test_to_snow_type(session):
    # Test unsupported numeric precision and scale
    snowpark_type = Psycopg2Driver(
        create_postgres_connection, DBMS_TYPE.POSTGRES_DB
    ).to_snow_type([("INVALID_COL", 1700, None, None, 1000, 1000, True)])
    assert len(snowpark_type.fields) == 1 and snowpark_type.fields[
        0
    ].datatype == DecimalType(38, 0)

    # Test unsupported type code
    with pytest.raises(NotImplementedError):
        nonexisting_type_code = -1
        Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB).to_snow_type(
            [("UNSUPPORTED_COL", nonexisting_type_code, None, None, None, None, True)]
        )

    # Test unsupported type code
    with pytest.raises(NotImplementedError):
        unimplemented_code = Psycopg2TypeCode.ACLITEMOID
        Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB).to_snow_type(
            [("UNSUPPORTED_COL", unimplemented_code, None, None, None, None, True)]
        )
