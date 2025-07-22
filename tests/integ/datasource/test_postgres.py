#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
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
    StructType,
    StructField,
    StringType,
    TimeType,
    BooleanType,
    IntegerType,
    FloatType,
    DoubleType,
    DateType,
    TimestampType,
    TimestampTimeZone,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.postgresql_dialect import (
    PostgresDialect,
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
        ("query", f"SELECT * FROM {POSTGRES_TABLE_NAME}"),
        ("query", f"(SELECT * FROM {POSTGRES_TABLE_NAME})"),
    ],
)
@pytest.mark.parametrize(
    "custom_schema",
    [
        EXPECTED_TYPE,
        None,
    ],
)
def test_basic_postgres(session, input_type, input_value, custom_schema):
    input_dict = {input_type: input_value, "custom_schema": custom_schema}
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
@pytest.mark.udf
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


def test_psycopg2_driver_udtf_class_builder():
    """Test the UDTF class builder in Psycopg2Driver using a real PostgreSQL connection"""
    # Create the driver with the real connection function
    driver = Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB)

    # Get the UDTF class with a small fetch size to test batching
    UDTFClass = driver.udtf_class_builder(fetch_size=2)

    # Instantiate the UDTF class
    udtf_instance = UDTFClass()

    # Test with a simple query that should return a few rows
    test_query = f"SELECT * FROM {POSTGRES_TABLE_NAME} LIMIT 5"
    result_rows = list(udtf_instance.process(test_query))

    # Verify we got some data back (we know the test table has data from other tests)
    assert len(result_rows) > 0

    # Test with a query that returns specific columns
    test_columns_query = (
        f"SELECT TEXT_COL, BIGINT_COL FROM {POSTGRES_TABLE_NAME} LIMIT 3"
    )
    column_result_rows = list(udtf_instance.process(test_columns_query))

    # Verify we got data with the right structure (2 columns)
    assert len(column_result_rows) > 0
    assert len(column_result_rows[0]) == 2  # Two columns


def test_unit_psycopg2_driver_to_snow_type_mapping():
    """Test the mapping of PostgreSQL types to Snowflake types in Psycopg2Driver.to_snow_type"""
    driver = Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB)

    # Test basic types
    basic_schema = [
        ("bool_col", Psycopg2TypeCode.BOOLOID.value, None, None, None, None, True),
        ("int2_col", Psycopg2TypeCode.INT2OID.value, None, None, None, None, True),
        ("int4_col", Psycopg2TypeCode.INT4OID.value, None, None, None, None, True),
        ("int8_col", Psycopg2TypeCode.INT8OID.value, None, None, None, None, True),
        ("text_col", Psycopg2TypeCode.TEXTOID.value, None, None, None, None, True),
        (
            "varchar_col",
            Psycopg2TypeCode.VARCHAROID.value,
            None,
            None,
            None,
            None,
            True,
        ),
        ("char_col", Psycopg2TypeCode.CHAROID.value, None, None, None, None, True),
    ]

    result = driver.to_snow_type(basic_schema)

    assert len(result.fields) == 7
    assert isinstance(result.fields[0].datatype, BooleanType)
    assert isinstance(result.fields[1].datatype, IntegerType)
    assert isinstance(result.fields[2].datatype, IntegerType)
    assert isinstance(result.fields[3].datatype, IntegerType)
    assert isinstance(result.fields[4].datatype, StringType)
    assert isinstance(result.fields[5].datatype, StringType)
    assert isinstance(result.fields[6].datatype, StringType)

    # Test float types
    float_schema = [
        ("float4_col", Psycopg2TypeCode.FLOAT4OID.value, None, None, None, None, True),
        ("float8_col", Psycopg2TypeCode.FLOAT8OID.value, None, None, None, None, True),
    ]

    result = driver.to_snow_type(float_schema)

    assert len(result.fields) == 2
    assert isinstance(result.fields[0].datatype, FloatType)
    assert isinstance(result.fields[1].datatype, DoubleType)

    # Test date and time types
    datetime_schema = [
        ("date_col", Psycopg2TypeCode.DATEOID.value, None, None, None, None, True),
        ("time_col", Psycopg2TypeCode.TIMEOID.value, None, None, None, None, True),
        ("timetz_col", Psycopg2TypeCode.TIMETZOID.value, None, None, None, None, True),
        (
            "timestamp_col",
            Psycopg2TypeCode.TIMESTAMPOID.value,
            None,
            None,
            None,
            None,
            True,
        ),
        (
            "timestamptz_col",
            Psycopg2TypeCode.TIMESTAMPTZOID.value,
            None,
            None,
            None,
            None,
            True,
        ),
        (
            "interval_col",
            Psycopg2TypeCode.INTERVALOID.value,
            None,
            None,
            None,
            None,
            True,
        ),
    ]

    result = driver.to_snow_type(datetime_schema)

    assert len(result.fields) == 6
    assert isinstance(result.fields[0].datatype, DateType)
    assert isinstance(result.fields[1].datatype, TimeType)
    assert isinstance(result.fields[2].datatype, TimeType)
    assert isinstance(result.fields[3].datatype, TimestampType)
    assert isinstance(result.fields[4].datatype, TimestampType)
    # Check timezone-aware timestamp
    assert result.fields[4].datatype.tz == TimestampTimeZone.TZ
    assert isinstance(result.fields[5].datatype, StringType)

    # Test binary and complex types
    complex_schema = [
        ("bytea_col", Psycopg2TypeCode.BYTEAOID.value, None, None, None, None, True),
        ("json_col", Psycopg2TypeCode.JSON.value, None, None, None, None, True),
        ("jsonb_col", Psycopg2TypeCode.JSONB.value, None, None, None, None, True),
        ("uuid_col", Psycopg2TypeCode.UUID.value, None, None, None, None, True),
        ("cash_col", Psycopg2TypeCode.CASHOID.value, None, None, None, None, True),
        ("inet_col", Psycopg2TypeCode.INETOID.value, None, None, None, None, True),
    ]

    result = driver.to_snow_type(complex_schema)

    assert len(result.fields) == 6
    assert isinstance(result.fields[0].datatype, BinaryType)
    assert isinstance(result.fields[1].datatype, VariantType)
    assert isinstance(result.fields[2].datatype, VariantType)
    assert isinstance(result.fields[3].datatype, StringType)
    assert isinstance(result.fields[4].datatype, VariantType)
    assert isinstance(result.fields[5].datatype, StringType)

    # Test numeric with various precision and scale
    numeric_schema = [
        (
            "numeric_default",
            Psycopg2TypeCode.NUMERICOID.value,
            None,
            None,
            None,
            None,
            True,
        ),
        ("numeric_valid", Psycopg2TypeCode.NUMERICOID.value, None, None, 10, 2, True),
        ("numeric_max", Psycopg2TypeCode.NUMERICOID.value, None, None, 38, 37, True),
        (
            "numeric_invalid",
            Psycopg2TypeCode.NUMERICOID.value,
            None,
            None,
            1000,
            1000,
            True,
        ),
    ]

    result = driver.to_snow_type(numeric_schema)

    assert len(result.fields) == 4
    # Default precision/scale
    assert isinstance(result.fields[0].datatype, DecimalType)
    assert result.fields[0].datatype.precision == 38
    assert result.fields[0].datatype.scale == 0

    # Valid precision/scale
    assert isinstance(result.fields[1].datatype, DecimalType)
    assert result.fields[1].datatype.precision == 10
    assert result.fields[1].datatype.scale == 2

    # Max valid precision/scale
    assert isinstance(result.fields[2].datatype, DecimalType)
    assert result.fields[2].datatype.precision == 38
    assert result.fields[2].datatype.scale == 37

    # Invalid precision/scale - should be defaulted
    assert isinstance(result.fields[3].datatype, DecimalType)
    assert result.fields[3].datatype.precision == 38
    assert result.fields[3].datatype.scale == 0

    # Test unsupported type code
    with pytest.raises(NotImplementedError, match="Postgres type not supported"):
        nonexisting_type_code = -1
        Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB).to_snow_type(
            [("UNSUPPORTED_COL", nonexisting_type_code, None, None, None, None, True)]
        )

    # Test unsupported type code
    with pytest.raises(NotImplementedError, match="Postgres type not supported"):
        unimplemented_code = Psycopg2TypeCode.ACLITEMOID
        Psycopg2Driver(create_postgres_connection, DBMS_TYPE.POSTGRES_DB).to_snow_type(
            [("UNSUPPORTED_COL", unimplemented_code, None, None, None, None, True)]
        )


def test_unit_generate_select_query():
    # Create a mock schema with different field types
    schema = StructType(
        [
            StructField("json_col", VariantType()),
            StructField("cash_col", VariantType()),
            StructField("bytea_col", BinaryType()),
            StructField("timetz_col", TimeType()),
            StructField("interval_col", StringType()),
            StructField("regular_col", StringType()),
        ]
    )

    # Create mock raw schema - each tuple represents (name, type_code, display_size, internal_size, precision, scale, null_ok)
    raw_schema = [
        ("json_col", Psycopg2TypeCode.JSON.value, None, None, None, None, True),
        ("cash_col", Psycopg2TypeCode.CASHOID.value, None, None, None, None, True),
        ("bytea_col", Psycopg2TypeCode.BYTEAOID.value, None, None, None, None, True),
        ("timetz_col", Psycopg2TypeCode.TIMETZOID.value, None, None, None, None, True),
        (
            "interval_col",
            Psycopg2TypeCode.INTERVALOID.value,
            None,
            None,
            None,
            None,
            True,
        ),
        ("regular_col", Psycopg2TypeCode.TEXTOID.value, None, None, None, None, True),
    ]

    # Test with table name
    table_query = PostgresDialect.generate_select_query(
        "test_table", schema, raw_schema, is_query=False, query_input_alias="mock_alias"
    )
    expected_table_query = (
        'SELECT TO_JSON("json_col")::TEXT AS json_col, '
        'CASE WHEN "cash_col" IS NULL THEN NULL ELSE FORMAT(\'"%s"\', "cash_col"::TEXT) END AS cash_col, '
        """ENCODE("bytea_col", 'HEX') AS bytea_col, """
        '"timetz_col"::TIME AS timetz_col, '
        '"interval_col"::TEXT AS interval_col, '
        '"regular_col" '
        "FROM test_table"
    )
    assert table_query == expected_table_query

    # Test with subquery
    subquery_query = PostgresDialect.generate_select_query(
        "(SELECT * FROM test_table)",
        schema,
        raw_schema,
        is_query=True,
        query_input_alias="mock_alias",
    )
    expected_subquery_query = (
        'SELECT TO_JSON(mock_alias."json_col")::TEXT AS json_col, '
        'CASE WHEN mock_alias."cash_col" IS NULL THEN NULL ELSE FORMAT(\'"%s"\', "cash_col"::TEXT) END AS cash_col, '
        """ENCODE(mock_alias."bytea_col", 'HEX') AS bytea_col, """
        'mock_alias."timetz_col"::TIME AS timetz_col, '
        'mock_alias."interval_col"::TEXT AS interval_col, '
        'mock_alias."regular_col" AS regular_col '
        "FROM ((SELECT * FROM test_table)) mock_alias"
    )
    assert subquery_query == expected_subquery_query

    # Test with JSONB type
    jsonb_raw_schema = [
        ("jsonb_col", Psycopg2TypeCode.JSONB.value, None, None, None, None, True)
    ]
    jsonb_schema = StructType([StructField("jsonb_col", VariantType())])
    jsonb_query = PostgresDialect.generate_select_query(
        "test_table",
        jsonb_schema,
        jsonb_raw_schema,
        is_query=False,
        query_input_alias="mock_alias",
    )
    expected_jsonb_query = (
        'SELECT TO_JSON("jsonb_col")::TEXT AS jsonb_col FROM test_table'
    )
    assert jsonb_query == expected_jsonb_query
