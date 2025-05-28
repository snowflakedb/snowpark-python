#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source import DataSourcePartitioner
from snowflake.snowpark._internal.data_source.drivers.databricks_driver import (
    DatabricksDriver,
)
from snowflake.snowpark._internal.data_source.utils import DBMS_TYPE
from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    TempObjectType,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BinaryType,
    VariantType,
    MapType,
)
from tests.parameters import DATABRICKS_CONNECTION_PARAMETERS
from tests.resources.test_data_source_dir.test_databricks_data import (
    EXPECTED_TEST_DATA,
    EXPECTED_TYPE,
    TEST_TABLE_NAME,
    DATABRICKS_TEST_EXTERNAL_ACCESS_INTEGRATION,
)
from tests.utils import IS_IN_STORED_PROC, Utils

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import databricks  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass

pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'databricks'"),
    pytest.mark.skipif(IS_IN_STORED_PROC, reason="Need External Access Integration"),
]


def create_databricks_connection():
    import databricks.sql

    return databricks.sql.connect(**DATABRICKS_CONNECTION_PARAMETERS)


@pytest.mark.parametrize(
    "input_type, input_value",
    [("table", TEST_TABLE_NAME), ("query", f"(SELECT * FROM {TEST_TABLE_NAME})")],
)
def test_basic_databricks(session, input_type, input_value):
    input_dict = {
        input_type: input_value,
    }
    df = session.read.dbapi(create_databricks_connection, **input_dict).order_by(
        "COL_BYTE", ascending=True
    )
    Utils.check_answer(df, EXPECTED_TEST_DATA)
    assert df.schema == EXPECTED_TYPE

    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    df.write.save_as_table(table_name, mode="overwrite", table_type="temp")
    df2 = session.table(table_name).order_by("COL_BYTE", ascending=True)
    Utils.check_answer(df2, EXPECTED_TEST_DATA)
    assert df2.schema == EXPECTED_TYPE


@pytest.mark.parametrize(
    "input_type, input_value, error_message",
    [
        ("table", "NOT EXIST", "TABLE_OR_VIEW_NOT_FOUND"),
        ("query", "SELEC ** FORM TABLE", "PARSE_SYNTAX_ERROR"),
    ],
)
def test_error_case(session, input_type, input_value, error_message):
    input_dict = {
        input_type: input_value,
    }
    with pytest.raises(SnowparkDataframeReaderException, match=error_message):
        session.read.dbapi(create_databricks_connection, **input_dict)


@pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'pandas'")
def test_unit_data_source_data_to_pandas_df():
    schema = StructType(
        [
            StructField("COL1", LongType(), nullable=True),
            StructField("COL2", MapType(StringType(), StringType()), nullable=True),
        ]
    )
    data = [
        (1, [("key1", "value1"), ("key2", "value2")]),
    ]
    df = DatabricksDriver.data_source_data_to_pandas_df(data, schema)
    assert df.to_dict(orient="records") == [
        {"COL1": 1, "COL2": [("key1", "value1"), ("key2", "value2")]}
    ]


def test_unicode_column_databricks(session):
    df = session.read.dbapi(create_databricks_connection, table="User_profile_unicode")

    assert df.collect() == [Row(编号=1, 姓名="山田太郎", 国家="日本", 备注="これはUnicodeテストです")]


def test_double_quoted_column_databricks(session):
    df = session.read.dbapi(create_databricks_connection, table="User_profile")
    assert df.collect() == [
        Row(
            id=1,
            name="Yamada Taro",
            country="Japan",
            remarks="This is a test remark",
        )
    ]


@pytest.mark.parametrize(
    "input_type, input_value",
    [("table", TEST_TABLE_NAME), ("query", f"(SELECT * FROM {TEST_TABLE_NAME})")],
)
def test_udtf_ingestion_databricks(session, input_type, input_value, caplog):
    # we define here to avoid test_databricks.py to be pickled and unpickled in UDTF
    def local_create_databricks_connection():
        import databricks.sql

        return databricks.sql.connect(**DATABRICKS_CONNECTION_PARAMETERS)

    input_dict = {
        input_type: input_value,
    }
    df = session.read.dbapi(
        local_create_databricks_connection,
        **input_dict,
        udtf_configs={
            "external_access_integration": DATABRICKS_TEST_EXTERNAL_ACCESS_INTEGRATION
        },
    ).order_by("COL_BYTE", ascending=True)
    ret = df.collect()
    assert ret == EXPECTED_TEST_DATA and df.schema == EXPECTED_TYPE

    assert (
        "TEMPORARY  FUNCTION  data_source_udtf_" "" in caplog.text
        and "table(data_source_udtf" in caplog.text
    )


def test_unit_udtf_ingestion():
    dbx_driver = DatabricksDriver(create_databricks_connection, DBMS_TYPE.DATABRICKS_DB)
    udtf_ingestion_class = dbx_driver.udtf_class_builder()
    udtf_ingestion_instance = udtf_ingestion_class()

    dsp = DataSourcePartitioner(
        create_databricks_connection,
        f"(select * from {TEST_TABLE_NAME}) SORT BY COL_BYTE NULLS FIRST",
        is_query=True,
    )
    yield_data = list(udtf_ingestion_instance.process(dsp.partitions[0]))
    # databricks sort by returns the all None row as the last row regardless of NULLS FIRST/LAST
    # while in snowflake test data after default sort None is the first row

    # databricks sort by seems to be non-deterministic, we sort the data locally to stabilize the outpout
    yield_data = sorted(
        yield_data, key=lambda x: (x[0] is not None, x[0] if x[0] is not None else 0)
    )

    for row, expected_row in zip(
        yield_data, EXPECTED_TEST_DATA
    ):  # None data ordering is the same
        for index, (field, value) in enumerate(zip(EXPECTED_TYPE.fields, row)):
            if isinstance(field.datatype, VariantType):
                # Convert ArrayType, MapType, and StructType to JSON
                if "map" in field.name.lower():
                    assert (
                        (json.loads(value) == json.loads(expected_row[index]))
                        if value is not None
                        else True
                    )
                else:
                    assert (
                        (value == json.loads(expected_row[index]))
                        if value is not None
                        else True
                    )
            elif isinstance(field.datatype, BinaryType):
                # Convert BinaryType to hex string
                assert (
                    (bytearray(bytes.fromhex(value)) == expected_row[index])
                    if value is not None
                    else True
                )
            else:
                # Keep other types as is
                assert value == expected_row[index]
