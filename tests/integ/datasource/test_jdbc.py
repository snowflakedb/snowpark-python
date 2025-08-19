#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark._internal.data_source import JDBC
from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    TempObjectType,
)
from tests.resources.test_data_source_dir.test_jdbc import (
    URL,
    EXTERNAL_ACCESS_INTEGRATION,
    SECRET,
    expected_schema,
    raw_schema,
    expected_data,
)


@pytest.fixture(scope="module")
def jar_path(session):
    stage_name = session.get_session_stage()
    return stage_name + "/ojdbc11-23.8.0.25.04.jar"


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    stage_name = session.get_session_stage()
    session.file.put(
        resources_path + "/test_data_source_dir/ojdbc11-23.8.0.25.04.jar", stage_name
    )
    yield


def test_basic_jdbc(session):
    pass


def test_partitions(session, jar_path):
    # no partitions
    client = JDBC(
        session,
        URL,
        properties={"database": "VirtualDatabase0"},
        table_or_query='SELECT _id, "double", "string", "object", "array", "binary","objectId", "bool", "date", "int32", "long", "decimal128", "timestamp", "code" FROM test_collection',
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
    )
    client.partitions

    # partition column

    # predicates
    pass


def test_custom_schema(session):
    pass


def test_unsupported_dbms_type(session):
    pass


def test_timestamp_type(session):
    pass


def test_infer_schema(session, jar_path):
    client = JDBC(
        session,
        URL,
        table_or_query="SELECT * FROM ALL_TYPE_TABLE",
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
    )
    assert client.schema == expected_schema
    assert client.raw_schema == raw_schema


def test_data_ingestion(session, jar_path):
    client = JDBC(
        session,
        URL,
        table_or_query="SELECT ID, NUMBER_COL, BINARY_FLOAT_COL, BINARY_DOUBLE_COL, VARCHAR2_COL, CHAR_COL, CLOB_COL, NCHAR_COL, NVARCHAR2_COL, NCLOB_COL, DATE_COL, TIMESTAMP_COL, TIMESTAMP_TZ_COL, TIMESTAMP_LTZ_COL, RAW_COL, GUID_COL FROM ALL_TYPE_TABLE",
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
    )
    client.schema
    partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [[query] for query in client.partitions], schema=["partition"]
    ).write.save_as_table(partitions_table, table_type="temp")

    df = client.to_result_snowpark_df(
        client.read(partitions_table), client.schema
    ).order_by("ID")
    assert df.collect() == expected_data
