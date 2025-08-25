#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import re

import pytest

from snowflake.snowpark._internal.data_source import JDBC
from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    TempObjectType,
)
from snowflake.snowpark.types import StructType, StructField, FloatType, DoubleType
from tests.resources.test_data_source_dir.test_jdbc import (
    URL,
    EXTERNAL_ACCESS_INTEGRATION,
    SECRET,
    expected_schema,
    raw_schema,
    expected_data,
    expected_sql_no_partition,
    expected_sql_partition_column,
    expected_sql_predicates,
    custom_schema_result,
)

SELECT_QUERY = "SELECT ID, NUMBER_COL, BINARY_FLOAT_COL, BINARY_DOUBLE_COL, VARCHAR2_COL, CHAR_COL, CLOB_COL, NCHAR_COL, NVARCHAR2_COL, NCLOB_COL, DATE_COL, TIMESTAMP_COL, TIMESTAMP_TZ_COL, TIMESTAMP_LTZ_COL, RAW_COL, GUID_COL FROM ALL_TYPE_TABLE"


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


def test_basic_jdbc(session, jar_path):
    udtf_configs = {
        "external_access_integration": EXTERNAL_ACCESS_INTEGRATION,
        "imports": [jar_path],
    }
    # use with session.read.jdbc
    df = session.read.jdbc(
        url=URL, udtf_configs=udtf_configs, query=SELECT_QUERY
    ).order_by("ID")
    assert df.collect() == expected_data

    # test partitions with partition column
    df = session.read.jdbc(
        url=URL,
        udtf_configs=udtf_configs,
        query=SELECT_QUERY,
        column="ID",
        num_partitions=3,
        upper_bound=10,
        lower_bound=0,
    ).order_by("ID")
    assert df.collect() == expected_data

    # test partitions with predicates
    df = session.read.jdbc(
        url=URL,
        udtf_configs=udtf_configs,
        query=SELECT_QUERY,
        predicates=["ID < 6", "ID >= 6"],
    ).order_by("ID")
    assert df.collect() == expected_data

    # use session.read.format("jdbc")
    df = (
        session.read.format("jdbc")
        .option("url", URL)
        .option("udtf_configs", udtf_configs)
        .option("query", SELECT_QUERY)
        .load()
        .order_by("ID")
    )
    assert df.collect() == expected_data


def test_partitions(session, jar_path):
    # no partitions
    client = JDBC(
        session,
        URL,
        table_or_query=SELECT_QUERY,
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
    )
    assert [
        re.sub(
            r"SNOWPARK_JDBC_SELECT_SQL_ALIAS_[a-z0-9]+",
            "SNOWPARK_JDBC_SELECT_SQL_ALIAS",
            par,
        )
        for par in client.partitions
    ] == expected_sql_no_partition

    # partition column

    client = JDBC(
        session,
        URL,
        table_or_query=SELECT_QUERY,
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
        column="ID",
        num_partitions=3,
        upper_bound=10,
        lower_bound=0,
    )
    assert [
        re.sub(
            r"SNOWPARK_JDBC_SELECT_SQL_ALIAS_[a-z0-9]+",
            "SNOWPARK_JDBC_SELECT_SQL_ALIAS",
            par,
        )
        for par in client.partitions
    ] == expected_sql_partition_column

    # predicates
    client = JDBC(
        session,
        URL,
        table_or_query=SELECT_QUERY,
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
        predicates=["ID < 6", "ID >= 6"],
    )
    assert [
        re.sub(
            r"SNOWPARK_JDBC_SELECT_SQL_ALIAS_[a-z0-9]+",
            "SNOWPARK_JDBC_SELECT_SQL_ALIAS",
            par,
        )
        for par in client.partitions
    ] == expected_sql_predicates


def test_custom_schema(session, jar_path):
    custom_schema = StructType(
        [
            StructField("BINARY_FLOAT_COL", FloatType(), True),
            StructField("BINARY_DOUBLE_COL", DoubleType(), True),
        ]
    )
    client = JDBC(
        session,
        URL,
        table_or_query=SELECT_QUERY,
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
        secret=SECRET,
        custom_schema=custom_schema,
    )
    assert client.schema == custom_schema_result
    assert client.raw_schema == raw_schema


def test_infer_schema(session, jar_path):
    client = JDBC(
        session,
        URL,
        table_or_query=SELECT_QUERY,
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
        table_or_query=SELECT_QUERY,
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
