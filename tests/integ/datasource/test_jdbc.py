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
from snowflake.snowpark.exceptions import SnowparkSQLException
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
    POSTGRES_SECRET,
    POSTGRES_URL,
    postgres_expected_data,
)
from tests.resources.test_data_source_dir.test_postgres_data import (
    POSTGRES_TEST_EXTERNAL_ACCESS_INTEGRATION,
)
from tests.utils import RUNNING_ON_JENKINS

SELECT_QUERY = "SELECT ID, NUMBER_COL, BINARY_FLOAT_COL, BINARY_DOUBLE_COL, VARCHAR2_COL, CHAR_COL, CLOB_COL, NCHAR_COL, NVARCHAR2_COL, NCLOB_COL, DATE_COL, TIMESTAMP_COL, TIMESTAMP_TZ_COL, TIMESTAMP_LTZ_COL, RAW_COL, GUID_COL FROM ALL_TYPE_TABLE_JDBC"
EMPTY_QUERY = "SELECT * FROM ALL_TYPE_TABLE_JDBC WHERE 1=0"
POSTGRES_SELECT_QUERY = "select BIGINT_COL, BIGSERIAL_COL, BIT_COL, BIT_VARYING_COL, BOOLEAN_COL, BOX_COL, BYTEA_COL, CHAR_COL, VARCHAR_COL, CIDR_COL, CIRCLE_COL, DATE_COL, DOUBLE_PRECISION_COL, INET_COL, INTEGER_COL, INTERVAL_COL, JSON_COL, JSONB_COL, LINE_COL, LSEG_COL, MACADDR_COL, MACADDR8_COL, NUMERIC_COL, PATH_COL, PG_LSN_COL, PG_SNAPSHOT_COL, POINT_COL, POLYGON_COL, REAL_COL, SMALLINT_COL, SMALLSERIAL_COL, SERIAL_COL, TEXT_COL, TIME_COL, TIMESTAMP_COL, TIMESTAMPTZ_COL, TSQUERY_COL, TSVECTOR_COL, TXID_SNAPSHOT_COL, UUID_COL, XML_COL from test_schema.ALL_TYPE_TABLE"
TABLE_NAME = "ALL_TYPE_TABLE_JDBC"


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS,
        reason="SNOW-2089683: oracledb real connection test failed on jenkins",
    ),
]


@pytest.fixture(scope="module")
def jar_path(session):
    stage_name = session.get_session_stage()
    return stage_name + "/ojdbc17-23.9.0.25.07.jar"


@pytest.fixture(scope="module")
def postgres_jar_path(session):
    stage_name = session.get_session_stage()
    return stage_name + "/postgresql-42.7.7.jar"


@pytest.fixture(scope="module")
def postgres_udtf_configs(session, postgres_jar_path):
    return {
        "external_access_integration": POSTGRES_TEST_EXTERNAL_ACCESS_INTEGRATION,
        "secret": POSTGRES_SECRET,
        "imports": [postgres_jar_path],
    }


@pytest.fixture(scope="module")
def udtf_configs(session, jar_path):
    return {
        "external_access_integration": EXTERNAL_ACCESS_INTEGRATION,
        "secret": SECRET,
        "imports": [jar_path],
    }


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    stage_name = session.get_session_stage()
    session.file.put(
        resources_path + "/test_data_source_dir/ojdbc17-23.9.0.25.07.jar", stage_name
    )
    session.file.put(
        resources_path + "/test_data_source_dir/postgresql-42.7.7.jar", stage_name
    )
    yield


def test_basic_jdbc(session, udtf_configs):
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


@pytest.mark.parametrize(
    "table, query",
    [
        (TABLE_NAME, None),
        (None, SELECT_QUERY),
        (None, f"({SELECT_QUERY})"),
    ],
)
def test_query_and_table(session, table, query, udtf_configs):
    df = session.read.jdbc(
        url=URL, udtf_configs=udtf_configs, query=query, table=table
    ).order_by("ID")
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


def test_jdbc_connection_error(session, udtf_configs, jar_path):
    udtf_configs_no_secret = {
        "external_access_integration": EXTERNAL_ACCESS_INTEGRATION,
        "imports": [jar_path],
    }

    # Invalid URL
    with pytest.raises(SnowparkSQLException, match="Failed to create JDBC connection"):
        session.read.jdbc(
            url="jdbc:invalid:url", udtf_configs=udtf_configs, query=SELECT_QUERY
        ).collect()

    # secret not in EAI
    with pytest.raises(
        ValueError,
        match="external_access_integration, secret and imports must be specified in udtf configs",
    ):
        session.read.jdbc(
            url=URL, udtf_configs=udtf_configs_no_secret, query=SELECT_QUERY
        )

    # Missing secret
    with pytest.raises(
        SnowparkSQLException,
        match="Secret 'INVALID_SECRET' does not exist or operation not authorized",
    ):
        client = JDBC(
            session,
            URL,
            table_or_query=SELECT_QUERY,
            external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
            imports=[jar_path],
            is_query=True,
            secret="INVALID_SECRET",
        )
        client.schema


def test_empty_query(session, udtf_configs):
    df = session.read.jdbc(
        url=URL, udtf_configs=udtf_configs, query=EMPTY_QUERY
    ).order_by("ID")
    assert df.collect() == []


def test_connect_postgres(session, postgres_udtf_configs):
    df = session.read.jdbc(
        url=POSTGRES_URL,
        udtf_configs=postgres_udtf_configs,
        query=POSTGRES_SELECT_QUERY,
    ).order_by("BIGSERIAL_COL")
    assert df.collect() == postgres_expected_data


def test_postgres_session_init_statement(session, postgres_udtf_configs):
    with pytest.raises(
        SnowparkSQLException,
        match="canceling statement due to user request",
    ):
        session.read.jdbc(
            url=POSTGRES_URL,
            udtf_configs=postgres_udtf_configs,
            query=POSTGRES_SELECT_QUERY,
            query_timeout=1,
            session_init_statement=["SELECT pg_sleep(5)"],
        ).collect()
