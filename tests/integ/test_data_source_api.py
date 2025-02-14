#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import os
import tempfile
import time
import datetime
from unittest import mock
import pytest

from snowflake.snowpark._internal.utils import (
    TempObjectType,
)
from snowflake.snowpark.dataframe_reader import (
    _task_fetch_from_data_source_with_retry,
    MAX_RETRY_TIME,
)
from snowflake.snowpark._internal.data_source_utils import (
    DATA_SOURCE_DBAPI_SIGNATURE,
    DATA_SOURCE_SQL_COMMENT,
    STATEMENT_PARAMS_DATA_SOURCE,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    MapType,
    FloatType,
    StringType,
    BinaryType,
    NullType,
    TimestampType,
    TimeType,
    ShortType,
    LongType,
    DoubleType,
    DecimalType,
    ArrayType,
    VariantType,
)
from tests.resources.test_data_source_dir.test_data_source_data import (
    sql_server_all_type_data,
    sql_server_all_type_small_data,
    sql_server_create_connection,
    sql_server_create_connection_small_data,
    sqlite3_db,
    create_connection_to_sqlite3_db,
    oracledb_all_type_data_result,
    oracledb_create_connection,
    oracledb_all_type_small_data_result,
    oracledb_create_connection_small_data,
    fake_detect_dbms_pyodbc,
)
from tests.utils import Utils, IS_WINDOWS

pytestmark = pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)

SQL_SERVER_TABLE_NAME = "AllDataTypesTable"
ORACLEDB_TABLE_NAME = "ALL_TYPES_TABLE"


def fake_task_fetch_from_data_source_with_retry(
    create_connection, query, schema, i, tmp_dir, query_timeout, fetch_size
):
    time.sleep(2)


def upload_and_copy_into_table_with_retry(
    self,
    local_file,
    snowflake_stage_name,
    snowflake_table_name,
    on_error,
):
    time.sleep(2)


def test_dbapi_with_temp_table(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ):
        df = session.read.dbapi(
            sql_server_create_connection, SQL_SERVER_TABLE_NAME, max_workers=4
        )
        assert df.collect() == sql_server_all_type_data


def test_dbapi_oracledb(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ):
        df = session.read.dbapi(
            oracledb_create_connection, ORACLEDB_TABLE_NAME, max_workers=4
        )
        assert df.collect() == oracledb_all_type_data_result


def test_dbapi_batch_fetch_oracledb(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ):
        df = session.read.dbapi(
            oracledb_create_connection, ORACLEDB_TABLE_NAME, max_workers=4, fetch_size=1
        )
        assert df.collect() == oracledb_all_type_data_result

        df = session.read.dbapi(
            oracledb_create_connection, ORACLEDB_TABLE_NAME, max_workers=4, fetch_size=3
        )
        assert df.collect() == oracledb_all_type_data_result

        df = session.read.dbapi(
            oracledb_create_connection_small_data,
            ORACLEDB_TABLE_NAME,
            max_workers=4,
            fetch_size=1,
        )
        assert df.collect() == oracledb_all_type_small_data_result

        df = session.read.dbapi(
            oracledb_create_connection_small_data,
            ORACLEDB_TABLE_NAME,
            max_workers=4,
            fetch_size=3,
        )
        assert df.collect() == oracledb_all_type_small_data_result


def test_dbapi_batch_fetch(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ):
        df = session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            fetch_size=1,
        )
        assert df.collect() == sql_server_all_type_data

        df = session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            fetch_size=3,
        )
        assert df.collect() == sql_server_all_type_data

        df = session.read.dbapi(
            sql_server_create_connection_small_data,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            fetch_size=1,
        )
        assert df.collect() == sql_server_all_type_small_data

        df = session.read.dbapi(
            sql_server_create_connection_small_data,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            fetch_size=3,
        )
        assert df.collect() == sql_server_all_type_small_data


def test_dbapi_retry(session):

    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ), mock.patch(
        "snowflake.snowpark.dataframe_reader._task_fetch_from_data_source",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = _task_fetch_from_data_source_with_retry(
            create_connection=sql_server_create_connection,
            query="SELECT * FROM test_table",
            schema=StructType([StructField("col1", IntegerType(), False)]),
            i=0,
            tmp_dir="/tmp",
        )
        assert mock_task.call_count == MAX_RETRY_TIME
        assert isinstance(result, Exception)

    with mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ), mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = session.read._upload_and_copy_into_table_with_retry(
            local_file="fake_file",
            snowflake_stage_name="fake_stage",
            snowflake_table_name="fake_table",
        )
        assert mock_task.call_count == MAX_RETRY_TIME
        assert isinstance(result, Exception)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_parallel(session):
    num_partitions = 3
    # this test meant to test whether ingest is fully parallelized
    # we cannot mock this function as process pool does not all mock object
    with mock.patch(
        "snowflake.snowpark.dataframe_reader._task_fetch_from_data_source_with_retry",
        new=fake_task_fetch_from_data_source_with_retry,
    ), mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ), mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table_with_retry",
        wrap=upload_and_copy_into_table_with_retry,
    ) as mock_upload_and_copy:

        start = time.time()
        session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            column="Id",
            upper_bound=100,
            lower_bound=0,
            num_partitions=num_partitions,
            max_workers=4,
        )
        end = time.time()
        # totally time without parallel is 12 seconds
        assert end - start < 12
        # verify that mocked function is called for each partition
        assert mock_upload_and_copy.call_count == num_partitions


def test_partition_logic(session):
    expected_queries1 = [
        "SELECT * FROM fake_table WHERE ID < '8' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '10'",
        "SELECT * FROM fake_table WHERE ID >= '10' AND ID < '12'",
        "SELECT * FROM fake_table WHERE ID >= '12'",
    ]

    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=5,
        upper_bound=15,
        num_partitions=4,
    )
    for r, expected_r in zip(queries, expected_queries1):
        assert r == expected_r

    expected_queries2 = [
        "SELECT * FROM fake_table WHERE ID < '-2' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '-2' AND ID < '0'",
        "SELECT * FROM fake_table WHERE ID >= '0' AND ID < '2'",
        "SELECT * FROM fake_table WHERE ID >= '2'",
    ]

    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=-5,
        upper_bound=5,
        num_partitions=4,
    )
    for r, expected_r in zip(queries, expected_queries2):
        assert r == expected_r

    expected_queries3 = [
        "SELECT * FROM fake_table",
    ]

    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=5,
        upper_bound=15,
        num_partitions=1,
    )
    for r, expected_r in zip(queries, expected_queries3):
        assert r == expected_r

    expected_queries4 = [
        "SELECT * FROM fake_table WHERE ID < '6' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '6' AND ID < '7'",
        "SELECT * FROM fake_table WHERE ID >= '7' AND ID < '8'",
        "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '9'",
        "SELECT * FROM fake_table WHERE ID >= '9' AND ID < '10'",
        "SELECT * FROM fake_table WHERE ID >= '10' AND ID < '11'",
        "SELECT * FROM fake_table WHERE ID >= '11' AND ID < '12'",
        "SELECT * FROM fake_table WHERE ID >= '12' AND ID < '13'",
        "SELECT * FROM fake_table WHERE ID >= '13' AND ID < '14'",
        "SELECT * FROM fake_table WHERE ID >= '14'",
    ]

    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=5,
        upper_bound=15,
        num_partitions=10,
    )
    for r, expected_r in zip(queries, expected_queries4):
        assert r == expected_r

    expected_queries5 = [
        "SELECT * FROM fake_table WHERE ID < '8' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '11'",
        "SELECT * FROM fake_table WHERE ID >= '11'",
    ]

    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=5,
        upper_bound=15,
        num_partitions=3,
    )
    for r, expected_r in zip(queries, expected_queries5):
        assert r == expected_r


def test_partition_date_timestamp(session):
    expected_queries1 = [
        "SELECT * FROM fake_table WHERE DATE < '2020-07-30 18:00:00+00:00' OR DATE is null",
        "SELECT * FROM fake_table WHERE DATE >= '2020-07-30 18:00:00+00:00' AND DATE < '2020-09-14 12:00:00+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 12:00:00+00:00' AND DATE < '2020-10-30 06:00:00+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 06:00:00+00:00'",
    ]
    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=DateType(),
        column="DATE",
        lower_bound=str(datetime.date(2020, 6, 15)),
        upper_bound=str(datetime.date(2020, 12, 15)),
        num_partitions=4,
    )

    for r, expected_r in zip(queries, expected_queries1):
        assert r == expected_r

    expected_queries2 = [
        "SELECT * FROM fake_table WHERE DATE < '2020-07-31 05:06:13+00:00' OR DATE is null",
        "SELECT * FROM fake_table WHERE DATE >= '2020-07-31 05:06:13+00:00' AND DATE < '2020-09-14 21:46:55+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 21:46:55+00:00' AND DATE < '2020-10-30 14:27:37+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 14:27:37+00:00'",
    ]
    queries = session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=DateType(),
        column="DATE",
        lower_bound=str(datetime.datetime(2020, 6, 15, 12, 25, 30)),
        upper_bound=str(datetime.datetime(2020, 12, 15, 7, 8, 20)),
        num_partitions=4,
    )

    for r, expected_r in zip(queries, expected_queries2):
        assert r == expected_r


def test_partition_unsupported_type(session):
    with pytest.raises(TypeError, match="unsupported column type for partition:"):
        session.read._generate_partition(
            select_query="SELECT * FROM fake_table",
            column_type=MapType(),
            column="DATE",
            lower_bound=0,
            upper_bound=1,
            num_partitions=4,
        )


def test_telemetry_tracking(caplog, session):
    original_func = session._conn.run_query
    called, comment_showed = 0, 0

    def assert_datasource_statement_params_run_query(*args, **kwargs):
        # assert we set statement_parameters to track datasourcee api usage
        nonlocal comment_showed
        statement_parameters = kwargs.get("_statement_params")
        query = args[0]
        assert statement_parameters[STATEMENT_PARAMS_DATA_SOURCE] == "1"
        if "select" not in query.lower():
            assert DATA_SOURCE_SQL_COMMENT in query
            comment_showed += 1
        nonlocal called
        called += 1
        return original_func(*args, **kwargs)

    with mock.patch(
        "snowflake.snowpark._internal.server_connection.ServerConnection.run_query",
        side_effect=assert_datasource_statement_params_run_query,
    ), mock.patch(
        "snowflake.snowpark._internal.data_source_utils.detect_dbms_pyodbc",
        new=fake_detect_dbms_pyodbc,
    ), mock.patch(
        "snowflake.snowpark._internal.telemetry.TelemetryClient.send_performance_telemetry"
    ) as mock_telemetry:
        df = session.read.dbapi(sql_server_create_connection, SQL_SERVER_TABLE_NAME)
    assert df._plan.api_calls == [{"name": DATA_SOURCE_DBAPI_SIGNATURE}]
    assert (
        called == 4 and comment_showed == 4
    )  # 4 queries: create table, create stage, put file, copy into
    assert mock_telemetry.called
    assert df.collect() == sql_server_all_type_data

    # assert when we save/copy, the statement_params is added
    temp_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    called = 0
    with mock.patch(
        "snowflake.snowpark._internal.server_connection.ServerConnection.run_query",
        side_effect=assert_datasource_statement_params_run_query,
    ):
        df.write.save_as_table(temp_table)
        df.write.copy_into_location(
            f"{temp_stage}/test.parquet",
            file_format_type="parquet",
            header=True,
            overwrite=True,
            single=True,
        )
        assert called == 2


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared accorss processes on windows",
)
@pytest.mark.parametrize(
    "custom_schema",
    [
        "id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
        StructType(
            [
                StructField("id", IntegerType()),
                StructField("int_col", IntegerType()),
                StructField("real_col", FloatType()),
                StructField("text_col", StringType()),
                StructField("blob_col", BinaryType()),
                StructField("null_col", NullType()),
                StructField("ts_col", TimestampType()),
                StructField("date_col", DateType()),
                StructField("time_col", TimeType()),
                StructField("short_col", ShortType()),
                StructField("long_col", LongType()),
                StructField("double_col", DoubleType()),
                StructField("decimal_col", DecimalType()),
                StructField("map_col", MapType()),
                StructField("array_col", ArrayType()),
                StructField("var_col", VariantType()),
            ]
        ),
    ],
)
def test_custom_schema(session, custom_schema):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, assert_data = sqlite3_db(dbpath)

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table_name,
            custom_schema=custom_schema,
        )
        assert df.columns == [col.upper() for col in columns]
        assert df.collect() == assert_data

        with pytest.raises(
            SnowparkDataframeReaderException, match="Unable to infer schema"
        ):
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table_name,
            )
