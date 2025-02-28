#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import math
import os
import queue
import tempfile
import time
import datetime
from unittest import mock
from unittest.mock import patch, MagicMock

import pytest

from snowflake.snowpark._internal.utils import (
    TempObjectType,
)
from snowflake.snowpark.dataframe_reader import _MAX_RETRY_TIME, DataFrameReader
from snowflake.snowpark._internal.data_source_utils import (
    DATA_SOURCE_DBAPI_SIGNATURE,
    DATA_SOURCE_SQL_COMMENT,
    STATEMENT_PARAMS_DATA_SOURCE,
    DBMS_TYPE,
    generate_sql_with_predicates,
    infer_data_source_schema,
    detect_dbms,
    sql_server_to_snowpark_type,
    oracledb_to_snowpark_type,
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
    BooleanType,
)
from tests.resources.test_data_source_dir.test_data_source_data import (
    sql_server_all_type_data,
    sql_server_all_type_small_data,
    sql_server_create_connection,
    sql_server_create_connection_small_data,
    sql_server_create_connection_with_exception,
    sqlite3_db,
    create_connection_to_sqlite3_db,
    oracledb_all_type_data_result,
    oracledb_create_connection,
    oracledb_all_type_small_data_result,
    oracledb_create_connection_small_data,
)
from tests.utils import Utils, IS_WINDOWS

pytestmark = pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)

SQL_SERVER_TABLE_NAME = "AllDataTypesTable"
ORACLEDB_TABLE_NAME = "ALL_TYPES_TABLE"


def upload_and_copy_into_table_with_retry(
    self,
    local_file,
    snowflake_stage_name,
    snowflake_table_name,
    on_error,
):
    time.sleep(2)


def test_dbapi_with_temp_table(session):
    df = session.read.dbapi(
        sql_server_create_connection, SQL_SERVER_TABLE_NAME, max_workers=4
    )
    assert df.collect() == sql_server_all_type_data


def test_dbapi_oracledb(session):
    df = session.read.dbapi(
        oracledb_create_connection, ORACLEDB_TABLE_NAME, max_workers=4
    )
    assert df.collect() == oracledb_all_type_data_result


@pytest.mark.parametrize(
    "create_connection, table_name, expected_result",
    [
        (
            oracledb_create_connection,
            ORACLEDB_TABLE_NAME,
            oracledb_all_type_data_result,
        ),
        (
            oracledb_create_connection_small_data,
            ORACLEDB_TABLE_NAME,
            oracledb_all_type_small_data_result,
        ),
        (sql_server_create_connection, SQL_SERVER_TABLE_NAME, sql_server_all_type_data),
        (
            sql_server_create_connection_small_data,
            SQL_SERVER_TABLE_NAME,
            sql_server_all_type_small_data,
        ),
    ],
)
@pytest.mark.parametrize("fetch_size", [1, 3])
def test_dbapi_batch_fetch(
    session, create_connection, table_name, expected_result, fetch_size
):
    df = session.read.dbapi(
        create_connection, table_name, max_workers=4, fetch_size=fetch_size
    )
    assert df.order_by("ID").collect() == expected_result


def test_dbapi_retry(session):
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._task_fetch_from_data_source",
        side_effect=RuntimeError("Test error"),
    ) as mock_task:
        mock_task.__name__ = "_task_fetch_from_data_source"
        with pytest.raises(
            SnowparkDataframeReaderException, match="\\[RuntimeError\\] Test error"
        ):
            DataFrameReader._task_fetch_from_data_source_with_retry(
                parquet_file_queue=queue.Queue(),
                create_connection=sql_server_create_connection,
                query="SELECT * FROM test_table",
                schema=StructType([StructField("col1", IntegerType(), False)]),
                partition_idx=0,
                tmp_dir="/tmp",
                dbms_type=DBMS_TYPE.SQL_SERVER_DB,
            )
        assert mock_task.call_count == _MAX_RETRY_TIME

    with mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table",
        side_effect=RuntimeError("Test error"),
    ) as mock_task:
        mock_task.__name__ = "_upload_and_copy_into_table"
        with pytest.raises(
            SnowparkDataframeReaderException, match="\\[RuntimeError\\] Test error"
        ):
            session.read._upload_and_copy_into_table_with_retry(
                local_file="fake_file",
                snowflake_stage_name="fake_stage",
                snowflake_table_name="fake_table",
            )
        assert mock_task.call_count == _MAX_RETRY_TIME


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_parallel(session):
    num_partitions = 3

    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, _ = sqlite3_db(dbpath)

        start = time.time()

        with mock.patch(
            "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table_with_retry",
            wrap=upload_and_copy_into_table_with_retry,
        ) as mock_upload_and_copy:
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table_name,
                column="id",
                upper_bound=100,
                lower_bound=0,
                num_partitions=num_partitions,
                max_workers=4,
                custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
            )
            # totally time without parallel is 12 seconds
            assert time.time() - start < 12
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
            SnowparkDataframeReaderException,
            match="Failed to infer Snowpark DataFrame schema",
        ):
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table_name,
            )


def test_predicates():
    select_query = "select * from fake_table"
    predicates = ["id > 1 AND id <= 1000", "id > 1001 AND id <= 2000", "id > 2001"]
    expected_result = [
        "select * from fake_table WHERE id > 1 AND id <= 1000",
        "select * from fake_table WHERE id > 1001 AND id <= 2000",
        "select * from fake_table WHERE id > 2001",
    ]
    res = generate_sql_with_predicates(select_query, predicates)
    assert res == expected_result


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_session_init_statement(session):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, assert_data = sqlite3_db(dbpath)

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table_name,
            custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
            session_init_statement="SELECT 1;",
        )
        assert df.collect() == assert_data

        with pytest.raises(
            SnowparkDataframeReaderException, match='near "FROM": syntax error'
        ):
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table_name,
                custom_schema="id INTEGER",
                session_init_statement="SELECT FROM NOTHING;",
            )


def test_negative_case(session):
    # error happening in fetching
    with pytest.raises(
        SnowparkDataframeReaderException, match="RuntimeError: Fake exception"
    ):
        session.read.dbapi(
            sql_server_create_connection_with_exception, SQL_SERVER_TABLE_NAME
        )

    # error happening during ingestion
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table",
        side_effect=ValueError("Ingestion exception"),
    ) as mock_task:
        mock_task.__name__ = "_upload_and_copy_into_table"
        with pytest.raises(
            SnowparkDataframeReaderException, match="ValueError: Ingestion exception"
        ):
            session.read.dbapi(
                sql_server_create_connection_small_data, SQL_SERVER_TABLE_NAME
            )


@pytest.mark.parametrize(
    "fetch_size, partition_idx, expected_error",
    [(0, 1, False), (2, 100, False), (10, 2, False), (-1, 1001, True)],
)
def test_task_fetch_from_data_source_with_fetch_size(
    fetch_size, partition_idx, expected_error
):
    parquet_file_queue = queue.Queue()
    schema = infer_data_source_schema(
        sql_server_create_connection_small_data(),
        SQL_SERVER_TABLE_NAME,
        DBMS_TYPE.SQL_SERVER_DB,
        "pyodbc",
    )
    file_count = (
        math.ceil(len(sql_server_all_type_small_data) / fetch_size)
        if fetch_size != 0
        else 1
    )

    with tempfile.TemporaryDirectory() as tmp_dir:

        params = {
            "parquet_file_queue": parquet_file_queue,
            "create_connection": sql_server_create_connection_small_data,
            "query": "SELECT * FROM test_table",
            "schema": schema,
            "partition_idx": partition_idx,
            "tmp_dir": tmp_dir,
            "dbms_type": DBMS_TYPE.SQL_SERVER_DB,
            "fetch_size": fetch_size,
        }

        if expected_error:
            with pytest.raises(
                ValueError,
                match="fetch size cannot be smaller than 0",
            ):
                DataFrameReader._task_fetch_from_data_source(**params)
        else:
            DataFrameReader._task_fetch_from_data_source(**params)

            file_idx = 0
            while not parquet_file_queue.empty():
                file_path = parquet_file_queue.get()
                assert (
                    f"data_partition{partition_idx}_fetch{file_idx}.parquet"
                    in file_path
                )
                file_idx += 1
            assert file_idx == file_count


def test_database_detector():
    mock_conn = MagicMock()
    with patch.object(type(mock_conn), "__module__", "UNKNOWN_DRIVER"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, "")

    mock_conn = MagicMock()
    mock_conn.getinfo.return_value = "UNKNOWN"
    with patch.object(type(mock_conn), "__module__", "pyodbc"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, "pyodbc")


def test_type_conversion():
    with pytest.raises(
        NotImplementedError, match="sql server type not supported: non-exist_type"
    ):
        sql_server_to_snowpark_type([("test_col", "non-exist_type", 0, 0, True)])

    with pytest.raises(
        NotImplementedError, match="oracledb type not supported: non-exist_type"
    ):
        oracledb_to_snowpark_type([("test_col", "non-exist_type", 0, 0, True)])


def test_custom_schema_false(session):
    with pytest.raises(ValueError, match="Invalid schema string: timestamp_tz."):
        session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            custom_schema="timestamp_tz",
        )
    with pytest.raises(ValueError, match="Invalid schema type: <class 'int'>."):
        session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            max_workers=4,
            custom_schema=1,
        )


def test_partition_wrong_input(session, caplog):
    with pytest.raises(
        ValueError,
        match="when column is not specified, lower_bound, upper_bound, num_partitions are expected to be None",
    ):
        session.read.dbapi(
            sql_server_create_connection, SQL_SERVER_TABLE_NAME, lower_bound=0
        )

    with pytest.raises(
        ValueError,
        match="when column is specified, lower_bound, upper_bound, num_partitions must be specified",
    ):
        session.read.dbapi(
            sql_server_create_connection, SQL_SERVER_TABLE_NAME, column="id"
        )
    with pytest.raises(
        ValueError, match="Specified column non_exist_column does not exist"
    ):
        session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            column="non_exist_column",
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
            custom_schema=StructType([StructField("ID", IntegerType(), False)]),
        )

    with pytest.raises(ValueError, match="unsupported type BooleanType()"):
        session.read.dbapi(
            sql_server_create_connection,
            SQL_SERVER_TABLE_NAME,
            column="ID",
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
            custom_schema=StructType([StructField("ID", BooleanType(), False)]),
        )
    with pytest.raises(
        ValueError, match="lower_bound cannot be greater than upper_bound"
    ):
        session.read._generate_partition(
            select_query="SELECT * FROM fake_table",
            column_type=IntegerType(),
            column="DATE",
            lower_bound=10,
            upper_bound=1,
            num_partitions=4,
        )

    session.read._generate_partition(
        select_query="SELECT * FROM fake_table",
        column_type=IntegerType(),
        column="DATE",
        lower_bound=0,
        upper_bound=10,
        num_partitions=20,
    )
    assert "The number of partitions is reduced" in caplog.text
