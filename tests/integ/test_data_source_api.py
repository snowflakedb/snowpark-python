#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import decimal
import functools
import logging
import math
import multiprocessing
import threading
import os
import subprocess
import tempfile
import datetime
import queue
from io import BytesIO
from textwrap import dedent
from unittest import mock
from unittest.mock import patch, MagicMock, PropertyMock

import pytest
from decimal import Decimal

from snowflake.snowpark import Row
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.drivers import (
    PyodbcDriver,
    SqliteDriver,
)
from snowflake.snowpark._internal.data_source.utils import (
    _task_fetch_data_from_source,
    _upload_and_copy_into_table_with_retry,
    _task_fetch_data_from_source_with_retry,
    STATEMENT_PARAMS_DATA_SOURCE,
    DATA_SOURCE_SQL_COMMENT,
    DATA_SOURCE_DBAPI_SIGNATURE,
    detect_dbms,
    DBMS_TYPE,
    DRIVER_TYPE,
    worker_process,
    PARTITION_TASK_COMPLETE_SIGNAL_PREFIX,
    PARTITION_TASK_ERROR_SIGNAL,
    process_completed_futures,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe_reader import _MAX_RETRY_TIME
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    MapType,
    StringType,
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
    OracleDBType,
    sql_server_create_connection_empty_data,
    unknown_dbms_create_connection,
    sql_server_all_type_schema,
    SQLITE3_DB_CUSTOM_SCHEMA_STRING,
    SQLITE3_DB_CUSTOM_SCHEMA_STRUCT_TYPE,
    sql_server_udtf_ingestion_data,
    sql_server_create_connection_unicode_data,
    sql_server_create_connection_double_quoted_data,
)
from tests.utils import Utils, IS_WINDOWS

try:
    import pandas  # noqa: F401

    is_pandas_available = True
except ImportError:
    is_pandas_available = False


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
    pytest.mark.skipif(
        not is_pandas_available,
        reason="pandas is not available",
    ),
]

SQL_SERVER_TABLE_NAME = "AllDataTypesTable"
ORACLEDB_TABLE_NAME = "ALL_TYPE_TABLE"
ORACLEDB_TABLE_NAME_SMALL = "ALL_TYPE_TABLE_SMALL"
ORACLEDB_TEST_EXTERNAL_ACCESS_INTEGRATION = "snowpark_dbapi_oracledb_test_integration"


def test_dbapi_with_temp_table(session, caplog):
    with caplog.at_level(logging.DEBUG):
        df = session.read.dbapi(
            sql_server_create_connection, table=SQL_SERVER_TABLE_NAME, max_workers=4
        )
        # default fetch size is 1k, so we should only see 1 parquet file generated as the data is less than 1k
        assert caplog.text.count("Retrieved BytesIO parquet from queue") == 1
        assert df.collect() == sql_server_all_type_data


@pytest.mark.parametrize(
    "create_connection, table_name, expected_result",
    [
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


def test_dbapi_retry(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source.utils._task_fetch_data_from_source",
        side_effect=RuntimeError("Test error"),
    ) as mock_task:
        mock_task.__name__ = "_task_fetch_from_data_source"
        parquet_queue = multiprocessing.Queue()
        with pytest.raises(
            SnowparkDataframeReaderException, match="\\[RuntimeError\\] Test error"
        ):
            _task_fetch_data_from_source_with_retry(
                worker=DataSourceReader(
                    PyodbcDriver,
                    sql_server_create_connection,
                    StructType([StructField("col1", IntegerType(), False)]),
                    DBMS_TYPE.SQL_SERVER_DB,
                ),
                partition="SELECT * FROM test_table",
                partition_idx=0,
                parquet_queue=parquet_queue,
            )
        assert mock_task.call_count == _MAX_RETRY_TIME

    with mock.patch(
        "snowflake.snowpark._internal.data_source.utils._upload_and_copy_into_table",
        side_effect=RuntimeError("Test error"),
    ) as mock_task:
        mock_task.__name__ = "_upload_and_copy_into_table"
        with pytest.raises(
            SnowparkDataframeReaderException, match="\\[RuntimeError\\] Test error"
        ):
            _upload_and_copy_into_table_with_retry(
                session=session,
                parquet_id="test.parquet",
                parquet_buffer=BytesIO(b"test data"),
                snowflake_stage_name="fake_stage",
                backpressure_semaphore=threading.Semaphore(),
                snowflake_table_name="fake_table",
            )
        assert mock_task.call_count == _MAX_RETRY_TIME


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
@pytest.mark.parametrize("upper_bound, expected_upload_cnt", [(5, 3), (100, 1)])
def test_parallel(session, upper_bound, expected_upload_cnt):
    num_partitions = 3

    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, assert_data = sqlite3_db(dbpath)

        with mock.patch(
            "snowflake.snowpark._internal.data_source.utils._upload_and_copy_into_table_with_retry",
            wraps=_upload_and_copy_into_table_with_retry,
        ) as mock_upload_and_copy:
            df = session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table=table_name,
                column="id",
                upper_bound=upper_bound,
                lower_bound=0,
                num_partitions=num_partitions,
                max_workers=4,
                custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
            )
            assert mock_upload_and_copy.call_count == expected_upload_cnt
            assert df.order_by("ID").collect() == assert_data


@pytest.mark.parametrize(
    "schema, column, lower_bound, upper_bound, num_partitions, expected_queries",
    [
        (
            StructType([StructField("ID", IntegerType())]),
            "ID",
            5,
            15,
            4,
            [
                "SELECT * FROM fake_table WHERE ID < '8' OR ID is null",
                "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '10'",
                "SELECT * FROM fake_table WHERE ID >= '10' AND ID < '12'",
                "SELECT * FROM fake_table WHERE ID >= '12'",
            ],
        ),
        (
            StructType([StructField("ID", IntegerType())]),
            "ID",
            -5,
            5,
            4,
            [
                "SELECT * FROM fake_table WHERE ID < '-2' OR ID is null",
                "SELECT * FROM fake_table WHERE ID >= '-2' AND ID < '0'",
                "SELECT * FROM fake_table WHERE ID >= '0' AND ID < '2'",
                "SELECT * FROM fake_table WHERE ID >= '2'",
            ],
        ),
        (
            StructType([StructField("ID", IntegerType())]),
            "ID",
            5,
            15,
            10,
            [
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
            ],
        ),
        (
            StructType([StructField("ID", IntegerType())]),
            "ID",
            5,
            15,
            3,
            [
                "SELECT * FROM fake_table WHERE ID < '8' OR ID is null",
                "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '11'",
                "SELECT * FROM fake_table WHERE ID >= '11'",
            ],
        ),
        (
            StructType([StructField("DATE", DateType())]),
            "DATE",
            str(datetime.date(2020, 6, 15)),
            str(datetime.date(2020, 12, 15)),
            4,
            [
                "SELECT * FROM fake_table WHERE DATE < '2020-07-30 18:00:00+00:00' OR DATE is null",
                "SELECT * FROM fake_table WHERE DATE >= '2020-07-30 18:00:00+00:00' AND DATE < '2020-09-14 12:00:00+00:00'",
                "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 12:00:00+00:00' AND DATE < '2020-10-30 06:00:00+00:00'",
                "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 06:00:00+00:00'",
            ],
        ),
        (
            StructType([StructField("DATE", DateType())]),
            "DATE",
            str(datetime.datetime(2020, 6, 15, 12, 25, 30)),
            str(datetime.datetime(2020, 12, 15, 7, 8, 20)),
            4,
            [
                "SELECT * FROM fake_table WHERE DATE < '2020-07-31 05:06:13+00:00' OR DATE is null",
                "SELECT * FROM fake_table WHERE DATE >= '2020-07-31 05:06:13+00:00' AND DATE < '2020-09-14 21:46:55+00:00'",
                "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 21:46:55+00:00' AND DATE < '2020-10-30 14:27:37+00:00'",
                "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 14:27:37+00:00'",
            ],
        ),
    ],
)
def test_partition_logic(
    session, schema, column, lower_bound, upper_bound, num_partitions, expected_queries
):
    with patch.object(
        DataSourcePartitioner, "schema", new_callable=PropertyMock
    ) as mock_schema:
        partitioner = DataSourcePartitioner(
            sql_server_create_connection,
            table_or_query="fake_table",
            is_query=False,
            column=column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
        )
        mock_schema.return_value = schema
        queries = partitioner.partitions
        for r, expected_r in zip(queries, expected_queries):
            assert r == expected_r


def test_partition_unsupported_type(session):
    with pytest.raises(ValueError, match="unsupported type"):
        with patch.object(
            DataSourcePartitioner, "schema", new_callable=PropertyMock
        ) as mock_schema:
            partitioner = DataSourcePartitioner(
                sql_server_create_connection,
                table_or_query="fake_table",
                is_query=False,
                column="DATE",
                lower_bound=0,
                upper_bound=1,
                num_partitions=4,
            )
            mock_schema.return_value = StructType(
                [StructField("DATE", MapType(), False)]
            )
            partitioner.partitions


def test_telemetry_tracking(caplog, session):
    original_func = session._conn.run_query
    called, comment_showed = 0, 0

    def assert_datasource_statement_params_run_query(*args, **kwargs):
        # assert we set statement_parameters to track datasourcee api usage
        nonlocal comment_showed
        statement_parameters = kwargs.get("_statement_params")
        query = args[0]
        if not query.lower().startswith("put"):
            # put_stream does not accept statement_params
            assert statement_parameters[STATEMENT_PARAMS_DATA_SOURCE] == "1"
        if "select" not in query.lower() and "put" not in query.lower():
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
        df = session.read.dbapi(
            sql_server_create_connection, table=SQL_SERVER_TABLE_NAME
        )
    assert df._plan.api_calls == [{"name": DATA_SOURCE_DBAPI_SIGNATURE}]
    assert (
        called == 4 and comment_showed == 3
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
    [SQLITE3_DB_CUSTOM_SCHEMA_STRING, SQLITE3_DB_CUSTOM_SCHEMA_STRUCT_TYPE],
)
def test_custom_schema(session, custom_schema):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, assert_data = sqlite3_db(dbpath)

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
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
                table=table_name,
            )


def test_predicates():
    with patch.object(
        DataSourcePartitioner, "schema", new_callable=PropertyMock
    ) as mock_schema:
        partitioner = DataSourcePartitioner(
            sql_server_create_connection,
            table_or_query="fake_table",
            is_query=False,
            predicates=[
                "id > 1 AND id <= 1000",
                "id > 1001 AND id <= 2000",
                "id > 2001",
            ],
        )
        mock_schema.return_value = StructType([StructField("ID", IntegerType(), False)])
        queries = partitioner.partitions
        expected_result = [
            "SELECT * FROM fake_table WHERE id > 1 AND id <= 1000",
            "SELECT * FROM fake_table WHERE id > 1001 AND id <= 2000",
            "SELECT * FROM fake_table WHERE id > 2001",
        ]
        assert queries == expected_result


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_session_init_statement(session):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, assert_data = sqlite3_db(dbpath)

        new_data1 = (8, 100) + (None,) * (len(columns) - 3) + ('"123"',)
        new_data2 = (9, 101) + (None,) * (len(columns) - 3) + ('"456"',)

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
            custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
            session_init_statement=f"insert into {table_name} (int_col, var_col) values (100, '123');",
        )
        assert df.collect() == assert_data + [new_data1]

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
            custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
            session_init_statement=[
                f"insert into {table_name} (int_col, var_col) values (100, '123');",
                f"insert into {table_name} (int_col, var_col) values (101, '456');",
            ],
        )
        assert df.collect() == assert_data + [new_data1, new_data2]

        with pytest.raises(
            SnowparkDataframeReaderException,
            match=r'Failed to execute session init statement: \'SELECT FROM NOTHING;\' due to exception \'OperationalError\(\'near "FROM": syntax error\'\)\'',
        ):
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table=table_name,
                custom_schema="id INTEGER",
                session_init_statement="SELECT FROM NOTHING;",
            )


def test_negative_case(session):
    # error happening in fetching
    with pytest.raises(
        SnowparkDataframeReaderException, match="RuntimeError: Fake exception"
    ):
        session.read.dbapi(
            sql_server_create_connection_with_exception, table=SQL_SERVER_TABLE_NAME
        )

    # error happening during ingestion
    with mock.patch(
        "snowflake.snowpark._internal.data_source.utils._upload_and_copy_into_table",
        side_effect=ValueError("Ingestion exception"),
    ) as mock_task:
        mock_task.__name__ = "_upload_and_copy_into_table"
        with pytest.raises(
            SnowparkDataframeReaderException, match="ValueError: Ingestion exception"
        ):
            session.read.dbapi(
                sql_server_create_connection_small_data, table=SQL_SERVER_TABLE_NAME
            )


@pytest.mark.parametrize(
    "fetch_size, partition_idx, expected_error",
    [(0, 1, False), (2, 100, False), (10, 2, False), (-1, 1001, True)],
)
def test_task_fetch_from_data_source_with_fetch_size(
    fetch_size, partition_idx, expected_error
):
    partitioner = DataSourcePartitioner(
        sql_server_create_connection_small_data,
        table_or_query="fake",
        is_query=False,
        fetch_size=fetch_size,
    )
    schema = partitioner.schema
    file_count = (
        math.ceil(len(sql_server_all_type_small_data) / fetch_size)
        if fetch_size != 0
        else 1
    )

    parquet_queue = multiprocessing.Queue()

    params = {
        "worker": DataSourceReader(
            PyodbcDriver,
            sql_server_create_connection_small_data,
            schema=schema,
            dbms_type=DBMS_TYPE.SQL_SERVER_DB,
            fetch_size=fetch_size,
        ),
        "partition": "SELECT * FROM test_table",
        "partition_idx": partition_idx,
        "parquet_queue": parquet_queue,
    }

    if expected_error:
        with pytest.raises(
            ValueError,
            match="fetch size cannot be smaller than 0",
        ):
            _task_fetch_data_from_source(**params)
    else:
        _task_fetch_data_from_source(**params)

        # Collect all parquet data from the queue
        parquet_files = []
        completion_signal_received = False
        timeout = 5.0  # 5 second timeout

        # Keep draining the queue until we get the completion signal or timeout
        while not completion_signal_received:
            try:
                parquet_id, parquet_buffer = parquet_queue.get(timeout=timeout)

                # Check for completion signal
                if parquet_id.startswith(PARTITION_TASK_COMPLETE_SIGNAL_PREFIX):
                    completion_signal_received = True
                    continue

                # Verify parquet file naming pattern
                assert parquet_id.startswith(
                    f"data_partition{partition_idx}_fetch"
                ), f"Unexpected parquet ID: {parquet_id}"
                assert parquet_id.endswith(
                    ".parquet"
                ), f"Parquet ID should end with .parquet: {parquet_id}"

                # Verify parquet_buffer is a BytesIO object
                assert isinstance(
                    parquet_buffer, BytesIO
                ), f"Expected BytesIO, got {type(parquet_buffer)}"

                parquet_files.append((parquet_id, parquet_buffer))

            except queue.Empty:
                pytest.fail(
                    f"Timeout waiting for completion signal after {timeout} seconds"
                )

        # Verify we received the completion signal
        assert (
            completion_signal_received
        ), "Should have received partition completion signal"

        # Verify the number of parquet files matches expected count
        assert (
            len(parquet_files) == file_count
        ), f"Expected {file_count} parquet files, got {len(parquet_files)}"

        # Verify the parquet files are named correctly in sequence
        for idx, (parquet_id, _) in enumerate(parquet_files):
            expected_name = f"data_partition{partition_idx}_fetch{idx}.parquet"
            assert (
                expected_name in parquet_id
            ), f"Expected {expected_name} in {parquet_id}"


def test_database_detector():
    mock_conn = MagicMock()
    with patch.object(type(mock_conn), "__module__", "UNKNOWN_DRIVER"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, DRIVER_TYPE.UNKNOWN)

    mock_conn = MagicMock()
    mock_conn.getinfo.return_value = "UNKNOWN"
    with patch.object(type(mock_conn), "__module__", "pyodbc"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, DRIVER_TYPE.PYODBC)


def test_type_conversion():
    invalid_type = OracleDBType("ID", "UNKNOWN", None, None, False)
    with pytest.raises(NotImplementedError, match="sql server type not supported"):
        PyodbcDriver(
            sql_server_create_connection, DBMS_TYPE.SQL_SERVER_DB
        ).to_snow_type([("test_col", invalid_type, None, None, 0, 0, True)])


def test_custom_schema_false(session):
    with pytest.raises(ValueError, match="Invalid schema string: timestamp_tz."):
        session.read.dbapi(
            sql_server_create_connection,
            table=SQL_SERVER_TABLE_NAME,
            max_workers=4,
            custom_schema="timestamp_tz",
        )
    with pytest.raises(ValueError, match="Invalid schema type: <class 'int'>."):
        session.read.dbapi(
            sql_server_create_connection,
            table=SQL_SERVER_TABLE_NAME,
            max_workers=4,
            custom_schema=1,
        )


def test_partition_wrong_input(session, caplog):
    with pytest.raises(
        ValueError,
        match="when column is not specified, lower_bound, upper_bound, num_partitions are expected to be None",
    ):
        session.read.dbapi(
            sql_server_create_connection, table=SQL_SERVER_TABLE_NAME, lower_bound=0
        )

    with pytest.raises(
        ValueError,
        match="when column is specified, lower_bound, upper_bound, num_partitions must be specified",
    ):
        session.read.dbapi(
            sql_server_create_connection, table=SQL_SERVER_TABLE_NAME, column="id"
        )
    with pytest.raises(
        ValueError, match="Specified column non_exist_column does not exist"
    ):
        session.read.dbapi(
            sql_server_create_connection,
            table=SQL_SERVER_TABLE_NAME,
            column="non_exist_column",
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
            custom_schema=StructType([StructField("ID", IntegerType(), False)]),
        )

    with pytest.raises(ValueError, match="unsupported type BooleanType()"):
        session.read.dbapi(
            sql_server_create_connection,
            table=SQL_SERVER_TABLE_NAME,
            column="ID",
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
            custom_schema=StructType([StructField("ID", BooleanType(), False)]),
        )
    with patch.object(
        DataSourcePartitioner, "schema", new_callable=PropertyMock
    ) as mock_schema:
        with pytest.raises(
            ValueError, match="lower_bound cannot be greater than upper_bound"
        ):
            partitioner = DataSourcePartitioner(
                sql_server_create_connection,
                table_or_query="fake_table",
                is_query=False,
                column="DATE",
                lower_bound=10,
                upper_bound=1,
                num_partitions=4,
            )
            mock_schema.return_value = StructType(
                [StructField("DATE", IntegerType(), False)]
            )
            partitioner.partitions

        partitioner = DataSourcePartitioner(
            sql_server_create_connection,
            table_or_query="fake_table",
            is_query=False,
            column="DATE",
            lower_bound=0,
            upper_bound=10,
            num_partitions=20,
        )
        mock_schema.return_value = StructType(
            [StructField("DATE", IntegerType(), False)]
        )
        partitioner.partitions
        assert "The number of partitions is reduced" in caplog.text


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_query_parameter(session):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, assert_data = sqlite3_db(dbpath)

        filter_idx = 2
        with pytest.raises(SnowparkDataframeReaderException, match="but not both"):
            session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table=table_name,
                query=f"(SELECT * FROM PrimitiveTypes WHERE id > {filter_idx}) as t",
            )

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            query=f"(SELECT * FROM PrimitiveTypes WHERE id > {filter_idx}) as t",
            custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
        )
        assert df.columns == [col.upper() for col in columns]
        assert df.collect() == assert_data[filter_idx:]

        def sqlite_to_snowpark_type(schema):
            assert (
                len(schema) == 2
                and schema[0][0] == "int_col"
                and schema[1][0] == "text_col"
            )
            return StructType(
                [
                    StructField("int_col", IntegerType()),
                    StructField("text_col", StringType()),
                ]
            )

        with mock.patch(
            "snowflake.snowpark._internal.data_source.drivers.sqlite_driver.SqliteDriver.to_snow_type",
            side_effect=sqlite_to_snowpark_type,
        ):
            query = (
                f"SELECT int_col, text_col FROM PrimitiveTypes WHERE id > {filter_idx}"
            )
            df = session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                query=f"({query}) as t",
            )
            assert df.columns == ["INT_COL", "TEXT_COL"]
            assert (
                df.collect()
                == create_connection_to_sqlite3_db(dbpath).execute(query).fetchall()
            )


@pytest.mark.skipif(
    "config.getoption('enable_ast', default=False)",
    reason="SNOW-1961756: Data source APIs not supported by AST",
)
def test_option_load(session):
    df = (
        session.read.format("dbapi")
        .option("create_connection", sql_server_create_connection)
        .option("table", SQL_SERVER_TABLE_NAME)
        .option("max_workers", 4)
        .option("fetch_size", 2)
        .load()
    )
    assert df.order_by("ID").collect() == sql_server_all_type_data

    with pytest.raises(TypeError):
        session.read.format("json").load()

    with pytest.raises(ValueError):
        session.read.format("dbapi").load("path")

    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'create_connection'"
    ):
        session.read.format("dbapi").load()


def test_empty_table(session):
    df = session.read.dbapi(
        sql_server_create_connection_empty_data, table=SQL_SERVER_TABLE_NAME
    )
    assert df.collect() == []


def test_sql_server_udtf_ingestion(session):
    raw_schema = [
        ("Id", int, None, None, 10, 0, False),
        ("SmallIntCol", int, None, None, 5, 0, True),
        ("TinyIntCol", int, None, None, 3, 0, True),
        ("BigIntCol", int, None, None, 19, None, True),
        ("DecimalCol", decimal.Decimal, None, None, 10, 2, True),
        ("FloatCol", float, None, None, 53, None, True),
        ("RealCol", float, None, None, 24, None, True),
        ("MoneyCol", decimal.Decimal, None, None, 19, 4, True),
        ("SmallMoneyCol", decimal.Decimal, None, None, 10, 4, True),
        ("CharCol", str, None, None, None, None, True),
        ("VarCharCol", str, None, None, None, None, True),
        ("TextCol", str, None, None, None, None, True),
        ("NCharCol", str, None, None, None, None, True),
        ("NVarCharCol", str, None, None, None, None, True),
        ("NTextCol", str, None, None, None, None, True),
        ("DateCol", datetime.date, None, None, None, None, True),
        ("TimeCol", datetime.time, None, None, None, None, True),
        ("DateTimeCol", datetime.datetime, None, None, None, None, True),
        ("DateTime2Col", datetime.datetime, None, None, None, None, True),
        ("SmallDateTimeCol", datetime.datetime, None, None, None, None, True),
        ("BinaryCol", bytes, None, None, None, None, True),
        ("VarBinaryCol", bytes, None, None, None, None, True),
        ("BitCol", bool, None, 1, None, None, True),
        ("UniqueIdentifierCol", bytes, None, None, None, None, True),
    ]

    def create_connection_udtf_sql_server():
        class FakeConnection:
            def __init__(self) -> None:
                self.sql = ""
                self.start_index = 0
                self.data = [
                    (
                        1,
                        100,
                        10,
                        100000,
                        Decimal("12345.67"),
                        1.23,
                        0.4560000002384186,
                        Decimal("1234.5600"),
                        Decimal("12.3400"),
                        "FixedStr1 ",
                        "VarStr1",
                        "Text1",
                        "UniFix1   ",
                        "UniVar1",
                        "UniText1",
                        datetime.date(2023, 1, 1),
                        datetime.time(12, 0),
                        datetime.datetime(2023, 1, 1, 12, 0),
                        datetime.datetime(2023, 1, 1, 12, 0, 0, 123000),
                        datetime.datetime(2023, 1, 1, 12, 0),
                        b"\x01\x02\x03\x04\x05".hex(),
                        b"\x01\x02\x03\x04".hex(),
                        True,
                        b"06D48351-6EA7-4E64-81A2-9921F0EC42A5".hex(),
                    ),
                ]

            def cursor(self):
                return self

            def execute(self, sql: str):
                self.sql = sql
                return self

            def add_output_converter(self, need_convert_type, function):
                pass

            def get_output_converter(self, convert_type):
                return None

            def fetchmany(self, row_count: int):
                end_index = self.start_index + row_count
                res = (
                    self.data[self.start_index : end_index]
                    if end_index < len(self.data)
                    else self.data[self.start_index :]
                )
                self.start_index = end_index
                return res

        return FakeConnection()

    partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [["SELECT * FROM ALL_TYPE_DATA"]], schema=["partition"]
    ).write.save_as_table(partitions_table, table_type="temp")

    driver = PyodbcDriver(create_connection_udtf_sql_server, DBMS_TYPE.SQL_SERVER_DB)
    df = driver.udtf_ingestion(
        session,
        driver.to_snow_type(raw_schema),
        partitions_table,
        "",
        packages=["pyodbc"],
    )
    Utils.check_answer(df, sql_server_udtf_ingestion_data)


def test_unknown_driver_with_custom_schema(session):
    with pytest.raises(
        SnowparkDataframeReaderException,
        match="Failed to infer Snowpark DataFrame schema",
    ):
        session.read.dbapi(
            unknown_dbms_create_connection,
            table=SQL_SERVER_TABLE_NAME,
        )

    df = session.read.dbapi(
        unknown_dbms_create_connection,
        table=SQL_SERVER_TABLE_NAME,
        custom_schema=",".join(
            [f"col_{i} Variant" for i in range(len(sql_server_all_type_schema))]
        ),
    )
    assert len(df.collect()) == len(sql_server_all_type_small_data)


@pytest.mark.parametrize(
    "fetch_size, fetch_merge_count, expected_batch_cnt",
    [(1, 1, 7), (5, 1, 2), (1, 5, 2), (2, 2, 2), (3, 3, 1), (100, 2, 1), (1, 100, 1)],
)
def test_fetch_merge_count_unit(fetch_size, fetch_merge_count, expected_batch_cnt):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, _ = sqlite3_db(dbpath)
        reader = DataSourceReader(
            SqliteDriver,
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            schema=SQLITE3_DB_CUSTOM_SCHEMA_STRUCT_TYPE,
            dbms_type=DBMS_TYPE.SQLITE_DB,
            fetch_size=fetch_size,
            fetch_merge_count=fetch_merge_count,
        )
        all_fetched_data = []
        batch_cnt = 0
        for data in reader.read(f"SELECT * FROM {table_name}"):
            assert 0 < len(data) <= fetch_merge_count * fetch_size
            all_fetched_data.extend(data)
            batch_cnt += 1
        assert all_fetched_data == example_data and batch_cnt == expected_batch_cnt


def test_fetch_merge_count_integ(session, caplog):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, assert_data = sqlite3_db(dbpath)
        with caplog.at_level(logging.DEBUG):
            df = session.read.dbapi(
                functools.partial(create_connection_to_sqlite3_db, dbpath),
                table=table_name,
                custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
                fetch_size=2,
                fetch_merge_count=2,
            )
            assert df.order_by("ID").collect() == assert_data
            # 2 batch + 2 fetch size = 2 parquet file
            assert caplog.text.count("Retrieved BytesIO parquet from queue") == 2


def test_unicode_column_name_sql_server(session):
    df = session.read.dbapi(sql_server_create_connection_unicode_data, table='"用户资料"')
    assert df.collect() == [Row(编号=1, 姓名="山田太郎", 国家="日本", 备注="これはUnicodeテストです")]


def test_double_quoted_column_name_sql_server(session):
    df = session.read.dbapi(
        sql_server_create_connection_double_quoted_data, table='"UserProfile"'
    )
    assert df.collect() == [
        Row(Id=1, FullName="John Doe", Country="USA", Notes="Fake note")
    ]


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_local_create_connection_function(session, db_parameters):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, assert_data = sqlite3_db(dbpath)

        # test local function definition
        def local_create_connection():
            import sqlite3

            return sqlite3.connect(dbpath)

        df = session.read.dbapi(
            local_create_connection,
            table=table_name,
            custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
        )
        assert df.order_by("ID").collect() == assert_data

        # test function is defined in the main
        code = dedent(
            f"""
        if __name__ == "__main__":
            import sqlite3
            from snowflake.snowpark import Session

            def local_create_connection():
                return sqlite3.connect("{str(dbpath)}")

            session = Session.builder.configs({str(db_parameters)}).create()
            df = session.read.dbapi(
                local_create_connection,
                table='{table_name}',
                custom_schema='{SQLITE3_DB_CUSTOM_SCHEMA_STRING}',
            )
            assert df.collect()
            print("successful ingestion")
        """
        )
        result = subprocess.run(["python", "-c", code], capture_output=True, text=True)
        assert "successful ingestion" in result.stdout


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_worker_process_unit():
    """Test worker_process function with sqlite3 database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, columns, example_data, _ = sqlite3_db(dbpath)

        # Create DataSourceReader for sqlite3
        reader = DataSourceReader(
            SqliteDriver,
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            schema=SQLITE3_DB_CUSTOM_SCHEMA_STRUCT_TYPE,
            dbms_type=DBMS_TYPE.SQLITE_DB,
            fetch_size=2,
        )

        partition_queue = multiprocessing.Queue()
        parquet_queue = multiprocessing.Queue()

        # Set up partition_queue to return test data, then raise queue.Empty
        partition_queue.put((0, f"SELECT * FROM {table_name} WHERE id <= 3"))
        partition_queue.put((1, f"SELECT * FROM {table_name} WHERE id > 3"))

        # Call the worker_process function directly (using real sqlite3 operations)
        worker_process(partition_queue, parquet_queue, reader)

        expected_order = [
            "data_partition0_fetch0.parquet",
            "data_partition0_fetch1.parquet",
            f"{PARTITION_TASK_COMPLETE_SIGNAL_PREFIX}0",
            "data_partition1_fetch0.parquet",
            "data_partition1_fetch1.parquet",
            f"{PARTITION_TASK_COMPLETE_SIGNAL_PREFIX}1",
        ]

        parquet_ids = []
        done_job = set()
        while len(done_job) < 2:
            try:
                parquet_id, parquet_buffer = parquet_queue.get(timeout=1)
                parquet_ids.append(parquet_id)
                if parquet_id.startswith("PARTITION_COMPLETE_"):
                    done_job.add(int(parquet_id.split("_")[-1]))
            except queue.Empty:
                break
        assert parquet_ids == expected_order, f"Order mismatch: {parquet_ids}"

        # check error handling
        partition_queue.put((0, "SELECT * FROM NON_EXISTING_TABLE"))
        worker_process(partition_queue, parquet_queue, reader)
        error_signal, error_instance = parquet_queue.get()
        assert error_signal == PARTITION_TASK_ERROR_SIGNAL
        assert isinstance(
            error_instance, SnowparkDataframeReaderException
        ) and "no such table: NON_EXISTING_TABLE" in str(error_instance)


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="sqlite3 file can not be shared across processes on windows",
)
def test_graceful_shutdown_on_worker_process_error(session):
    """Test graceful shutdown when worker_process raises an exception."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, _ = sqlite3_db(dbpath)

        # Track created processes by patching the Process.__init__ method
        created_processes = []
        original_init = multiprocessing.Process.__init__

        def track_process_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            created_processes.append(self)

        with mock.patch.object(
            multiprocessing.Process, "__init__", track_process_init
        ), mock.patch(
            "snowflake.snowpark.dataframe_reader.process_parquet_queue_with_threads",
            side_effect=RuntimeError("Simulated error in queue processing"),
        ):

            # This should trigger the graceful shutdown
            with pytest.raises(
                SnowparkDataframeReaderException,
                match="Simulated error in queue processing",
            ):
                session.read.dbapi(
                    functools.partial(create_connection_to_sqlite3_db, dbpath),
                    table=table_name,
                    column="id",
                    upper_bound=10,
                    lower_bound=0,
                    num_partitions=2,
                    max_workers=2,
                    custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
                )

            # Verify that processes were created
            assert len(created_processes) == 2, "Should have created 2 processes"

            # Give a moment for processes to be terminated
            import time

            time.sleep(0.1)

            # Verify that all processes have been terminated
            for i, process in enumerate(created_processes):
                assert (
                    not process.is_alive()
                ), f"Process {i} should not be alive after shutdown"
                # Check that exitcode indicates termination (not successful completion)
                assert (
                    process.exitcode != 0
                ), f"Process {i} should have non-zero exit code after termination"


def test_unit_process_completed_futures_comprehensive():
    """Comprehensive test covering all lines and branches of process_completed_futures."""
    from concurrent.futures import Future

    # Test 1: Normal successful completion path
    successful_future = MagicMock(spec=Future)
    successful_future.done.return_value = True
    successful_future.result.return_value = None

    # Test 2: Not done future (should remain)
    pending_future = MagicMock(spec=Future)
    pending_future.done.return_value = False

    thread_futures = {
        ("success.parquet", successful_future),
        ("pending.parquet", pending_future),
    }

    # Call function - should process successfully
    process_completed_futures(thread_futures)

    # Verify: successful future removed, pending remains
    assert len(thread_futures) == 1
    assert ("pending.parquet", pending_future) in thread_futures
    successful_future.result.assert_called_once()
    pending_future.result.assert_not_called()

    # Test 3: Exception handling path
    error_future = MagicMock(spec=Future)
    error_future.done.return_value = True
    error_future.result.side_effect = RuntimeError("Test error")

    cancellable_future = MagicMock(spec=Future)
    cancellable_future.done.return_value = False

    done_future = MagicMock(spec=Future)
    done_future.done.return_value = True

    thread_futures = {
        ("error.parquet", error_future),
        ("cancellable.parquet", cancellable_future),
        ("done.parquet", done_future),
    }

    # Call function - should raise exception and cancel undone futures
    with pytest.raises(RuntimeError, match="Test error"):
        process_completed_futures(thread_futures)

    # Verify: exception raised, undone futures cancelled, set cleared
    assert len(thread_futures) == 0
    cancellable_future.cancel.assert_called_once()
    done_future.cancel.assert_not_called()  # Don't cancel already done futures

    # Test 4: Empty set edge case
    empty_set = set()
    process_completed_futures(empty_set)  # Should not raise any exceptions
    assert len(empty_set) == 0


@pytest.mark.skipif(not is_pandas_available, reason="pandas not available")
def test_case_sensitive_copy_into(session):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    temp_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
    create_temp_stage = f"""create temp stage {temp_stage_name}"""
    # create_temp_table = f"""create temp table {temp_table_name} ("Same_Word_Different_Case" STRING, "SAME_WORD_DIFFERENT_CASE" STRING)"""
    create_temp_table = (
        f"""create temp table {temp_table_name} ("SAME_WORD_DIFFERENT_CASE" STRING)"""
    )

    session.sql(create_temp_table).collect()
    session.sql(create_temp_stage).collect()

    # df = pandas.DataFrame([['sensitive_value', 'insensitive_value']], columns=["Same_Word_Different_Case", "SAME_WORD_DIFFERENT_CASE"], dtype=object)
    df = pandas.DataFrame(
        [["sensitive_value"]], columns=["Same_Word_Different_Case"], dtype=object
    )

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer)

    parquet_buffer.seek(0)

    try:
        # Upload BytesIO directly to stage using put_stream
        stage_file_path = f"@{temp_stage_name}/test_parquet.parquet"
        session.file.put_stream(
            parquet_buffer,
            stage_file_path,
            overwrite=True,
        )
        copy_into_table_query = f"""
        COPY INTO {temp_table_name} FROM @{temp_stage_name}/test_parquet.parquet
        FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER=TRUE)
        MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
        PURGE=TRUE
        """
        session.sql(copy_into_table_query).collect()

        df = session.table(temp_table_name)
        assert df.collect() == [Row(SAME_WORD_DIFFERENT_CASE=None)]

    finally:
        # proactively close the buffer to release memory
        parquet_buffer.close()
