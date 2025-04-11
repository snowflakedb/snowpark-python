#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import logging
import math
import os
import tempfile
import datetime
from unittest import mock
from unittest.mock import patch, MagicMock, PropertyMock

import oracledb
import pytest

from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.drivers.oracledb_driver import (
    OracledbDriver,
    output_type_handler,
)
from snowflake.snowpark._internal.data_source.drivers.pyodbc_driver import PyodbcDriver
from snowflake.snowpark._internal.data_source.utils import (
    _task_fetch_data_from_source_with_retry,
    _upload_and_copy_into_table_with_retry,
    _task_fetch_data_from_source,
    STATEMENT_PARAMS_DATA_SOURCE,
    DATA_SOURCE_SQL_COMMENT,
    DATA_SOURCE_DBAPI_SIGNATURE,
    detect_dbms,
    DBMS_TYPE,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.dataframe_reader import _MAX_RETRY_TIME
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
    OracleDBType,
    sql_server_create_connection_empty_data,
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
ORACLEDB_TABLE_NAME = "ALL_TYPES_TABLE"


def test_dbapi_with_temp_table(session, caplog):
    with caplog.at_level(logging.DEBUG):
        df = session.read.dbapi(
            sql_server_create_connection, table=SQL_SERVER_TABLE_NAME, max_workers=4
        )
        # default fetch size is 1k, so we should only see 1 parquet file generated as the data is less than 1k
        assert caplog.text.count("Retrieved file from parquet queue") == 1
        assert df.collect() == sql_server_all_type_data


def test_dbapi_oracledb(session):
    df = session.read.dbapi(
        oracledb_create_connection,
        table=ORACLEDB_TABLE_NAME,
        max_workers=4,
        query_timeout=5,
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
    session, create_connection, table_name, expected_result, fetch_size, caplog
):
    with caplog.at_level(logging.DEBUG):
        df = session.read.dbapi(
            create_connection, table=table_name, max_workers=4, fetch_size=fetch_size
        )
        # we only expect math.ceil(len(expected_result) / fetch_size) parquet files to be generated
        # for example, 5 rows, fetch size 2, we expect 3 parquet files
        assert caplog.text.count("Retrieved file from parquet queue") == math.ceil(
            len(expected_result) / fetch_size
        )
        assert df.order_by("ID").collect() == expected_result


def test_dbapi_retry(session):
    with mock.patch(
        "snowflake.snowpark._internal.data_source.utils._task_fetch_data_from_source",
        side_effect=RuntimeError("Test error"),
    ) as mock_task:
        mock_task.__name__ = "_task_fetch_from_data_source"
        with pytest.raises(
            SnowparkDataframeReaderException, match="\\[RuntimeError\\] Test error"
        ):
            _task_fetch_data_from_source_with_retry(
                worker=DataSourceReader(
                    PyodbcDriver,
                    sql_server_create_connection,
                    StructType([StructField("col1", IntegerType(), False)]),
                ),
                partition="SELECT * FROM test_table",
                partition_idx=0,
                tmp_dir="/tmp",
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
                local_file="fake_file",
                snowflake_stage_name="fake_stage",
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
            "snowflake.snowpark.dataframe_reader._upload_and_copy_into_table_with_retry",
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
                custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
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

        new_data1 = (5, 100) + (None,) * (len(columns) - 3) + ('"123"',)
        new_data2 = (6, 101) + (None,) * (len(columns) - 3) + ('"456"',)

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
            custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
            session_init_statement=f"insert into {table_name} (int_col, var_col) values (100, '123');",
        )
        assert df.collect() == assert_data + [new_data1]

        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
            custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
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
        fetch_size=fetch_size,
    )
    schema = partitioner.schema
    file_count = (
        math.ceil(len(sql_server_all_type_small_data) / fetch_size)
        if fetch_size != 0
        else 1
    )

    with tempfile.TemporaryDirectory() as tmp_dir:

        params = {
            "worker": DataSourceReader(
                PyodbcDriver,
                sql_server_create_connection_small_data,
                schema=schema,
                fetch_size=fetch_size,
            ),
            "partition": "SELECT * FROM test_table",
            "partition_idx": partition_idx,
            "tmp_dir": tmp_dir,
        }

        if expected_error:
            with pytest.raises(
                ValueError,
                match="fetch size cannot be smaller than 0",
            ):
                _task_fetch_data_from_source(**params)
        else:
            _task_fetch_data_from_source(**params)

            files = sorted(os.listdir(tmp_dir))
            for idx, file in enumerate(files):
                assert (
                    f"data_partition{partition_idx}_fetch{idx}.parquet" in file
                ), f"file: {file} does not match"
            assert len(files) == file_count


def test_database_detector():
    mock_conn = MagicMock()
    with patch.object(type(mock_conn), "__module__", "UNKNOWN_DRIVER"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, "unknown_driver")

    mock_conn = MagicMock()
    mock_conn.getinfo.return_value = "UNKNOWN"
    with patch.object(type(mock_conn), "__module__", "pyodbc"):
        result = detect_dbms(mock_conn)
        assert result == (DBMS_TYPE.UNKNOWN, "pyodbc")


def test_type_conversion():
    invalid_type = OracleDBType("ID", "UNKNOWN", None, None, False)
    with pytest.raises(NotImplementedError, match="sql server type not supported"):
        PyodbcDriver(sql_server_create_connection).to_snow_type(
            [("test_col", invalid_type, None, None, 0, 0, True)]
        )

    with pytest.raises(NotImplementedError, match="oracledb type not supported"):
        OracledbDriver(oracledb_create_connection).to_snow_type([invalid_type])


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
            custom_schema="id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT",
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


def test_oracledb_driver_coverage(caplog):
    oracledb_driver = OracledbDriver(oracledb_create_connection_small_data)
    conn = oracledb_driver.prepare_connection(oracledb_driver.create_connection(), 0)
    assert conn.outputtypehandler == output_type_handler

    oracledb_driver.to_snow_type(
        [OracleDBType("NUMBER_COL", oracledb.DB_TYPE_NUMBER, 40, 2, True)]
    )
    assert "Snowpark does not support column" in caplog.text
