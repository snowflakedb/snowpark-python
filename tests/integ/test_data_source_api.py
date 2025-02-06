#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import time
import unittest.mock
from _decimal import Decimal
import datetime
from unittest import mock
import pytest

from snowflake.snowpark._internal.utils import (
    TempObjectType,
    DATA_SOURCE_DBAPI_SIGNATURE,
    DATA_SOURCE_SQL_COMMENT,
    STATEMENT_PARAMS_DATA_SOURCE,
)
from snowflake.snowpark.dataframe_reader import (
    task_fetch_from_data_source_with_retry,
    MAX_RETRY_TIME,
)
from snowflake.snowpark.types import (
    IntegerType,
    DateType,
    MapType,
)
from tests.utils import Utils

SQL_SERVER_TABLE_NAME = "AllDataTypesTable"

all_type_schema = (
    ("Id", "int", None, 10, 0, "NO"),
    ("SmallIntCol", "smallint", None, 5, 0, "YES"),
    ("TinyIntCol", "tinyint", None, 3, 0, "YES"),
    ("BigIntCol", "bigint", None, 19, 0, "YES"),
    ("DecimalCol", "decimal", None, 10, 2, "YES"),
    ("FloatCol", "float", None, 53, None, "YES"),
    ("RealCol", "real", None, 24, None, "YES"),
    ("MoneyCol", "money", None, 19, 4, "YES"),
    ("SmallMoneyCol", "smallmoney", None, 10, 4, "YES"),
    ("CharCol", "char", 10, None, None, "YES"),
    ("VarCharCol", "varchar", 50, None, None, "YES"),
    ("TextCol", "text", 2147483647, None, None, "YES"),
    ("NCharCol", "nchar", 10, None, None, "YES"),
    ("NVarCharCol", "nvarchar", 50, None, None, "YES"),
    ("NTextCol", "ntext", 1073741823, None, None, "YES"),
    ("DateCol", "date", None, None, None, "YES"),
    ("TimeCol", "time", None, None, None, "YES"),
    ("DateTimeCol", "datetime", None, None, None, "YES"),
    ("DateTime2Col", "datetime2", None, None, None, "YES"),
    ("SmallDateTimeCol", "smalldatetime", None, None, None, "YES"),
    ("BinaryCol", "binary", 5, None, None, "YES"),
    ("VarBinaryCol", "varbinary", 50, None, None, "YES"),
    ("BitCol", "bit", None, None, None, "YES"),
    ("UniqueIdentifierCol", "uniqueidentifier", None, None, None, "YES"),
)

all_type_data = [
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
        b"\x01\x02\x03\x04\x05",
        b"\x01\x02\x03\x04",
        True,
        "06D48351-6EA7-4E64-81A2-9921F0EC42A5",
    ),
    (
        2,
        200,
        20,
        200000,
        Decimal("23456.78"),
        2.34,
        1.5670000314712524,
        Decimal("2345.6700"),
        Decimal("23.4500"),
        "FixedStr2 ",
        "VarStr2",
        "Text2",
        "UniFix2   ",
        "UniVar2",
        "UniText2",
        datetime.date(2023, 2, 1),
        datetime.time(13, 0),
        datetime.datetime(2023, 2, 1, 13, 0),
        datetime.datetime(2023, 2, 1, 13, 0, 0, 234000),
        datetime.datetime(2023, 2, 1, 13, 0),
        b"\x02\x03\x04\x05\x06",
        b"\x02\x03\x04\x05",
        False,
        "41B116E8-7D42-420B-A28A-98D53C782C79",
    ),
    (
        3,
        300,
        30,
        300000,
        Decimal("34567.89"),
        3.45,
        2.677999973297119,
        Decimal("3456.7800"),
        Decimal("34.5600"),
        "FixedStr3 ",
        "VarStr3",
        "Text3",
        "UniFix3   ",
        "UniVar3",
        "UniText3",
        datetime.date(2023, 3, 1),
        datetime.time(14, 0),
        datetime.datetime(2023, 3, 1, 14, 0),
        datetime.datetime(2023, 3, 1, 14, 0, 0, 345000),
        datetime.datetime(2023, 3, 1, 14, 0),
        b"\x03\x04\x05\x06\x07",
        b"\x03\x04\x05\x06",
        True,
        "F418999E-15F9-4FB0-9161-3383E0BC1B3E",
    ),
    (
        4,
        400,
        40,
        400000,
        Decimal("45678.90"),
        4.56,
        3.7890000343322754,
        Decimal("4567.8900"),
        Decimal("45.6700"),
        "FixedStr4 ",
        "VarStr4",
        "Text4",
        "UniFix4   ",
        "UniVar4",
        "UniText4",
        datetime.date(2023, 4, 1),
        datetime.time(15, 0),
        datetime.datetime(2023, 4, 1, 15, 0),
        datetime.datetime(2023, 4, 1, 15, 0, 0, 456000),
        datetime.datetime(2023, 4, 1, 15, 0),
        b"\x04\x05\x06\x07\x08",
        b"\x04\x05\x06\x07",
        False,
        "13DF4C45-682A-4C17-81BA-7B00C77E3F9C",
    ),
    (
        5,
        500,
        50,
        500000,
        Decimal("56789.01"),
        5.67,
        4.889999866485596,
        Decimal("5678.9000"),
        Decimal("56.7800"),
        "FixedStr5 ",
        "VarStr5",
        "Text5",
        "UniFix5   ",
        "UniVar5",
        "UniText5",
        datetime.date(2023, 5, 1),
        datetime.time(16, 0),
        datetime.datetime(2023, 5, 1, 16, 0),
        datetime.datetime(2023, 5, 1, 16, 0, 0, 567000),
        datetime.datetime(2023, 5, 1, 16, 0),
        b"\x05\x06\x07\x08\t",
        b"\x05\x06\x07\x08",
        True,
        "16592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
]


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def __init__(self) -> None:
        self.sql = ""

    def cursor(self):
        return self

    def execute(self, sql: str):
        self.sql = sql
        return self

    def fetchall(self):
        if "INFORMATION_SCHEMA" in self.sql:
            return all_type_schema
        else:
            return all_type_data


def create_connection():
    return FakeConnection()


def fake_task_fetch_from_data_source_with_retry(
    create_connection, query, schema, i, tmp_dir, query_timeout
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_dbapi_with_temp_table(session):
    df = session.read.dbapi(create_connection, SQL_SERVER_TABLE_NAME, max_workers=4)
    assert df.collect() == all_type_data


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_dbapi_retry(session):

    with mock.patch(
        "snowflake.snowpark.dataframe_reader._task_fetch_from_data_source",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = task_fetch_from_data_source_with_retry(
            create_connection=create_connection,
            query="SELECT * FROM test_table",
            schema=(("col1", int, 0, 0, 0, 0, False),),
            i=0,
            tmp_dir="/tmp",
        )
        assert mock_task.call_count == MAX_RETRY_TIME
        assert isinstance(result, Exception)

    with mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = session.read.upload_and_copy_into_table_with_retry(
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
        "snowflake.snowpark.dataframe_reader.task_fetch_from_data_source_with_retry",
        new=fake_task_fetch_from_data_source_with_retry,
    ):
        with mock.patch(
            "snowflake.snowpark.dataframe_reader.DataFrameReader.upload_and_copy_into_table_with_retry",
            wrap=upload_and_copy_into_table_with_retry,
        ) as mock_upload_and_copy:
            start = time.time()
            session.read.dbapi(
                create_connection,
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
        table="fake_table",
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
        table="fake_table",
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
        table="fake_table",
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
        table="fake_table",
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
        table="fake_table",
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
        table="fake_table",
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
        table="fake_table",
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
            table="fake_table",
            column_type=MapType(),
            column="DATE",
            lower_bound=0,
            upper_bound=1,
            num_partitions=4,
        )


def test_telemetry_tracking(caplog, session):
    original_func = session._conn.run_query
    called = 0

    def assert_datasource_statement_params_run_query(*args, **kwargs):
        # assert we set statement_parameters to track datasourcee api usage
        statement_parameters = kwargs.get("_statement_params")
        assert statement_parameters[STATEMENT_PARAMS_DATA_SOURCE] == "1"
        nonlocal called
        called += 1
        return original_func(*args, **kwargs)

    with caplog.at_level(
        logging.DEBUG,
        logger="snowflake.snowpark._internal.server_connection:server_connection",
    ):
        with mock.patch(
            "snowflake.snowpark._internal.server_connection.ServerConnection.run_query",
            side_effect=assert_datasource_statement_params_run_query,
        ), unittest.mock.patch(
            "snowflake.snowpark._internal.telemetry.TelemetryClient.send_performance_telemetry"
        ) as mock_telemetry:
            df = session.read.dbapi(create_connection, SQL_SERVER_TABLE_NAME)
        assert df._plan.api_calls == [{"name": DATA_SOURCE_DBAPI_SIGNATURE}]
        assert called == 4  # 4 queries: create table, create stage, put file, copy into
        assert mock_telemetry.called
        assert df.collect() == all_type_data
        for record in caplog.records:
            # we create temp stage, temp table, put files, and copy into
            # apart from the table() call which uses select, all other calls should have the comment
            if "select" not in record.message.lower():
                assert DATA_SOURCE_SQL_COMMENT in record.message
        caplog.clear()

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
