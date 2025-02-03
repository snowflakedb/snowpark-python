#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import decimal
import time
from _decimal import Decimal
import datetime
from unittest import mock
import pytest

from snowflake.snowpark.dataframe_reader import (
    task_fetch_from_data_source_with_retry,
    MAX_RETRY_TIME,
)
from snowflake.snowpark.types import IntegerType, DateType, MapType

SQL_SERVER_TABLE_NAME = "RandomDataWith100Columns"

all_type_schema = (
    ("Id", int, None, 10, 10, 0, False),
    ("IntCol", int, None, 5, 5, 0, True),
    ("DecimalCol", decimal.Decimal, None, 10, 10, 2, True),
    ("FloatCol", float, None, 53, 53, 0, True),
    ("MoneyCol", decimal.Decimal, None, 19, 19, 4, True),
    ("SmallMoneyCol", decimal.Decimal, None, 10, 10, 4, True),
    ("StringCol", str, None, 10, 10, 0, True),
    ("DateCol", datetime.date, None, 10, 10, 0, True),
    ("TimeCol", datetime.time, None, 16, 16, 7, True),
    ("DateTimeCol", datetime.datetime, None, 23, 23, 3, True),
    ("BinaryCol", bytearray, None, 5, 5, 0, True),
    ("BitCol", bool, None, 1, 1, 0, True),
)
test_type = (("BinaryCol", bytearray, None, 5, 5, 0, True),)
test_data = [(b"\x01\x02\x03\x04\x05",)]
all_type_data = [
    (
        31301,
        70242,
        Decimal("37367.89"),
        71408.96709598973,
        Decimal("47632.5252"),
        Decimal("61582.9918"),
        "BUBPWJABGD",
        datetime.date(2000, 12, 11),
        datetime.time(11, 37, 48),
        datetime.datetime(2000, 2, 28, 23, 18, 24),
        bytearray(b"<\xbb\xaa\xf1x"),
        True,
    ),
    (
        36684,
        43892,
        Decimal("6344.44"),
        27005.1362048059,
        Decimal("67558.6088"),
        Decimal("65207.3376"),
        "NWGWEGQTOT",
        datetime.date(2020, 4, 12),
        datetime.time(23, 26, 48),
        datetime.datetime(2000, 4, 19, 17, 48, 47),
        bytearray(b"\xaam\xb5o\xdb"),
        False,
    ),
    (
        96306,
        79769,
        Decimal("79590.64"),
        16290.060409851114,
        Decimal("26360.6335"),
        Decimal("9770.7736"),
        "HFCJIYRCLK",
        datetime.date(2018, 10, 3),
        datetime.time(14, 9, 42),
        datetime.datetime(2000, 1, 18, 1, 48, 12),
        bytearray(b"\x06k\xa3\xfe\x85"),
        False,
    ),
    (
        82883,
        719,
        Decimal("61072.54"),
        6420.648565704867,
        Decimal("891.6963"),
        Decimal("20891.7863"),
        "JUFMDYGGZP",
        datetime.date(2005, 12, 18),
        datetime.time(3, 17, 6),
        datetime.datetime(2000, 3, 30, 18, 21, 49),
        bytearray(b"\xf8\xc6\xd6U\xa2"),
        True,
    ),
    (
        75488,
        73246,
        Decimal("42508.36"),
        47913.77734513048,
        Decimal("25589.1343"),
        Decimal("12137.0901"),
        "TQTGLZKOPJ",
        datetime.date(2014, 11, 21),
        datetime.time(16, 12, 53),
        datetime.datetime(2000, 1, 25, 17, 4, 22),
        bytearray(b"\xe2\x11E\xb6\xd7"),
        False,
    ),
    (
        73888,
        26730,
        Decimal("16097.45"),
        75948.2888965026,
        Decimal("61859.4983"),
        Decimal("27712.6816"),
        "SDNCXTQGOE",
        datetime.date(2025, 12, 12),
        datetime.time(2, 48, 36),
        datetime.datetime(2000, 2, 16, 18, 32, 11),
        bytearray(b'\xe0"l\xdc\xe3'),
        False,
    ),
    (
        22142,
        86323,
        Decimal("61670.44"),
        85982.74527507684,
        Decimal("34860.6688"),
        Decimal("95540.8440"),
        "LPOKEKBVCA",
        datetime.date(2002, 3, 19),
        datetime.time(6, 47, 4),
        datetime.datetime(2000, 2, 25, 11, 11, 19),
        bytearray(b"\x17GT\xf6\xbf"),
        True,
    ),
    (
        31744,
        12427,
        Decimal("26169.56"),
        91899.38007923993,
        Decimal("77075.3872"),
        Decimal("18535.6178"),
        "AIKKVQMYYA",
        datetime.date(2010, 2, 23),
        datetime.time(16, 48, 27),
        datetime.datetime(2000, 2, 9, 2, 38, 26),
        bytearray(b"m+\xeaY\xc1"),
        False,
    ),
    (
        39019,
        33390,
        Decimal("82930.03"),
        67432.95093443731,
        Decimal("49083.8871"),
        Decimal("88248.6094"),
        "BCNYNJUXRB",
        datetime.date(2006, 7, 2),
        datetime.time(6, 34, 58),
        datetime.datetime(2000, 4, 1, 9, 33, 35),
        bytearray(b"\x16\x1e\xa25\xcb"),
        False,
    ),
    (
        18452,
        72259,
        Decimal("14013.92"),
        85892.00027292754,
        Decimal("33821.2066"),
        Decimal("86359.7786"),
        "PBYYCAEHJP",
        datetime.date(2025, 5, 11),
        datetime.time(20, 44, 9),
        datetime.datetime(2000, 3, 1, 6, 11, 12),
        bytearray(b"\xe4,B\x95\xd6"),
        True,
    ),
    (
        8499,
        93349,
        Decimal("55867.91"),
        1426.6924724148255,
        Decimal("90148.7861"),
        Decimal("6476.1648"),
        "CAZYCFQUGM",
        datetime.date(2023, 7, 29),
        datetime.time(5, 6, 11),
        datetime.datetime(2000, 4, 7, 15, 29, 57),
        bytearray(b"#Oy><"),
        False,
    ),
    (
        99184,
        60322,
        Decimal("22487.42"),
        92938.46015248382,
        Decimal("72903.6823"),
        Decimal("63789.1005"),
        "SDIWSCNMNU",
        datetime.date(2012, 4, 26),
        datetime.time(4, 18, 54),
        datetime.datetime(2000, 2, 18, 18, 41, 12),
        bytearray(b"O\xc8Y\xa6\xa3"),
        True,
    ),
    (
        20703,
        52921,
        Decimal("57867.61"),
        64622.38550247046,
        Decimal("44778.0456"),
        Decimal("10069.3572"),
        "CMJDNMYOMP",
        datetime.date(2017, 7, 30),
        datetime.time(18, 30, 7),
        datetime.datetime(2000, 1, 23, 15, 52, 31),
        bytearray(b"!\x9d4?\x9b"),
        True,
    ),
    (
        88839,
        63178,
        Decimal("89944.95"),
        36799.223133206215,
        Decimal("51481.6452"),
        Decimal("44217.7552"),
        "LRGOUSTHIR",
        datetime.date(2011, 6, 13),
        datetime.time(14, 41, 54),
        datetime.datetime(2000, 2, 26, 20, 6, 58),
        bytearray(b"bY\xa2)/"),
        False,
    ),
    (
        56606,
        84113,
        Decimal("96538.49"),
        96154.1264935039,
        Decimal("86508.1306"),
        Decimal("84793.8686"),
        "AYGNJWYSLN",
        datetime.date(2000, 11, 17),
        datetime.time(11, 55, 41),
        datetime.datetime(2000, 2, 25, 11, 1, 39),
        bytearray(b"\xe2\xd8\xed\xae\x96"),
        False,
    ),
    (
        21590,
        91290,
        Decimal("19064.70"),
        1696.2891708355853,
        Decimal("5223.9960"),
        Decimal("88286.3886"),
        "DEWKALSJAI",
        datetime.date(2007, 6, 4),
        datetime.time(22, 46),
        datetime.datetime(2000, 1, 15, 15, 13, 21),
        bytearray(b'"\xae\xc2\xb9\xcc'),
        False,
    ),
    (
        32425,
        67815,
        Decimal("66587.28"),
        24680.86711206836,
        Decimal("10527.9496"),
        Decimal("17421.9384"),
        "WKBLBGXXUZ",
        datetime.date(2021, 7, 14),
        datetime.time(15, 46, 8),
        datetime.datetime(2000, 4, 4, 13, 16, 49),
        bytearray(b"\x1f.\xcd\xce2"),
        False,
    ),
    (
        37417,
        62530,
        Decimal("92493.73"),
        438.85510375407665,
        Decimal("14816.0070"),
        Decimal("90222.1367"),
        "UXXNWDEAZH",
        datetime.date(2021, 9, 12),
        datetime.time(23, 20, 7),
        datetime.datetime(2000, 3, 17, 10, 16, 46),
        bytearray(b"A\x82\xea\xc7\x0b"),
        False,
    ),
    (
        59679,
        32881,
        Decimal("82601.50"),
        65984.29522726519,
        Decimal("8656.0469"),
        Decimal("77288.2406"),
        "MFGYWXAZBI",
        datetime.date(2016, 2, 25),
        datetime.time(17, 26, 7),
        datetime.datetime(2000, 4, 18, 1, 38, 28),
        bytearray(b"v\xb6\x88\xe4\xc4"),
        False,
    ),
    (
        58502,
        45520,
        Decimal("20273.24"),
        88458.6394344244,
        Decimal("2518.7741"),
        Decimal("37661.7761"),
        "BJKVQJRZDI",
        datetime.date(2014, 5, 18),
        datetime.time(18, 19, 53),
        datetime.datetime(2000, 4, 13, 15, 24, 17),
        bytearray(b"\xc9g\x1e\xb3\xa8"),
        False,
    ),
]


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def cursor(self):
        return self

    def execute(self, sql: str):
        return self

    def fetchall(self):
        return all_type_data

    description = all_type_schema


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
                column="ID",
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
