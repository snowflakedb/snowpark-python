#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import functools
import os
import tempfile

import pytest
import sqlite3

from snowflake.snowpark import Row
from tests.utils import RUNNING_ON_GH

SQLITE3_DB_CUSTOM_SCHEMA_STRING = "id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP_NTZ, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT"


def create_connection_to_sqlite3_db(db_path):
    return sqlite3.connect(db_path)


def sqlite3_db(db_path):
    conn = create_connection_to_sqlite3_db(db_path)
    cursor = conn.cursor()
    table_name = "PrimitiveTypes"
    columns = [
        "id",
        "int_col",
        "real_col",
        "text_col",
        "blob_col",
        "null_col",
        "ts_col",
        "date_col",
        "time_col",
        "short_col",
        "long_col",
        "double_col",
        "decimal_col",
        "map_col",
        "array_col",
        "var_col",
    ]
    # Create a table with different primitive types
    # sqlite3 only supports 5 types: NULL, INTEGER, REAL, TEXT, BLOB
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,   -- Auto-incrementing primary key
        int_col INTEGER,          -- Integer column
        real_col REAL,            -- Floating point column
        text_col TEXT,            -- String column
        blob_col BLOB,            -- Binary data column
        null_col NULL,            -- Explicit NULL type (for testing purposes)
        ts_col TEXT,              -- Timestamp column in TEXT format
        date_col TEXT,            -- Date column in TEXT format
        time_col TEXT,            -- Time column in TEXT format
        short_col INTEGER,        -- Short integer column
        long_col INTEGER,         -- Long integer column
        double_col REAL,          -- Double column
        decimal_col REAL,         -- Decimal column
        map_col TEXT,             -- Map column in TEXT format
        array_col TEXT,           -- Array column in TEXT format
        var_col TEXT              -- Variant column in TEXT format
    )
    """
    )
    test_datetime = datetime.datetime(2021, 1, 2, 12, 34, 56)
    test_date = test_datetime.date()
    test_time = test_datetime.time()
    example_data = [
        (
            1,
            42,
            3.14,
            "Hello, world!",
            b"\x00\x01\x02\x03",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "1",
        ),
        (
            2,
            -10,
            2.718,
            "SQLite",
            b"\x04\x05\x06\x07",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "2",
        ),
        (
            3,
            9999,
            -0.99,
            "Python",
            b"\x08\x09\x0A\x0B",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "3",
        ),
    ]
    assert_data = [
        Row(
            ID=1,
            INT_COL=42,
            REAL_COL=3.14,
            TEXT_COL="Hello, world!",
            BLOB_COL=bytearray(b"\x00\x01\x02\x03"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"1"',
        ),
        Row(
            ID=2,
            INT_COL=-10,
            REAL_COL=2.718,
            TEXT_COL="SQLite",
            BLOB_COL=bytearray(b"\x04\x05\x06\x07"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"2"',
        ),
        Row(
            ID=3,
            INT_COL=9999,
            REAL_COL=-0.99,
            TEXT_COL="Python",
            BLOB_COL=bytearray(b"\x08\t\n\x0b"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"3"',
        ),
    ]
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES ({','.join('?' * 16)})", example_data
    )
    conn.commit()
    conn.close()
    return table_name, columns, example_data, assert_data


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
    pytest.mark.skipif(
        RUNNING_ON_GH,
        reason="tests only suppose to run on snowfort",
    ),
]


@pytest.mark.parametrize("fetch_with_process", [True, False])
def test_dbapi_local(session, fetch_with_process):
    with tempfile.TemporaryDirectory() as temp_dir:
        dbpath = os.path.join(temp_dir, "testsqlite3.db")
        table_name, _, _, assert_data = sqlite3_db(dbpath)
        df = session.read.dbapi(
            functools.partial(create_connection_to_sqlite3_db, dbpath),
            table=table_name,
            custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
            fetch_size=2,
            fetch_merge_count=2,
            fetch_with_process=fetch_with_process,
        )
        assert df.order_by("ID").collect() == assert_data


def test_dbapi_udtf(session):
    udtf_configs = {"external_access_integration": ""}
    test_datetime = datetime.datetime(2021, 1, 2, 12, 34, 56)
    test_date = test_datetime.date()
    test_time = test_datetime.time()
    table_name = "PrimitiveTypes"
    example_data = [
        (
            1,
            42,
            3.14,
            "Hello, world!",
            b"\x00\x01\x02\x03".hex(),
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "1",
        ),
        (
            2,
            -10,
            2.718,
            "SQLite",
            b"\x04\x05\x06\x07".hex(),
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "2",
        ),
        (
            3,
            9999,
            -0.99,
            "Python",
            b"\x08\x09\x0A\x0B".hex(),
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "3",
        ),
    ]
    expected_data = [
        Row(
            ID=1,
            INT_COL=42,
            REAL_COL=3.14,
            TEXT_COL="Hello, world!",
            BLOB_COL=bytearray(b"\x00\x01\x02\x03"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"1"',
        ),
        Row(
            ID=2,
            INT_COL=-10,
            REAL_COL=2.718,
            TEXT_COL="SQLite",
            BLOB_COL=bytearray(b"\x04\x05\x06\x07"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"2"',
        ),
        Row(
            ID=3,
            INT_COL=9999,
            REAL_COL=-0.99,
            TEXT_COL="Python",
            BLOB_COL=bytearray(b"\x08\t\n\x0b"),
            NULL_COL=None,
            TS_COL=datetime.datetime(2021, 1, 2, 12, 34, 56),
            DATE_COL=datetime.date(2021, 1, 2),
            TIME_COL=datetime.time(12, 34, 56),
            SHORT_COL=1,
            LONG_COL=2,
            DOUBLE_COL=3.0,
            DECIMAL_COL=4,
            MAP_COL='{\n  "a": 1,\n  "b": 2\n}',
            ARRAY_COL='[\n  "[1, 2, 3]"\n]',
            VAR_COL='"3"',
        ),
    ]

    def create_connection_sqlite3():
        import sqlite3

        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        # Create a table with different primitive types
        # sqlite3 only supports 5 types: NULL, INTEGER, REAL, TEXT, BLOB
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,   -- Auto-incrementing primary key
                int_col INTEGER,          -- Integer column
                real_col REAL,            -- Floating point column
                text_col TEXT,            -- String column
                blob_col BLOB,            -- Binary data column
                null_col NULL,            -- Explicit NULL type (for testing purposes)
                ts_col TEXT,              -- Timestamp column in TEXT format
                date_col TEXT,            -- Date column in TEXT format
                time_col TEXT,            -- Time column in TEXT format
                short_col INTEGER,        -- Short integer column
                long_col INTEGER,         -- Long integer column
                double_col REAL,          -- Double column
                decimal_col REAL,         -- Decimal column
                map_col TEXT,             -- Map column in TEXT format
                array_col TEXT,           -- Array column in TEXT format
                var_col TEXT              -- Variant column in TEXT format
            )
            """
        )

        cursor.executemany(
            f"INSERT INTO {table_name} VALUES ({','.join('?' * 16)})", example_data
        )
        conn.commit()
        return conn

    df = session.read.dbapi(
        create_connection_sqlite3,
        table=table_name,
        custom_schema=SQLITE3_DB_CUSTOM_SCHEMA_STRING,
        fetch_size=2,
        udtf_configs=udtf_configs,
    )
    assert df.order_by("ID").collect() == expected_data
