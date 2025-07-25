#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest
import re
import math

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator
from unittest import mock

from snowflake.connector.errors import ProgrammingError

from snowflake.snowpark._internal.analyzer.analyzer_utils import write_arrow
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.functions import col
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DecimalType
from snowflake.snowpark.session import WRITE_ARROW_CHUNK_SIZE

from tests.utils import TestData, Utils, TestFiles

try:
    import pyarrow as pa
except ImportError:
    pytest.skip("pyarrow is not available", allow_module_level=True)


@pytest.fixture(scope="module")
def basic_arrow_table():
    yield pa.Table.from_arrays([[1, 2, 3]], names=["a"])


@pytest.fixture(scope="module")
def arrow_table():
    yield pa.Table.from_arrays(
        [[1, 2, 3], ["a", "b", "c"], [1.0, 1.11, 0]], names=["A", "B", "C"]
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
@pytest.mark.parametrize(
    "example,expected",
    [
        (TestData.integer1, {"A": [1, 2, 3]}),
        (
            TestData.null_data1,
            {"A": [None, Decimal("2"), Decimal("1"), Decimal("3"), None]},
        ),
        (
            TestData.double1,
            {"A": [Decimal("1.111"), Decimal("2.222"), Decimal("3.333")]},
        ),
        (TestData.string1, {"A": ["test1", "test2", "test3"], "B": ["a", "b", "c"]}),
        pytest.param(
            TestData.array1,
            {
                "ARR1": ["[\n  1,\n  2,\n  3\n]", "[\n  6,\n  7,\n  8\n]"],
                "ARR2": ["[\n  3,\n  4,\n  5\n]", "[\n  9,\n  0,\n  1\n]"],
            },
            id="semi-structured array",
        ),
        pytest.param(
            TestData.object2,
            {
                "OBJ": [
                    '{\n  "age": 21,\n  "name": "Joe",\n  "zip": 21021\n}',
                    '{\n  "age": 26,\n  "name": "Jay",\n  "zip": 94021\n}',
                ],
                "K": ["age", "key"],
                "V": [Decimal("0"), Decimal("0")],
                "FLAG": [True, False],
            },
            id="semi-structured object",
        ),
        (
            TestData.datetime_primitives2,
            {
                "TIMESTAMP": [
                    datetime(9999, 12, 31, 0, 0, 0, 123456),
                    datetime(1583, 1, 1, 23, 59, 59, 567890),
                ]
            },
        ),
        (
            TestData.date1,
            {
                "A": [date(2020, 8, 1), date(2010, 12, 1)],
                "B": [Decimal("1"), Decimal("2")],
            },
        ),
    ],
)
def test_to_arrow(session, example, expected):
    df = example(session)
    # Compare as python dict in order to avoid precision differences
    assert df.to_arrow().to_pydict() == expected


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_arrow_decimal_precision(session):
    data = [
        [1111111111111111111, 222222222222222222],
        [3333333333333333333, 444444444444444444],
        [5555555555555555555, 666666666666666666],
        [7777777777777777777, 888888888888888888],
        [9223372036854775807, 111111111111111111],
        [2222222222222222222, 333333333333333333],
        [4444444444444444444, 555555555555555555],
        [6666666666666666666, 777777777777777777],
        [-9223372036854775808, 999999999999999999],
    ]
    df = session.create_dataframe(data, schema=["A", "B"],).select(
        col("A").cast(DecimalType(38, 0)).alias("A"),
        col("B").cast(DecimalType(18, 0)).alias("B"),
    )

    padf = df.to_arrow()
    assert str(padf.schema[0].type) == "decimal128(38, 0)"
    assert str(padf.schema[1].type) == "int64"
    assert [[int(x) for x in y.values()] for y in padf.to_pylist()] == data


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_arrow_batches(session):
    df = session.range(100000).cache_result()
    iterator = df.to_arrow_batches()
    assert isinstance(iterator, Iterator)

    padf = df.to_arrow()
    padf_list = list(iterator)
    assert len(padf_list) > 1
    assert pa.concat_tables(padf_list) == padf


def test_create_dataframe_round_trip(session):
    # Create a basic dataframe
    df = session.create_dataframe([(1,), (2,), (3,)], schema=["A"])

    # Convert to arrow Table
    table = df.to_arrow()

    # create df from arrow table
    df2 = session.create_dataframe(table)

    # Round trip should result in the same df
    Utils.check_answer(df, df2)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_write_arrow_overwrite_auto_create(session, basic_arrow_table):
    table_name = Utils.random_table_name()
    try:
        # Initial auto_create should create the table and have three rows
        session.write_arrow(basic_arrow_table, table_name, auto_create_table=True)
        table1 = session.table(table_name)
        Utils.check_answer(table1, [Row(1), Row(2), Row(3)])

        # Second auto_create should just append to now existing table
        session.write_arrow(basic_arrow_table, table_name, auto_create_table=True)
        table2 = session.table(table_name)
        Utils.check_answer(table2, [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)])

        # Third auto_create should truncate existing table and replace rows
        session.write_arrow(
            basic_arrow_table, table_name, auto_create_table=True, overwrite=True
        )
        table3 = session.table(table_name)
        Utils.check_answer(table3, [Row(1), Row(2), Row(3)])

        # Overwriting without autocreate should replace rows as well
        session.write_arrow(
            basic_arrow_table, table_name, auto_create_table=False, overwrite=True
        )
        table4 = session.table(table_name)
        Utils.check_answer(table4, [Row(1), Row(2), Row(3)])
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
@pytest.mark.parametrize("table_type", ["", "TEMPORARY", "TRANSIENT"])
def test_write_arrow_table_type(session, arrow_table, table_type):
    table_name = Utils.random_table_name()
    try:
        session.write_arrow(
            arrow_table, table_name, auto_create_table=True, table_type=table_type
        )
        table_type = table_type or "replace TABLE"
        ddl = session._run_query(f"select get_ddl('table', '{table_name}')")
        assert table_type in ddl[0][0]
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_write_arrow_chunk_size(session):
    table_name = Utils.random_table_name()
    try:
        chunk_size, expected_num_rows = 25, 101
        # create medium-sized df that can be chunked
        df = session.range(expected_num_rows)
        table = df.to_arrow()
        success, num_chunks, num_rows, _ = write_arrow(
            session._conn._conn.cursor(),
            table,
            table_name,
            auto_create_table=True,
            chunk_size=chunk_size,
        )
        # 25 rows per chunk = 5 chunks
        assert (
            success
            and num_chunks == math.ceil(expected_num_rows / chunk_size)
            and num_rows == expected_num_rows
        )

        # Import the original write_arrow to create a wrapper
        from snowflake.snowpark.session import write_arrow as original_write_arrow

        expected_chunk_size = 10

        # Create a wrapper that intercepts calls but lets the real function execute
        def write_arrow_wrapper(*args, **kwargs):
            input_chunk_size = kwargs.get("chunk_size")
            assert input_chunk_size == expected_chunk_size
            expected_num_chunks = (
                math.ceil(expected_num_rows / expected_chunk_size)
                if input_chunk_size != WRITE_ARROW_CHUNK_SIZE
                else 1
            )
            # Call the real function and return its actual result
            ret = original_write_arrow(*args, **kwargs)
            success, num_chunks, num_rows, _ = ret
            assert (
                success
                and num_chunks == expected_num_chunks
                and num_rows == expected_num_rows
            )
            return ret

        with mock.patch(
            "snowflake.snowpark.session.write_arrow", side_effect=write_arrow_wrapper
        ) as mock_write_arrow:
            session.create_dataframe(table, chunk_size=10)
            # Verify that write_arrow was called once
            mock_write_arrow.assert_called_once()

        expected_chunk_size = WRITE_ARROW_CHUNK_SIZE
        with mock.patch(
            "snowflake.snowpark.session.write_arrow", side_effect=write_arrow_wrapper
        ) as mock_write_arrow:
            session.create_dataframe(table)
            # Verify that write_arrow was called once
            mock_write_arrow.assert_called_once()
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
@pytest.mark.parametrize("quote_identifiers", [True, False])
def test_write_arrow_alternate_schema(session, basic_arrow_table, quote_identifiers):
    db = session.get_current_database().strip('"')
    table_name = Utils.random_table_name()
    schema_name = (
        "SNOWPARK_PYTHON_TEST_WRITE_ARROW_" + Utils.random_alphanumeric_str(4).upper()
    )
    try:
        Utils.create_schema(session, schema_name)
        table = session.write_arrow(
            basic_arrow_table,
            table_name,
            database=db,
            schema=schema_name,
            auto_create_table=True,
            quote_identifiers=quote_identifiers,
        )
        if quote_identifiers:
            assert table.table_name == f'"{db}"."{schema_name}"."{table_name}"'
            assert table.columns == ['"a"']
        else:
            assert table.table_name == f"{db}.{schema_name}.{table_name}"
            assert table.columns == ["A"]
    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_schema(session, schema_name)


@pytest.mark.parametrize(
    "compression,parallel,use_logical_type,on_error",
    [
        ("gzip", 2, None, "SKIP_FILE"),
        ("snappy", 3, True, "CONTINUE"),
        ("gzip", 4, False, "ABORT_STATEMENT"),
    ],
)
def test_misc_settings(
    session, arrow_table, compression, parallel, use_logical_type, on_error
):
    copy_compression = {"gzip": "auto", "snappy": "snappy"}[compression]
    sql_use_logical_type = (
        "" if use_logical_type is None else f" USE_LOGICAL_TYPE = {use_logical_type}"
    )
    queries = [
        f"^CREATE .*TEMP.* STAGE .* FILE_FORMAT=\\(TYPE=PARQUET COMPRESSION={compression}\\)",
        f"PUT.*PARALLEL={parallel}",
        f'COPY INTO "SNOWPARK_PYTHON_MOCKED_ARROW_TABLE" .* FILE_FORMAT=\\(TYPE=PARQUET COMPRESSION={copy_compression}{sql_use_logical_type.upper()}\\) PURGE=TRUE ON_ERROR={on_error}',
    ]

    with mock.patch("snowflake.connector.cursor.SnowflakeCursor.execute") as execute:
        session.write_arrow(
            arrow_table,
            "SNOWPARK_PYTHON_MOCKED_ARROW_TABLE",
            compression=compression,
            parallel=parallel,
            use_logical_type=use_logical_type,
            on_error=on_error,
        )
        for query in queries:
            assert any(
                re.match(query, call.args[0]) for call in execute.call_args_list
            ), f"query not matched: {query}"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_write_arrow_negative(session, basic_arrow_table):
    with pytest.raises(
        ProgrammingError,
        match="Schema has to be provided to write_arrow when a database is provided",
    ):
        session.write_arrow(basic_arrow_table, "temp_table", database="foo")

    with pytest.raises(
        ProgrammingError,
        match="Invalid compression",
    ):
        session.write_arrow(basic_arrow_table, "temp_table", compression="invalid")

    with pytest.raises(
        ProgrammingError,
        match="Unsupported table type.",
    ):
        session.write_arrow(basic_arrow_table, "temp_table", table_type="picnic")

    # Table name does not exist and is not auto-created
    table_name = Utils.random_table_name()
    with pytest.raises(
        ProgrammingError,
        match="^.*SQL compilation error:\nTable .* does not exist",
    ):
        session.write_arrow(basic_arrow_table, table_name)

    # Truncate does not cause a table to be auto-generated
    table_name = Utils.random_table_name()
    with pytest.raises(
        ProgrammingError,
        match="^.*SQL compilation error:\nTable .* does not exist",
    ):
        session.write_arrow(basic_arrow_table, table_name, overwrite=True)

    with mock.patch(
        "snowflake.snowpark.session.write_arrow",
        return_value=(False, 0, 0, "<output here>"),
    ):
        with pytest.raises(
            SnowparkSessionException,
            match="Failed to write arrow table to Snowflake. COPY INTO output <output here>",
        ):
            session.write_arrow(basic_arrow_table, "temp_table")


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_arrow_from_stage(session, resources_path):
    stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)
    table = pa.Table.from_arrays(
        [[1, 2], ["one", "two"], [1.2, 2.2]], names=["c1", "c2", "c3"]
    ).to_pylist()

    try:
        Utils.create_stage(session, stage_name)
        Utils.upload_to_stage(
            session, stage_name, test_files.test_file_csv, compress=False
        )
        df = session.read.csv(
            f"@{stage_name}/{os.path.basename(test_files.test_file_csv)}"
        )
        assert df.to_arrow().to_pylist() == table
        for t in df.to_arrow_batches():
            assert t.to_pylist() == table
    finally:
        Utils.drop_stage(session, stage_name)
