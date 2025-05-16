#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import copy
import datetime
import decimal
import json
import logging
import math
import sys
from array import array
from collections import namedtuple
from decimal import Decimal
from itertools import product
from textwrap import dedent
from typing import Tuple
from unittest import mock

from snowflake.snowpark.dataframe import map
from snowflake.snowpark.session import Session
from tests.conftest import local_testing_mode

try:
    import pandas as pd  # noqa: F401
    from pandas import DataFrame as PandasDF
    from pandas.testing import assert_frame_equal

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

import pytest

from snowflake.connector import IntegrityError, ProgrammingError
from snowflake.snowpark import Column, Row, Window
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark._internal.analyzer.expression import Attribute, Star
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.dataframe_na_functions import _SUBSET_CHECK_ERROR_MESSAGE
from snowflake.snowpark.exceptions import (
    SnowparkColumnException,
    SnowparkCreateDynamicTableException,
    SnowparkCreateViewException,
    SnowparkDataframeException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import (
    col,
    column,
    concat,
    count,
    explode,
    get_path,
    lit,
    make_interval,
    rank,
    seq1,
    seq2,
    seq4,
    seq8,
    table_function,
    udtf,
    uniform,
    when,
)
from snowflake.snowpark.types import (
    FileType,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)
from tests.utils import (
    IS_IN_STORED_PROC,
    IS_IN_STORED_PROC_LOCALFS,
    TestData,
    TestFiles,
    Utils,
    multithreaded_run,
    structured_types_enabled_session,
    structured_types_supported,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

tmp_stage_name = Utils.random_stage_name()
test_file_on_stage = f"@{tmp_stage_name}/testCSV.csv"
user_schema = StructType(
    [
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", DoubleType()),
    ]
)


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    test_files = TestFiles(resources_path)
    Utils.upload_to_stage(
        session, f"@{tmp_stage_name}", test_files.test_file_csv, compress=False
    )


@pytest.fixture(scope="function")
def table_name_1(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [[1], [2], [3]], schema=StructType([StructField("num", IntegerType())])
    ).write.save_as_table(table_name)
    yield table_name
    Utils.drop_table(session, table_name)


def test_dataframe_get_item(session):
    df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"], [4, "d"]]).to_df(
        "id", "value"
    )
    df["id"]
    df[0]
    df[col("id")]
    df[["id"]]
    df[("id")]
    with pytest.raises(TypeError) as exc_info:
        df[11.1]
    assert "Unexpected item type: " in str(exc_info)


def test_dataframe_get_attr(session):
    df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"], [4, "d"]]).to_df(
        "id", "value"
    )
    df.id
    df.value

    with pytest.raises(AttributeError) as exc_info:
        df.non_existent
    assert "object has no attribute" in str(exc_info)


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
def test_read_stage_file_show(session, resources_path, local_testing_mode):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)
    test_file_on_stage = f"@{tmp_stage_name}/testCSV.csv"

    try:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name, test_files.test_file_csv, compress=False
        )
        user_schema = StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", DoubleType()),
            ]
        )
        result_str = (
            session.read.option("purge", False)
            .schema(user_schema)
            .csv(test_file_on_stage)
            ._show_string(_emit_ast=session.ast_enabled)
        )
        assert (
            result_str
            == """
-------------------
|"A"  |"B"  |"C"  |
-------------------
|1    |one  |1.2  |
|2    |two  |2.2  |
-------------------
""".lstrip()
        )
    finally:
        if not local_testing_mode:
            Utils.drop_stage(session, tmp_stage_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is a SQL test",
    run=False,
)
def test_show_using_with_select_statement(session):
    df = session.sql(
        "with t1 as (select 1 as a union all select 2 union all select 3 "
        "   union all select 4 union all select 5 union all select 6 "
        "   union all select 7 union all select 8 union all select 9 "
        "   union all select 10 union all select 11 union all select 12) "
        "select * from t1"
    )
    assert (
        df._show_string(_emit_ast=session.ast_enabled)
        == """
-------
|"A"  |
-------
|1    |
|2    |
|3    |
|4    |
|5    |
|6    |
|7    |
|8    |
|9    |
|10   |
-------\n""".lstrip()
    )


@pytest.mark.parametrize("use_simplification", [True, False])
def test_distinct(session, use_simplification, local_testing_mode):
    """Tests df.distinct()."""

    session.conf.set("use_simplified_query_generation", use_simplification)
    df = session.create_dataframe(
        [
            [1, 1],
            [1, 1],
            [2, 2],
            [3, 3],
            [4, 4],
            [5, 5],
            [None, 1],
            [1, None],
            [None, None],
        ]
    ).to_df("id", "v")

    res = df.distinct().sort(["id", "v"]).collect()
    assert res == [
        Row(None, None),
        Row(None, 1),
        Row(1, None),
        Row(1, 1),
        Row(2, 2),
        Row(3, 3),
        Row(4, 4),
        Row(5, 5),
    ]

    res = df.select(col("id")).distinct().sort(["id"]).collect()
    assert res == [Row(None), Row(1), Row(2), Row(3), Row(4), Row(5)]

    res = df.select(col("v")).distinct().sort(["v"]).collect()
    assert res == [Row(None), Row(1), Row(2), Row(3), Row(4), Row(5)]

    if not local_testing_mode:
        queries = df.distinct().queries["queries"]
        if use_simplification:
            assert "SELECT  DISTINCT" in queries[0]
        else:
            assert "GROUP BY" in queries[0]


def test_first(session):
    """Tests df.first()."""

    df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"], [4, "d"]]).to_df(
        "id", "v"
    )

    # empty first, should default to 1
    res = df.first()
    assert res == Row(1, "a")

    res = df.first(0)
    assert res == []

    res = df.first(1)
    assert res == [Row(1, "a")]

    res = df.first(2)
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "a"), Row(2, "b")]

    res = df.first(3)
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c")]

    res = df.first(4)
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

    # Negative value is equivalent to collect()
    res = df.first(-1)
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

    # first-value larger than cardinality
    res = df.first(123)
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

    # test invalid type argument passed to first
    with pytest.raises(ValueError) as ex_info:
        df.first("abc")
    assert "Invalid type of argument passed to first()" in str(ex_info)


def test_new_df_from_range(session):
    """Tests df.range()."""

    # range(start, end, step)
    df = session.range(1, 10, 2)
    res = df.collect()
    expected = [Row(1), Row(3), Row(5), Row(7), Row(9)]
    assert res == expected

    # range(start, end)
    df = session.range(1, 10)
    res = df.collect()
    expected = [
        Row(1),
        Row(2),
        Row(3),
        Row(4),
        Row(5),
        Row(6),
        Row(7),
        Row(8),
        Row(9),
    ]
    assert res == expected

    # range(end)
    df = session.range(10)
    res = df.collect()
    expected = [
        Row(0),
        Row(1),
        Row(2),
        Row(3),
        Row(4),
        Row(5),
        Row(6),
        Row(7),
        Row(8),
        Row(9),
    ]
    assert res == expected


def test_select_single_column(session):
    """Tests df.select() on dataframes with a single column."""

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 4).select("id").collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") < 4).select("id").collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = session.range(1, 10, 2).select("id").filter(col("id") <= 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = session.range(1, 10, 2).select("id").filter(col("id") <= 3).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = session.range(1, 10, 2).select("id").filter(col("id") <= 0).collect()
    expected = []
    assert res == expected


def test_select_star(session):
    """Tests df.select('*')."""

    # Single column
    res = session.range(3, 8).select("*").collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    result = res == expected
    assert result

    # Two columns
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df.select("*").collect()
    expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
    assert res == expected


@pytest.mark.udf
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
def test_select_table_function(session):
    df = session.create_dataframe(
        [(1, "one o one", 10), (2, "twenty two", 20), (3, "thirty three", 30)]
    ).to_df(["a", "b", "c"])

    # test single output column udtf
    class TwoXUDTF:
        def process(self, n: int):
            yield (2 * n,)

    table_func = udtf(
        TwoXUDTF,
        output_schema=StructType([StructField("two_x", IntegerType())]),
        input_types=[IntegerType()],
    )

    # test single column selection
    expected_result = [Row(TWO_X=2), Row(TWO_X=4), Row(TWO_X=6)]
    Utils.check_answer(df.select(table_func("a")), expected_result)
    Utils.check_answer(df.select(table_func(col("a"))), expected_result)
    Utils.check_answer(df.select(table_func(df.a)), expected_result)

    # test multiple column selection
    expected_result = [Row(A=1, TWO_X=2), Row(A=2, TWO_X=4), Row(A=3, TWO_X=6)]
    Utils.check_answer(df.select("a", table_func("a")), expected_result)
    Utils.check_answer(df.select(col("a"), table_func(col("a"))), expected_result)
    Utils.check_answer(df.select(df.a, table_func(df.a)), expected_result)

    # test multiple column selection with order preservation
    expected_result = [
        Row(A=1, TWO_X=2, C=10),
        Row(A=2, TWO_X=4, C=20),
        Row(A=3, TWO_X=6, C=30),
    ]
    Utils.check_answer(df.select("a", table_func("a"), "c"), expected_result)
    Utils.check_answer(
        df.select(col("a"), table_func(col("a")), col("c")), expected_result
    )
    Utils.check_answer(df.select(df.a, table_func(df.a), df.c), expected_result)

    # test multiple output column udtf
    class TwoXSixXUDTF:
        def process(self, n: int):
            yield (2 * n, 6 * n)

    table_func = udtf(
        TwoXSixXUDTF,
        output_schema=StructType(
            [StructField("two_x", IntegerType()), StructField("six_x", IntegerType())]
        ),
        input_types=[IntegerType()],
    )

    # test single column selection
    expected_result = [
        Row(TWO_X=2, SIX_X=6),
        Row(TWO_X=4, SIX_X=12),
        Row(TWO_X=6, SIX_X=18),
    ]
    Utils.check_answer(df.select(table_func("a")), expected_result)
    Utils.check_answer(df.select(table_func(col("a"))), expected_result)
    Utils.check_answer(df.select(table_func(df.a)), expected_result)

    # test multiple column selection
    expected_result = [
        Row(A=1, TWO_X=2, SIX_X=6),
        Row(A=2, TWO_X=4, SIX_X=12),
        Row(A=3, TWO_X=6, SIX_X=18),
    ]
    Utils.check_answer(df.select("a", table_func("a")), expected_result)
    Utils.check_answer(df.select(col("a"), table_func(col("a"))), expected_result)
    Utils.check_answer(df.select(df.a, table_func(df.a)), expected_result)

    # test multiple column selection with order preservation
    expected_result = [
        Row(A=1, TWO_X=2, SIX_X=6, C=10),
        Row(A=2, TWO_X=4, SIX_X=12, C=20),
        Row(A=3, TWO_X=6, SIX_X=18, C=30),
    ]
    Utils.check_answer(df.select("a", table_func("a"), "c"), expected_result)
    Utils.check_answer(
        df.select(col("a"), table_func(col("a")), col("c")), expected_result
    )
    Utils.check_answer(df.select(df.a, table_func(df.a), df.c), expected_result)

    # test with aliases
    expected_result = [
        Row(A=1, DOUBLE=2, SIX_X=6, C=10),
        Row(A=2, DOUBLE=4, SIX_X=12, C=20),
        Row(A=3, DOUBLE=6, SIX_X=18, C=30),
    ]
    Utils.check_answer(
        df.select("a", table_func("a").alias("double", "six_x"), "c"), expected_result
    )
    Utils.check_answer(
        df.select(col("a"), table_func(col("a")).alias("double", "six_x"), col("c")),
        expected_result,
    )
    Utils.check_answer(
        df.select(df.a, table_func(df.a).alias("double", "six_x"), df.c),
        expected_result,
    )

    # testing in-built table functions
    table_func = table_function("split_to_table")
    expected_result = [
        Row(A=1, SEQ=1, INDEX=1, VALUE="one"),
        Row(A=1, SEQ=1, INDEX=2, VALUE="o"),
        Row(A=1, SEQ=1, INDEX=3, VALUE="one"),
        Row(A=2, SEQ=2, INDEX=1, VALUE="twenty"),
        Row(A=2, SEQ=2, INDEX=2, VALUE="two"),
        Row(A=3, SEQ=3, INDEX=1, VALUE="thirty"),
        Row(A=3, SEQ=3, INDEX=2, VALUE="three"),
    ]
    Utils.check_answer(df.select("a", table_func("b", lit(" "))), expected_result)
    Utils.check_answer(
        df.select(col("a"), table_func(col("b"), lit(" "))), expected_result
    )
    Utils.check_answer(df.select(df.a, table_func(df.b, lit(" "))), expected_result)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Session.generator is not supported in Local Testing",
)
def test_generator_table_function(session):
    # works with rowcount
    expected_result = [Row(-108, 3), Row(-107, 3), Row(0, 3)]
    df = (
        session.generator(seq1(1), uniform(1, 10, 2), rowcount=150)
        .order_by(seq1(1))
        .limit(3, offset=20)
    )
    Utils.check_answer(df, expected_result)

    # works with timelimit
    expected_result = [Row(0, 3), Row(0, 3), Row(0, 3)]
    df = (
        session.generator(seq2(0), uniform(1, 10, 2), timelimit=1)
        .order_by(seq2(0))
        .limit(3)
    )
    Utils.check_answer(df, expected_result)

    # works with combination of both
    expected_result = [Row(-108, 3), Row(-107, 3), Row(0, 3)]
    df = (
        session.generator(seq1(1), uniform(1, 10, 2), timelimit=1, rowcount=150)
        .order_by(seq1(1))
        .limit(3, offset=20)
    )
    Utils.check_answer(df, expected_result)

    # works without both
    df = session.generator(seq4(1), uniform(1, 10, 2))
    Utils.check_answer(df, [])

    # aliasing works
    df = (
        session.generator(
            seq1(1).as_("pixel"), uniform(1, 10, 2).as_("unicorn"), rowcount=150
        )
        .order_by("pixel")
        .limit(3, offset=20)
    )
    expected_result = [
        Row(pixel=-108, unicorn=3),
        Row(pixel=-107, unicorn=3),
        Row(pixel=0, unicorn=3),
    ]
    Utils.check_answer(df, expected_result)

    # aggregation works
    df = session.generator(count(seq1(0)).as_("rows"), rowcount=150)
    expected_result = [Row(rows=150)]
    Utils.check_answer(df, expected_result)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Session.generator is not supported in Local Testing",
)
def test_generator_table_function_negative(session):
    # fails when no operators added
    with pytest.raises(ValueError) as ex_info:
        _ = session.generator(rowcount=10)
    assert "Columns cannot be empty for generator table function" in str(ex_info)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
@pytest.mark.udf
def test_select_table_function_negative(session):
    df = session.create_dataframe([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).to_df(
        ["a", "b", "c"]
    )

    class TwoXUDTF:
        def process(self, n: int):
            yield (2 * n,)

    two_x_udtf = udtf(
        TwoXUDTF,
        output_schema=StructType([StructField("two_x", IntegerType())]),
        input_types=[IntegerType()],
    )

    with pytest.raises(ValueError) as ex_info:
        df.select(two_x_udtf("a"), "b", two_x_udtf("c"))
    assert "At most one table function can be called" in str(ex_info)

    @udtf(output_schema=["two_x", "three_x"])
    class multiplier_udtf:
        def process(self, n: int) -> Iterable[Tuple[int, int]]:
            yield (2 * n, 3 * n)

    with pytest.raises(ValueError) as ex_info:
        df.select(multiplier_udtf(df.a).alias("double", "double"))
    assert "All output column names after aliasing must be unique" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        df.select(multiplier_udtf(df.a).alias("double"))
    assert (
        "The number of aliases should be same as the number of cols added by table function"
        in str(ex_info)
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="hash is not supported in Local Testing",
)
def test_random_split(session):
    original_enabled = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set("use_simplified_query_generation", True)
        # test the cache_result is not invoked
        df = session.range(1, 50)
        with session.query_history() as history:
            df1, df2 = df.random_split([0.5, 0.5])
        assert len(history.queries) == 0

        # test the that seed is respected
        df1, df2, df3 = df.random_split([0.5, 0.4, 0.1], seed=1729)
        dfa, dfb, dfc = df.random_split([0.5, 0.4, 0.1], seed=1729)
        Utils.check_answer(df1, dfa)
        Utils.check_answer(df2, dfb)
        Utils.check_answer(df3, dfc)

        # assert that there in no overlap between the splits
        df1, df2 = df.random_split([0.5, 0.5])
        assert df1.intersect(df2).count() == 0
    finally:
        session.conf.set("use_simplified_query_generation", original_enabled)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
@pytest.mark.udf
def test_select_with_table_function_column_overlap(session):
    df = session.create_dataframe([[1, 2, 3], [4, 5, 6]], schema=["A", "B", "C"])

    class TwoXUDTF:
        def process(self, n: int):
            yield (2 * n,)

    two_x_udtf = udtf(
        TwoXUDTF,
        output_schema=StructType([StructField("A", IntegerType())]),
        input_types=[IntegerType()],
    )

    # ensure aliasing works
    Utils.check_answer(
        df.select(df.a, df.b, two_x_udtf(df.a).alias("a2")),
        [Row(A=1, B=2, A2=2), Row(A=4, B=5, A2=8)],
    )

    Utils.check_answer(
        df.select(col("a").alias("a1"), df.b, two_x_udtf(df.a).alias("a2")),
        [Row(A1=1, B=2, A2=2), Row(A1=4, B=5, A2=8)],
    )

    # join_table_function works
    Utils.check_answer(
        df.join_table_function(two_x_udtf(df.a)), [Row(1, 2, 3, 2), Row(4, 5, 6, 8)]
    )

    Utils.check_answer(
        df.join_table_function(two_x_udtf(df.a).alias("a2")),
        [Row(A=1, B=2, C=3, A2=2), Row(A=4, B=5, C=6, A2=8)],
    )

    # ensure explode works
    df = session.create_dataframe([(1, [1, 2]), (2, [3, 4])], schema=["id", "value"])
    Utils.check_answer(
        df.select(df.id, explode(df.value).as_("VAL")),
        [
            Row(ID=1, VAL="1"),
            Row(ID=1, VAL="2"),
            Row(ID=2, VAL="3"),
            Row(ID=2, VAL="4"),
        ],
    )

    # ensure overlapping columns work if a single table function is selected
    Utils.check_answer(
        df.select(explode(df.value)),
        [Row(VALUE="1"), Row(VALUE="2"), Row(VALUE="3"), Row(VALUE="4")],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="functions.explode is not supported in Local Testing",
)
def test_explode(session):
    df = session.create_dataframe(
        [[1, [1, 2, 3], {"a": "b"}, "Kimura"]], schema=["idx", "lists", "maps", "strs"]
    )

    # col is str
    expected_result = [
        Row(value="1"),
        Row(value="2"),
        Row(value="3"),
    ]
    Utils.check_answer(df.select(explode("lists")), expected_result)

    expected_result = [Row(key="a", value='"b"')]
    Utils.check_answer(df.select(explode("maps")), expected_result)

    # col is Column
    expected_result = [
        Row(value="1"),
        Row(value="2"),
        Row(value="3"),
    ]
    Utils.check_answer(df.select(explode(col("lists"))), expected_result)

    expected_result = [Row(key="a", value='"b"')]
    Utils.check_answer(df.select(explode(df.maps)), expected_result)

    # with other non table cols
    expected_result = [
        Row(idx=1, value="1"),
        Row(idx=1, value="2"),
        Row(idx=1, value="3"),
    ]
    Utils.check_answer(df.select(df.idx, explode(col("lists"))), expected_result)

    expected_result = [Row(strs="Kimura", key="a", value='"b"')]
    Utils.check_answer(df.select(df.strs, explode(df.maps)), expected_result)

    # with alias
    expected_result = [
        Row(idx=1, uno="1"),
        Row(idx=1, uno="2"),
        Row(idx=1, uno="3"),
    ]
    Utils.check_answer(
        df.select(df.idx, explode(col("lists")).alias("uno")), expected_result
    )

    expected_result = [Row(strs="Kimura", primo="a", secundo='"b"')]
    Utils.check_answer(
        df.select(df.strs, explode(df.maps).as_("primo", "secundo")), expected_result
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="functions.explode is not supported in Local Testing",
)
def test_explode_negative(session):
    df = session.create_dataframe(
        [[1, [1, 2, 3], {"a": "b"}, "Kimura"]], schema=["idx", "lists", "maps", "strs"]
    )
    split_to_table = table_function("split_to_table")

    # mix explode and table function
    with pytest.raises(
        ValueError, match="At most one table function can be called inside"
    ):
        df.select(split_to_table(df.strs, lit("")), explode(df.lists))

    # mismatch in number of alias given array
    with pytest.raises(
        ValueError,
        match="Invalid number of aliases given for explode. Expecting 1, got 2",
    ):
        df.select(explode(df.lists).alias("key", "val"))

    # mismatch in number of alias given map
    with pytest.raises(
        ValueError,
        match="Invalid number of aliases given for explode. Expecting 2, got 1",
    ):
        df.select(explode(df.maps).alias("val"))

    # invalid column type
    with pytest.raises(ValueError, match="Invalid column type for explode"):
        df.select(explode(df.idx))

    with pytest.raises(ValueError, match="Invalid column type for explode"):
        df.select(explode(col("DOES_NOT_EXIST")))


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF is not supported in Local Testing",
)
@pytest.mark.udf
def test_with_column(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    expected = [Row(A=1, B=2, MEAN=1.5), Row(A=3, B=4, MEAN=3.5)]
    Utils.check_answer(df.with_column("mean", (df["a"] + df["b"]) / 2), expected)

    @udtf(output_schema=["number"])
    class sum_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            yield (a + b,)

    expected = [Row(A=1, B=2, TOTAL=3), Row(A=3, B=4, TOTAL=7)]
    Utils.check_answer(df.with_column("total", sum_udtf(df.a, df.b)), expected)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF is not supported in Local Testing",
)
@pytest.mark.udf
def test_with_column_negative(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])

    # raise error when table function returns multiple columns
    @udtf(output_schema=["sum", "diff"])
    class sum_diff_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int, int]]:
            yield (a + b, a - b)

    with pytest.raises(ValueError) as ex_info:
        df.with_column("total", sum_diff_udtf(df.a, df.b))
    assert (
        "The number of aliases should be same as the number of cols added by table function"
        in str(ex_info)
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF is not supported in Local Testing",
)
@pytest.mark.udf
def test_with_columns(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])

    @udtf(output_schema=["number"])
    class sum_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            yield (a + b,)

    expected = [Row(A=1, B=2, MEAN=1.5, TOTAL=3), Row(A=3, B=4, MEAN=3.5, TOTAL=7)]
    Utils.check_answer(
        df.with_columns(
            ["mean", "total"], [(df["a"] + df["b"]) / 2, sum_udtf(df.a, df.b)]
        ),
        expected,
    )

    # test with a udtf sandwiched between names
    @udtf(output_schema=["sum", "diff"])
    class sum_diff_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int, int]]:
            yield (a + b, a - b)

    expected = [
        Row(A=1, B=2, MEAN=1.5, ADD=3, SUB=-1, TWO_A=2),
        Row(A=3, B=4, MEAN=3.5, ADD=7, SUB=-1, TWO_A=6),
    ]
    Utils.check_answer(
        df.with_columns(
            ["mean", "add", "sub", "two_a"],
            [(df["a"] + df["b"]) / 2, sum_diff_udtf(df.a, df.b), df.a + df.a],
        ),
        expected,
    )

    # test with built-in table function
    split_to_table = table_function("split_to_table")
    df = session.sql(
        "select 'James' as name, 'address1 address2 address3' as addresses"
    )
    expected = [
        Row(
            NAME="James",
            ADDRESSES="address1 address2 address3",
            SEQ=1,
            IDX=1,
            VAL="address1",
        ),
        Row(
            NAME="James",
            ADDRESSES="address1 address2 address3",
            SEQ=1,
            IDX=2,
            VAL="address2",
        ),
        Row(
            NAME="James",
            ADDRESSES="address1 address2 address3",
            SEQ=1,
            IDX=3,
            VAL="address3",
        ),
    ]
    Utils.check_answer(
        df.with_columns(
            ["seq", "idx", "val"], [split_to_table(df.addresses, lit(" "))]
        ),
        expected,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF is not supported in Local Testing",
)
@pytest.mark.udf
def test_with_columns_negative(session):
    df = session.create_dataframe(
        [[1, 2, "one o one"], [3, 4, "two o two"]], schema=["a", "b", "c"]
    )

    # raise error when more column names are added than cols
    with pytest.raises(ValueError) as ex_info:
        df.with_columns(["sum", "diff"], [(df["a"] + df["b"]) / 2])
    assert (
        "The size of column names (2) is not equal to the size of columns (1)"
        in str(ex_info)
    )

    # raise when more than one table function is called
    split_to_table = table_function("split_to_table")

    @udtf(output_schema=["number"])
    class sum_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            yield (a + b,)

    with pytest.raises(ValueError) as ex_info:
        df.with_columns(
            ["total", "sum", "diff"], [sum_udtf(df.a, df.b), split_to_table(df.c)]
        )
    assert (
        "Only one table function call accepted inside with_columns call, (2) provided"
        in str(ex_info)
    )

    # raise when len(cols) < len(values)
    with pytest.raises(ValueError) as ex_info:
        df.with_columns(["total"], [sum_udtf(df.a, df.b), (df.a + df.b) / 2])
    assert "Fewer columns provided." in str(ex_info)

    # raise when col names don't match output cols
    @udtf(output_schema=["sum", "diff"])
    class sum_diff_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int, int]]:
            yield (a + b, a - b)

    with pytest.raises(ValueError) as ex_info:
        df.with_columns(
            ["mean", "total"], [(df["a"] + df["b"]) / 2, split_to_table(df.c, lit(" "))]
        )
    assert (
        "The number of aliases should be same as the number of cols added by table function"
        in str(ex_info)
    )


def test_df_subscriptable(session):
    """Tests select & filter as df[...]"""

    # Star, single column
    res = session.range(3, 8)[["*"]].collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected

    # Star, two columns
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df[["*"]].collect()
    expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
    assert res == expected
    # without double brackets should refer to a Column object
    assert type(df["*"]) == Column

    # single column, str type
    df = session.range(3, 8)
    res = df[["ID"]].collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected
    assert type(df["ID"]) == Column

    # single column, int type
    df = session.range(3, 8)
    res = df[df[0] > 5].collect()
    expected = [Row(6), Row(7)]
    assert res == expected
    assert type(df[0]) == Column

    # two columns, list type
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df[["ID", "ID_PRIME"]].collect()
    expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
    assert res == expected

    # two columns, tuple type
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df[("ID", "ID_PRIME")].collect()
    expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
    assert res == expected

    # two columns, int type
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df[[df[1].getName()]].collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected


def test_filter(session):
    """Tests for df.filter()."""
    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 4).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    res = df.filter(col("id") < 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 3).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 0).collect()
    expected = []
    assert res == expected


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL is not supported in Local Testing",
    run=False,
)
def test_filter_with_sql_str(session):
    df = session.range(1, 10, 2)
    # sql text
    assert (
        df.filter(col("id") > 4).collect()
        == df.filter("id > 4").collect()
        == [Row(5), Row(7), Row(9)]
    )
    assert df.filter(col("id") <= 0).collect() == df.filter("id <= 0").collect() == []

    df = session.create_dataframe(["aa", "bb"], schema=["a"])
    # In SQL expression, we need to use the upper case here when put double quotes
    # around an identifier, as the case in double quotes will be preserved.
    assert (
        df.filter("\"A\" = 'aa'").collect()
        == df.filter("a = 'aa'").collect()
        == [Row("aa")]
    )


def test_filter_incorrect_type(session):
    """Tests for incorrect type passed to DataFrame.filter()."""
    df = session.range(1, 10, 2)

    with pytest.raises(TypeError) as ex_info:
        df.filter(1234)
    assert (
        "'filter/where' expected Column or str as SQL expression, got: <class 'int'>"
        in str(ex_info)
    )


def test_filter_chained(session):
    """Tests for chained DataFrame.filter() operations"""

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") < 4).filter(col("id") < 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = (
        session.range(1, 10, 2).filter(col("id") <= 4).filter(col("id") >= 0).collect()
    )
    expected = [Row(1), Row(3)]
    assert res == expected

    res = (
        session.range(1, 10, 2).filter(col("id") <= 3).filter(col("id") != 5).collect()
    )
    expected = [Row(1), Row(3)]
    assert res == expected


def test_filter_chained_col_objects_int(session):
    """Tests for chained DataFrame.filter() operations."""

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 1).filter(col("id") >= 5).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") >= 1).filter(col("id") >= 5).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    df = session.range(1, 10, 2)
    res = df.filter(col("id") == 5).collect()
    expected = [Row(5)]
    assert res == expected


def test_drop(session):
    """Test for dropping columns from a dataframe."""

    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    res = df.drop("id").select("id_prime").collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected

    # dropping an empty list should raise exception
    with pytest.raises(ValueError) as exc_info:
        df.drop()
    assert "The input of drop() cannot be empty" in str(exc_info)

    df.drop([])  # This is acceptable

    # dropping all columns should raise exception
    with pytest.raises(SnowparkColumnException):
        df.drop("id").drop("id_prime")

    # Drop second column renamed several times
    df2 = (
        session.range(3, 8)
        .select(["id", col("id").alias("id_prime")])
        .select(["id", col("id_prime").alias("id_prime_2")])
        .select(["id", col("id_prime_2").alias("id_prime_3")])
        .select(["id", col("id_prime_3").alias("id_prime_4")])
        .drop("id_prime_4")
    )
    res = df2.select("id").collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected


def test_alias(session):
    """Test for dropping columns from a dataframe."""

    # Selecting non-existing column (already renamed) should fail
    with pytest.raises(SnowparkSQLException):
        session.range(3, 8).select(col("id").alias("id_prime")).select(
            col("id").alias("id_prime")
        ).collect()

    # Rename column several times
    df = (
        session.range(3, 8)
        .select(col("id").alias("id_prime"))
        .select(col("id_prime").alias("id_prime_2"))
        .select(col("id_prime_2").alias("id_prime_3"))
        .select(col("id_prime_3").alias("id_prime_4"))
    )
    res = df.select("id_prime_4").collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert res == expected


def test_join_inner(session):
    """Test for inner join of dataframes."""

    # Implicit inner join on single column
    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id").collect()
    expected = [Row(5), Row(6), Row(7)]
    assert res == expected

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "inner").collect()
    expected = [Row(5), Row(6), Row(7)]
    assert res == expected

    # Join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id").collect()
    expected = [Row(5, 5, 5), Row(6, 6, 6), Row(7, 7, 7)]
    assert res == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    expected = [Row(5, 5, 5), Row(6, 6, 6), Row(7, 7, 7)]
    res = df1.join(df2, "id").collect()
    assert res == expected


def test_join_left_anti(session):
    """Test for left-anti join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "left_anti").collect()
    expected = [Row(3), Row(4)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id", "left_anti").collect()
    expected = [Row(3, 3), Row(4, 4)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    res = df1.join(df2, "id", "left_anti").collect()
    expected = [Row(3, 3), Row(4, 4)]
    assert sorted(res, key=lambda r: r[0]) == expected

    df3 = session.create_dataframe(
        [[i if i & 2 else None] for i in range(3, 8)], schema=["id"]
    )
    res = df3.join(df1, "id", "leftanti").collect()
    assert res == [Row(ID=None), Row(ID=None)]


def test_join_left_outer(session):
    """Test for left-outer join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "left_outer").collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id", "left_outer").collect()
    expected = [
        Row(3, 3, None),
        Row(4, 4, None),
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    res = df1.join(df2, "id", "left_outer").collect()
    expected = [
        Row(3, 3, None),
        Row(4, 4, None),
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected


def test_join_on_order(session, local_testing_mode):
    """
    Test that an 'on' clause in a different order from the data frame re-orders the columns correctly.
    """
    df1 = session.create_dataframe([(1, "A", 3)], schema=["A", "B", "C"])
    df2 = session.create_dataframe([(1, "A", 4)], schema=["A", "B", "D"])

    df3 = df1.join(df2, on=["B", "A"])
    assert df3.schema == StructType(
        [
            StructField("B", StringType(), nullable=False),
            StructField("A", LongType(), nullable=False),
            StructField("C", LongType(), nullable=False),
            StructField("D", LongType(), nullable=False),
        ]
    )
    Utils.check_answer(df3, [Row(B="A", A=1, C=3, D=4)])


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="schema_query is not supported in Local Testing",
    run=False,
)
def test_in_with_subquery_multiple_query(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_threshold = analyzer.ARRAY_BIND_THRESHOLD

    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2

        df0 = session.create_dataframe([[1], [2], [3]], schema=["a"])
        df1 = session.create_dataframe(
            [[1, 11, 111], [2, 22, 222], [3, 33, 333]], schema=["a", "b", "c"]
        )
        df_filter = df0.filter(df0.a < 3)
        df_in = df1.filter(~df1.a.in_(df_filter))

        df_in = df_in.select("a", "b")
        Utils.check_answer(df_in, [Row(3, 33)])
        # check that schema query does not depend on temp tables
        assert "SNOWPARK_TEMP_TABLE_" not in df_in._plan.schema_query
        assert df_in.schema == StructType(
            [StructField("A", LongType(), False), StructField("B", LongType(), False)]
        )
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_join_right_outer(session):
    """Test for right-outer join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "right_outer").collect()
    expected = [Row(5), Row(6), Row(7), Row(8), Row(9)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id", "right_outer").collect()
    expected = [
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
        Row(8, None, 8),
        Row(9, None, 9),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    res = df1.join(df2, "id", "right_outer").collect()
    expected = [
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
        Row(8, None, 8),
        Row(9, None, 9),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected


def test_join_left_semi(session):
    """Test for left semi join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "left_semi").collect()
    expected = [Row(5), Row(6), Row(7)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id", "left_semi").collect()
    expected = [Row(5, 5), Row(6, 6), Row(7, 7)]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    expected = [Row(5, 5), Row(6, 6), Row(7, 7)]
    res = df1.join(df2, "id", "left_semi").collect()
    assert sorted(res, key=lambda r: r[0]) == expected


@multithreaded_run()
def test_join_cross(session):
    """Test for cross join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)

    res1 = df1.cross_join(df2).collect()
    expected = [Row(x, y) for x, y in product(range(3, 8), range(5, 10))]
    assert sorted(res1, key=lambda r: (r[0], r[1])) == expected
    res2 = df1.join(df2, how="cross").collect()
    assert sorted(res2, key=lambda r: (r[0], r[1])) == expected

    # Join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.cross_join(df2).collect()
    expected = [Row(x, x, y, y) for x, y in product(range(3, 8), range(5, 10))]
    assert sorted(res, key=lambda r: (r[0], r[1])) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    expected = [Row(x, x, y, y) for x, y in product(range(3, 8), range(5, 10))]
    res = df1.cross_join(df2).collect()
    assert sorted(res, key=lambda r: (r[0], r[2])) == expected

    with pytest.raises(Exception) as ex:
        df1.join(df2, col("id"), "cross")
    assert "Cross joins cannot take columns as input." in str(ex.value)

    # Case, join on same-name column, other columns have different name, select common column.
    this = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    other = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    df_cross = this.cross_join(other).select([this.col("id"), other.col("id")])
    res = df_cross.collect()
    expected = [Row(x, y) for x, y in product(range(3, 8), range(5, 10))]
    assert sorted(res, key=lambda r: (r[0], r[1])) == expected


def test_join_outer(session):
    """Test for outer join of dataframes."""

    df1 = session.range(3, 8)
    df2 = session.range(5, 10)
    res = df1.join(df2, "id", "outer").collect()
    expected = [
        Row(3),
        Row(4),
        Row(5),
        Row(6),
        Row(7),
        Row(8),
        Row(9),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Join on same-name column, other columns have same name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
    res = df1.join(df2, "id", "outer").collect()
    expected = [
        Row(3, 3, None),
        Row(4, 4, None),
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
        Row(8, None, 8),
        Row(9, None, 9),
    ]
    assert sorted(res, key=lambda r: r[0]) == expected

    # Case, join on same-name column, other columns have different name
    df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
    df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
    expected = [
        Row(3, 3, None),
        Row(4, 4, None),
        Row(5, 5, 5),
        Row(6, 6, 6),
        Row(7, 7, 7),
        Row(8, None, 8),
        Row(9, None, 9),
    ]
    res = df1.join(df2, "id", "outer").collect()
    assert sorted(res, key=lambda r: r[0]) == expected


def test_toDF(session):
    """Test df.to_df()."""

    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])

    # calling to_df() with fewer new names than columns should fail
    with pytest.raises(Exception) as ex:
        df.to_df(["new_name"])
    assert "The number of columns doesn't match. Old column names (2):" in str(ex.value)

    res = (
        df.to_df(["rename1", "rename2"])
        .select([col("rename1"), col("rename2")])
        .collect()
    )
    expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
    assert sorted(res, key=lambda r: r[0]) == expected

    res = df.to_df(["rename1", "rename2"]).columns
    assert res == ["RENAME1", "RENAME2"]

    df_prime = df.to_df(["rename1", "rename2"])
    res = df_prime.select(df_prime.RENAME1).collect()
    expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
    assert sorted(res, key=lambda r: r[0]) == expected


def test_df_col(session):
    """Test df.col()"""

    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    c = df.col("id")
    assert isinstance(c, Column)
    assert isinstance(c._expression, Attribute)

    c = df.col("*")
    assert isinstance(c, Column)
    assert isinstance(c._expression, Star)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Session.query_history is not supported",
    run=False,
)
def test_cache_result_query(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    with session.query_history() as history:
        df.cache_result()

    assert len(history.queries) == 2
    assert "CREATE  SCOPED TEMPORARY  TABLE" in history.queries[0].sql_text
    assert (
        "INSERT  INTO" in history.queries[1].sql_text
        and 'SELECT $1 AS "A", $2 AS "B" FROM  VALUES (1 :: INT, 2 :: INT)'
        in history.queries[1].sql_text
    )


def test_create_dataframe_with_basic_data_types(session):
    data1 = [
        1,
        "one",
        1.0,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        Decimal(0.5),
    ]
    data2 = [
        0,
        "",
        0.0,
        datetime.datetime.min,
        datetime.time.min,
        datetime.date.min,
        False,
        bytes(),
        Decimal(0),
    ]
    expected_names = [f"_{idx + 1}" for idx in range(len(data1))]
    expected_rows = [Row(*data1), Row(*data2)]
    df = session.create_dataframe([data1, data2])
    assert [field.name for field in df.schema.fields] == expected_names
    assert [type(field.datatype) for field in df.schema.fields] == [
        LongType,
        StringType,
        DoubleType,
        TimestampType,
        TimeType,
        DateType,
        BooleanType,
        BinaryType,
        DecimalType,
    ]
    result = df.collect()
    assert result == expected_rows
    assert result[0].asDict(True) == {k: v for k, v in zip(expected_names, data1)}
    assert result[1].asDict(True) == {k: v for k, v in zip(expected_names, data2)}
    assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_with_semi_structured_data_types(session):
    data = [
        [
            ["'", 2],
            ("'", 2),
            [[1, 2], [2, 1]],
            array("I", [1, 2, 3]),
            {"'": 1},
        ],
        [
            ["'", 3],
            ("'", 3),
            [[1, 3], [3, 1]],
            array("I", [1, 2, 3, 4]),
            {"'": 3},
        ],
    ]
    df = session.create_dataframe(data)
    assert [type(field.datatype) for field in df.schema.fields] == [
        ArrayType,
        ArrayType,
        ArrayType,
        ArrayType,
        MapType,
    ]
    Utils.check_answer(
        df.collect(),
        [
            Row(
                '[\n  "\'",\n  2\n]',
                '[\n  "\'",\n  2\n]',
                "[\n  [\n    1,\n    2\n  ],\n  [\n    2,\n    1\n  ]\n]",
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "\'": 1\n}',
            ),
            Row(
                '[\n  "\'",\n  3\n]',
                '[\n  "\'",\n  3\n]',
                "[\n  [\n    1,\n    3\n  ],\n  [\n    3,\n    1\n  ]\n]",
                "[\n  1,\n  2,\n  3,\n  4\n]",
                '{\n  "\'": 3\n}',
            ),
        ],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1439717 create dataframe from pandas dataframe containing timestamp with tzinfo is not supported.",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_create_dataframe_with_pandas_df(session):
    data = {
        "pandas_datetime": ["2021-09-30 12:00:00", "2021-09-30 13:00:00"],
        "date": [pd.to_datetime("2010-1-1"), pd.to_datetime("2011-1-1")],
        "datetime.datetime": [
            datetime.datetime(2010, 1, 1),
            datetime.datetime(2010, 1, 1),
        ],
    }
    pdf = pd.DataFrame(data)
    pdf["pandas_datetime"] = pd.to_datetime(pdf["pandas_datetime"])
    df = session.create_dataframe(pdf)

    assert df.schema[0].name == '"pandas_datetime"'
    assert df.schema[1].name == '"date"'
    assert df.schema[2].name == '"datetime.datetime"'
    assert df.schema[0].datatype == TimestampType(TimestampTimeZone.NTZ)
    assert df.schema[1].datatype == TimestampType(TimestampTimeZone.NTZ)
    assert df.schema[2].datatype == TimestampType(TimestampTimeZone.NTZ)

    # test with timezone added to timestamp
    pdf["pandas_datetime"] = pdf["pandas_datetime"].dt.tz_localize("US/Pacific")
    pdf["date"] = pdf["date"].dt.tz_localize("US/Pacific")
    pdf["datetime.datetime"] = pdf["datetime.datetime"].dt.tz_localize("US/Pacific")
    df = session.create_dataframe(pdf)

    assert df.schema[0].name == '"pandas_datetime"'
    assert df.schema[1].name == '"date"'
    assert df.schema[2].name == '"datetime.datetime"'
    assert df.schema[0].datatype == TimestampType(TimestampTimeZone.LTZ)
    assert df.schema[1].datatype == TimestampType(TimestampTimeZone.LTZ)
    assert df.schema[2].datatype == TimestampType(TimestampTimeZone.LTZ)


def test_create_dataframe_with_dict(session):
    data = {f"snow_{idx + 1}": idx**3 for idx in range(5)}
    expected_names = [name.upper() for name in data.keys()]
    expected_rows = [Row(*data.values())]
    df = session.create_dataframe([data])
    for field, expected_name in zip(df.schema.fields, expected_names):
        assert Utils.equals_ignore_case(field.name, expected_name)
    result = df.collect()
    assert result == expected_rows
    assert result[0].asDict(True) == {
        k: v for k, v in zip(expected_names, data.values())
    }
    assert df.select(expected_names).collect() == expected_rows

    # dicts with different keys
    df = session.createDataFrame([{"a": 1}, {"b": 2}])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, None), Row(None, 2)])

    df = session.createDataFrame([{"a": 1}, {"d": 2, "e": 3}, {"c": 4, "b": 5}])
    assert [field.name for field in df.schema.fields] == ["A", "D", "E", "C", "B"]
    Utils.check_answer(
        df,
        [
            Row(1, None, None, None, None),
            Row(None, 2, 3, None, None),
            Row(None, None, None, 4, 5),
        ],
    )


def test_create_dataframe_with_dict_given_schema(session):
    schema = StructType(
        [
            StructField("A", LongType(), nullable=True),
            StructField("B", LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{"a": 1, "b": 1}, {"a": 2, "b": 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 1), Row(2, 2)])

    schema = StructType(
        [
            StructField("a", LongType(), nullable=True),
            StructField("b", LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{"A": 1, "B": 1}, {"A": 2, "B": 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 1), Row(2, 2)])

    schema = StructType(
        [
            StructField("A", LongType(), nullable=True),
            StructField("B", LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{'"a"': 1, '"b"': 1}, {'"a"': 2, '"b"': 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(None, None), Row(None, None)])

    schema = StructType(
        [
            StructField('"A"', LongType(), nullable=True),
            StructField('"B"', LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{'"A"': 1, '"B"': 1}, {'"A"': 2, '"B"': 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 1), Row(2, 2)])

    schema = StructType(
        [
            StructField('"A"', LongType(), nullable=True),
            StructField('"B"', LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{"A": 1, "B": 1}, {"A": 2, "B": 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 1), Row(2, 2)])

    schema = StructType(
        [
            StructField('"A"', LongType(), nullable=True),
            StructField('"B"', LongType(), nullable=True),
        ]
    )
    df = session.createDataFrame([{'"a"': 1, '"b"': 1}, {'"a"': 2, '"b"': 2}], schema)
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(None, None), Row(None, None)])


@multithreaded_run()
def test_create_dataframe_with_namedtuple(session):
    Data = namedtuple("Data", [f"snow_{idx + 1}" for idx in range(5)])
    data = Data(*[idx**3 for idx in range(5)])
    expected_names = [name.upper() for name in data._fields]
    expected_rows = [Row(*data)]
    df = session.createDataFrame([data])
    for field, expected_name in zip(df.schema.fields, expected_names):
        assert Utils.equals_ignore_case(field.name, expected_name)
    result = df.collect()
    assert result == expected_rows
    assert result[0].asDict(True) == {k: v for k, v in zip(expected_names, data)}
    assert df.select(expected_names).collect() == expected_rows

    # dicts with different namedtuples
    Data1 = namedtuple("Data", ["a", "b"])
    Data2 = namedtuple("Data", ["d", "c"])
    df = session.createDataFrame([Data1(1, 2), Data2(3, 4)])
    assert [field.name for field in df.schema.fields] == ["A", "B", "D", "C"]
    Utils.check_answer(df, [Row(1, 2, None, None), Row(None, None, 3, 4)])


def test_create_dataframe_with_row(session):
    row1 = Row(a=1, b=2)
    row2 = Row(a=3, b=4)
    row3 = Row(d=5, c=6, e=7)
    row4 = Row(7, 8)
    row5 = Row(9, 10)

    df = session.createDataFrame([row1, row2])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [row1, row2])

    df = session.createDataFrame([row4, row5])
    assert [field.name for field in df.schema.fields] == ["_1", "_2"]
    Utils.check_answer(df, [row4, row5])

    df = session.createDataFrame([row3])
    assert [field.name for field in df.schema.fields] == ["D", "C", "E"]
    Utils.check_answer(df, [row3])

    df = session.createDataFrame([row1, row2, row3])
    assert [field.name for field in df.schema.fields] == ["A", "B", "D", "C", "E"]
    Utils.check_answer(
        df,
        [
            Row(1, 2, None, None, None),
            Row(3, 4, None, None, None),
            Row(None, None, 5, 6, 7),
        ],
    )

    with pytest.raises(ValueError) as ex_info:
        session.createDataFrame([row1, row4])
    assert "4 fields are required by schema but 2 values are provided" in str(ex_info)


def test_create_dataframe_with_mixed_dict_namedtuple_row(session):
    d = {"a": 1, "b": 2}
    Data = namedtuple("Data", ["a", "b"])
    t = Data(3, 4)
    r = Row(a=5, b=6)
    df = session.createDataFrame([d, t, r])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 2), Row(3, 4), Row(5, 6)])

    r2 = Row(c=7, d=8)
    df = session.createDataFrame([d, t, r2])
    assert [field.name for field in df.schema.fields] == ["A", "B", "C", "D"]
    Utils.check_answer(
        df, [Row(1, 2, None, None), Row(3, 4, None, None), Row(None, None, 7, 8)]
    )


def test_create_dataframe_with_schema_col_names(session):
    col_names = ["a", "b", "c", "d"]
    df = session.create_dataframe([[1, 2, 3, 4]], schema=col_names)
    for field, expected_name in zip(df.schema.fields, col_names):
        assert Utils.equals_ignore_case(field.name, expected_name)

    # only give first two column names,
    # and the rest will be populated as "_#num"
    df = session.create_dataframe([[1, 2, 3, 4]], schema=col_names[:2])
    for field, expected_name in zip(df.schema.fields, col_names[:2] + ["_3", "_4"]):
        assert Utils.equals_ignore_case(field.name, expected_name)

    # specify nullable in structtype to avoid insert null value into non-nullable column
    struct_col_name = StructType([StructField(col, StringType()) for col in col_names])

    # the column names provided via schema keyword will overwrite other column names
    df = session.create_dataframe(
        [{"aa": 1, "bb": 2, "cc": 3, "dd": 4}], schema=struct_col_name
    )
    for field, expected_name in zip(df.schema.fields, col_names):
        assert Utils.equals_ignore_case(field.name, expected_name)


def test_create_dataframe_with_variant(session):
    data = [
        1,
        "one",
        1.1,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        Decimal(0.5),
        [1, 2, 3],
        {"a": "foo"},
    ]
    df = session.create_dataframe(
        [data],
        schema=StructType(
            [StructField(f"col_{i + 1}", VariantType()) for i in range(len(data))]
        ),
    )
    assert df.collect() == [
        Row(
            "1",
            '"one"',
            "1.1",
            '"2017-02-24T12:00:05.456000"',
            '"20:57:06"',
            '"2017-02-25"',
            "true",
            '"61"',
            "0.5",
            "[\n  1,\n  2,\n  3\n]",
            '{\n  "a": "foo"\n}',
        )
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_show_dataframe_spark(session):
    if not structured_types_supported(session, False):
        pytest.skip("Test requires structured type support.")

    data = [
        1,
        "one",
        1.1,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        False,
        None,
        bytearray("a", "utf-8"),
        bytearray("abc", "utf-16"),
        Decimal(0.5),
        [1, 2, 3],
        [
            bytearray("abc", "utf-8"),
            bytearray("a", "utf-16"),
        ],
        {"a": "foo"},
        {"Street": "123 Elm St", "ZipCode": 12345},
        [1, 2, 3],
        {"a": "foo"},
    ]

    with structured_types_enabled_session(session) as session:
        schema = StructType(
            [
                StructField("col_1", IntegerType()),
                StructField("col_2", StringType()),
                StructField("col_3", FloatType()),
                StructField("col_4", TimestampType()),
                StructField("col_5", TimeType()),
                StructField("col_6", DateType()),
                StructField("col_7", BooleanType()),
                StructField("col_8", BooleanType()),
                StructField("col_9", VariantType()),
                StructField("col_10", BinaryType()),
                StructField("col_11", BinaryType()),
                StructField("col_12", DecimalType()),
                StructField("col_13", ArrayType(IntegerType())),
                StructField("col_14", ArrayType(BinaryType())),
                StructField("col_15", MapType(StringType(), StringType())),
                StructField(
                    "col_16",
                    StructType(
                        [
                            StructField("Street", StringType(), True),
                            StructField("ZipCode", IntegerType(), True),
                        ]
                    ),
                ),
                StructField("col_17", ArrayType()),
                StructField("col_18", StructType()),
            ]
        )
        df = session.create_dataframe([data], schema=schema)
        spark_col_names = [f"col_{i + 1}" for i in range(len(data))]

        def assert_show_string_equals(actual: str, expected: str):
            actual_lines = actual.strip().split("\n")
            expected_lines = expected.strip().split("\n")
            if len(actual_lines) != len(expected_lines):
                print(
                    f"\nactual_lines:\n{actual_lines}\nexpected_lines:\n{expected_lines}"
                )
                pytest.fail()
            for a, e in zip(actual_lines, expected_lines):
                if a.strip() != e.strip():
                    print(f"\nactual:\n{actual}\nexpected:{expected}")
                    pytest.fail()

        assert_show_string_equals(
            df._show_string_spark().strip(),
            dedent(
                """
            +-------+-------+-------+--------------------+--------+----------+-------+-------+-------+--------+--------------------+--------+---------+--------------------+----------+-------------------+--------------------+------------------+
            |"COL_1"|"COL_2"|"COL_3"|             "COL_4"| "COL_5"|   "COL_6"|"COL_7"|"COL_8"|"COL_9"|"COL_10"|            "COL_11"|"COL_12"| "COL_13"|            "COL_14"|  "COL_15"|           "COL_16"|            "COL_17"|          "COL_18"|
            +-------+-------+-------+--------------------+--------+----------+-------+-------+-------+--------+--------------------+--------+---------+--------------------+----------+-------------------+--------------------+------------------+
            |      1|    one|    1.1|2017-02-24 12:00:...|20:57:06|2017-02-25|   true|  false|   NULL|    [61]|[FF FE 61 00 62 0...|       1|[1, 2, 3]|[[61 62 63], [FF ...|{a -> foo}|{123 Elm St, 12345}|[\\n  1,\\n  2,\\n  ...|{\\n  "a": "foo"\\n}|
            +-------+-------+-------+--------------------+--------+----------+-------+-------+-------+--------+--------------------+--------+---------+--------------------+----------+-------------------+--------------------+------------------+
            """
            ),
        )
        assert_show_string_equals(
            df._show_string_spark(_spark_column_names=spark_col_names),
            dedent(
                """
            +-----+-----+-----+--------------------+--------+----------+-----+-----+-----+------+--------------------+------+---------+--------------------+----------+-------------------+--------------------+------------------+
            |col_1|col_2|col_3|               col_4|   col_5|     col_6|col_7|col_8|col_9|col_10|              col_11|col_12|   col_13|              col_14|    col_15|             col_16|              col_17|            col_18|
            +-----+-----+-----+--------------------+--------+----------+-----+-----+-----+------+--------------------+------+---------+--------------------+----------+-------------------+--------------------+------------------+
            |    1|  one|  1.1|2017-02-24 12:00:...|20:57:06|2017-02-25| true|false| NULL|  [61]|[FF FE 61 00 62 0...|     1|[1, 2, 3]|[[61 62 63], [FF ...|{a -> foo}|{123 Elm St, 12345}|[\\n  1,\\n  2,\\n  ...|{\\n  "a": "foo"\\n}|
            +-----+-----+-----+--------------------+--------+----------+-----+-----+-----+------+--------------------+------+---------+--------------------+----------+-------------------+--------------------+------------------+
            """
            ),
        )
        assert_show_string_equals(
            df._show_string_spark(
                vertical=True,
                _spark_column_names=spark_col_names,
            ),
            dedent(
                """
            -RECORD 0----------------------
             col_1  | 1
             col_2  | one
             col_3  | 1.1
             col_4  | 2017-02-24 12:00:...
             col_5  | 20:57:06
             col_6  | 2017-02-25
             col_7  | true
             col_8  | false
             col_9  | NULL
             col_10 | [61]
             col_11 | [FF FE 61 00 62 0...
             col_12 | 1
             col_13 | [1, 2, 3]
             col_14 | [[61 62 63], [FF ...
             col_15 | {a -> foo}
             col_16 | {123 Elm St, 12345}
             col_17 | [\\n  1,\\n  2,\\n  ...
             col_18 | {\\n  "a": "foo"\\n}
            """
            ),
        )
        assert_show_string_equals(
            df._show_string_spark(
                vertical=True,
                truncate=False,
                _spark_column_names=spark_col_names,
            ),
            dedent(
                """
            -RECORD 0-----------------------------
            col_1  | 1
            col_2  | one
            col_3  | 1.1
            col_4  | 2017-02-24 12:00:05.456000
            col_5  | 20:57:06
            col_6  | 2017-02-25
            col_7  | true
            col_8  | false
            col_9  | NULL
            col_10 | [61]
            col_11 | [FF FE 61 00 62 00 63 00]
            col_12 | 1
            col_13 | [1, 2, 3]
            col_14 | [[61 62 63], [FF FE 61 00]]
            col_15 | {a -> foo}
            col_16 | {123 Elm St, 12345}
            col_17 | [\\n  1,\\n  2,\\n  3\\n]
            col_18 | {\\n  "a": "foo"\\n}
            """
            ),
        )
        assert_show_string_equals(
            df._show_string_spark(
                truncate=False,
                _spark_column_names=spark_col_names,
            ),
            dedent(
                """
            +-----+-----+-----+--------------------------+--------+----------+-----+-----+-----+------+-------------------------+------+---------+---------------------------+----------+-------------------+---------------------+------------------+
            |col_1|col_2|col_3|col_4                     |col_5   |col_6     |col_7|col_8|col_9|col_10|col_11                   |col_12|col_13   |col_14                     |col_15    |col_16             |col_17               |col_18            |
            +-----+-----+-----+--------------------------+--------+----------+-----+-----+-----+------+-------------------------+------+---------+---------------------------+----------+-------------------+---------------------+------------------+
            |1    |one  |1.1  |2017-02-24 12:00:05.456000|20:57:06|2017-02-25|true |false|NULL |[61]  |[FF FE 61 00 62 00 63 00]|1     |[1, 2, 3]|[[61 62 63], [FF FE 61 00]]|{a -> foo}|{123 Elm St, 12345}|[\\n  1,\\n  2,\\n  3\\n]|{\\n  "a": "foo"\\n}|
            +-----+-----+-----+--------------------------+--------+----------+-----+-----+-----+------+-------------------------+------+---------+---------------------------+----------+-------------------+---------------------+------------------+
            """
            ),
        )
        assert_show_string_equals(
            df._show_string_spark(
                truncate=10,
                _spark_column_names=spark_col_names,
            ),
            dedent(
                """
            +-----+-----+-----+----------+--------+----------+-----+-----+-----+------+----------+------+---------+----------+----------+----------+----------+----------+
            |col_1|col_2|col_3|     col_4|   col_5|     col_6|col_7|col_8|col_9|col_10|    col_11|col_12|   col_13|    col_14|    col_15|    col_16|    col_17|    col_18|
            +-----+-----+-----+----------+--------+----------+-----+-----+-----+------+----------+------+---------+----------+----------+----------+----------+----------+
            |    1|  one|  1.1|2017-02...|20:57:06|2017-02-25| true|false| NULL|  [61]|[FF FE ...|     1|[1, 2, 3]|[[61 62...|{a -> foo}|{123 El...|[\\n  1,...|{\\n  "a...|
            +-----+-----+-----+----------+--------+----------+-----+-----+-----+------+----------+------+---------+----------+----------+----------+----------+----------+
            """
            ),
        )


@pytest.mark.parametrize("data", [[0, 1, 2, 3], ["", "a"], [False, True], [None]])
def test_create_dataframe_with_single_value(session, data):
    expected_names = ["_1"]
    expected_rows = [Row(d) for d in data]
    df = session.create_dataframe(data)
    assert [field.name for field in df.schema.fields] == expected_names
    Utils.check_answer(df, expected_rows)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame.describe is not supported in Local Testing",
)
def test_create_dataframe_empty(session):
    Utils.check_answer(session.create_dataframe([[]]), [Row(None)])
    Utils.check_answer(session.create_dataframe([[], []]), [Row(None), Row(None)])

    with pytest.raises(ValueError) as ex_info:
        session.createDataFrame([])
    assert "Cannot infer schema from empty data" in str(ex_info)

    schema = StructType(
        [StructField("a", IntegerType()), StructField("b", IntegerType())]
    )
    df = session.create_dataframe([], schema=schema)

    # collect
    Utils.check_answer(df, [])
    Utils.check_answer(df.select("a"), [])

    # show
    assert (
        df._show_string(_emit_ast=session.ast_enabled)
        == """
-------------
|"A"  |"B"  |
-------------
|     |     |
-------------
""".lstrip()
    )

    # columns
    assert df.columns == ["A", "B"]

    # count
    assert df.count() == 0

    # fillna should not fill any value in an empty df
    Utils.check_answer(df.fillna(1), [])

    # all stats should be 0 or None
    Utils.check_answer(
        df.describe("b").collect(),
        [
            Row("count", 0),
            Row("mean", None),
            Row("stddev", None),
            Row("min", None),
            Row("max", None),
        ],
    )

    # with_column can append a column, but still no rows
    Utils.check_answer(df.with_column("c", lit(2)), [])
    assert df.with_column("c", lit(2)).columns == ["A", "B", "C"]


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Large result")
def test_create_dataframe_from_none_data(session):
    assert session.create_dataframe([None, None]).collect() == [
        Row(None),
        Row(None),
    ]
    assert session.create_dataframe([[None, None], [1, "1"]]).collect() == [
        Row(None, None),
        Row(1, "1"),
    ]
    assert session.create_dataframe([[1, "1"], [None, None]]).collect() == [
        Row(1, "1"),
        Row(None, None),
    ]

    # large None data
    assert session.create_dataframe([None] * 20000).collect() == [Row(None)] * 20000


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Array binding is SQL feature",
    run=False,
)
def test_create_dataframe_large_without_batch_insert(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 400_000
        with pytest.raises(SnowparkSQLException) as ex_info:
            session.create_dataframe([1] * 200_001).collect()
        assert "SQL compilation error" in str(ex_info.value)
        assert "maximum number of expressions in a list exceeded" in str(ex_info.value)
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
@pytest.mark.parametrize("paramstyle", ["pyformat", "format", "qmark", "numeric"])
def test_create_dataframe_large_respects_paramstyle(db_parameters, paramstyle):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    db_parameters["paramstyle"] = paramstyle
    session_builder = Session.builder.configs(db_parameters)
    new_session = session_builder.create()
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df = new_session.create_dataframe([[1], [2], [3]])
        Utils.check_answer(df, [Row(1), Row(2), Row(3)])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value
        new_session.close()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Connections with paramstyle are not supported in local testing",
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_create_dataframe_large_respects_paramstyle_negative(db_parameters):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    session_builder = Session.builder.configs(db_parameters)
    new_session = session_builder.create()
    new_session._conn._conn._paramstyle = "unsupported"
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        with pytest.raises(
            ValueError, match="'unsupported' is not a recognized paramstyle"
        ):
            new_session.create_dataframe([[1], [2], [3]])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value
        new_session.close()


def test_create_dataframe_with_invalid_data(session):
    # None input
    with pytest.raises(ValueError) as ex_info:
        session.create_dataframe(None)
    assert "data cannot be None" in str(ex_info)

    # input other than list and tuple
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe(1)
    assert "only accepts data as a list, tuple or a pandas DataFrame" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe({1, 2})
    assert "only accepts data as a list, tuple or a pandas DataFrame" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe({"a": 1, "b": 2})
    assert "only accepts data as a list, tuple or a pandas DataFrame" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe(Row(a=1, b=2))
    assert "create_dataframe() function does not accept a Row object" in str(ex_info)

    # inconsistent type
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([1, "1"])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([1, 1.0])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([1.0, Decimal(1.0)])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe(["1", bytearray("1", "utf-8")])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([datetime.datetime.now(), datetime.date.today()])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([datetime.datetime.now(), datetime.time()])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([[[1, 2, 3], 1], [1, 1]])
    assert "Cannot merge type" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe([[[1, 2, 3], 1], [{1: 2}, 1]])
    assert "Cannot merge type" in str(ex_info)

    # inconsistent length
    with pytest.raises(ValueError) as ex_info:
        session.create_dataframe([[1], [1, 2]])
    assert "data consists of rows with different lengths" in str(ex_info)


def test_attribute_reference_to_sql(session):
    from snowflake.snowpark.functions import sum as sum_

    df = session.create_dataframe([(3, 1), (None, 2), (1, None), (4, 5)]).to_df(
        "a", "b"
    )
    agg_results = (
        df.agg(
            [
                sum_(df["a"].is_null().cast(IntegerType())),
                sum_(df["b"].is_null().cast(IntegerType())),
            ]
        )
        .to_df("a", "b")
        .collect()
    )

    Utils.check_answer([Row(1, 1)], agg_results)


def test_dataframe_duplicated_column_names(session, local_testing_mode):
    tmpdf = session.create_dataframe([(1, 2)]).to_df(["v1", "v2"])
    df = tmpdf.select(col("v1").alias("a"), col("v2").alias("a"))

    # collect() works and return a row with duplicated keys
    res = df.collect()
    assert len(res[0]) == 2
    assert res[0].A == 1

    if not local_testing_mode:
        # however, create a table/view doesn't work because
        # Snowflake doesn't allow duplicated column names
        with pytest.raises(SnowparkSQLException) as ex_info:
            df.create_or_replace_view(
                Utils.random_name_for_temp_object(TempObjectType.VIEW)
            )
        assert "duplicate column name 'A'" in str(ex_info.value)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Async query is not supported in stored procedure yet"
)
def test_case_insensitive_collect(session, local_testing_mode):
    df = session.create_dataframe(
        [["Gordon", 153]], schema=["firstname", "matches_won"]
    )
    df_quote = session.create_dataframe(
        [["Gordon", 153]], schema=["'quotedName'", "quoted-won"]
    )

    # tests for sync collect
    row = df.collect(case_sensitive=False)[0]
    assert row.firstName == "Gordon"
    assert row.FIRSTNAME == "Gordon"
    assert row.FiRstNamE == "Gordon"
    assert row["firstname"] == "Gordon"
    assert row["FIRSTNAME"] == "Gordon"
    assert row["FirstName"] == "Gordon"

    assert row.matches_won == 153
    assert row.MATCHES_WON == 153
    assert row.MaTchEs_WoN == 153
    assert row["matches_won"] == 153
    assert row["Matches_Won"] == 153
    assert row["MATCHES_WON"] == 153

    with pytest.raises(
        ValueError,
        match="Case insensitive fields is not supported in presence of quoted columns",
    ):
        row = df_quote.collect(case_sensitive=False)[0]

    # tests for async collect
    if not local_testing_mode:
        async_job = df.collect_nowait(case_sensitive=False)
        row = async_job.result()[0]

        assert row.firstName == "Gordon"
        assert row.FIRSTNAME == "Gordon"
        assert row.FiRstNamE == "Gordon"
        assert row["firstname"] == "Gordon"
        assert row["FIRSTNAME"] == "Gordon"
        assert row["FirstName"] == "Gordon"

        assert row.matches_won == 153
        assert row.MATCHES_WON == 153
        assert row.MaTchEs_WoN == 153
        assert row["matches_won"] == 153
        assert row["Matches_Won"] == 153
        assert row["MATCHES_WON"] == 153

        async_job = df_quote.collect_nowait(case_sensitive=False)
        with pytest.raises(
            ValueError,
            match="Case insensitive fields is not supported in presence of quoted columns",
        ):
            row = async_job.result()[0]

    # special character tests
    df_login = session.create_dataframe(
        [["admin", "test"], ["snowman", "test"]], schema=["username", "p@$$w0rD"]
    )
    row = df_login.collect(case_sensitive=False)[0]

    assert row.username == "admin"
    assert row.UserName == "admin"
    assert row.usErName == "admin"

    assert row["p@$$w0rD"] == "test"
    assert row["p@$$w0rd"] == "test"
    assert row["P@$$W0RD"] == "test"


def test_case_insensitive_local_iterator(session):
    df = session.create_dataframe(
        [["Gordon", 153]], schema=["firstname", "matches_won"]
    )
    df_quote = session.create_dataframe(
        [["Gordon", 153]], schema=["'quotedName'", "quoted-won"]
    )

    # tests for sync collect
    row = next(df.to_local_iterator(case_sensitive=False))
    assert row.firstName == "Gordon"
    assert row.FIRSTNAME == "Gordon"
    assert row.FiRstNamE == "Gordon"
    assert row["firstname"] == "Gordon"
    assert row["FIRSTNAME"] == "Gordon"
    assert row["FirstName"] == "Gordon"

    assert row.matches_won == 153
    assert row.MATCHES_WON == 153
    assert row.MaTchEs_WoN == 153
    assert row["matches_won"] == 153
    assert row["Matches_Won"] == 153
    assert row["MATCHES_WON"] == 153

    with pytest.raises(
        ValueError,
        match="Case insensitive fields is not supported in presence of quoted columns",
    ):
        next(df_quote.to_local_iterator(case_sensitive=False))

    # special character tests
    df_login = session.create_dataframe(
        [["admin", "test"], ["snowman", "test"]], schema=["username", "p@$$w0rD"]
    )
    row = next(df_login.to_local_iterator(case_sensitive=False))

    assert row.username == "admin"
    assert row.UserName == "admin"
    assert row.usErName == "admin"

    assert row["p@$$w0rD"] == "test"
    assert row["p@$$w0rd"] == "test"
    assert row["P@$$W0RD"] == "test"


def test_dropna(session, local_testing_mode):
    Utils.check_answer(
        TestData.double3(session, local_testing_mode).dropna(), [Row(1.0, 1)]
    )

    res = TestData.double3(session, local_testing_mode).dropna(how="all").collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    Utils.check_answer(
        TestData.double3(session, local_testing_mode).dropna(subset=["a"]),
        [Row(1.0, 1), Row(4.0, None)],
    )

    res = TestData.double3(session, local_testing_mode).dropna(thresh=1).collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    with pytest.raises(TypeError) as ex_info:
        TestData.double3(session, local_testing_mode).dropna(subset={1: "a"})
    assert _SUBSET_CHECK_ERROR_MESSAGE in str(ex_info)


@multithreaded_run()
def test_dropna_large_num_of_columns(session):
    n = 1000
    data = [str(i) for i in range(n)]
    none_data = [None for _ in range(n)]
    df = session.create_dataframe([data, none_data], schema=data)
    Utils.check_answer(df.dropna(how="all"), [Row(*data)])


def test_fillna(session, local_testing_mode):
    Utils.check_answer(
        TestData.double3(session, local_testing_mode).fillna(11),
        [
            Row(1.0, 1),
            Row(11.0, 2),
            Row(11.0, 3),
            Row(4.0, 11),
            Row(11.0, 11),
            Row(11.0, 11),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.double3(session, local_testing_mode).fillna(11.0, subset=["a"]),
        [
            Row(1.0, 1),
            Row(11.0, 2),
            Row(11.0, 3),
            Row(4.0, None),
            Row(11.0, None),
            Row(11.0, None),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.double3(session, local_testing_mode).fillna(None),
        [
            Row(1.0, 1),
            Row(None, 2),
            Row(None, 3),
            Row(4.0, None),
            Row(None, None),
            Row(None, None),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.null_data1(session).fillna({}), TestData.null_data1(session).collect()
    )
    Utils.check_answer(
        TestData.null_data1(session).fillna(1, subset=[]),
        TestData.null_data1(session).collect(),
    )

    # fillna for all basic data types
    data = [
        1,
        "one",
        1.0,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        Decimal(0.5),
    ]
    none_data = [None] * len(data)
    none_data[2] = float("nan")
    col_names = [f"col{idx + 1}" for idx in range(len(data))]
    value_dict = {
        col_name: (
            json.dumps(value) if isinstance(value, (list, dict, tuple)) else value
        )
        for col_name, value in zip(col_names, data)
    }
    df = session.create_dataframe([data, none_data], schema=col_names)
    Utils.check_answer(df.fillna(value_dict), [Row(*data), Row(*data)])

    # Python `int` can be filled into FloatType/DoubleType,
    # but Python `float` can't be filled into IntegerType/LongType (will be ignored)
    Utils.check_answer(
        session.create_dataframe(
            [[1, 1.1], [None, None]], schema=["col1", "col2"]
        ).fillna({"col1": 1.1, "col2": 1.1}),
        [Row(1, 1.1), Row(None, 1.1)],
    )

    # negative case
    with pytest.raises(TypeError) as ex_info:
        df.fillna(1, subset={1: "a"})
    assert _SUBSET_CHECK_ERROR_MESSAGE in str(ex_info)

    # Fill Decimal columns with int
    Utils.check_answer(
        TestData.null_data4(session).fillna(123, include_decimal=True),
        [
            Row(decimal.Decimal(1), decimal.Decimal(123)),
            Row(decimal.Decimal(123), 2),
        ],
        sort=False,
    )
    # Fill Decimal columns with float
    Utils.check_answer(
        TestData.null_data4(session).fillna(123.0, include_decimal=True),
        [
            Row(decimal.Decimal(1), decimal.Decimal(123)),
            Row(decimal.Decimal(123), 2),
        ],
        sort=False,
    )
    # Making sure default still reflects old behavior
    Utils.check_answer(
        TestData.null_data4(session).fillna(123),
        [
            Row(decimal.Decimal(1), None),
            Row(None, 2),
        ],
        sort=False,
    )
    Utils.check_answer(
        TestData.null_data4(session).fillna(123.0),
        [
            Row(decimal.Decimal(1), None),
            Row(None, 2),
        ],
        sort=False,
    )


def test_replace_with_coercion(session, local_testing_mode):
    df = session.create_dataframe(
        [[1, 1.0, "1.0"], [2, 2.0, "2.0"]], schema=["a", "b", "c"]
    )

    # empty to_replace or subset will return the original dataframe
    Utils.check_answer(df.replace({}), [Row(1, 1.0, "1.0"), Row(2, 2.0, "2.0")])
    Utils.check_answer(
        df.replace({1: 4}, subset=[]), [Row(1, 1.0, "1.0"), Row(2, 2.0, "2.0")]
    )

    # subset=None will apply the replacement to all columns
    # we can replace a float with an integer
    Utils.check_answer(df.replace(1, 3), [Row(3, 3.0, "1.0"), Row(2, 2.0, "2.0")])
    Utils.check_answer(
        df.replace([1, 2], [3, 4]), [Row(3, 3.0, "1.0"), Row(4, 4.0, "2.0")]
    )
    Utils.check_answer(df.replace([1, 2], 3), [Row(3, 3.0, "1.0"), Row(3, 3.0, "2.0")])
    # value will be ignored
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, value=5), [Row(3, 3.0, "1.0"), Row(4, 4.0, "2.0")]
    )

    # subset
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, subset=["a"]), [Row(3, 1.0, "1.0"), Row(4, 2.0, "2.0")]
    )
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, subset="b"), [Row(1, 3.0, "1.0"), Row(2, 4.0, "2.0")]
    )

    # we can't replace an integer with a float
    # and replace a string with a float (will be skipped)
    Utils.check_answer(
        df.replace({1: 3.0, 2: 4.0, "1.0": 1.0, "2.0": "3.0"}),
        [Row(1, 3.0, "1.0"), Row(2, 4.0, "3.0")],
    )

    # we can replace any value with a None
    Utils.check_answer(
        df.replace({1: None, 2: None, "2.0": None}),
        [Row(None, None, "1.0"), Row(None, None, None)],
    )

    df = session.create_dataframe([[[1, 2], (1, 3)]], schema=["col1", "col2"])
    Utils.check_answer(
        df.replace([(1, 3)], [[2, 3]]),
        [Row("[\n  1,\n  2\n]", "[\n  2,\n  3\n]")],
    )

    df = session.create_dataframe(
        [[[1, 2], (1, 3)], [None, None]], schema=["col1", "col2"]
    )
    Utils.check_answer(
        df.fillna([1, 3]),
        [
            Row("[\n  1,\n  2\n]", "[\n  1,\n  3\n]"),
            Row("[\n  1,\n  3\n]", "[\n  1,\n  3\n]"),
        ],
    )

    # negative case
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.replace({1: 3}, subset=["d"])
    assert "The DataFrame does not contain the column named" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        df.replace({1: 2}, subset={1: "a"})
    assert _SUBSET_CHECK_ERROR_MESSAGE in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace([1], [2, 3])
    assert "to_replace and value lists should be of the same length" in str(ex_info)
    if local_testing_mode:
        # SNOW-1989698: local test gap
        return
    # Replace Decimal value with int
    Utils.check_answer(
        TestData.null_data4(session).replace(
            decimal.Decimal(1), 123, include_decimal=True
        ),
        [
            Row(decimal.Decimal(123), None),
            Row(None, 2),
        ],
        sort=False,
    )
    # Replace Decimal value with float
    Utils.check_answer(
        TestData.null_data4(session).replace(
            decimal.Decimal(1), 123.0, include_decimal=True
        ),
        [
            Row(decimal.Decimal(123.0), None),
            Row(None, 2),
        ],
        sort=False,
    )
    # Make sure old behavior is untouched
    Utils.check_answer(
        TestData.null_data4(session).replace(decimal.Decimal(1), 123),
        [
            Row(decimal.Decimal(1), None),
            Row(None, 2),
        ],
        sort=False,
    )
    Utils.check_answer(
        TestData.null_data4(session).replace(decimal.Decimal(1), 123.0),
        [
            Row(decimal.Decimal(1), None),
            Row(None, 2),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    local_testing_mode,
    reason="Bug in local testing mode, broadcast behavior "
    "not implemented correctly in handle_expression for table_emulator.",
)
def test_replace_with_coercion_II(session):
    # TODO: Once fixed in local testing mode, merge this test case with the ones in
    # test_replace_with_coercion
    df = session.create_dataframe(
        [[1, 1.0, "1.0"], [2, 2.0, "2.0"]], schema=["a", "b", "c"]
    )

    Utils.check_answer(
        df.replace(1.0, None),
        [Row(1, None, "1.0"), Row(2, 2.0, "2.0")],
    )


def test_select_case_expr(session):
    df = session.create_dataframe([1, 2, 3], schema=["a"])
    Utils.check_answer(
        df.select(when(col("a") == 1, 4).otherwise(col("a"))), [Row(4), Row(2), Row(3)]
    )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL expr is not supported in Local Testing",
    raises=NotImplementedError,
)
def test_select_expr(session):
    df = session.create_dataframe([-1, 2, 3], schema=["a"])
    Utils.check_answer(
        df.select_expr("abs(a)", "a + 2", "cast(a as string)"),
        [Row(1, 1, "-1"), Row(2, 4, "2"), Row(3, 5, "3")],
    )
    Utils.check_answer(
        df.select_expr(["abs(a)", "a + 2", "cast(a as string)"]),
        [Row(1, 1, "-1"), Row(2, 4, "2"), Row(3, 5, "3")],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame.describe is not supported in Local Testing",
)
def test_describe(session):
    assert TestData.test_data2(session).describe().columns == [
        "SUMMARY",
        "A",
        "B",
    ]
    Utils.check_answer(
        TestData.test_data2(session).describe("a", "b").collect(),
        [
            Row("count", 6, 6),
            Row("mean", 2.0, 1.5),
            Row("stddev", 0.8944271909999159, 0.5477225575051661),
            Row("min", 1, 1),
            Row("max", 3, 2),
        ],
    )
    Utils.check_answer(
        TestData.test_data3(session).describe().collect(),
        [
            Row("count", 2, 1),
            Row("mean", 1.5, 2.0),
            Row("stddev", 0.7071067811865476, None),
            Row("min", 1, 2),
            Row("max", 2, 2),
        ],
    )

    Utils.check_answer(
        session.create_dataframe(["a", "a", "c", "z", "b", "a"]).describe(),
        [
            Row("count", "6"),
            Row("mean", None),
            Row("stddev", None),
            Row("min", "a"),
            Row("max", "z"),
        ],
    )

    # describe() will ignore all non-numeric and non-string columns
    data = [
        1,
        "one",
        1.0,
        Decimal(0.5),
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
    ]
    assert session.create_dataframe([data]).describe().columns == [
        "SUMMARY",
        "_1",
        "_2",
        "_3",
        "_4",
    ]

    # describe() will still work when there are more than two string columns
    # ambiguity will be eliminated
    Utils.check_answer(
        TestData.string1(session).describe(),
        [
            Row("count", "3", "3"),
            Row("mean", None, None),
            Row("stddev", None, None),
            Row("min", "test1", "a"),
            Row("max", "test3", "c"),
        ],
    )

    # return an "empty" dataframe if no numeric or string column is present
    Utils.check_answer(
        TestData.timestamp1(session).describe(),
        [
            Row("count"),
            Row("mean"),
            Row("stddev"),
            Row("min"),
            Row("max"),
        ],
    )

    mixed_identifiers_dataframe = session.create_dataframe(
        data=[
            [1, Decimal("1.0"), "a", 1, 1, "aa"],
            [2, Decimal("2.0"), "b", None, 2, "bb"],
        ],
        schema=["ほげ", "ふが", "a_ほげ", "ふが_1", "a", "b"],
    )

    assert mixed_identifiers_dataframe.describe().columns == [
        "SUMMARY",
        '"ほげ"',
        '"ふが"',
        '"a_ほげ"',
        '"ふが_1"',
        "A",
        "B",
    ]

    Utils.check_answer(
        mixed_identifiers_dataframe.describe(),
        [
            Row("count", 2.0, 2.0, "2", 1.0, 2.0, "2"),
            Row("mean", 1.5, 1.5, None, 1.0, 1.5, None),
            Row(
                "stddev",
                0.7071067811865476,
                0.7071067811865476,
                None,
                None,
                0.7071067811865476,
                None,
            ),
            Row("min", 1.0, 1.0, "a", 1.0, 1.0, "aa"),
            Row("max", 2.0, 2.0, "b", 1.0, 2.0, "bb"),
        ],
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.test_data2(session).describe("c")
    assert "invalid identifier" in str(ex_info.value)


def test_truncate_preserves_schema(session, local_testing_mode):
    tmp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df1 = session.create_dataframe([(1, 2), (3, 4)], schema=["a", "b"])
    df2 = session.create_dataframe([(1, 2, 3), (4, 5, 6)], schema=["a", "b", "c"])

    df1.write.save_as_table(tmp_table_name, table_type="temp")
    exception_msg = (
        "invalid identifier 'C'"
        if not local_testing_mode
        else "incoming data has different schema"
    )

    # truncate preserves old schema
    with pytest.raises(SnowparkSQLException, match=exception_msg):
        df2.write.save_as_table(tmp_table_name, mode="truncate", table_type="temp")

    # overwrite drops old schema
    df2.write.save_as_table(tmp_table_name, mode="overwrite", table_type="temp")
    Utils.check_answer(session.table(tmp_table_name), [Row(1, 2, 3), Row(4, 5, 6)])


def test_truncate_existing_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    df.write.save_as_table(table_name, mode="overwrite", table_type="temp")
    df = session.create_dataframe([(1, 1), (2, 2), (3, 3)]).toDF("a", "b")
    df.write.save_as_table(table_name, mode="truncate", table_type="temp")
    assert session.table(table_name).count() == 3


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_table_types_in_save_as_table(
    session, save_mode, table_type, local_testing_mode
):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    df.write.save_as_table(table_name, mode=save_mode, table_type=table_type)
    Utils.check_answer(session.table(table_name), df, True)
    if not local_testing_mode:
        Utils.assert_table_type(session, table_name, table_type)


@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_save_as_table_respects_schema(session, save_mode, local_testing_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    schema1 = StructType(
        [
            StructField("A", LongType(), False),
            StructField("B", LongType(), True),
        ]
    )
    schema2 = StructType([StructField("A", LongType(), False)])
    schema3 = StructType(
        [
            StructField("A", LongType(), False),
            StructField("B", LongType(), True),
            StructField("C", LongType(), False),
        ]
    )

    df1 = session.create_dataframe([(1, 2), (3, 4)], schema=schema1)
    df2 = session.create_dataframe([(1), (2)], schema=schema2)
    df3 = session.create_dataframe([(1, 2, 3), (4, 5, 6)], schema=schema3)

    try:
        df1.write.save_as_table(table_name, mode=save_mode)
        saved_df = session.table(table_name)
        Utils.is_schema_same(saved_df.schema, schema1)

        if save_mode == "overwrite":
            df2.write.save_as_table(table_name, mode=save_mode)
            saved_df = session.table(table_name)
            Utils.is_schema_same(saved_df.schema, schema2)
        elif save_mode == "ignore":
            df2.write.save_as_table(table_name, mode=save_mode)
            saved_df = session.table(table_name)
            Utils.is_schema_same(saved_df.schema, schema1)
        elif save_mode == "truncate":
            df2.write.save_as_table(table_name, mode=save_mode)
            saved_df = session.table(table_name)
            Utils.is_schema_same(saved_df.schema, schema1)
            exception_msg = (
                "invalid identifier 'C'"
                if not local_testing_mode
                else "Cannot truncate because incoming data has different schema"
            )
            with pytest.raises(SnowparkSQLException, match=exception_msg):
                df3.write.save_as_table(table_name, mode=save_mode)
        else:  # save_mode in ('append', 'errorifexists')
            with pytest.raises(SnowparkSQLException):
                df2.write.save_as_table(table_name, mode=save_mode)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("large_data", [True, False])
@pytest.mark.parametrize(
    "data_type",
    [
        BinaryType(),
        BooleanType(),
        StringType(),
        TimestampType(),
        TimeType(),
        ByteType(),
        ShortType(),
        IntegerType(),
        LongType(),
        FloatType(),
        DoubleType(),
        DecimalType(),
    ],
)
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_save_as_table_nullable_test(
    session, save_mode, data_type, large_data, local_testing_mode
):
    if isinstance(data_type, DecimalType) and local_testing_mode:
        pytest.skip(
            "SNOW-1447052 local testing nullable information loss in decimal type column because of to_decimal call"
        )
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    schema = StructType(
        [
            StructField("A", data_type, False),
            StructField("B", data_type, True),
        ]
    )

    try:
        with pytest.raises(
            (IntegrityError, SnowparkSQLException),
            match="NULL result in a non-nullable column",
        ):
            df = session.create_dataframe(
                [(None, None)] * (5000 if large_data else 1), schema=schema
            )
            df.write.save_as_table(table_name, mode=save_mode)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_nullable_without_create_temp_table_access(session, save_mode):
    original_run_query = session._run_query

    def mock_run_query(*args, **kwargs):
        if "CREATE SCOPED TEMP TABLE" in args[0]:
            raise ProgrammingError("Cannot create temp table in the schema")
        return original_run_query(*args, **kwargs)

    with mock.patch.object(session, "_run_query") as mocked_run_query:
        mocked_run_query.side_effect = mock_run_query
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        schema = StructType(
            [
                StructField("A", IntegerType(), False),
                StructField("B", IntegerType(), True),
            ]
        )

        try:
            with pytest.raises(
                (IntegrityError, SnowparkSQLException),
                match="NULL result in a non-nullable column",
            ):
                df = session.create_dataframe([(None, None)], schema=schema)
                df.write.save_as_table(table_name, mode=save_mode)
        finally:
            Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table Sproc is not supported in Local Testing",
)
@pytest.mark.udf
@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_save_as_table_with_table_sproc_output(session, save_mode, table_type):
    temp_sp_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        select_sp = session.sproc.register(
            lambda session_: session_.sql("SELECT 1 as A"),
            packages=["snowflake-snowpark-python"],
            name=temp_sp_name,
            return_type=StructType([StructField("A", IntegerType())], structured=False),
            input_types=[],
            replace=True,
        )
        df = select_sp()
        Utils.check_answer(df, [Row(A=1)])
        df.write.save_as_table(table_name, mode=save_mode, table_type=table_type)
        saved_df = session.table(table_name)
        Utils.check_answer(saved_df, [Row(A=1)])
    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_procedure(session, f"{temp_sp_name}()")


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Clustering is a SQL feature",
    run=False,
)
@pytest.mark.parametrize("include_comment", [True, False])
@pytest.mark.parametrize("save_mode", ["append", "overwrite", "truncate"])
def test_write_table_with_clustering_keys_and_comment(
    session, save_mode, include_comment
):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name3 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df1 = session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("c1", DateType()),
                StructField("c2", StringType()),
                StructField("c3", IntegerType()),
            ]
        ),
    )
    df2 = session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("c1", TimestampType()),
                StructField("c2", StringType()),
                StructField("c3", IntegerType()),
            ]
        ),
    )
    df3 = session.create_dataframe(
        [],
        schema=StructType(
            [StructField("t", TimestampType()), StructField("v", VariantType())]
        ),
    )
    comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}" if include_comment else None

    try:
        df1.write.save_as_table(
            table_name1,
            mode=save_mode,
            clustering_keys=["c1", "c2"],
            comment=comment,
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name1}')")[0][0]
        assert 'cluster by ("C1", "C2")' in ddl
        assert not include_comment or comment in ddl

        df2.write.save_as_table(
            table_name2,
            mode=save_mode,
            clustering_keys=[
                col("c1").cast(DateType()),
                col("c2").substring(0, 10),
            ],
            comment=comment,
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name2}')")[0][0]
        assert 'cluster by ( CAST ("C1" AS DATE), substring("C2", 0, 10))' in ddl
        assert not include_comment or comment in ddl

        df3.write.save_as_table(
            table_name3,
            mode=save_mode,
            clustering_keys=[get_path(col("v"), lit("Data.id")).cast(IntegerType())],
            comment=comment,
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name3}')")[0][0]
        assert "cluster by ( CAST (get_path(\"V\", 'Data.id') AS INT))" in ddl
        assert not include_comment or comment in ddl
    finally:
        Utils.drop_table(session, table_name1)
        Utils.drop_table(session, table_name2)
        Utils.drop_table(session, table_name3)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Clustering is a SQL feature",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="show parameters is not supported in SP")
def test_write_table_with_all_options(session):
    try:
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        df = session.create_dataframe(
            [],
            schema=StructType(
                [
                    StructField("c1", DateType()),
                    StructField("c2", StringType()),
                    StructField("c3", IntegerType()),
                ]
            ),
        )
        comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"

        with session.query_history() as history:
            df.write.save_as_table(
                table_name,
                mode="overwrite",
                clustering_keys=["c1", "c2"],
                comment=comment,
                enable_schema_evolution=True,
                data_retention_time=1,
                max_data_extension_time=4,
                change_tracking=True,
                copy_grants=True,
            )
        ddl = session._run_query(f"select get_ddl('table', '{table_name}')")[0][0]
        assert 'cluster by ("C1", "C2")' in ddl
        assert comment in ddl

        # data retention and max data extension time cannot be queried from get_ddl
        # we run a show parameters query to get the values for these parameters
        show_params_sql = f"show parameters like '%TIME_IN_DAYS%' in table {table_name}"
        show_params_result = session._run_query(show_params_sql)
        for row in show_params_result:
            if row[0] == "DATA_RETENTION_TIME_IN_DAYS":
                assert row[1] == "1"
            elif row[0] == "MAX_DATA_EXTENSION_TIME_IN_DAYS":
                assert row[1] == "4"

        for query in history.queries:
            # for the create table query, check we set the following options
            # because it does not show up in ddl, or table parameters
            if f"CREATE  OR  REPLACE    TABLE  {table_name}" in query.sql_text:
                assert "ENABLE_SCHEMA_EVOLUTION  = True" in query.sql_text
                assert "CHANGE_TRACKING  = True" in query.sql_text
                assert "COPY GRANTS" in query.sql_text
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("table_type", ["temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists", "truncate"]
)
def test_write_temp_table_no_breaking_change(
    session, save_mode, table_type, caplog, local_testing_mode
):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    try:
        with caplog.at_level(logging.WARNING):
            df.write.save_as_table(
                table_name,
                mode=save_mode,
                create_temp_table=True,
                table_type=table_type,
            )
        if not IS_IN_STORED_PROC:
            # SNOW-1437979: caplog.text is empty in sp pre-commit env
            assert "create_temp_table is deprecated" in caplog.text
        Utils.check_answer(session.table(table_name), df, True)
        if not local_testing_mode:
            Utils.assert_table_type(session, table_name, "temp")
    finally:
        Utils.drop_table(session, table_name)


def test_write_invalid_table_type(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    with pytest.raises(ValueError, match="Unsupported table type"):
        df.write.save_as_table(table_name, table_type="invalid")


def test_append_existing_table(session, local_testing_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [],
        schema=StructType(
            [StructField("a", IntegerType()), StructField("b", IntegerType())]
        ),
    ).write.save_as_table(table_name, table_type="temporary")
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    try:
        with session.query_history() as history:
            df.write.save_as_table(table_name, mode="append")
        Utils.check_answer(session.table(table_name), df, True)
        if not local_testing_mode:
            assert len(history.queries) == 2  # SHOW + INSERT
            assert history.queries[0].sql_text.startswith("show")
            assert history.queries[1].sql_text.startswith("INSERT")
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Dynamic table is a SQL feature",
    run=False,
)
@pytest.mark.parametrize("is_transient", [True, False])
def test_create_dynamic_table(session, table_name_1, is_transient):
    try:
        df = session.table(table_name_1)
        dt_name = Utils.random_name_for_temp_object(TempObjectType.DYNAMIC_TABLE)
        comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"
        if is_transient:
            # for transient dynamic tables, we cannot set data retention time
            # and max data extension time
            data_retention_time = None
            max_data_extension_time = None
        else:
            data_retention_time = 1
            max_data_extension_time = 4
        df.create_or_replace_dynamic_table(
            dt_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            comment=comment,
            refresh_mode="FULL",
            initialize="ON_SCHEDULE",
            clustering_keys=["num"],
            is_transient=is_transient,
            data_retention_time=data_retention_time,
            max_data_extension_time=max_data_extension_time,
        )
        # scheduled refresh is not deterministic which leads to flakiness that dynamic table is not initialized
        # here we manually refresh the dynamic table
        session.sql(f"alter dynamic table {dt_name} refresh").collect()
        res = session.sql(f"show dynamic tables like '{dt_name}'").collect()
        assert len(res) == 1

        ddl_sql = f"select get_ddl('TABLE', '{dt_name}')"
        ddl_result = session.sql(ddl_sql).collect()[0][0]
        assert comment in ddl_result, ddl_result
        assert "refresh_mode = FULL" in ddl_result, ddl_result
        assert "initialize = ON_SCHEDULE" in ddl_result, ddl_result
        assert 'cluster by ("NUM")' in ddl_result, ddl_result
        if is_transient:
            assert "create or replace transient" in ddl_result, ddl_result
        else:
            if IS_IN_STORED_PROC:
                pytest.skip("show parameters is not supported in SP")
            # data retention and max data extension time cannot be queried from get_ddl
            # we run a show parameters query to get the values for these parameters
            show_params_sql = (
                f"show parameters like '%TIME_IN_DAYS%' in table {dt_name}"
            )
            show_params_result = session.sql(show_params_sql).collect()
            for row in show_params_result:
                if row[0] == "DATA_RETENTION_TIME_IN_DAYS":
                    assert row[1] == "1"
                elif row[0] == "MAX_DATA_EXTENSION_TIME_IN_DAYS":
                    assert row[1] == "4"

    finally:
        Utils.drop_dynamic_table(session, dt_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Dynamic table is a SQL feature",
    run=False,
)
def test_create_dynamic_table_with_explode(session):
    dt_name = Utils.random_name_for_temp_object(TempObjectType.DYNAMIC_TABLE)
    temp_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = session.create_dataframe(
            [[1, [1, 2, 3]], [2, [11, 22]]], schema=["idx", "lists"]
        )
        df.write.mode("overwrite").save_as_table(temp_table)
        df = session.table(temp_table)
        df1 = df.select(df.idx, explode(df.lists))
        df1.create_or_replace_dynamic_table(
            dt_name, warehouse=session.get_current_warehouse(), lag="1 min"
        )
        session.sql(f"alter dynamic table {dt_name} refresh").collect()
        res = session.sql(f"show dynamic tables like '{dt_name}'").collect()
        assert len(res) == 1
    finally:
        Utils.drop_table(session, temp_table)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Dynamic table is a SQL feature",
    run=False,
)
def test_create_dynamic_table_mode(session, table_name_1):
    """We test create dynamic table modes in using the following sequence of
    commands:
    0. Drop dynamic table t1 if it exists
    1. Create a dynamic table t1 with mode "errorifexists" (expect to succeed)
    2. Create dynamic table t1 again with mode "errorifexists" (expect to fail)
    3. Create dynamic table t1 again with mode "overwrite" and comment c1 (expect to succeed)
    4a. Create dynamic table t1 again with mode "ignore" and no comment (expect to succeed)
    4b. Check ddl for table t1 to ensure that the comment is still c1.
    5a. Drop dynamic table t1.
    5b. Create dynamic table t1 with mode "ignore" (expect to succeed)
    5c. Check ddl for table t1 to ensure that comment is empty.

    Args:
        session: _description_
        table_name_1: _description_
    """
    try:
        df = session.table(table_name_1)
        dt_name = Utils.random_name_for_temp_object(TempObjectType.DYNAMIC_TABLE)
        comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"
        ddl_sql = f"select get_ddl('TABLE', '{dt_name}')"

        # step 0
        Utils.drop_dynamic_table(session, dt_name)

        # step 1
        df.create_or_replace_dynamic_table(
            dt_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            mode="errorifexists",
        )

        # step 2
        with pytest.raises(SnowparkSQLException, match=f"'{dt_name}' already exists."):
            df.create_or_replace_dynamic_table(
                dt_name,
                warehouse=session.get_current_warehouse(),
                lag="1000 minutes",
                mode="errorifexists",
            )

        # step 3
        df.create_or_replace_dynamic_table(
            dt_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            mode="overwrite",
            comment=comment,
        )
        assert comment in session.sql(ddl_sql).collect()[0][0]

        # step 4
        df.create_or_replace_dynamic_table(
            dt_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            mode="ignore",
        )
        assert comment in session.sql(ddl_sql).collect()[0][0]

        # step 5
        Utils.drop_dynamic_table(session, dt_name)
        df.create_or_replace_dynamic_table(
            dt_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            mode="ignore",
        )
        assert comment not in session.sql(ddl_sql).collect()[0][0]
    finally:
        Utils.drop_dynamic_table(session, dt_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame.copy_into_location is not supported in Local Testing",
)
def test_write_copy_into_location_basic(session):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    try:
        df = session.create_dataframe(
            [["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df.write.copy_into_location(temp_stage)
        copied_files = session.sql(f"list @{temp_stage}").collect()
        assert len(copied_files) == 1
        assert ".csv" in copied_files[0][0]
    finally:
        Utils.drop_stage(session, temp_stage)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame.copy_into_location is not supported in Local Testing",
)
@pytest.mark.parametrize(
    "partition_by",
    [
        col("last_name"),
        "last_name",
        concat(col("last_name"), lit("s")),
        "last_name || 's'",
    ],
)
def test_write_copy_into_location_csv(session, partition_by):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    try:
        df = session.create_dataframe(
            [["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df.write.copy_into_location(
            temp_stage,
            partition_by=partition_by,
            file_format_type="csv",
            format_type_options={"COMPRESSION": "GZIP"},
            header=True,
            overwrite=False,
        )
        copied_files = session.sql(f"list @{temp_stage}").collect()
        assert len(copied_files) == 2
        assert ".csv.gz" in copied_files[0][0]
        assert ".csv.gz" in copied_files[1][0]
    finally:
        Utils.drop_stage(session, temp_stage)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL generation",
    run=False,
)
def test_queries(session):
    df = TestData.column_has_special_char(session)
    queries = df.queries
    assert len(queries["queries"]) == 1
    assert len(queries["post_actions"]) == 0
    assert df._plan.queries[0].sql.strip() in queries["queries"]

    # multiple queries and
    df = session.create_dataframe([1] * 20000)
    queries, post_actions = df.queries["queries"], df.queries["post_actions"]
    assert len(queries) == 3
    assert queries[0].startswith("CREATE")
    assert queries[1].startswith("INSERT")
    assert queries[2].startswith("SELECT")
    assert len(post_actions) == 1
    assert post_actions[0].startswith("DROP")


def test_df_columns(session):
    assert session.create_dataframe([1], schema=["a"]).columns == ["A"]

    temp_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [[1, 2, 3, 4]], schema=['"a b"', '"a""b"', '"a"', "a"]
    ).write.save_as_table(temp_table, table_type="temporary")
    try:
        df = session.table(temp_table)
        assert df.columns == ['"a b"', '"a""b"', '"a"', "A"]
        assert df.select(df.a).collect()[0][0] == 4
        assert df.select(df.A).collect()[0][0] == 4
        assert df.select(df["a"]).collect()[0][0] == 4
        assert (
            df.select(df["A"]).collect()[0][0] == 4
        )  # Snowflake finds column a without quotes.
        assert df.select(df['"a b"']).collect()[0][0] == 1
        assert df.select(df['"a""b"']).collect()[0][0] == 2
        assert df.select(df['"a"']).collect()[0][0] == 3
        assert df.select(df['"A"']).collect()[0][0] == 4

        with pytest.raises(SnowparkColumnException) as sce:
            df.select(df['"A B"']).collect()
        assert (
            sce.value.message
            == 'The DataFrame does not contain the column named "A B".'
        )
    finally:
        Utils.drop_table(session, temp_table)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame.pivot is not supported in Local Testing",
)
@pytest.mark.parametrize(
    "column_list",
    [["jan", "feb", "mar", "apr"], [col("jan"), col("feb"), col("mar"), col("apr")]],
)
@pytest.mark.parametrize("include_nulls", [True, False])
def test_unpivot(session, column_list, include_nulls):
    df = (
        TestData.monthly_sales_flat(session)
        .unpivot("sales", "month", column_list, include_nulls)
        .sort("empid")
    )
    expected_rows = [
        Row(1, "electronics", "JAN", 100),
        Row(1, "electronics", "FEB", 200),
        Row(1, "electronics", "MAR", 300),
        Row(1, "electronics", "APR", 100),
        Row(2, "clothes", "JAN", 100),
        Row(2, "clothes", "FEB", 300),
        Row(2, "clothes", "MAR", 150),
        Row(2, "clothes", "APR", 200),
        Row(3, "cars", "JAN", 200),
        Row(3, "cars", "FEB", 400),
        Row(3, "cars", "MAR", 100),
        Row(3, "cars", "APR", 50),
    ]
    if include_nulls:
        expected_rows.extend(
            [
                Row(4, "appliances", "JAN", 100),
                Row(4, "appliances", "FEB", None),
                Row(4, "appliances", "MAR", 100),
                Row(4, "appliances", "APR", 50),
            ]
        )
    else:
        expected_rows.extend(
            [
                Row(4, "appliances", "JAN", 100),
                Row(4, "appliances", "MAR", 100),
                Row(4, "appliances", "APR", 50),
            ]
        )
    Utils.check_answer(
        df,
        expected_rows,
    )


def test_create_dataframe_string_length(session, local_testing_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe(["ab", "abc", "abcd"], schema=["a"])
    df.write.mode("overwrite").save_as_table(table_name, table_type="temporary")
    if not local_testing_mode:
        datatype = json.loads(
            session.sql(f"show columns in {table_name}").collect()[0]["data_type"]
        )
        assert datatype["type"] == "TEXT"
        assert datatype["length"] == session._conn.max_string_size
    else:
        datatype = df.schema[0].datatype
        assert isinstance(datatype, StringType)
        assert datatype.length == 2**20 * 16  # max length (16 MB)

    session.create_dataframe(["abcde"]).write.save_as_table(table_name, mode="append")
    Utils.check_answer(
        session.table(table_name), [Row("ab"), Row("abc"), Row("abcd"), Row("abcde")]
    )


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
def test_create_table_twice_no_error(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    # 1) large local data in create_dataframe
    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df1 = session.create_dataframe([[1, 2], [1, 3], [4, 4]], schema=["a", "b"])
        df2, df3 = df1.select("a"), df1.select("b")
        Utils.check_answer(df2.join(df3, df2.a == df3.b), [Row(4, 4)])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value

    # 2) read file
    df1 = (
        session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    )
    df2, df3 = df1.select("a"), df1.select("c")
    Utils.check_answer(
        df2.join(df3),
        [Row(A=1, C=1.2), Row(A=1, C=2.2), Row(A=2, C=1.2), Row(A=2, C=2.2)],
    )


def check_df_with_query_id_result_scan(session, df):
    query_id = df._execute_and_get_query_id()
    df_from_result_scan = session.sql(result_scan_statement(query_id))
    assert df.columns == df_from_result_scan.columns
    Utils.check_answer(df, df_from_result_scan)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Result scan is a SQL feature",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
def test_query_id_result_scan(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    # create dataframe (small data)
    df = session.create_dataframe([[1, 2], [1, 3], [4, 4]], schema=["a", "b"])
    check_df_with_query_id_result_scan(session, df)

    # create dataframe (large data)
    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df = session.create_dataframe([[1, 2], [1, 3], [4, 4]], schema=["a", "b"])
        check_df_with_query_id_result_scan(session, df)
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value

    # read file
    df = session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    check_df_with_query_id_result_scan(session, df)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="statement parameters are not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_call_with_statement_params(session):
    statement_params_wrong_date_format = {
        "DATE_INPUT_FORMAT": "YYYY-MM-DD",
        "SF_PARTNER": "FAKE_PARTNER",
    }
    statement_params_correct_date_format = {
        "DATE_INPUT_FORMAT": "MM-DD-YYYY",
        "SF_PARTNER": "FAKE_PARTNER",
    }

    # Note: When testing, pass statement_params as copy().
    # Some operations may define the reference which leads to test failure in AST mode.

    schema = StructType([StructField("A", DateType())])
    df = session.create_dataframe(["01-01-1970", "12-31-2000"], schema=schema)
    pandas_df = PandasDF(
        [
            datetime.date(1970, 1, 1),
            datetime.date(2000, 12, 31),
        ],
        columns=["A"],
    )
    expected_rows = [Row(datetime.date(1970, 1, 1)), Row(datetime.date(2000, 12, 31))]

    # collect
    with pytest.raises(SnowparkSQLException) as exc:
        df.collect(statement_params=statement_params_wrong_date_format.copy())
    assert "is not recognized" in str(exc.value)
    assert (
        df.collect(statement_params=statement_params_correct_date_format)
        == expected_rows
    )

    # to_local_iterator
    with pytest.raises(SnowparkSQLException) as exc:
        list(
            df.to_local_iterator(
                statement_params=statement_params_wrong_date_format.copy()
            )
        )
    assert "is not recognized" in str(exc.value)
    assert (
        list(
            df.to_local_iterator(
                statement_params=statement_params_correct_date_format.copy()
            )
        )
        == expected_rows
    )

    # to_pandas
    with pytest.raises(SnowparkSQLException) as exc:
        df.to_pandas(statement_params=statement_params_wrong_date_format.copy())
    assert "is not recognized" in str(exc.value)
    assert_frame_equal(
        df.to_pandas(statement_params=statement_params_correct_date_format.copy()),
        pandas_df,
        check_dtype=False,
    )

    # to_pandas_batches
    with pytest.raises(SnowparkSQLException) as exc:
        pd.concat(
            list(
                df.to_pandas_batches(
                    statement_params=statement_params_wrong_date_format.copy()
                )
            ),
            ignore_index=True,
        )
    assert "is not recognized" in str(exc.value)
    assert_frame_equal(
        pd.concat(
            list(
                df.to_pandas_batches(
                    statement_params=statement_params_correct_date_format.copy()
                )
            ),
            ignore_index=True,
        ),
        pandas_df,
    )

    # count
    # passing statement_params_wrong_date_format does not trigger error
    assert df.count(statement_params=statement_params_correct_date_format.copy()) == 2

    # copy_into_table test is covered in test_datafrom_copy_into as it requires complex config

    # show
    with pytest.raises(SnowparkSQLException) as exc:
        df.show(statement_params=statement_params_wrong_date_format.copy())
    assert "is not recognized" in str(exc.value)
    df.show(statement_params=statement_params_correct_date_format.copy())

    # create_or_replace_view
    # passing statement_params_wrong_date_format does not trigger error
    assert (
        "successfully created"
        in df.create_or_replace_view(
            Utils.random_view_name(),
            statement_params=statement_params_correct_date_format.copy(),
        )[0]["status"]
    )

    # create_or_replace_temp_view
    # passing statement_params_wrong_date_format does not trigger error
    assert (
        "successfully created"
        in df.create_or_replace_temp_view(
            Utils.random_view_name(),
            statement_params=statement_params_correct_date_format.copy(),
        )[0]["status"]
    )

    # first
    with pytest.raises(SnowparkSQLException) as exc:
        df.first(statement_params=statement_params_wrong_date_format.copy())
    assert "is not recognized" in str(exc.value)

    assert (
        df.first(statement_params=statement_params_correct_date_format.copy())
        == expected_rows[0]
    )

    # cache_result
    with pytest.raises(SnowparkSQLException) as exc:
        df.cache_result(
            statement_params=statement_params_wrong_date_format.copy()
        ).collect()
    assert "is not recognized" in str(exc.value)

    assert (
        df.cache_result(
            statement_params=statement_params_correct_date_format.copy()
        ).collect()
        == expected_rows
    )

    # random_split
    with pytest.raises(SnowparkSQLException) as exc:
        df.random_split(
            weights=[0.5, 0.5],
            statement_params=statement_params_wrong_date_format.copy(),
        )
    assert "is not recognized" in str(exc.value)
    assert (
        len(
            df.random_split(
                weights=[0.5, 0.5],
                statement_params=statement_params_correct_date_format.copy(),
            )
        )
        == 2
    )

    # save_as_table
    # passing statement_params_wrong_date_format does not trigger error
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe(["01-01-1970", "12-31-2000"]).toDF("a")
    try:
        df.write.save_as_table(
            table_name,
            mode="append",
            table_type="temporary",
            statement_params=statement_params_correct_date_format.copy(),
        )
        Utils.check_answer(session.table(table_name), df, True)
        table_info = session.sql(f"show tables like '{table_name}'").collect()
        assert table_info[0]["kind"] == "TEMPORARY"
    finally:
        Utils.drop_table(session, table_name)

    # copy_into_location
    # passing statement_params_wrong_date_format does not trigger error
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    df = session.create_dataframe(["01-01-1970", "12-31-2000"]).toDF("a")
    df.write.copy_into_location(
        temp_stage, statement_params=statement_params_correct_date_format.copy()
    )
    copied_files = session.sql(f"list @{temp_stage}").collect()
    assert len(copied_files) == 1
    assert ".csv" in copied_files[0][0]
    Utils.drop_stage(session, temp_stage)


def test_limit_offset(session):
    df = session.create_dataframe([[1, 2, 3], [4, 5, 6]], schema=["a", "b", "c"])
    assert df.limit(1).collect() == [Row(A=1, B=2, C=3)]
    assert df.limit(1, offset=1).collect() == [Row(A=4, B=5, C=6)]


def test_df_join_how_on_overwrite(session):
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(
        ["int", "int2", "str"]
    )
    df2 = session.create_dataframe([[1, 1, "1"], [2, 3, "5"]]).to_df(
        ["int", "int2", "str"]
    )
    # using_columns will overwrite on, and join_type will overwrite how
    df = df1.join(df2, on="int", using_columns="int2", how="outer", join_type="inner")
    Utils.check_answer(df, [Row(1, 1, "1", 1, "1")])

    df = df1.natural_join(df2, how="left", join_type="right")
    Utils.check_answer(df, [Row(1, 1, "1"), Row(2, 3, "5")])


def test_create_dataframe_special_char_column_name(session):
    df1 = session.create_dataframe(
        [[1, 2, 3], [1, 2, 3]], schema=["a b", '"abc"', "@%!^@&#"]
    )
    expected_columns = ['"a b"', '"abc"', '"@%!^@&#"']
    assert df1.columns == expected_columns
    Utils.check_answer(df1, [Row(1, 2, 3), Row(1, 2, 3)])

    df2 = session.create_dataframe([[1, 2, 3], [1, 2, 3]], schema=expected_columns)
    assert df2.columns == expected_columns
    Utils.check_answer(df2, [Row(1, 2, 3), Row(1, 2, 3)])


def test_create_dataframe_with_tuple_schema(session):
    df = session.create_dataframe(
        [(20000101, 1, "x"), (20000101, 2, "y")], schema=("TIME", "ID", "V2")
    )
    Utils.check_answer(df, [Row(20000101, 1, "x"), Row(20000101, 2, "y")])


def test_df_join_suffix(session):
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(["a", "b", "c"])
    df2 = session.create_dataframe([[1, 1, "1"], [2, 3, "5"]]).to_df(["a", "b", "c"])
    df3 = df1.join(
        df2, (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]), lsuffix="_l", rsuffix="_r"
    )
    assert df3.columns == ["A_L", "B_L", "C_L", "A_R", "B_R", "C_R"]
    Utils.check_answer(df3, Row(A_L=1, B_L=1, C_L="1", A_R=1, B_R=1, C_R="1"))

    df4 = df1.join(df2, (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]), lsuffix="_l")
    assert df4.columns == ["A_L", "B_L", "C_L", "A", "B", "C"]
    Utils.check_answer(df4, Row(A_L=1, B_L=1, C_L="1", A=1, B=1, C="1"))

    df5 = df1.join(df2, (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]), rsuffix="_r")
    assert df5.columns == ["A", "B", "C", "A_R", "B_R", "C_R"]
    Utils.check_answer(df3, Row(A=1, B=1, C="1", A_R=1, B_R=1, C_R="1"))

    df6 = df1.join(df2, (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]))
    for i in range(0, 3):
        assert df6.columns[i].startswith('"l_')
    for j in range(3, 6):
        assert df6.columns[j].startswith('"r_')
    Utils.check_answer(df3, Row(A=1, B=1, C="1", A_R=1, B_R=1, C_R="1"))

    df7 = df1.join(
        df2,
        (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]),
        lsuffix='"_l"',
        rsuffix='"_r"',
    )
    assert df7.columns == ['"A_l"', '"B_l"', '"C_l"', '"A_r"', '"B_r"', '"C_r"']

    df8 = df1.join(
        df2,
        (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]),
        lsuffix='"_L"',
        rsuffix='"_R"',
    )
    assert df8.columns == ["A_L", "B_L", "C_L", "A_R", "B_R", "C_R"]

    df9 = df1.join(
        df2, (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]), lsuffix="8l", rsuffix="9r"
    )
    assert df9.columns == ["A8L", "B8L", "C8L", "A9R", "B9R", "C9R"]

    df10 = df1.join(
        df2,
        (df1["a"] == df2["a"]) & (df1["b"] == df2["b"]),
        lsuffix="8 l",
        rsuffix="9 r",
    )
    assert df10.columns == ['"A8 l"', '"B8 l"', '"C8 l"', '"A9 r"', '"B9 r"', '"C9 r"']

    df11 = session.create_dataframe([[1]]).to_df(['"a"'])
    df12 = session.create_dataframe([[1]]).to_df(['"a"'])
    df13 = df11.join(df12, df11['"a"'] == df12['"a"'], lsuffix="_l", rsuffix="_r")
    assert df13.columns == ['"a_l"', '"a_r"']

    df14 = df11.join(df12, df11['"a"'] == df12['"a"'], lsuffix='"_l"', rsuffix='"_r"')
    assert df14.columns == ['"a_l"', '"a_r"']


def test_df_cross_join_suffix(session):
    df1 = session.create_dataframe([[1, 1, "1"]]).to_df(["a", "b", "c"])
    df2 = session.create_dataframe([[1, 1, "1"]]).to_df(["a", "b", "c"])
    df3 = df1.cross_join(df2, lsuffix="_l", rsuffix="_r")
    assert df3.columns == ["A_L", "B_L", "C_L", "A_R", "B_R", "C_R"]
    Utils.check_answer(df3, Row(A_L=1, B_L=1, C_L="1", A_R=1, B_R=1, C_R="1"))

    df4 = df1.cross_join(df2, lsuffix="_l")
    assert df4.columns == ["A_L", "B_L", "C_L", "A", "B", "C"]
    Utils.check_answer(df4, Row(A_L=1, B_L=1, C_L="1", A=1, B=1, C="1"))

    df5 = df1.cross_join(df2, rsuffix="_r")
    assert df5.columns == ["A", "B", "C", "A_R", "B_R", "C_R"]
    Utils.check_answer(df3, Row(A=1, B=1, C="1", A_R=1, B_R=1, C_R="1"))

    df6 = df1.cross_join(df2)
    for i in range(0, 3):
        assert df6.columns[i].startswith('"l_')
    for j in range(3, 6):
        assert df6.columns[j].startswith('"r_')
    Utils.check_answer(df3, Row(A=1, B=1, C="1", A_R=1, B_R=1, C_R="1"))

    df7 = df1.cross_join(df2, lsuffix='"_l"', rsuffix='"_r"')
    assert df7.columns == ['"A_l"', '"B_l"', '"C_l"', '"A_r"', '"B_r"', '"C_r"']

    df8 = df1.cross_join(df2, lsuffix='"_L"', rsuffix='"_R"')
    assert df8.columns == ["A_L", "B_L", "C_L", "A_R", "B_R", "C_R"]

    df9 = df1.cross_join(df2, lsuffix="8l", rsuffix="9r")
    assert df9.columns == ["A8L", "B8L", "C8L", "A9R", "B9R", "C9R"]

    df10 = df1.cross_join(df2, lsuffix="8 l", rsuffix="9 r")
    assert df10.columns == ['"A8 l"', '"B8 l"', '"C8 l"', '"A9 r"', '"B9 r"', '"C9 r"']

    df11 = session.create_dataframe([[1]]).to_df(['"a"'])
    df12 = session.create_dataframe([[1]]).to_df(['"a"'])
    df13 = df11.cross_join(df12, lsuffix="_l", rsuffix="_r")
    assert df13.columns == ['"a_l"', '"a_r"']

    df14 = df11.cross_join(df12, lsuffix='"_l"', rsuffix='"_r"')
    assert df14.columns == ['"a_l"', '"a_r"']


def test_suffix_negative(session):
    df1 = session.create_dataframe([[1, 1, "1"]]).to_df(["a", "b", "c"])
    df2 = session.create_dataframe([[1, 1, "1"]]).to_df(["a", "b", "c"])
    with pytest.raises(
        ValueError,
        match="'lsuffix' and 'rsuffix' must be different if they're not empty. You set 'suffix' to both.",
    ):
        df1.cross_join(df2, lsuffix="suffix", rsuffix="suffix")
    with pytest.raises(
        ValueError,
        match="'lsuffix' and 'rsuffix' must be different if they're not empty. You set 'suffix' to both.",
    ):
        df1.join(df2, lsuffix="suffix", rsuffix="suffix")


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL generation",
    run=False,
)
def test_create_or_replace_view_with_multiple_queries(session):
    df = session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    with pytest.raises(
        SnowparkCreateViewException,
        match="Your dataframe may include DDL or DML operations",
    ):
        df.create_or_replace_view("temp")


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL generation",
    run=False,
)
def test_create_or_replace_dynamic_table_with_multiple_queries(session):
    df = session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    with pytest.raises(
        SnowparkCreateDynamicTableException,
        match="Your dataframe may include DDL or DML operations",
    ):
        df.create_or_replace_dynamic_table(
            "temp", warehouse="warehouse", lag="1000 minute"
        )


def test_nested_joins(session):
    df1 = session.create_dataframe([[1, 2], [4, 5]], schema=["a", "b"])
    df2 = session.create_dataframe([[1, 3], [4, 6]], schema=["c", "d"])
    df3 = session.create_dataframe([[1, 4], [4, 7]], schema=["e", "f"])
    res1 = sorted(
        df1.join(df2)
        .join(df3)
        .sort("a", "b", "c", "d", "e", "f")
        .select("a", "b", "c", "d", "e", "f")
        .collect(),
        key=lambda r: r[0],
    )
    res2 = sorted(
        df2.join(df3)
        .join(df1)
        .sort("a", "b", "c", "d", "e", "f")
        .select("a", "b", "c", "d", "e", "f")
        .collect(),
        key=lambda r: r[0],
    )
    res3 = sorted(
        df3.join(df1)
        .join(df2)
        .sort("a", "b", "c", "d", "e", "f")
        .select("a", "b", "c", "d", "e", "f")
        .collect(),
        key=lambda r: r[0],
    )
    assert res1 == res2 == res3


def test_dataframe_copy(session):
    df = session.create_dataframe([[1]], schema=["a"])
    df = df.select((col("a") + 1).as_("b"))
    df = df.select((col("b") + 1).as_("c"))
    df = df.select(col("c"))

    df_c = copy.copy(df)
    df1 = df_c.select(df_c["c"].as_("c1"))
    df2 = df_c.select(col("c").as_("c2"))
    Utils.check_answer(df1, Row(3))
    assert df1.columns == ["C1"]
    Utils.check_answer(df2, Row(3))
    assert df2.columns == ["C2"]


def test_to_df_then_copy(session):
    data = [
        ["2023-01-01", 101, 200],
        ["2023-01-02", 101, 100],
        ["2023-01-03", 101, 300],
        ["2023-01-04", 102, 250],
    ]
    df = session.create_dataframe(data).to_df("ORDERDATE", "PRODUCTKEY", "SALESAMOUNT")
    df_copy = copy.copy(df)
    df1 = df_copy.select("ORDERDATE").filter(col("ORDERDATE") == "2023-01-01")
    Utils.check_answer(df1, Row("2023-01-01"))


def test_to_df_then_alias_and_join(session):
    data = [
        ["2023-01-01", 101, 200],
        ["2023-01-02", 101, 100],
        ["2023-01-03", 101, 300],
        ["2023-01-04", 102, 250],
    ]
    df = session.create_dataframe(data).to_df("ORDERDATE", "PRODUCTKEY", "SALESAMOUNT")

    left_df = df.alias("left")
    right_df = df.alias("right")
    result_df = left_df.join(
        right_df, on="PRODUCTKEY", how="leftouter", lsuffix="A", rsuffix="B"
    )
    Utils.check_answer(
        result_df,
        [
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-01",
                SALESAMOUNTA=200,
                ORDERDATEB="2023-01-01",
                SALESAMOUNTB=200,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-02",
                SALESAMOUNTA=100,
                ORDERDATEB="2023-01-01",
                SALESAMOUNTB=200,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-03",
                SALESAMOUNTA=300,
                ORDERDATEB="2023-01-01",
                SALESAMOUNTB=200,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-01",
                SALESAMOUNTA=200,
                ORDERDATEB="2023-01-02",
                SALESAMOUNTB=100,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-02",
                SALESAMOUNTA=100,
                ORDERDATEB="2023-01-02",
                SALESAMOUNTB=100,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-03",
                SALESAMOUNTA=300,
                ORDERDATEB="2023-01-02",
                SALESAMOUNTB=100,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-01",
                SALESAMOUNTA=200,
                ORDERDATEB="2023-01-03",
                SALESAMOUNTB=300,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-02",
                SALESAMOUNTA=100,
                ORDERDATEB="2023-01-03",
                SALESAMOUNTB=300,
            ),
            Row(
                PRODUCTKEY=101,
                ORDERDATEA="2023-01-03",
                SALESAMOUNTA=300,
                ORDERDATEB="2023-01-03",
                SALESAMOUNTB=300,
            ),
            Row(
                PRODUCTKEY=102,
                ORDERDATEA="2023-01-04",
                SALESAMOUNTA=250,
                ORDERDATEB="2023-01-04",
                SALESAMOUNTB=250,
            ),
        ],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame alias is not supported in Local Testing",
)
def test_dataframe_alias(session):
    """Test `dataframe.alias`"""
    df1 = session.create_dataframe([[1, 6], [3, 8], [7, 7]], schema=["col1", "col2"])
    df2 = session.create_dataframe([[1, 2], [3, 4], [5, 5]], schema=["col1", "col2"])

    # Test select aliased df's columns
    Utils.check_answer(
        df1.alias("A").select(col("A", "col1"), column("A", "col2")), df1.select("*")
    )

    Utils.check_answer(df1.alias("A").select(col("A", "*")), df1.select("*"))

    # Test join with one aliased datafeame
    Utils.check_answer(
        df1.alias("L").join(df2, col("L", "col1") == col("col1")),
        df1.join(df2, df1["col1"] == df2["col1"]),
    )

    # Test join with two aliased dataframes
    Utils.check_answer(
        df1.alias("L")
        .join(df2.alias("R"), col("L", "col1") == col("R", "col1"))
        .select(col("L", "col1"), col("R", "col2")),
        df1.join(df2, df1["col1"] == df2["col1"]).select(df1["col1"], df2["col2"]),
    )

    Utils.check_answer(
        df1.alias("L")
        .join(df2.alias("R"), col("L", "col1") == col("R", "col1"))
        .select(col("L", "*")),
        df1.join(df2, df1["col1"] == df2["col1"]).select(df1["*"]),
    )

    # Test self join with aliased dataframe
    df1_copy = copy.copy(df1)
    Utils.check_answer(
        df1.alias("L")
        .join(df1.alias("R"), on="col1")
        .select(col("L", "col1"), col("R", "col2")),
        df1.join(df1_copy, on="col1").select(df1["col1"], df1_copy["col2"]),
    )

    # Test dropping columns from aliased dataframe
    Utils.check_answer(df1.alias("df1").drop(col("df1", "col1")), df1.select("col2"))
    Utils.check_answer(df2.alias("df2").drop(col("df2", "col2")), df2.select("col1"))

    # Test renaming columns from aliased dataframe
    Utils.check_answer(
        df1.alias("df1").with_column_renamed(col("df1", "col1"), "col3"),
        df1.with_column_renamed("col1", "col3"),
    )

    # Test renaming columns (using rename function) from aliased dataframe
    Utils.check_answer(
        df1.alias("df1").rename(col("df1", "col1"), "col3"),
        df1.rename("col1", "col3"),
    )

    # Test alias, join, with_column
    Utils.check_answer(
        df1.alias("L")
        .join(df2.alias("R"), col("L", "col1") == col("R", "col1"))
        .with_columns(
            ["L_mean", "R_sum"],
            [
                (col("L", "col1") + col("L", "col2")) / 2,
                col("R", "col1") + col("R", "col2"),
            ],
        ),
        df1.join(df2, df1["col1"] == df2["col1"]).with_columns(
            ["L_mean", "R_sum"],
            [(df1["col1"] + df1["col2"]) / 2, df2["col1"] + df2["col2"]],
        ),
    )

    df3 = session.create_dataframe([[1, 2], [3, 4], [5, 5]], schema=["col1", "col4"])

    # Test nested alias, join
    Utils.check_answer(
        df1.alias("df1")
        .join(df3.alias("df3"), col("df1", "col1") == col("df3", "col1"))
        .alias("intermediate")
        .join(df2.alias("df2"), col("df2", "col1") == col("df3", "col1"))
        .select(col("intermediate", "*"), col("df2", "col1")),
        df1.join(df3, df1.col1 == df3.col1)
        .join(df2, df2.col1 == df3.col1)
        .select(df1["*"], df3["*"], df2["col1"]),
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="DataFrame alias is not supported in Local Testing",
)
def test_dataframe_alias_negative(session):
    df = session.sql("select 1 as a")
    with pytest.raises(SnowparkDataframeException):
        df.alias("df").select(col("non_existent", "a"))

    with pytest.raises(SnowparkDataframeException):
        df.alias("b c.d").select(col("d", "a"))

    with pytest.raises(SnowparkDataframeException):
        df.alias("df").select(col("non_existent", "*"))

    with pytest.raises(ValueError):
        col("df", df["a"])


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot change schema in SP")
def test_dataframe_result_cache_changing_schema(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]).to_df(
        ["a", "b"]
    )
    old_cached_df = df.cache_result()
    session.use_schema("public")  # schema change
    old_cached_df.show()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="functions.seq2 is not supported in Local Testing",
)
@pytest.mark.parametrize("select_again", [True, False])
def test_dataframe_data_generator(session, select_again):
    df1 = session.create_dataframe([1, 2, 3], schema=["a"])
    df2 = df1.with_column("b", seq1())
    if select_again:
        df2 = df2.select("a", "b")
    df2 = df2.sort(col("a").desc())
    Utils.check_answer(df2, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df3 = df1.with_column("b", seq2())
    if select_again:
        df3 = df3.select("a", "b")
    df3 = df3.sort(col("a").desc())
    Utils.check_answer(df3, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df4 = df1.with_column("b", seq4())
    df4 = df4.select("a", "b")
    df4 = df4.sort(col("a").desc())
    Utils.check_answer(df4, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df5 = df1.with_column("b", seq8())
    if select_again:
        df5 = df5.select("a", "b")
    df5 = df5.sort(col("a").desc())
    Utils.check_answer(df5, [Row(3, 2), Row(2, 1), Row(1, 0)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Rank is not supported in Local Testing",
)
@pytest.mark.parametrize("select_again", [True, False])
def test_dataframe_select_window(session, select_again):
    df1 = session.create_dataframe([1, 2, 3], schema=["a"])
    df2 = df1.select("a", rank().over(Window.order_by(col("a").desc())).alias("b"))
    if select_again:
        df2 = df2.select("a", "b")
    df2 = df2.sort(col("a").desc())
    Utils.check_answer(df2, [Row(3, 1), Row(2, 2), Row(1, 3)])


def test_select_alias_select_star(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_star = df.select(df["a"].alias("a1"), df["b"].alias("b1")).select("*")
    df2 = df_star.select(df["a"], df["b"])
    Utils.check_answer(df2, [Row(1, 2)])
    assert df2.columns == ["A1", "B1"]


def test_select_star_select_alias(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_star = df.select("*").select(df["a"].alias("a1"), df["b"].alias("b1"))
    df2 = df_star.select(df["a"], df["b"])
    Utils.check_answer(df2, [Row(1, 2)])
    assert df2.columns == ["A1", "B1"]


def test_select_star_select_columns(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_star = df.select("*")
    df2 = df_star.select("a", "b")
    Utils.check_answer(df2, [Row(1, 2)])
    df3 = df2.select("a", "b")
    Utils.check_answer(df3, [Row(1, 2)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1373887 Basic diamond shaped joins are not supported",
)
def test_select_star_join(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_star = df.select("*")
    df_joined = df.join(df_star, df["a"] == df_star["a"])
    Utils.check_answer(df_joined, [Row(1, 2, 1, 2)])


def test_select_star_and_more_columns(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_star = df.select("*", (col("a") + col("b")).as_("c"))
    df2 = df_star.select("a", "b", "c")
    Utils.check_answer(df2, [Row(1, 2, 3)])
    df3 = df2.select("a", "b", "c")
    Utils.check_answer(df3, [Row(1, 2, 3)])


def test_drop_columns_special_names(session):
    """Test whether columns with newlines can be dropped."""
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [["a", 1], ["b", 2]], schema=['"a\nb"', "id"]
    ).write.save_as_table(table_name, table_type="temp")
    df = session.table(table_name)
    try:
        Utils.check_answer(df, [Row("a", 1), Row("b", 2)])
        df2 = df.drop('"a\nb"')
        Utils.check_answer(df2, [Row(1), Row(2)])
    finally:
        Utils.drop_table(session, table_name)


def test_dataframe_interval_operation(session):
    df = session.create_dataframe(
        [
            [datetime.datetime(2010, 1, 1)],
            [datetime.datetime(2012, 1, 1)],
        ],
        schema=["a"],
    )
    interval = make_interval(
        years=1,
        quarters=1,
        months=1,
        weeks=2,
        days=2,
        hours=2,
        minutes=3,
        seconds=3,
        milliseconds=3,
        microseconds=4,
        nanoseconds=4,
    )
    Utils.check_answer(
        df.select(df.a + interval),
        [
            Row(
                datetime.datetime(2011, 5, 17, 2, 3, 3, 3004),
            ),
            Row(
                datetime.datetime(2013, 5, 17, 2, 3, 3, 3004),
            ),
        ],
    )
    interval = make_interval(
        years=1,
        quarters=1,
        months=1,
        weeks=2,
        days=2,
        hours=2,
        minutes=3,
        seconds=3,
        milliseconds=3,
    )
    Utils.check_answer(
        df.select(df.a - interval),
        [
            Row(datetime.datetime(2008, 8, 15, 21, 56, 56, 997000)),
            Row(datetime.datetime(2010, 8, 15, 21, 56, 56, 997000)),
        ],
    )


def test_dataframe_to_local_iterator_isolation(session):
    ROW_NUMBER = 10
    df = session.create_dataframe(
        [[1, 2, 3] for _ in range(ROW_NUMBER)], schema=["a", "b", "c"]
    )
    my_iter = df.to_local_iterator()
    row_counter = 0
    for _ in my_iter:
        len(df.schema.fields)  # this executes a schema query internally
        row_counter += 1

    # my_iter should be iterating on df.collect()'s query's results, not the schema query (1 row)
    assert (
        row_counter == ROW_NUMBER
    ), f"Expect {ROW_NUMBER} rows, Got {row_counter} instead"


def test_create_empty_dataframe(session):
    schema = StructType(
        [
            StructField("COL1", IntegerType()),
            StructField("COL2", ByteType()),
            StructField("COL3", ShortType()),
            StructField("COL4", LongType()),
            StructField("COL5", FloatType()),
            StructField("COL6", DoubleType()),
            StructField("COL7", DecimalType()),
            StructField("COL8", BooleanType()),
            StructField("COL9", BinaryType()),
            StructField("COL10", VariantType()),
            StructField("COL11", StringType()),
            StructField("COL12", DateType()),
            StructField("COL13", TimestampType()),
            StructField("COL14", TimeType()),
            StructField("COL15", TimestampType(TimestampTimeZone.NTZ)),
            StructField("COL16", MapType()),
            StructField("COL17", NullType()),
        ]
    )
    assert not session.create_dataframe(data=[], schema=schema).collect()


@pytest.mark.skipif(
    not is_pandas_available or IS_IN_STORED_PROC_LOCALFS,
    reason="pandas is not available, or in stored procedure local fs it's expected to fail when the result set is large",
)
def test_dataframe_to_local_iterator_with_to_pandas_isolation(
    session, local_testing_mode
):
    if local_testing_mode:
        df = session.create_dataframe(
            [["xyz", int("1" * 19)] for _ in range(200000)], schema=["a1", "b1"]
        )
    else:
        df = session.sql(
            "select 'xyz' as A1, 1111111111111111111 as B1 from table(generator(rowCount => 200000))"
        )
    trigger_df = session.create_dataframe(
        [[1.0]], schema=StructType([StructField("A", DecimalType())])
    )
    my_iter = df.to_pandas_batches()
    batch_count = 0
    for pdf in my_iter:
        # modify result_cursor and trigger _fix_pandas_df_fixed_type()
        trigger_df.select(col("A")).collect()
        # column name should remain unchanged
        assert tuple(pdf.columns) == ("A1", "B1")
        batch_count += 1
        print(batch_count)
    # local testing always give 1 chunk
    if not local_testing_mode:
        assert batch_count > 1


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
@pytest.mark.udf
@pytest.mark.parametrize("overlapping_columns", [True, False])
@pytest.mark.parametrize(
    "func,output_types,output_col_names,expected",
    [
        (lambda row: row[1] + 1, [IntegerType()], ["A"], [(i + 1,) for i in range(5)]),
        (
            lambda row: (row.B * 2, row.C),
            [IntegerType(), StringType()],
            ["B", "C"],
            [
                (
                    i * 2,
                    f"w{i}",
                )
                for i in range(5)
            ],
        ),
        (
            lambda row: Row(row.B * row.B, f"-{row.C}"),
            [IntegerType(), StringType()],
            ["B", "C"],
            [(i * i, f"-w{i}") for i in range(5)],
        ),
    ],
)
def test_map_basic(
    session, func, output_types, output_col_names, expected, overlapping_columns
):
    df = session.create_dataframe(
        [
            (
                True,
                i,
                f"w{i}",
            )
            for i in range(5)
        ],
        schema=["A", "B", "C"],
    )

    if overlapping_columns:
        row = Row(*output_col_names)
        expected = [row(*e) for e in expected]
        Utils.check_answer(
            map(df, func, output_types, output_column_names=output_col_names), expected
        )
    else:
        row = Row(*[f"c_{i+1}" for i in range(len(output_types))])
        expected = [row(*e) for e in expected]
        Utils.check_answer(map(df, func, output_types), expected)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required for this test")
@pytest.mark.udf
@pytest.mark.parametrize(
    "func,output_types,expected",
    [
        (lambda df: df["B"] + 1, [IntegerType()], [(i + 1,) for i in range(5)]),
        (
            lambda df: pd.concat([df["B"] * 2, df["C"]], axis=1),
            [IntegerType(), StringType()],
            [
                (
                    i * 2,
                    f"w{i}",
                )
                for i in range(5)
            ],
        ),
    ],
)
def test_map_vectorized(session, func, output_types, expected):
    df = session.create_dataframe(
        [
            (
                True,
                i,
                f"w{i}",
            )
            for i in range(5)
        ],
        schema=["A", "B", "C"],
    )

    Utils.check_answer(
        map(df, func, output_types, vectorized=True, packages=["pandas"]), expected
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Table function is not supported in Local Testing",
)
@pytest.mark.udf
def test_map_chained(session):
    df = session.create_dataframe(
        [[True, i, f"w{i}"] for i in range(5)], schema=["A", "B", "C"]
    )

    new_df = map(
        map(
            df,
            lambda x: (x.B * x.B, f"_{x.C}_"),
            output_types=[IntegerType(), StringType()],
        ),
        lambda x: len(x[1]) + x[0],
        output_types=[IntegerType()],
    )
    expected = [(len(f"_w{i}_") + i * i,) for i in range(5)]

    Utils.check_answer(new_df, expected)

    # chained calls with repeated column names
    new_df = map(
        map(
            df,
            lambda x: Row(x.B * x.B, f"_{x.C}_"),
            output_types=[IntegerType(), StringType()],
            output_column_names=["A", "B"],
        ),
        lambda x: Row(len(x.B) + x.A),
        output_types=[IntegerType()],
        output_column_names=["A"],
        packages=["snowflake-snowpark-python"],
    )
    Utils.check_answer(new_df, expected)


def test_map_negative(session):
    df1 = session.create_dataframe(
        [[True, i, f"w{i}"] for i in range(5)], schema=["A", "B", "C"]
    )

    with pytest.raises(ValueError, match="output_types cannot be empty."):
        map(df1, lambda row: [row.B, row.C], output_types=[])

    with pytest.raises(
        ValueError,
        match="'output_column_names' and 'output_types' must be of the same size.",
    ):
        map(
            df1,
            lambda row: [row.B, row.C],
            output_types=[IntegerType(), StringType()],
            output_column_names=["a", "b", "c"],
        )


def test_with_column_keep_column_order(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    df1 = df.with_column("A", lit(0), keep_column_order=True)
    assert df1.columns == ["A", "B"]
    df2 = df.with_columns(["A"], [lit(0)], keep_column_order=True)
    assert df2.columns == ["A", "B"]
    df3 = df.with_columns(["A", "C"], [lit(0), lit(0)], keep_column_order=True)
    assert df3.columns == ["A", "B", "C"]
    df3 = df.with_columns(["C", "A"], [lit(0), lit(0)], keep_column_order=True)
    assert df3.columns == ["A", "B", "C"]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="replace function is not supported in Local Testing",
)
def test_SNOW_1879403_replace_with_lit(session):

    # TODO SNOW-1880749: support for local testing mode.

    from snowflake.snowpark.functions import replace

    df = session.create_dataframe(
        [["apple"], ["apple pie"], ["apple juice"]], schema=["a"]
    )
    ans = df.select(
        replace(col("a"), lit("apple"), lit("orange")).alias("result")
    ).collect()

    Utils.check_answer(ans, [Row("orange"), Row("orange pie"), Row("orange juice")])


def test_create_dataframe_with_implicit_struct_simple(session):
    """
    Test an implicit struct string with two integer columns.
    """
    data = [
        [1, 2],
        [3, 4],
    ]
    # The new feature: implicit struct string "col1: int, col2: int"
    schema_str = "col1: int, col2: int"

    # Create the dataframe
    df = session.create_dataframe(data, schema=schema_str)
    # Check schema
    # We expect the schema to be a StructType with 2 fields
    assert isinstance(df.schema, StructType)
    assert len(df.schema.fields) == 2
    expected_fields = [
        StructField("COL1", LongType(), nullable=True),
        StructField("COL2", LongType(), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Collect rows
    result = df.collect()
    expected_rows = [
        Row(COL1=1, COL2=2),
        Row(COL1=3, COL2=4),
    ]
    assert result == expected_rows


def test_create_dataframe_with_implicit_struct_nested(session):
    """
    Test an implicit struct string with nested array and decimal columns.
    """
    data = [
        [["1", "2"], Decimal("3.14")],
        [["5", "6"], Decimal("2.72")],
    ]
    # Nested schema: first column is array<string>, second is decimal(10,2)
    schema_str = "arr: array<string>, val: decimal(10,2)"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert len(df.schema.fields) == 2
    expected_fields = [
        StructField("ARR", ArrayType(StringType()), nullable=True),
        StructField("VAL", DecimalType(10, 2), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Verify rows
    result = df.collect()
    expected_rows = [
        Row(ARR='[\n  "1",\n  "2"\n]', VAL=Decimal("3.14")),
        Row(ARR='[\n  "5",\n  "6"\n]', VAL=Decimal("2.72")),
    ]
    assert result == expected_rows


def test_create_dataframe_with_explicit_struct_string(session):
    """
    Test an explicit struct string "struct<colA: string, colB: double>"
    to confirm it also works (even though it's not strictly 'implicit').
    """
    data = [
        ["hello", 3.14],
        ["world", 2.72],
    ]
    schema_str = "struct<colA: string, colB: double>"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert len(df.schema.fields) == 2
    expected_fields = [
        StructField("COLA", StringType(), nullable=True),
        StructField("COLB", DoubleType(), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Verify rows
    result = df.collect()
    expected_rows = [
        Row(COLA="hello", COLB=3.14),
        Row(COLA="world", COLB=2.72),
    ]
    assert result == expected_rows


def test_create_dataframe_with_implicit_struct_malformed(session):
    """
    Test malformed implicit struct string, which should raise an error.
    """
    data = [[1, 2]]
    # Missing type for second column
    schema_str = "col1: int, col2"

    with pytest.raises(ValueError) as ex_info:
        session.create_dataframe(data, schema=schema_str)
    # Check that the error message mentions the problem
    assert (
        "col2" in str(ex_info.value).lower()
    ), f"Unexpected error message: {ex_info.value}"


def test_create_dataframe_with_implicit_struct_datetime(session):
    """
    Another example mixing basic data with boolean and dates, ensuring
    the implicit struct string handles them properly.
    """
    data = [
        [True, datetime.date(2020, 1, 1)],
        [False, datetime.date(2021, 12, 31)],
    ]
    schema_str = "flag: boolean, d: date"

    df = session.create_dataframe(data, schema=schema_str)
    # Check schema
    assert len(df.schema.fields) == 2
    expected_fields = [
        StructField("FLAG", BooleanType(), nullable=True),
        StructField("D", DateType(), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Check rows
    result = df.collect()
    expected_rows = [
        Row(FLAG=True, D=datetime.date(2020, 1, 1)),
        Row(FLAG=False, D=datetime.date(2021, 12, 31)),
    ]
    assert result == expected_rows


def test_create_dataframe_invalid_schema_string_not_struct(session):
    """
    Verifies that a non-struct schema string (e.g. "int") raises ValueError
    because the resulting type is not an instance of StructType.
    """
    data = [1, 2, 3]
    # "int" does not represent a struct, so we expect a ValueError
    with pytest.raises(ValueError) as ex_info:
        session.create_dataframe(data, schema="int")

    # Check that the error message mentions "Invalid schema string" or "struct type"
    err_msg = str(ex_info.value).lower()
    assert (
        "invalid schema string" in err_msg and "struct type" in err_msg
    ), f"Expected error message about invalid schema string or struct type. Got: {ex_info.value}"


def test_create_dataframe_implicit_struct_not_null_single(session):
    """
    Test a schema with one NOT NULL field.
    """
    data = [
        [1],
        [2],
    ]
    # One field 'col1: int not null'
    schema_str = "col1: int NOT    NULL"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert isinstance(df.schema, StructType)
    assert len(df.schema.fields) == 1

    expected_field = StructField("COL1", LongType(), nullable=False)
    assert df.schema.fields[0] == expected_field

    # Collect rows
    result = df.collect()
    expected_rows = [Row(COL1=1), Row(COL1=2)]
    assert result == expected_rows


def test_create_dataframe_implicit_struct_not_null_multiple(session):
    """
    Test a schema with multiple fields, one of which is NOT NULL.
    """
    data = [
        [10, "foo"],
        [20, "bar"],
    ]
    schema_str = "col1: int not null, col2: string"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert len(df.schema.fields) == 2

    expected_fields = [
        StructField("COL1", LongType(), nullable=False),
        StructField("COL2", StringType(2**24), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Verify rows
    result = df.collect()
    expected_rows = [
        Row(COL1=10, COL2="foo"),
        Row(COL1=20, COL2="bar"),
    ]
    assert result == expected_rows


def test_create_dataframe_implicit_struct_not_null_nested(session):
    """
    Test a schema with nested array and a NOT NULL decimal field.
    """
    data = [
        [["1", "2"], Decimal("3.14")],
        [["5", "6"], Decimal("2.72")],
    ]
    schema_str = "arr: array<string>, val: decimal(10,2) NOT NULL"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert len(df.schema.fields) == 2

    expected_fields = [
        StructField("ARR", ArrayType(StringType()), nullable=True),
        StructField("VAL", DecimalType(10, 2), nullable=False),
    ]
    assert df.schema.fields == expected_fields

    # Verify rows
    result = df.collect()
    expected_rows = [
        Row(ARR='[\n  "1",\n  "2"\n]', VAL=Decimal("3.14")),
        Row(ARR='[\n  "5",\n  "6"\n]', VAL=Decimal("2.72")),
    ]
    assert result == expected_rows


def test_create_dataframe_implicit_struct_not_null_mixed(session):
    """
    Test a schema mixing NOT NULL columns with normal columns,
    plus various data types like boolean or date.
    """
    data = [
        [True, datetime.date(2020, 1, 1), "Hello"],
        [False, datetime.date(2021, 1, 2), "World"],
    ]
    schema_str = "flag: boolean not null, dt: date, txt: string not null"

    df = session.create_dataframe(data, schema=schema_str)
    # Verify schema
    assert len(df.schema.fields) == 3

    expected_fields = [
        StructField("FLAG", BooleanType(), nullable=False),
        StructField("DT", df.schema.fields[1].datatype, nullable=True),
        StructField("TXT", StringType(2**24), nullable=False),
    ]

    assert df.schema.fields == expected_fields

    # Verify rows
    result = df.collect()
    expected_rows = [
        Row(FLAG=True, DT=datetime.date(2020, 1, 1), TXT="Hello"),
        Row(FLAG=False, DT=datetime.date(2021, 1, 2), TXT="World"),
    ]
    assert result == expected_rows


def test_create_dataframe_arr_array(session):
    """
    Verifies schema="arr array" is interpreted as a single field named "ARR"
    with an 'ArrayType'. If your parser maps 'array' => ArrayType() by default,
    the element type might be undefined or assumed.
    """
    data = [
        [[1, 2]],
        [[3, 4]],
    ]
    schema_str = "arr array"

    df = session.create_dataframe(data, schema_str)
    # Check schema
    assert len(df.schema.fields) == 1
    # Since "array" is mapped via your DATA_TYPE_STRING_OBJECT_MAPPINGS["array"] = ArrayType,
    # we should expect an ArrayType(...) with no specific element type.
    # For Snowpark, ArrayType() can be valid (element type might be 'AnyType' internally).
    actual_field = df.schema.fields[0]
    assert actual_field.name == "ARR"
    assert isinstance(
        actual_field.datatype, ArrayType
    ), f"Expected ArrayType(), got {actual_field.datatype}"
    # default is nullable=True unless "not null" is specified
    assert actual_field.nullable is True

    # Check data
    result = df.collect()
    expected_rows = [Row(ARR="[\n  1,\n  2\n]"), Row(ARR="[\n  3,\n  4\n]")]
    assert result == expected_rows


def test_create_dataframe_x_string(session):
    """
    Verifies schema="x STRING" is interpreted as a single field named "X"
    with type StringType().
    """
    data = [
        ["hello"],
        ["world"],
    ]
    schema_str = "x STRING"

    df = session.create_dataframe(data, schema_str)
    # Check schema
    assert len(df.schema.fields) == 1
    expected_field = StructField("X", StringType(), nullable=True)
    assert df.schema.fields[0] == expected_field

    # Check rows
    result = df.collect()
    expected_rows = [
        Row(X="hello"),
        Row(X="world"),
    ]
    assert result == expected_rows


def test_create_dataframe_x_string_y_integer(session):
    """
    Verifies schema="x STRING, y INTEGER" is interpreted as a struct with two fields:
    'X' => StringType (nullable), 'Y' => IntegerType (nullable).
    """
    data = [
        ["a", 1],
        ["b", 2],
    ]
    schema_str = "x STRING, y INTEGER"

    df = session.create_dataframe(data, schema_str)
    # Check schema
    assert len(df.schema.fields) == 2
    expected_fields = [
        StructField("X", StringType(), nullable=True),
        StructField("Y", LongType(), nullable=True),
    ]
    assert df.schema.fields == expected_fields

    # Check rows
    result = df.collect()
    expected_rows = [
        Row(X="a", Y=1),
        Row(X="b", Y=2),
    ]
    assert result == expected_rows


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="File data type is not supported in Local Testing",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC_LOCALFS, reason="FILE type does not work in localfs"
)
def test_create_dataframe_file_type(session, resources_path):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    session.sql(f"create or replace temp stage {stage_name}").collect()
    test_files = TestFiles(resources_path)
    session.file.put(
        test_files.test_file_csv, f"@{stage_name}", auto_compress=False, overwrite=True
    )
    session.file.put(
        test_files.test_dog_image, f"@{stage_name}", auto_compress=False, overwrite=True
    )
    csv_url = f"@{stage_name}/testCSV.csv"
    image_url = f"@{stage_name}/dog.jpg"
    df = session.create_dataframe(
        [csv_url, image_url],
        schema=StructType([StructField("col", FileType(), nullable=True)]),
    )
    result = df.collect()
    csv_row = json.loads(result[0][0])
    assert csv_row["CONTENT_TYPE"] == "text/csv"
    image_row = json.loads(result[1][0])
    assert image_row["CONTENT_TYPE"] == "image/jpeg"
