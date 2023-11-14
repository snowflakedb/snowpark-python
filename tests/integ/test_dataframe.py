#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import copy
import datetime
import json
import logging
import math
import sys
from array import array
from collections import namedtuple
from decimal import Decimal
from itertools import product
from typing import Tuple

try:
    import pandas as pd  # noqa: F401
    from pandas import DataFrame as PandasDF
    from pandas.testing import assert_frame_equal

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

import pytest

from snowflake.connector import IntegrityError
from snowflake.snowpark import Column, Row, Window
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark._internal.analyzer.expression import Attribute, Star
from snowflake.snowpark._internal.utils import TempObjectType, warning_dict
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
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
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
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, f"@{tmp_stage_name}", test_files.test_file_csv, compress=False
    )


@pytest.fixture(scope="function")
def table_name_1(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
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
def test_read_stage_file_show(session, resources_path):
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
            ._show_string()
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
        Utils.drop_stage(session, tmp_stage_name)


def test_show_using_with_select_statement(session):
    df = session.sql(
        "with t1 as (select 1 as a union all select 2 union all select 3 "
        "   union all select 4 union all select 5 union all select 6 "
        "   union all select 7 union all select 8 union all select 9 "
        "   union all select 10 union all select 11 union all select 12) "
        "select * from t1"
    )
    assert (
        df._show_string()
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


def test_distinct(session):
    """Tests df.distinct()."""

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


def test_generator_table_function_negative(session):
    # fails when no operators added
    with pytest.raises(ValueError) as ex_info:
        _ = session.generator(rowcount=10)
    assert "Columns cannot be empty for generator table function" in str(ex_info)


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

    # the column names provided via schema keyword will overwrite other column names
    df = session.create_dataframe(
        [{"aa": 1, "bb": 2, "cc": 3, "dd": 4}], schema=col_names
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


@pytest.mark.parametrize("data", [[0, 1, 2, 3], ["", "a"], [False, True], [None]])
def test_create_dataframe_with_single_value(session, data):
    expected_names = ["_1"]
    expected_rows = [Row(d) for d in data]
    df = session.create_dataframe(data)
    assert [field.name for field in df.schema.fields] == expected_names
    Utils.check_answer(df, expected_rows)


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
        df._show_string()
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


def test_create_dataframe_large_without_batch_insert(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 40000
        with pytest.raises(SnowparkSQLException) as ex_info:
            session.create_dataframe([1] * 20000).collect()
        assert "SQL compilation error" in str(ex_info)
        assert "maximum number of expressions in a list exceeded" in str(ex_info)
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


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


def test_dataframe_duplicated_column_names(session):
    df = session.sql("select 1 as a, 2 as a")
    # collect() works and return a row with duplicated keys
    res = df.collect()
    assert len(res[0]) == 2
    assert res[0].A == 1

    # however, create a table/view doesn't work because
    # Snowflake doesn't allow duplicated column names
    with pytest.raises(SnowparkSQLException) as ex_info:
        df.create_or_replace_view(
            Utils.random_name_for_temp_object(TempObjectType.VIEW)
        )
    assert "duplicate column name 'A'" in str(ex_info)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Async query is not supported in stored procedure yet"
)
def test_case_insensitive_collect(session):
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


def test_dropna(session):
    Utils.check_answer(TestData.double3(session).dropna(), [Row(1.0, 1)])

    res = TestData.double3(session).dropna(how="all").collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    Utils.check_answer(
        TestData.double3(session).dropna(subset=["a"]), [Row(1.0, 1), Row(4.0, None)]
    )

    res = TestData.double3(session).dropna(thresh=1).collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    with pytest.raises(TypeError) as ex_info:
        TestData.double3(session).dropna(subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)


def test_fillna(session):
    Utils.check_answer(
        TestData.double3(session).fillna(11),
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
        TestData.double3(session).fillna(11, subset=["a"]),
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
        TestData.double3(session).fillna(None),
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
        ).fillna({"col1": 1.1, "col2": 1}),
        [Row(1, 1.1), Row(None, 1)],
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
    with pytest.raises(TypeError) as ex_info:
        df.fillna(1, subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)


def test_replace(session):
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
    Utils.check_answer(
        df.replace(1.0, None),
        [Row(1, None, "1.0"), Row(2, 2.0, "2.0")],
    )

    df = session.create_dataframe([[[1, 2], (1, 3)]], schema=["col1", "col2"])
    Utils.check_answer(
        df.replace([(1, 3)], [[2, 3]]),
        [Row("[\n  1,\n  2\n]", "[\n  2,\n  3\n]")],
    )

    # negative case
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.replace({1: 3}, subset=["d"])
    assert "The DataFrame does not contain the column named" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        df.replace({1: 2}, subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace([1], [2, 3])
    assert "to_replace and value lists should be of the same length" in str(ex_info)


def test_select_case_expr(session):
    df = session.create_dataframe([1, 2, 3], schema=["a"])
    Utils.check_answer(
        df.select(when(col("a") == 1, 4).otherwise(col("a"))), [Row(4), Row(2), Row(3)]
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
    assert "invalid identifier" in str(ex_info)


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_table_types_in_save_as_table(session, save_mode, table_type):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    try:
        df.write.save_as_table(table_name, mode=save_mode, table_type=table_type)
        Utils.check_answer(session.table(table_name), df, True)
        Utils.assert_table_type(session, table_name, table_type)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_save_as_table_respects_schema(session, save_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    schema1 = StructType(
        [
            StructField("A", LongType(), False),
            StructField("B", LongType(), True),
        ]
    )
    schema2 = StructType([StructField("A", LongType(), False)])

    df1 = session.create_dataframe([(1, 2), (3, 4)], schema=schema1)
    df2 = session.create_dataframe([(1), (2)], schema=schema2)

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
        else:  # save_mode in ('append', 'errorifexists')
            with pytest.raises(SnowparkSQLException):
                df2.write.save_as_table(table_name, mode=save_mode)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_save_as_table_nullable_test(session, save_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    schema = StructType(
        [
            StructField("A", IntegerType(), False),
            StructField("B", IntegerType(), True),
        ]
    )
    df = session.create_dataframe([(None, None)], schema=schema)

    try:
        with pytest.raises(
            (IntegrityError, SnowparkSQLException),
            match="NULL result in a non-nullable column",
        ):
            df.write.save_as_table(table_name, mode=save_mode)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.udf
@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_save_as_table_with_table_sproc_output(session, save_mode, table_type):
    temp_sp_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        select_sp = session.sproc.register(
            lambda session_: session_.sql("SELECT 1 as A"),
            packages=["snowflake-snowpark-python"],
            name=temp_sp_name,
            return_type=StructType([StructField("A", IntegerType())]),
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


@pytest.mark.parametrize("save_mode", ["append", "overwrite"])
def test_write_table_with_clustering_keys(session, save_mode):
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
    try:
        df1.write.save_as_table(
            table_name1,
            mode=save_mode,
            clustering_keys=["c1", "c2"],
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name1}')")[0][0]
        assert 'cluster by ("C1", "C2")' in ddl

        df2.write.save_as_table(
            table_name2,
            mode=save_mode,
            clustering_keys=[
                col("c1").cast(DateType()),
                col("c2").substring(0, 10),
            ],
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name2}')")[0][0]
        assert 'cluster by ( CAST ("C1" AS DATE), substring("C2", 0, 10))' in ddl

        df3.write.save_as_table(
            table_name3,
            mode=save_mode,
            clustering_keys=[get_path(col("v"), lit("Data.id")).cast(IntegerType())],
        )
        ddl = session._run_query(f"select get_ddl('table', '{table_name3}')")[0][0]
        assert "cluster by ( CAST (get_path(\"V\", 'Data.id') AS INT))" in ddl
    finally:
        Utils.drop_table(session, table_name1)
        Utils.drop_table(session, table_name2)
        Utils.drop_table(session, table_name3)


@pytest.mark.parametrize("table_type", ["temp", "temporary", "transient"])
@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_write_temp_table_no_breaking_change(session, save_mode, table_type, caplog):
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
        assert "create_temp_table is deprecated" in caplog.text
        Utils.check_answer(session.table(table_name), df, True)
        Utils.assert_table_type(session, table_name, "temp")
    finally:
        Utils.drop_table(session, table_name)
        # clear the warning dict otherwise it will affect the future tests
        warning_dict.clear()


def test_write_invalid_table_type(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    with pytest.raises(ValueError, match="Unsupported table type"):
        df.write.save_as_table(table_name, table_type="invalid")


def test_append_existing_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "a int, b int", is_temporary=True)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    try:
        with session.query_history() as history:
            df.write.save_as_table(table_name, mode="append")
        Utils.check_answer(session.table(table_name), df, True)
        assert len(history.queries) == 2  # SHOW + INSERT
        assert history.queries[0].sql_text.startswith("show")
        assert history.queries[1].sql_text.startswith("INSERT")
    finally:
        Utils.drop_table(session, table_name)


def test_create_dynamic_table(session, table_name_1):
    try:
        df = session.table(table_name_1)
        dt_name = Utils.random_name_for_temp_object(TempObjectType.DYNAMIC_TABLE)
        df.create_or_replace_dynamic_table(
            dt_name, warehouse=session.get_current_warehouse(), lag="1000 minutes"
        )
        # scheduled refresh is not deterministic which leads to flakiness that dynamic table is not initialized
        # here we manually refresh the dynamic table
        session.sql(f"alter dynamic table {dt_name} refresh").collect()
        res = session.sql(f"show dynamic tables like '{dt_name}'").collect()
        assert len(res) == 1
    finally:
        Utils.drop_dynamic_table(session, dt_name)


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
    Utils.create_table(
        session, temp_table, '"a b" int, "a""b" int, "a" int, a int', is_temporary=True
    )
    session.sql(f"insert into {temp_table} values (1, 2, 3, 4)").collect()
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


@pytest.mark.parametrize(
    "column_list",
    [["jan", "feb", "mar", "apr"], [col("jan"), col("feb"), col("mar"), col("apr")]],
)
def test_unpivot(session, column_list):
    Utils.check_answer(
        TestData.monthly_sales_flat(session)
        .unpivot("sales", "month", column_list)
        .sort("empid"),
        [
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
        ],
        sort=False,
    )


def test_create_dataframe_string_length(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe(["ab", "abc", "abcd"], schema=["a"])
    df.write.mode("overwrite").save_as_table(table_name, table_type="temporary")
    datatype = json.loads(
        session.sql(f"show columns in {table_name}").collect()[0]["data_type"]
    )
    assert datatype["type"] == "TEXT"
    assert datatype["length"] == 2**20 * 16  # max length (16 MB)
    session.sql(f"insert into {table_name} values('abcde')").collect()
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
        df.collect(statement_params=statement_params_wrong_date_format)
    assert "is not recognized" in str(exc)
    assert (
        df.collect(statement_params=statement_params_correct_date_format)
        == expected_rows
    )

    # to_local_iterator
    with pytest.raises(SnowparkSQLException) as exc:
        list(df.to_local_iterator(statement_params=statement_params_wrong_date_format))
    assert "is not recognized" in str(exc)
    assert (
        list(
            df.to_local_iterator(statement_params=statement_params_correct_date_format)
        )
        == expected_rows
    )

    # to_pandas
    with pytest.raises(SnowparkSQLException) as exc:
        df.to_pandas(statement_params=statement_params_wrong_date_format)
    assert "is not recognized" in str(exc)
    assert_frame_equal(
        df.to_pandas(statement_params=statement_params_correct_date_format),
        pandas_df,
        check_dtype=False,
    )

    # to_pandas_batches
    with pytest.raises(SnowparkSQLException) as exc:
        pd.concat(
            list(
                df.to_pandas_batches(
                    statement_params=statement_params_wrong_date_format
                )
            ),
            ignore_index=True,
        )
    assert "is not recognized" in str(exc)
    assert_frame_equal(
        pd.concat(
            list(
                df.to_pandas_batches(
                    statement_params=statement_params_correct_date_format
                )
            ),
            ignore_index=True,
        ),
        pandas_df,
    )

    # count
    # passing statement_params_wrong_date_format does not trigger error
    assert df.count(statement_params=statement_params_correct_date_format) == 2

    # copy_into_table test is covered in test_datafrom_copy_into as it requires complex config

    # show
    with pytest.raises(SnowparkSQLException) as exc:
        df.show(statement_params=statement_params_wrong_date_format)
    assert "is not recognized" in str(exc)
    df.show(statement_params=statement_params_correct_date_format)

    # create_or_replace_view
    # passing statement_params_wrong_date_format does not trigger error
    assert (
        "successfully created"
        in df.create_or_replace_view(
            Utils.random_view_name(),
            statement_params=statement_params_correct_date_format,
        )[0]["status"]
    )

    # create_or_replace_temp_view
    # passing statement_params_wrong_date_format does not trigger error
    assert (
        "successfully created"
        in df.create_or_replace_temp_view(
            Utils.random_view_name(),
            statement_params=statement_params_correct_date_format,
        )[0]["status"]
    )

    # first
    with pytest.raises(SnowparkSQLException) as exc:
        df.first(statement_params=statement_params_wrong_date_format)
    assert "is not recognized" in str(exc)

    assert (
        df.first(statement_params=statement_params_correct_date_format)
        == expected_rows[0]
    )

    # cache_result
    with pytest.raises(SnowparkSQLException) as exc:
        df.cache_result(statement_params=statement_params_wrong_date_format).collect()
    assert "is not recognized" in str(exc)

    assert (
        df.cache_result(statement_params=statement_params_correct_date_format).collect()
        == expected_rows
    )

    # random_split
    with pytest.raises(SnowparkSQLException) as exc:
        df.random_split(
            weights=[0.5, 0.5], statement_params=statement_params_wrong_date_format
        )
    assert "is not recognized" in str(exc)
    assert (
        len(
            df.random_split(
                weights=[0.5, 0.5],
                statement_params=statement_params_correct_date_format,
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
            statement_params=statement_params_correct_date_format,
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
        temp_stage, statement_params=statement_params_correct_date_format
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


def test_create_or_replace_view_with_multiple_queries(session):
    df = session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    with pytest.raises(
        SnowparkCreateViewException,
        match="Your dataframe may include DDL or DML operations",
    ):
        df.create_or_replace_view("temp")


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


def test_dataframe_data_generator(session):
    df1 = session.create_dataframe([1, 2, 3], schema=["a"])
    df2 = df1.with_column("b", seq1()).sort(col("a").desc())
    Utils.check_answer(df2, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df3 = df1.with_column("b", seq2()).sort(col("a").desc())
    Utils.check_answer(df3, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df4 = df1.with_column("b", seq4()).sort(col("a").desc())
    Utils.check_answer(df4, [Row(3, 2), Row(2, 1), Row(1, 0)])

    df5 = df1.with_column("b", seq8()).sort(col("a").desc())
    Utils.check_answer(df5, [Row(3, 2), Row(2, 1), Row(1, 0)])


def test_dataframe_select_window(session):
    df1 = session.create_dataframe([1, 2, 3], schema=["a"])
    df2 = df1.select(
        "a", rank().over(Window.order_by(col("a").desc())).alias("b")
    ).sort(col("a").desc())
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
    Utils.create_table(
        session, table_name, '"a\nb" string, id number', is_temporary=True
    )
    session._conn.run_query(f"insert into {table_name} values ('a', 1), ('b', 2)")
    df = session.table(table_name)
    try:
        Utils.check_answer(df, [Row("a", 1), Row("b", 2)])
        df2 = df.drop('"a\nb"')
        Utils.check_answer(df2, [Row(1), Row(2)])
    finally:
        Utils.drop_table(session, table_name)
