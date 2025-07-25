#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import decimal
import math

import pytest

import snowflake.snowpark.mock._functions as snowpark_mock_functions
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    approx_percentile_combine,
    avg,
    col,
    count,
    count_distinct,
    covar_pop,
    listagg,
    max as max_,
    mean,
    median,
    min as min_,
    stddev,
    stddev_pop,
    sum as sum_,
    upper,
    grouping,
    grouping_id,
)
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator, ColumnType
from snowflake.snowpark.types import DoubleType, IntegerType, StructType, StructField
import snowflake.snowpark.context as context
from tests.utils import Utils


def test_df_agg_tuples_basic_without_std(session):
    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )

    # Aggregations on 'first' column
    res = df.agg([("first", "min")]).collect()
    Utils.assert_rows(res, [Row(1)])

    res = df.agg([("first", "count")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("first", "max")]).collect()
    Utils.assert_rows(res, [Row(2)])

    res = df.agg([("first", "avg")]).collect()
    Utils.assert_rows(res, [Row(1.5)])

    # combine those together
    res = df.agg(
        [
            ("first", "min"),
            ("first", "count"),
            ("first", "max"),
            ("first", "avg"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(1, 4, 2, 1.5)])

    # Aggregations on 'second' column
    res = df.agg([("second", "min")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("second", "count")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("second", "max")]).collect()
    Utils.assert_rows(res, [Row(6)])

    res = df.agg([("second", "avg")]).collect()
    Utils.assert_rows(res, [Row(4.75)])

    # combine those together
    res = df.agg(
        [
            ("second", "min"),
            ("second", "count"),
            ("second", "max"),
            ("second", "avg"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(4, 4, 6, 4.75)])

    # Get aggregations for both columns
    res = df.agg(
        [
            ("first", "min"),
            ("second", "count"),
            ("first", "max"),
            ("second", "avg"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(1, 4, 2, 4.75)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: stddev function not supported",
)
def test_df_agg_tuples_basic(session):
    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )

    # Aggregations on 'first' column
    res = df.agg([("first", "min")]).collect()
    Utils.assert_rows(res, [Row(1)])

    res = df.agg([("first", "count")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("first", "max")]).collect()
    Utils.assert_rows(res, [Row(2)])

    res = df.agg([("first", "avg")]).collect()
    Utils.assert_rows(res, [Row(1.5)])

    res = df.agg([("first", "std")]).collect()
    Utils.assert_rows(res, [Row(0.577349980514419)])

    # combine those together
    res = df.agg(
        [
            ("first", "min"),
            ("first", "count"),
            ("first", "max"),
            ("first", "avg"),
            ("first", "std"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(1, 4, 2, 1.5, 0.577349980514419)])

    # Aggregations on 'second' column
    res = df.agg([("second", "min")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("second", "count")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("second", "max")]).collect()
    Utils.assert_rows(res, [Row(6)])

    res = df.agg([("second", "avg")]).collect()
    Utils.assert_rows(res, [Row(4.75)])

    res = df.agg([("second", "std")]).collect()
    Utils.assert_rows(res, [Row(0.9574272818339783)])

    # combine those together
    res = df.agg(
        [
            ("second", "min"),
            ("second", "count"),
            ("second", "max"),
            ("second", "avg"),
            ("second", "std"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(4, 4, 6, 4.75, 0.9574272818339783)])

    # Get aggregations for both columns
    res = df.agg(
        [
            ("first", "min"),
            ("second", "count"),
            ("first", "max"),
            ("second", "avg"),
            ("first", "std"),
        ]
    ).collect()
    Utils.assert_rows(res, [Row(1, 4, 2, 4.75, 0.577349980514419)])


def test_df_agg_tuples_avg_basic(session):
    """Test for making sure all avg word-variations work as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    # Aggregations on 'first' column
    res = df.agg([("first", "avg")]).collect()
    Utils.assert_rows(res, [Row(1.5)])

    res = df.agg([("first", "average")]).collect()
    Utils.assert_rows(res, [Row(1.5)])

    res = df.agg([("first", "mean")]).collect()
    Utils.assert_rows(res, [Row(1.5)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: stddev function not supported",
)
def test_df_agg_tuples_std_basic(session):
    """Test for making sure all stddev variations work as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    # Aggregations on 'first' column
    res = df.agg([("first", "stddev")]).collect()
    Utils.assert_rows(res, [Row(0.577349980514419)])

    res = df.agg([("first", "std")]).collect()
    Utils.assert_rows(res, [Row(0.577349980514419)])


def test_df_agg_tuples_count_basic(session):
    """Test for making sure all count variations work as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    # Aggregations on 'first' column
    res = df.agg([("first", "count")]).collect()
    Utils.assert_rows(res, [Row(4)])

    res = df.agg([("second", "size")]).collect()
    Utils.assert_rows(res, [Row(4)])


def test_df_group_by_invalid_input(session):
    """Test for check invalid input for group_by function"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    with pytest.raises(TypeError) as ex_info:
        df.group_by([], []).count().collect()
    assert (
        "group_by() only accepts str and Column objects,"
        " or a list containing str and Column objects" in str(ex_info)
    )
    with pytest.raises(TypeError) as ex_info:
        df.group_by(1).count().collect()
    assert (
        "group_by() only accepts str and Column objects,"
        " or a list containing str and Column objects" in str(ex_info)
    )


def test_df_agg_tuples_sum_basic(session):
    """Test for making sure sum works as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    # Aggregations on 'first' column
    res = df.agg([("first", "sum")]).collect()
    Utils.assert_rows(res, [Row(6)])

    res = df.agg([("second", "sum")]).collect()
    Utils.assert_rows(res, [Row(19)])

    res = df.agg([("second", "sum"), ("first", "sum")]).collect()
    Utils.assert_rows(res, [Row(19, 6)])

    res = df.group_by("first").sum("second").collect()
    res.sort(key=lambda x: x[0])
    Utils.assert_rows(res, [Row(1, 8), Row(2, 11)])

    # same as above, but pass Column object to group_by() and sum()
    res = df.group_by(col("first")).sum(col("second")).collect()
    res.sort(key=lambda x: x[0])
    Utils.assert_rows(res, [Row(1, 8), Row(2, 11)])


def test_df_agg_dict_arg(session):
    """Test for making sure dict when passed to agg() works as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    res = df.agg({"first": "sum"}).collect()
    Utils.assert_rows(res, [Row(6)])

    res = df.agg({"second": "sum"}).collect()
    Utils.assert_rows(res, [Row(19)])

    res = df.agg({"second": "sum", "first": "sum"}).collect()
    Utils.assert_rows(res, [Row(19, 6)])

    res = df.agg({"first": "count", "second": "size"}).collect()
    Utils.assert_rows(res, [Row(4, 4)])

    # negative tests
    with pytest.raises(TypeError) as ex_info:
        df.agg({"second": 1, "first": "sum"})
    assert (
        "Dictionary passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only strings: "
        "got key-value pair with types (<class 'str'>, <class 'int'>)" in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.agg({"second": "sum", 1: "sum"})
    assert (
        "Dictionary passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only strings: "
        "got key-value pair with types (<class 'int'>, <class 'str'>)" in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.agg({"second": "sum", 1: 1})
    assert (
        "Dictionary passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only strings: "
        "got key-value pair with types (<class 'int'>, <class 'int'>)" in str(ex_info)
    )


def test_df_agg_invalid_args_in_list_negative(session):
    """Test for making sure when a list passed to agg() produces correct errors."""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )

    assert df.agg([("first", "count")]).collect() == [Row(4)]

    # invalid type
    with pytest.raises(TypeError) as ex_info:
        df.agg([int])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.agg(["first"])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )

    # not a pair
    with pytest.raises(TypeError) as ex_info:
        df.agg([("first", "count", "invalid_arg")])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.agg(["first", "count", "invalid_arg"])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )

    # pairs with invalid type
    with pytest.raises(TypeError) as ex_info:
        df.agg([("first", 123)])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.agg([(123, "sum")])
    assert (
        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should contain only"
        in str(ex_info)
    )


def test_df_agg_empty_args(session):
    """Test for making sure dict when passed to agg() works as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )

    Utils.assert_rows(df.agg({}).collect(), [Row(1, 4)])


def test_df_agg_varargs_tuple_list(session):
    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )
    Utils.check_answer(df.agg(count("first")), [Row(4)])
    Utils.check_answer(df.agg(count("first"), sum_("second")), [Row(4, 19)])
    Utils.check_answer(df.agg(("first", "count")), [Row(4)])
    Utils.check_answer(df.agg(("first", "count"), ("second", "sum")), [Row(4, 19)])
    Utils.check_answer(df.agg(["first", "count"]), [Row(4)])
    Utils.check_answer(df.agg(["first", "count"], ["second", "sum"]), [Row(4, 19)])
    Utils.check_answer(df.agg(["first", "count"], ("second", "sum")), [Row(4, 19)])


@pytest.mark.parametrize(
    "col1,col2,alias1,alias2",
    [
        ("価格", "数量", '"COUNT(価格)"', '"SUM(数量)"'),
        ("ราคา", "ปริมาณ", '"COUNT(ราคา)"', '"SUM(ปริมาณ)"'),
        ("😀", "😂", '"COUNT(😀)"', '"SUM(😂)"'),
        (
            'A"A',
            'B"B',
            '"COUNT(AA)"',
            '"SUM(BB)"',
        ),  # Removing double quotes is a past decision
    ],
)
def test_df_agg_with_nonascii_column_names(session, col1, col2, alias1, alias2):
    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df([col1, col2])
    df.agg(count(col1)).show()
    df.agg(count(col(col1))).show()
    Utils.check_answer(df.agg(count(col1), sum_(col2)), [Row(4, 19)])

    assert df.agg(count(col1), sum_(col2)).columns == [alias1, alias2]


def test_agg_single_column(session, local_testing_mode):
    val = "86.333333"
    origin_df = session.create_dataframe(
        [[1], [8], [6], [3], [100], [400], [None]], schema=["v"]
    )
    assert origin_df.select(sum_("v")).collect() == [Row(518)]
    assert origin_df.select(max_("v")).collect() == [Row(400)]
    assert origin_df.select(min_("v")).collect() == [Row(1)]
    assert origin_df.select(median("v")).collect() == [Row(7.0)]
    assert origin_df.select(avg("v")).collect() == [
        Row(decimal.Decimal(val))
    ]  # snowflake keeps scale of 5
    assert origin_df.select(mean("v")).collect() == [Row(decimal.Decimal(val))]
    assert origin_df.select(count("v")).collect() == [Row(6)]
    assert origin_df.count() == 7


def test_agg_double_column(session):
    origin_df = session.create_dataframe(
        [
            [10.0, 11.0],
            [20.0, 22.0],
            [25.0, 0.0],
            [30.0, 35.0],
            [999.0, None],
            [None, 1234.0],
        ],
        schema=["m", "n"],
    )
    assert origin_df.select(covar_pop("m", "n")).collect() == [Row(38.75)]
    assert origin_df.select(sum_(col("m") + col("n"))).collect() == [Row(153.0)]
    assert origin_df.select(sum_(col("m") - col("n"))).collect() == [Row(17.0)]

    # nan value will lead to nan result
    origin_df = session.create_dataframe(
        [
            [10.0, 11.0],
            [20.0, 22.0],
            [25.0, 0.0],
            [30.0, 35.0],
            [999.0, None],
            [None, 1234.0],
            [math.nan, None],
            [math.nan, 1.0],
        ],
        schema=["m", "n"],
    )
    assert math.isnan(origin_df.select(covar_pop("m", "n")).collect()[0][0])
    assert math.isnan(origin_df.select(sum_(col("m") + col("n"))).collect()[0][0])
    assert math.isnan(origin_df.select(sum_(col("m") - col("n"))).collect()[0][0])


def test_agg_function_multiple_parameters(session):
    origin_df = session.create_dataframe(["k1", "k1", "k3", "k4", [None]], schema=["v"])
    assert origin_df.select(
        listagg("v", delimiter='~!1,."').within_group(origin_df.v.asc())
    ).collect() == [Row('k1~!1,."k1~!1,."k3~!1,."k4')]

    assert origin_df.select(
        listagg("v", delimiter='~!1,."', is_distinct=True).within_group(
            origin_df.v.asc()
        )
    ).collect() == [Row('k1~!1,."k3~!1,."k4')]


def test_group_by(session, local_testing_mode):
    origin_df = session.create_dataframe(
        [
            ["a", "ddd", 11.0],
            ["a", "ddd", 22.0],
            ["b", "ccc", 9.0],
            ["b", "ccc", 9.0],
            ["b", "aaa", 35.0],
            ["b", "aaa", 99.0],
        ],
        schema=["m", "n", "q"],
    )

    Utils.check_answer(
        origin_df.group_by("m").agg(sum_("q")).collect(),
        [
            Row("a", 33.0),
            Row("b", 152.0),
        ],
    )

    Utils.check_answer(
        origin_df.group_by("n").agg(min_("q")).collect(),
        [
            Row("ddd", 11.0),
            Row("ccc", 9.0),
            Row("aaa", 35.0),
        ],
    )

    with pytest.raises(
        NotImplementedError if local_testing_mode else SnowparkSQLException
    ):
        origin_df.group_by("n", "m").agg(approx_percentile_combine("q")).collect()

    if not local_testing_mode:
        pytest.skip("mock implementation does not apply to live code")

    @snowpark_mock_functions.patch(approx_percentile_combine)
    def mock_approx_percentile_combine(state: ColumnEmulator):
        if state.iat[0] == 11:
            return ColumnEmulator(data=-1.0, sf_type=ColumnType(DoubleType(), False))
        if state.iat[0] == 9:
            return ColumnEmulator(data=0.0, sf_type=ColumnType(DoubleType(), False))
        if state.iat[0] == 35:
            return ColumnEmulator(data=1.0, sf_type=ColumnType(DoubleType(), False))
        raise RuntimeError("This error shall never be raised")

    Utils.check_answer(
        origin_df.group_by("n").agg(approx_percentile_combine("q")).collect(),
        [
            Row("ddd", -1.0),
            Row("ccc", 0.0),
            Row("aaa", 1.0),
        ],
    )

    Utils.check_answer(
        origin_df.group_by("m", "n").agg(mean("q")).collect(),
        [
            Row("a", "ddd", 16.5),
            Row("b", "ccc", 9.0),
            Row("b", "aaa", 67.0),
        ],
    )


def test_agg(session, local_testing_mode):
    origin_df = session.create_dataframe(
        [
            [15.0, 11.0],
            [2.0, 22.0],
            [29.0, 9.0],
            [30.0, 9.0],
            [4.0, 35.0],
            [54.0, 99.0],
        ],
        schema=["m", "n"],
    )

    Utils.check_answer(origin_df.agg(sum_("m")).collect(), Row(134.0))

    Utils.check_answer(origin_df.agg(min_("m"), max_("n")).collect(), Row(2.0, 99.0))

    Utils.check_answer(
        origin_df.agg({"m": "count", "n": "sum"}).collect(), Row(6.0, 185.0)
    )

    if not local_testing_mode:
        pytest.skip("mock implementation does not apply to live code")

    registry = snowpark_mock_functions.MockedFunctionRegistry.get_or_create()
    registry.unregister("stddev_pop")

    with pytest.raises(NotImplementedError):
        origin_df.select(stddev("n"), stddev_pop("m")).collect()

    # stddev_pop is not implemented yet
    with pytest.raises(NotImplementedError):
        origin_df.select(stddev("n"), stddev_pop("m")).collect()

    @snowpark_mock_functions.patch("stddev_pop")
    def mock_stddev_pop(column: ColumnEmulator):
        assert column.tolist() == [15.0, 2.0, 29.0, 30.0, 4.0, 54.0]
        return ColumnEmulator(data=456, sf_type=ColumnType(DoubleType(), False))

    Utils.check_answer(
        origin_df.select(stddev("n"), stddev_pop("m")).collect(),
        Row(34.89, 456.0),
        float_equality_threshold=0.1,
    )


def test_agg_column_naming(session):
    df = session.create_dataframe(
        [
            ("x", 1),
            ("x", 2),
            ("y", 1),
        ],
        schema=["a", "b"],
    )

    # DF with automatic naming
    df2 = df.group_by(upper(df.a)).agg(max_(df.b))

    # DF with specific naming
    df3 = df.group_by(upper(df.a).alias("UPPER")).agg(max_(df.b).alias("max"))

    assert df2.columns == ['"UPPER(A)"', '"MAX(B)"']
    assert df3.columns == ["UPPER", "MAX"]
    expected = [Row("X", 2), Row("Y", 1)]
    Utils.check_answer(df2, expected)
    Utils.check_answer(df3, expected)


def test_agg_on_empty_df(session):
    df = session.create_dataframe([], StructType([StructField("a", IntegerType())]))
    aggs = [
        avg("a"),
        count("a"),
        count_distinct("a"),
        listagg("a"),
        max_("a"),
        mean("a"),
        median("a"),
        min_("a"),
        stddev("a"),
        sum_("a"),
    ]
    agged_with_group_by = df.group_by("a").agg(*aggs)
    agged_no_group_by = df.group_by().agg(*aggs)

    Utils.check_answer(agged_with_group_by, [])
    Utils.check_answer(
        agged_no_group_by, [Row(None, 0, 0, "", None, None, None, None, None, None)]
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING clause is not supported in local testing mode",
)
def test_agg_filter_snowpark_connect_compatible(session):
    original_value = context._is_snowpark_connect_compatible_mode

    try:
        context._is_snowpark_connect_compatible_mode = True
        df = session.create_dataframe(
            [(1, 2, 3), (3, 2, 1), (3, 2, 1)], ["a", "b", "c"]
        )
        df1 = df.group_by("a").agg(sum_("c")).filter(count("a") > 1)
        assert "HAVING" in df1.queries["queries"][0]
        Utils.check_answer(df1, [Row(3, 2)])

        # Test with grouping function
        df2 = df.group_by("a", "b").agg(
            sum_("c").alias("sum_c"),
            grouping("a").alias("ga"),
            grouping("b").alias("gb"),
        )
        # Filter on grouping column - should use HAVING
        df3 = df2.filter(col("ga") == 0)
        assert "HAVING" in df3.queries["queries"][0]
        Utils.check_answer(df3, [Row(1, 2, 3, 0, 0), Row(3, 2, 2, 0, 0)])

        # original behavior on dataframe without group by
        with pytest.raises(
            SnowparkSQLException, match="Invalid aggregate function in where clause"
        ):
            df.filter(count("a") > 1).collect()

        # Test that filtering on grouping without group by is invalid
        with pytest.raises(
            SnowparkSQLException, match="GROUPING function without a GROUP BY"
        ):
            df.filter(grouping("a") == 0).collect()

        Utils.check_answer(df.filter(col("a") > 1), [Row(3, 2, 1), Row(3, 2, 1)])
    finally:
        context._is_snowpark_connect_compatible_mode = original_value


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="ORDER BY append is not supported in local testing mode",
)
def test_agg_sort_snowpark_connect_compatible(session):
    original_value = context._is_snowpark_connect_compatible_mode

    try:
        context._is_snowpark_connect_compatible_mode = True
        df = session.create_dataframe(
            [(1, 2, 3), (3, 2, 1), (3, 2, 1)], ["a", "b", "c"]
        )
        df1 = df.group_by("a").agg(sum_("c")).sort("a", ascending=False)
        Utils.check_answer(df1, [Row(3, 2), Row(1, 3)])

        # Test with grouping function
        df2 = df.group_by("a", "b").agg(
            sum_("c").alias("sum_c"),
            grouping("a").alias("ga"),
            grouping("b").alias("gb"),
        )
        # Sort on grouping column
        df3 = df2.sort(col("ga").desc(), col("gb").desc())
        Utils.check_answer(df3, [Row(1, 2, 3, 0, 0), Row(3, 2, 2, 0, 0)])

        # Test that sorting on grouping without group by is invalid
        with pytest.raises(
            SnowparkSQLException, match="GROUPING function without a GROUP BY"
        ):
            df.sort(grouping("a")).collect()

        # original behavior on dataframe without group by
        df4 = df.sort(col("a"))
        Utils.check_answer(df4, [Row(1, 2, 3), Row(3, 2, 1), Row(3, 2, 1)])
    finally:
        context._is_snowpark_connect_compatible_mode = original_value


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING and ORDER BY append are not supported in local testing mode",
)
def test_agg_filter_and_sort_with_grouping_snowpark_connect_compatible(session):
    original_value = context._is_snowpark_connect_compatible_mode

    try:
        context._is_snowpark_connect_compatible_mode = True
        df = session.create_dataframe(
            [
                ("dotNET", 2012, 10000),
                ("Java", 2012, 20000),
                ("dotNET", 2012, 5000),
                ("dotNET", 2013, 48000),
                ("Java", 2013, 30000),
            ],
            ["course", "year", "earnings"],
        )

        # Test 1: cube with filter on grouping columns and orderBy
        df1 = (
            df.cube("course", "year")
            .agg(
                sum_("earnings").alias("sum_earnings"),
                grouping("course").alias("gc"),
                grouping("year").alias("gy"),
                grouping_id("course", "year").alias("gid"),
            )
            .filter((col("gy") == 1) & (col("gid") > 0))
            .sort("course", "year")
        )

        # Verify results
        Utils.check_answer(
            df1,
            [
                Row(None, None, 113000, 1, 1, 3),
                Row("Java", None, 50000, 0, 1, 1),
                Row("dotNET", None, 63000, 0, 1, 1),
            ],
        )

        # Test 2: cube with orderBy on grouping columns
        df2 = (
            df.cube("course", "year")
            .agg(
                sum_("earnings").alias("sum_earnings"),
                grouping("course").alias("gc"),
                grouping("year").alias("gy"),
            )
            .sort(col("gc"), col("gy"), "course", "year")
        )

        # Verify results - first rows should have gc=0, gy=0
        results2 = df2.collect()
        assert len(results2) == 9  # 3x3 cube results
        # Check first few rows are sorted correctly
        assert results2[0][3] == 0 and results2[0][4] == 0  # gc=0, gy=0
        assert results2[1][3] == 0 and results2[1][4] == 0  # gc=0, gy=0

        # Test 3: rollup with filter and orderBy
        df3 = (
            df.rollup("course")
            .agg(sum_("earnings").alias("sum_earnings"), grouping("course").alias("gc"))
            .filter(col("gc") > 0)
            .sort(col("gc"), "course")
        )

        Utils.check_answer(df3, [Row(None, 113000, 1)])

        # Test 4: Complex filter and sort with multiple grouping functions
        df4 = (
            df.cube("course", "year")
            .agg(
                count("*").alias("count"),
                grouping("course").alias("gc"),
                grouping("year").alias("gy"),
            )
            .filter((col("gc") == 0) | (col("gy") == 0))
            .sort(col("gc").desc(), col("gy").desc(), "course", "year")
        )

        # Verify results include rows where at least one grouping is 0
        results4 = df4.collect()
        for row in results4:
            assert row[3] == 0 or row[4] == 0  # gc=0 or gy=0

        # Test 5: Combination of multiple operations
        df5 = (
            df.rollup("course", "year")
            .agg(
                sum_("earnings").alias("sum_earnings"),
                grouping("course").alias("gc"),
                grouping("year").alias("gy"),
                grouping_id("course", "year").alias("gid"),
            )
            .filter(col("gid") < 3)
            .sort("gid", "course", "year")
        )

        # Verify gid values are less than 3 and sorted
        results5 = df5.collect()
        assert all(row[4] < 3 for row in results5)
        # Check sorting by gid
        for i in range(1, len(results5)):
            assert results5[i - 1][4] <= results5[i][4]  # gid is sorted

        # Test 6: cube with orderBy using grouping in sort expression
        df6 = (
            df.cube("course")
            .agg(count("*").alias("count"), grouping("course").alias("gc"))
            .sort(grouping("course").desc(), "course")
        )

        # First row should have highest grouping value (1)
        results6 = df6.collect()
        assert results6[0][2] == 1  # gc=1 for NULL course
    finally:
        context._is_snowpark_connect_compatible_mode = original_value


def test_filter_sort_limit_snowpark_connect_compatible(session):
    original_value = context._is_snowpark_connect_compatible_mode

    try:
        context._is_snowpark_connect_compatible_mode = True
        df = session.create_dataframe(
            [(1, 2, 3), (3, 2, 1), (3, 2, 1)], ["a", "b", "c"]
        )

        # Basic aggregation with filter, sort, limit - should be in same level
        agg_df = df.group_by("a").agg(
            sum_("b").alias("sum_b"), count("c").alias("count_c")
        )
        result_df1 = agg_df.filter(col("sum_b") > 1).sort("a").limit(10)

        # Check the result
        Utils.check_answer(result_df1, [Row(1, 2, 1), Row(3, 4, 2)])

        # Check that filter, sort, and limit are in the same query level (single SELECT)
        query1 = result_df1.queries["queries"][-1]
        # Count SELECT statements - should be 3 for operations in same level
        assert query1.upper().count("SELECT") == 3
        assert "ORDER BY" in query1.upper()
        assert "LIMIT" in query1.upper()
        assert "HAVING" in query1.upper()

        # Duplicate sort operations - second sort should be in next level
        result_df2 = agg_df.sort("a").sort("sum_b")

        # Check the result
        Utils.check_answer(result_df2, [Row(1, 2, 1), Row(3, 4, 2)])

        # Check that the second sort creates a new query level
        query2 = result_df2.queries["queries"][-1]
        # Should have 4 SELECT statements for nested query
        assert query2.upper().count("SELECT") == 4

        # filter.sort().limit().sort() - last sort should be in next level
        result_df3 = (
            agg_df.filter(col("count_c") >= 1)
            .sort("a")
            .limit(10)
            .sort("sum_b", ascending=False)
        )

        # Check the result
        Utils.check_answer(result_df3, [Row(3, 4, 2), Row(1, 2, 1)])

        # Check query structure - should have nested SELECT due to sort after limit
        query3 = result_df3.queries["queries"][-1]
        assert query3.upper().count("SELECT") == 4

        # limit().limit() - second limit should create new level
        result_df5 = agg_df.limit(10).limit(1)

        # Check the result (should return only first row)
        assert result_df5.count() == 1

        # Check query structure - nested due to second limit
        query5 = result_df5.queries["queries"][-1]
        assert query5.upper().count("SELECT") == 4

        # Complex chain - filter().sort().limit().filter().sort()
        result_df6 = (
            agg_df.filter(col("sum_b") >= 2)
            .sort("a")
            .limit(10)
            .filter(col("count_c") > 1)
            .sort("sum_b", ascending=False)
        )

        # Check the result
        Utils.check_answer(result_df6, [Row(3, 4, 2)])

        # Check query structure - should have multiple levels due to operations after limit
        query6 = result_df6.queries["queries"][-1]
        # Should have 4 SELECT statements
        assert query6.upper().count("SELECT") == 4

    finally:
        context._is_snowpark_connect_compatible_mode = original_value
