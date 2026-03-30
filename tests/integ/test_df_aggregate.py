#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import decimal
import math
from unittest import mock

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
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="ORDER BY append is not supported in local testing mode",
)
def test_agg_sort_snowpark_connect_compatible(session):
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
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


def test_agg_no_grouping_exprs_limit_snowpark_connect_compatible(session):
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        df = session.create_dataframe([[1, 2], [3, 4], [1, 4]], schema=["A", "B"])
        result = df.agg(sum_(col("a"))).limit(2)
        Utils.check_answer(result, [Row(5)])
        result = df.group_by().agg(sum_(col("b"))).limit(2)
        Utils.check_answer(result, [Row(10)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING and ORDER BY append are not supported in local testing mode",
)
def test_agg_filter_and_sort_with_grouping_snowpark_connect_compatible(session):
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING, ORDER BY append, and limit append are not supported in local testing mode",
)
def test_filter_sort_limit_snowpark_connect_compatible(session, sql_simplifier_enabled):
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        df = session.create_dataframe(
            [(1, 2, 3), (3, 2, 1), (3, 2, 1)], ["a", "b", "c"]
        )

        # Basic aggregation with filter, sort, limit - should be in same level
        agg_df = df.group_by("a").agg(
            sum_("b").alias("sum_b"), count("c").alias("count_c")
        )
        result_df1 = agg_df.filter(col("sum_b") > 1).sort("a").limit(10)
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
        Utils.check_answer(result_df2, [Row(1, 2, 1), Row(3, 4, 2)])
        # Check that the second sort creates a new query level
        query2 = result_df2.queries["queries"][-1]
        assert query2.upper().count("SELECT") == 3

        # filter.sort().limit().sort() - last sort should be in next level
        result_df3 = (
            agg_df.filter(col("count_c") >= 1)
            .sort("a")
            .limit(10)
            .sort("sum_b", ascending=False)
        )
        Utils.check_answer(result_df3, [Row(3, 4, 2), Row(1, 2, 1)])
        # Check query structure - should have nested SELECT due to sort after limit
        query3 = result_df3.queries["queries"][-1]
        assert query3.upper().count("SELECT") == 4

        # limit().limit() - second limit should create new level
        result_df5 = agg_df.limit(10).limit(1)
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
        Utils.check_answer(result_df6, [Row(3, 4, 2)])

        # Check query structure - should have multiple levels due to operations after limit
        query6 = result_df6.queries["queries"][-1]
        assert query6.upper().count("SELECT") == 4 if sql_simplifier_enabled else 5


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING, ORDER BY append, and limit append are not supported in local testing mode",
)
def test_group_by_agg_sort_filter_sanity(session):
    """
    Tests that post-aggregation clauses (HAVING, ORDER BY, LIMIT) are emitted in valid SQL order
    regardless of the DataFrame call order (see SNOW-3266495).

    After a GROUP BY, HAVING must appear before ORDER BY, which in turn must appear before LIMIT.
    """
    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        df = session.createDataFrame(
            [
                (1, "engineering", 80000),
                (2, "engineering", 90000),
                (3, "sales", 50000),
                (4, "sales", 60000),
                (5, "hr", 45000),
                (6, "hr", 55000),
                (7, "engineering", 85000),
            ],
            ["id", "dept", "salary"],
        )
        agg_df = df.groupBy("dept").agg(
            count("*").alias("headcount"),
            avg("salary").alias("avg_salary"),
        )
        # Checking against exact query text structure is less than ideal, but these tests need to
        # verify the level of nesting of certain sub-queries, making it a necessary evil.
        agg_query_base = """
        SELECT "DEPT", count(1) AS "HEADCOUNT", avg("SALARY") AS "AVG_SALARY"
        FROM (
            SELECT "ID", "DEPT", "SALARY" FROM (
                SELECT $1 AS "ID", $2 AS "DEPT", $3 AS "SALARY" FROM VALUES
                (1 :: INT, 'engineering' :: STRING, 80000 :: INT),
                (2 :: INT, 'engineering' :: STRING, 90000 :: INT),
                (3 :: INT, 'sales' :: STRING, 50000 :: INT),
                (4 :: INT, 'sales' :: STRING, 60000 :: INT),
                (5 :: INT, 'hr' :: STRING, 45000 :: INT),
                (6 :: INT, 'hr' :: STRING, 55000 :: INT),
                (7 :: INT, 'engineering' :: STRING, 85000 :: INT)
            )
        )
        GROUP BY "DEPT"
        """

        def check_agg_sql(df, expected_sql):
            assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
                expected_sql
            )

        base_expected_result = [
            Row("engineering", 3, 85000.0),
            Row("sales", 2, 55000.0),
            Row("hr", 2, 50000.0),
        ]

        # sort -> filter: ORDER BY before HAVING in user code, but SQL must be HAVING before ORDER BY.
        result1 = agg_df.orderBy(col("avg_salary").desc()).filter(col("headcount") > 1)
        Utils.check_answer(result1, base_expected_result)
        check_agg_sql(
            result1,
            f"""
            {agg_query_base}
            HAVING ("HEADCOUNT" > 1)
            ORDER BY "AVG_SALARY" DESC NULLS LAST
            """,
        )

        # filter -> sort: already in correct SQL clause order.
        result2 = agg_df.filter(col("headcount") > 1).orderBy(col("avg_salary").desc())
        Utils.check_answer(result2, base_expected_result)
        check_agg_sql(
            result2,
            f"""
            {agg_query_base}
            HAVING ("HEADCOUNT" > 1)
            ORDER BY "AVG_SALARY" DESC NULLS LAST
            """,
        )

        # sort -> filter -> limit (must swap filter with sort)
        result3 = (
            agg_df.orderBy(col("avg_salary").desc())
            .filter(col("headcount") > 1)
            .limit(2)
        )
        Utils.check_answer(
            result3,
            [
                Row("engineering", 3, 85000.0),
                Row("sales", 2, 55000.0),
            ],
        )
        check_agg_sql(
            result3,
            f"""
            {agg_query_base}
            HAVING ("HEADCOUNT" > 1)
            ORDER BY "AVG_SALARY" DESC NULLS LAST
            LIMIT 2 OFFSET 0
            """,
        )

        # A new select between sort and filter should break the
        # _ops_after_agg chain, so the subsequent filter uses a regular
        # WHERE via subquery rather than a flattened HAVING.
        result4 = (
            agg_df.orderBy(col("avg_salary").desc())
            .select("dept", "headcount", "avg_salary")
            .filter(col("headcount") > 1)
        )
        Utils.check_answer(result4, base_expected_result)
        check_agg_sql(
            result4,
            (
                f"""
            SELECT "DEPT", "HEADCOUNT", "AVG_SALARY"
            FROM (
                {agg_query_base}
                ORDER BY "AVG_SALARY" DESC NULLS LAST
            )
            WHERE ("HEADCOUNT" > 1)
            """
                if session.sql_simplifier_enabled
                else f"""
            SELECT * FROM (
                SELECT "DEPT", "HEADCOUNT", "AVG_SALARY"
                FROM (
                    {agg_query_base}
                    ORDER BY "AVG_SALARY" DESC NULLS LAST
                )
            )
            WHERE ("HEADCOUNT" > 1)
            """
            ),
        )

        # Repeated sort: all clauses are placed in a single ORDER BY clause, in the reverse
        # order of their declaration. Note that referencing the same column multiple times is valid.
        result5 = (
            agg_df.orderBy(col("avg_salary").asc())
            .filter(col("headcount") > 1)
            .orderBy(col("avg_salary").desc())
            .orderBy(col("headcount").asc())
        )
        Utils.check_answer(
            result5,
            [
                Row("sales", 2, 55000.0),
                Row("hr", 2, 50000.0),
                Row("engineering", 3, 85000.0),
            ],
        )
        check_agg_sql(
            result5,
            f"""
            {agg_query_base}
            HAVING ("HEADCOUNT" > 1)
            ORDER BY "HEADCOUNT" ASC NULLS FIRST,
                "AVG_SALARY" DESC NULLS LAST,
                "AVG_SALARY" ASC NULLS FIRST
            """,
        )

        # Repeated filter: each clause is ANDed together.
        result6 = (
            agg_df.filter(col("headcount") > 1)
            .orderBy(col("avg_salary").desc())
            .filter(col("avg_salary") > 50000)
        )
        Utils.check_answer(
            result6,
            [
                Row("engineering", 3, 85000.0),
                Row("sales", 2, 55000.0),
            ],
        )
        check_agg_sql(
            result6,
            f"""
            {agg_query_base}
            HAVING (("HEADCOUNT" > 1) AND ("AVG_SALARY" > 50000))
            ORDER BY "AVG_SALARY" DESC NULLS LAST
            """,
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="HAVING, ORDER BY append, and limit append are not supported in local testing mode",
)
def test_group_by_agg_sort_filter_limit_ordering(session):
    """
    Tests that aggregations involving HAVING and ORDER BY/LIMIT apply operations in the correct order.

    ORDER BY -> LIMIT -> FILTER and FILTER -> ORDER BY -> LIMIT do not generally commute.
    """
    df = session.createDataFrame(
        [
            (1, "engineering", 80000),
            (2, "engineering", 90000),
            (3, "sales", 50000),
            (4, "sales", 60000),
            (5, "hr", 45000),
            (6, "hr", 55000),
            (7, "engineering", 85000),
            (8, "research", 90000),
            (9, "research", 100000),
            (10, "research", 140000),
            (11, "AAA", 130000),
        ],
        ["id", "dept", "salary"],
    )
    agg_df = df.groupBy("dept").agg(
        count("*").alias("headcount"),
        avg("salary").alias("avg_salary"),
    )

    # 1. ORDER BY -> LIMIT -> FILTER
    # The ordering drops the AAA group before the filter occurs.
    result1 = (
        agg_df.orderBy(col("avg_salary").asc())
        .limit(3)
        .filter(col("avg_salary") > 54000)
    )
    Utils.check_answer(
        result1,
        [
            Row("sales", 2, 55000.0),
            Row("engineering", 3, 85000.0),
        ],
    )

    # 2. FILTER -> ORDER BY -> LIMIT
    # This is different from case (1), as filtering occurs before ordering/limiting.
    result2 = (
        agg_df.filter(col("avg_salary") > 54000)
        .orderBy(col("avg_salary").asc())
        .limit(3)
    )
    Utils.check_answer(
        result2,
        [
            Row("sales", 2, 55000.0),
            Row("engineering", 3, 85000.0),
            Row("research", 3, 110000.0),
        ],
    )

    # 3. FILTER -> ORDER BY -> LIMIT -> FILTER
    # The ordering drops the RESEARCH group, so even though it should survive the final filter, it
    # gets dropped from the final result.
    result3 = (
        agg_df.filter(col("headcount") > 1)
        .orderBy(col("avg_salary").asc())
        .limit(3)
        .filter(col("avg_salary") > 54000)
    )
    Utils.check_answer(
        result3,
        [
            Row("sales", 2, 55000.0),
            Row("engineering", 3, 85000.0),
        ],
    )

    # 4. FILTER -> ORDER BY -> LIMIT -> FILTER -> LIMIT
    # The final limit is not necessarily deterministic, but does not commute with the prior LIMIT.
    result4 = result2.limit(1)
    Utils.check_answer(result4, [Row("sales", 2, 55000.0)])

    # 5. ORDER BY -> LIMIT -> ORDER BY -> LIMIT
    # The RESEARCH group is dropped by the first ORDER BY + LIMIT.
    # The SALES and HR groups are dropped by the second ORDER BY + LIMIT.
    result5 = (
        agg_df.orderBy(col("headcount").asc())
        .limit(4)
        .orderBy(col("avg_salary").desc())
        .limit(2)
    )
    Utils.check_answer(
        result5, [Row("AAA", 1, 130000.0), Row("engineering", 3, 85000.0)]
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="exclude_grouping_columns is not supported",
)
def test_group_by_exclude_grouping_columns(session):
    """Test the exclude_grouping_columns parameter for all aggregate functions."""

    # Create test data
    df = session.create_dataframe(
        [
            ("A", "X", 1, 100),
            ("A", "X", 2, 200),
            ("A", "Y", 3, 300),
            ("B", "X", 4, 400),
            ("B", "Y", 5, 500),
            ("B", "Y", 6, 600),
        ],
        schema=["group1", "group2", "value1", "value2"],
    )

    # Test agg() with exclude_grouping_columns
    # Default behavior (include grouping columns)
    result_default = df.group_by("group1").agg(sum_("value1").alias("sum_v1")).collect()
    assert len(result_default[0]) == 2  # group1 + sum_v1
    Utils.check_answer(result_default, [Row("A", 6), Row("B", 15)])

    # Exclude grouping columns
    result_exclude = (
        df.group_by("group1")
        .agg(sum_("value1").alias("sum_v1"), exclude_grouping_columns=True)
        .collect()
    )
    assert len(result_exclude[0]) == 1  # only sum_v1
    print(result_exclude)
    Utils.check_answer(result_exclude, [Row(6), Row(15)])

    # Test with multiple grouping columns
    result_multi_default = (
        df.group_by("group1", "group2").agg(sum_("value1").alias("sum_v1")).collect()
    )
    assert len(result_multi_default[0]) == 3  # group1 + group2 + sum_v1

    result_multi_exclude = (
        df.group_by("group1", "group2")
        .agg(sum_("value1").alias("sum_v1"), exclude_grouping_columns=True)
        .collect()
    )
    assert len(result_multi_exclude[0]) == 1  # only sum_v1
    # Group by produces [('A', 'X', 3), ('A', 'Y', 3), ('B', 'X', 4), ('B', 'Y', 11)]
    Utils.check_answer(result_multi_exclude, [Row(3), Row(3), Row(4), Row(11)])

    # Test with multiple aggregations
    result_multi_agg = (
        df.group_by("group1")
        .agg(
            sum_("value1").alias("sum_v1"),
            avg("value2").alias("avg_v2"),
            exclude_grouping_columns=True,
        )
        .collect()
    )
    assert len(result_multi_agg[0]) == 2  # sum_v1 + avg_v2
    Utils.check_answer(result_multi_agg, [Row(6, 200.0), Row(15, 500.0)])

    # Test count() with exclude_grouping_columns
    result_count_default = df.group_by("group1").count().collect()
    assert len(result_count_default[0]) == 2  # group1 + count
    Utils.check_answer(result_count_default, [Row("A", 3), Row("B", 3)])

    result_count_exclude = (
        df.group_by("group1").count(exclude_grouping_columns=True).collect()
    )
    assert len(result_count_exclude[0]) == 1  # only count
    Utils.check_answer(result_count_exclude, [Row(3), Row(3)])

    # Test avg() with exclude_grouping_columns
    result_avg_default = df.group_by("group1").avg("value1").collect()
    assert len(result_avg_default[0]) == 2  # group1 + avg

    result_avg_exclude = (
        df.group_by("group1").avg("value1", exclude_grouping_columns=True).collect()
    )
    assert len(result_avg_exclude[0]) == 1  # only avg
    Utils.check_answer(result_avg_exclude, [Row(2.0), Row(5.0)])

    # Test sum() with exclude_grouping_columns
    result_sum_default = df.group_by("group1").sum("value1", "value2").collect()
    assert len(result_sum_default[0]) == 3  # group1 + sum(value1) + sum(value2)

    result_sum_exclude = (
        df.group_by("group1")
        .sum("value1", "value2", exclude_grouping_columns=True)
        .collect()
    )
    assert len(result_sum_exclude[0]) == 2  # only sums
    Utils.check_answer(result_sum_exclude, [Row(6, 600), Row(15, 1500)])

    # Test min() with exclude_grouping_columns
    result_min_default = df.group_by("group1").min("value1").collect()
    assert len(result_min_default[0]) == 2  # group1 + min

    result_min_exclude = (
        df.group_by("group1").min("value1", exclude_grouping_columns=True).collect()
    )
    assert len(result_min_exclude[0]) == 1  # only min
    Utils.check_answer(result_min_exclude, [Row(1), Row(4)])

    # Test max() with exclude_grouping_columns
    result_max_default = df.group_by("group1").max("value1").collect()
    assert len(result_max_default[0]) == 2  # group1 + max

    result_max_exclude = (
        df.group_by("group1").max("value1", exclude_grouping_columns=True).collect()
    )
    assert len(result_max_exclude[0]) == 1  # only max
    Utils.check_answer(result_max_exclude, [Row(3), Row(6)])

    # Test median() with exclude_grouping_columns
    result_median_default = df.group_by("group1").median("value1").collect()
    assert len(result_median_default[0]) == 2  # group1 + median

    result_median_exclude = (
        df.group_by("group1").median("value1", exclude_grouping_columns=True).collect()
    )
    assert len(result_median_exclude[0]) == 1  # only median
    Utils.check_answer(result_median_exclude, [Row(2.0), Row(5.0)])

    # Test function() / builtin() with exclude_grouping_columns
    result_builtin_default = df.group_by("group1").builtin("sum")("value1").collect()
    assert len(result_builtin_default[0]) == 2  # group1 + sum

    result_builtin_exclude = (
        df.group_by("group1")
        .builtin("sum", exclude_grouping_columns=True)("value1")
        .collect()
    )
    assert len(result_builtin_exclude[0]) == 1  # only sum
    Utils.check_answer(result_builtin_exclude, [Row(6), Row(15)])
