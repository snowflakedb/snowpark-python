#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import decimal
import math

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    covar_pop,
    max as max_,
    mean,
    median,
    min as min_,
    sum as sum_,
)
from tests.utils import Utils


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
def test_df_agg_invalid_args_in_list(session):
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


@pytest.mark.localtest
def test_df_agg_empty_args(session):
    """Test for making sure dict when passed to agg() works as expected"""

    df = session.create_dataframe([[1, 4], [1, 4], [2, 5], [2, 6]]).to_df(
        ["first", "second"]
    )

    Utils.assert_rows(df.agg({}).collect(), [Row(1, 4)])


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
def test_agg_single_column(session, local_testing_mode):
    # TODO: SNOW-1348452 precision
    val = "86.333333" if not local_testing_mode else "86.33333333333333"
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


@pytest.mark.localtest
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
