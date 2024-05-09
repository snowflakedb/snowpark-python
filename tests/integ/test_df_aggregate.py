#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import decimal
import math

import pytest

import snowflake.snowpark.mock._functions as snowpark_mock_functions
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    approx_percentile_combine,
    array_agg,
    avg,
    col,
    count,
    covar_pop,
    covar_samp,
    function,
    grouping,
    listagg,
    lit,
    max as max_,
    mean,
    median,
    min as min_,
    stddev,
    stddev_pop,
    sum as sum_,
)
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator, ColumnType
from snowflake.snowpark.types import DoubleType
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


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
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


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
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


@pytest.mark.localtest
def test_agg_function_multiple_parameters(session):
    origin_df = session.create_dataframe(["k1", "k1", "k3", "k4", [None]], schema=["v"])
    assert origin_df.select(listagg("v", delimiter='~!1,."')).collect() == [
        Row('k1~!1,."k1~!1,."k3~!1,."k4')
    ]

    assert origin_df.select(
        listagg("v", delimiter='~!1,."', is_distinct=True)
    ).collect() == [Row('k1~!1,."k3~!1,."k4')]


@pytest.mark.localtest
def test_register_new_methods(session, local_testing_mode):
    if not local_testing_mode:
        pytest.skip("mock implementation does not apply to live code")

    origin_df = session.create_dataframe(
        [
            [10.0, 11.0],
            [20.0, 22.0],
            [25.0, 0.0],
            [30.0, 35.0],
        ],
        schema=["m", "n"],
    )

    # approx_percentile
    with pytest.raises(NotImplementedError):
        origin_df.select(function("approx_percentile")(col("m"), lit(0.5))).collect()
        # snowflake.snowpark.functions.approx_percentile is being updated to use lit
        # so `function` won't be needed here.

    @snowpark_mock_functions.patch("approx_percentile")
    def mock_approx_percentile(
        column: ColumnEmulator, percentile: float
    ) -> ColumnEmulator:
        assert column.tolist() == [10.0, 20.0, 25.0, 30.0]
        assert percentile == 0.5
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    assert origin_df.select(
        function("approx_percentile")(col("m"), lit(0.5))
    ).collect() == [Row(123)]

    # covar_samp
    with pytest.raises(NotImplementedError):
        origin_df.select(covar_samp(col("m"), "n")).collect()

    @snowpark_mock_functions.patch(covar_samp)
    def mock_covar_samp(
        column1: ColumnEmulator,
        column2: ColumnEmulator,
    ):
        assert column1.tolist() == [10.0, 20.0, 25.0, 30.0]
        assert column2.tolist() == [11.0, 22.0, 0.0, 35.0]
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    assert origin_df.select(covar_samp(col("m"), "n")).collect() == [Row(123)]

    # stddev
    with pytest.raises(NotImplementedError):
        origin_df.select(stddev("n")).collect()

    @snowpark_mock_functions.patch(stddev)
    def mock_stddev(column: ColumnEmulator):
        assert column.tolist() == [11.0, 22.0, 0.0, 35.0]
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    assert origin_df.select(stddev("n")).collect() == [Row(123)]

    # array_agg
    with pytest.raises(NotImplementedError):
        origin_df.select(array_agg("n", False)).collect()

    # instead of kwargs, positional argument also works
    @snowpark_mock_functions.patch(array_agg)
    def mock_mock_array_agg(column: ColumnEmulator, is_distinct):
        assert is_distinct is True
        assert column.tolist() == [11.0, 22.0, 0.0, 35.0]
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    assert origin_df.select(array_agg("n", True)).collect() == [Row(123)]

    # grouping
    with pytest.raises(NotImplementedError):
        origin_df.select(grouping("m", col("n"))).collect()

    @snowpark_mock_functions.patch(grouping)
    def mock_mock_grouping(*columns):
        assert len(columns) == 2
        assert columns[0].tolist() == [10.0, 20.0, 25.0, 30.0]
        assert columns[1].tolist() == [11.0, 22.0, 0.0, 35.0]
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    assert origin_df.select(grouping("m", col("n"))).collect() == [Row(123)]


@pytest.mark.localtest
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


@pytest.mark.localtest
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

    snowpark_mock_functions._unregister_func_implementation("stddev")
    snowpark_mock_functions._unregister_func_implementation("stddev_pop")

    with pytest.raises(NotImplementedError):
        origin_df.select(stddev("n"), stddev_pop("m")).collect()

    @snowpark_mock_functions.patch("stddev")
    def mock_stddev(column: ColumnEmulator):
        assert column.tolist() == [11.0, 22.0, 9.0, 9.0, 35.0, 99.0]
        return ColumnEmulator(data=123, sf_type=ColumnType(DoubleType(), False))

    # stddev_pop is not implemented yet
    with pytest.raises(NotImplementedError):
        origin_df.select(stddev("n"), stddev_pop("m")).collect()

    @snowpark_mock_functions.patch("stddev_pop")
    def mock_stddev_pop(column: ColumnEmulator):
        assert column.tolist() == [15.0, 2.0, 29.0, 30.0, 4.0, 54.0]
        return ColumnEmulator(data=456, sf_type=ColumnType(DoubleType(), False))

    Utils.check_answer(
        origin_df.select(stddev("n"), stddev_pop("m")).collect(), Row(123.0, 456.0)
    )
