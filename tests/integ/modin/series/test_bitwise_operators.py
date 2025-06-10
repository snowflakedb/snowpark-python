#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import operator
from typing import Any

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pandas.testing as tm
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

# this is full test data
# SCALAR_BITWISE_TEST_DATA = [-1, 0, 1, True, False]
# SERIES_BITWISE_TEST_DATA = [
#     native_pd.Series([1, 2, 3, 4]),
#     native_pd.Series([True, False, True, False]),
#     native_pd.Series([1, 2, None, 3], dtype="Int64"),
#     native_pd.Series([True, None, False, True], dtype="bool"),
#     native_pd.Series([20121231, 20141231, 99991231, True], dtype="object"),
# ]
# BITWISE_TEST_DATA = SCALAR_BITWISE_TEST_DATA + SERIES_BITWISE_TEST_DATA

# this is Snowflake compatible test data, where mapping actually works
SCALAR_BITWISE_TEST_DATA = [-1, True, False]
SERIES_BITWISE_TEST_DATA = [
    native_pd.Series([True, False, True, False]),
    native_pd.Series([True, None, False, True], dtype="bool"),
]
BITWISE_TEST_DATA = SCALAR_BITWISE_TEST_DATA + SERIES_BITWISE_TEST_DATA


def try_cast_to_snow_series(value: Any) -> Any:
    if isinstance(value, native_pd.Series):
        return pd.Series(
            data=value.values, dtype=value.dtype, name=value.name, index=value.index
        )

    return pd.Series(value)


@pytest.mark.parametrize("value", BITWISE_TEST_DATA)
@sql_count_checker(query_count=1)
def test_bitwise_unary(value):

    # Note: In pandas, using NaN values without specfiying a null-compatible dtype will yield an error.
    # SnowPandas will allow this behavior.
    # Note: NaN values like pd.NA, pd.NaT, np.nan will raise a TypeError: boolean value of NA is ambiguous
    snow_value = try_cast_to_snow_series(value)

    eval_snowpark_pandas_result(snow_value, native_pd.Series(value), lambda s: ~s)


@pytest.mark.parametrize("series", SERIES_BITWISE_TEST_DATA)
@pytest.mark.parametrize("scalar", SCALAR_BITWISE_TEST_DATA)
@pytest.mark.parametrize(
    "op", [operator.or_, operator.and_]
)  # |, &.  ^ is not supported in Snowflake
@sql_count_checker(query_count=2)
def test_bitwise_binary_scalar(series, scalar, op):
    def check_op(native_lhs, native_rhs, snow_lhs, snow_rhs):
        snow_ans = op(snow_lhs, snow_rhs)
        native_ans = op(native_lhs, native_rhs)
        eval_snowpark_pandas_result(snow_ans, native_ans, lambda x: x)

    # check with left side being a series
    check_op(series, scalar, pd.Series(series), scalar)

    # check with right side being a series
    check_op(scalar, series, scalar, pd.Series(series))


@pytest.mark.parametrize(
    "lhs,rhs",
    [
        (native_pd.Series(), native_pd.Series()),
        (
            native_pd.Series([True, False, True, False]),
            native_pd.Series([True, True, False, False]),
        ),
        (
            native_pd.Series([True, False, True, False], name="A"),
            native_pd.Series([True, True, False, False]),
        ),
        (
            native_pd.Series([True, False, True, False], name="A"),
            native_pd.Series([True, True, False, False], name="B"),
        ),
        (
            native_pd.Series([True, False, True, False], name="A"),
            native_pd.Series([True, True, False, False], name="A"),
        ),
        (
            native_pd.Series([True, False, True, False], index=[5, 2, 8, 9]),
            native_pd.Series([True, True, False, False], index=[9, 8, 2, 5]),
        ),
        # multi-index series
        (
            native_pd.Series(
                [True, False, True, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(2, 0), (0, 0), (2, 1), (1, 0)]
                ),
            ),
            native_pd.Series(
                [True, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(0, 0), (1, 0), (2, 0), (2, 1)]
                ),
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "op", [operator.or_, operator.and_]
)  # |, &.  ^ is not supported in Snowflake
@sql_count_checker(query_count=2, join_count=2)
def test_bitwise_binary_between_series(lhs, rhs, op):
    def check_op(native_lhs, native_rhs, snow_lhs, snow_rhs):
        snow_ans = op(snow_lhs, snow_rhs)
        native_ans = op(native_lhs, native_rhs)
        # for one multi-index test case (marked with comment) the "inferred_type" doesn't match (Snowpark: float vs. pandas integer)
        eval_snowpark_pandas_result(
            snow_ans, native_ans, lambda s: s, check_index_type=False
        )

    check_op(lhs, rhs, try_cast_to_snow_series(lhs), try_cast_to_snow_series(rhs))

    # commute series
    check_op(rhs, lhs, try_cast_to_snow_series(rhs), try_cast_to_snow_series(lhs))


# Due to differences in logical or/and in SQL and pandas' |,& implementation, behavior doesn't match here, in particular
# this test case differs between pandas and Snowpark pandas
# pandas:
# or: data=[True, False, True, False, True, False], index=[1, 2, 3, 4, 5, 7]
# and: data=[False, False, False, False, False, False], index=[1, 2, 3, 4, 5, 7]
# Snowpark pandas:
# or: data=[True, True, True, None, True, None], index=[1, 2, 3, 4, 5, 7]
# and: data=[None, None, False, False, False, False], index=[1, 2, 3, 4, 5, 7]
# (
#     native_pd.Series([True, False, True, False], index=[1, 3, 5, 7]),
#     native_pd.Series([True, True, False, False], index=[2, 3, 4, 5]),
# ),

# this test case also differs between pandas and Snowpark pandas
# pandas:
# or: [True, True, False, True, False, False, True, False, False]
# and: [True, False, False, False, False, False, False, False, False]
# Snowpark pandas:
# or: [True, True, True, True, False, None, True, None, None]
# and: [True, False, None, False, False, False, None, False, None]
# (
#     native_pd.Series([True, False, None, True, False, None, True, False, None]),
#     native_pd.Series([True, True, True, False, False, False, None, None, None]),
# ),
@pytest.mark.parametrize(
    "lhs,rhs,expected_pandas,expected_snowpark_pandas",
    [
        (
            native_pd.Series([True, False, True, False], index=[1, 3, 5, 7]),
            native_pd.Series([True, True, False, False], index=[2, 3, 4, 5]),
            native_pd.Series(
                [True, False, True, False, True, False], index=[1, 2, 3, 4, 5, 7]
            ),
            native_pd.Series(
                data=[True, True, True, None, True, None], index=[1, 2, 3, 4, 5, 7]
            ),
        ),
        (
            native_pd.Series([True, False, None, True, False, None, True, False, None]),
            native_pd.Series([True, True, True, False, False, False, None, None, None]),
            native_pd.Series(
                [True, True, False, True, False, False, True, False, False]
            ),
            native_pd.Series([True, True, True, True, False, None, True, None, None]),
        ),
        (
            native_pd.Series(
                [True, False, True, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(2, 0), (0, 0), (2, 1), (1, 0)], names=["y", "x"]
                ),
            ),
            native_pd.Series(
                [True, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(0, 0), (1, 0), (2, 0), (2, 1)], names=["x", "z"]
                ),
            ),
            native_pd.Series(
                [True, True, True, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [
                        (2, 0, 0),
                        (0, 0, 0),
                        (1, 0, 0),
                        (2, 1, 0),
                        (None, 2, 0),
                        (None, 2, 1),
                    ],
                    names=[
                        "y",
                        "x",
                        "z",
                    ],  # pandas 2.2.x introduces a new, weird order, In pandas 2.1.x the order is correctly preserved.
                ),
            ),
            native_pd.Series(
                [True, True, True, True, None, None],
                index=native_pd.MultiIndex.from_tuples(
                    [
                        (2.0, 0, 0),
                        (0.0, 0, 0),
                        (1.0, 0, 0),
                        (2.0, 1, 0),
                        (np.nan, 2, 0),
                        (np.nan, 2, 1),
                    ],
                    names=["y", "x", "z"],
                ),
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_bitwise_binary_between_series_with_deviating_behavior_or(
    lhs, rhs, expected_pandas, expected_snowpark_pandas
):
    snow_ans = try_cast_to_snow_series(lhs) | try_cast_to_snow_series(rhs)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_ans, expected_snowpark_pandas
    )

    # test here pandas to track any version regressions
    native_ans = lhs | rhs
    tm.assert_series_equal(native_ans, expected_pandas, check_index_type=False)


@pytest.mark.parametrize(
    "lhs,rhs,expected_pandas,expected_snowpark_pandas",
    [
        (
            native_pd.Series([True, False, True, False], index=[1, 3, 5, 7]),
            native_pd.Series([True, True, False, False], index=[2, 3, 4, 5]),
            native_pd.Series(
                [False, False, False, False, False, False], index=[1, 2, 3, 4, 5, 7]
            ),
            native_pd.Series(
                [None, None, False, False, False, False], index=[1, 2, 3, 4, 5, 7]
            ),
        ),
        (
            native_pd.Series([True, False, None, True, False, None, True, False, None]),
            native_pd.Series([True, True, True, False, False, False, None, None, None]),
            native_pd.Series(
                [True, False, False, False, False, False, False, False, False]
            ),
            native_pd.Series(
                [True, False, None, False, False, False, None, False, None]
            ),
        ),
        (
            native_pd.Series(
                [True, False, True, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(2, 0), (0, 0), (2, 1), (1, 0)], names=["y", "x"]
                ),
            ),
            native_pd.Series(
                [True, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [(0, 0), (1, 0), (2, 0), (2, 1)], names=["x", "z"]
                ),
            ),
            native_pd.Series(
                [True, False, False, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [
                        (2, 0, 0),
                        (0, 0, 0),
                        (1, 0, 0),
                        (2, 1, 0),
                        (None, 2, 0),
                        (None, 2, 1),
                    ],
                    names=[
                        "y",
                        "x",
                        "z",
                    ],  # pandas 2.2.x introduces a new, weird order, In pandas 2.1.x the order is correctly preserved.
                ),
            ),
            native_pd.Series(
                [True, False, False, True, False, False],
                index=native_pd.MultiIndex.from_tuples(
                    [
                        (2.0, 0, 0),
                        (0.0, 0, 0),
                        (1.0, 0, 0),
                        (2.0, 1, 0),
                        (np.nan, 2, 0),
                        (np.nan, 2, 1),
                    ],
                    names=["y", "x", "z"],
                ),
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_bitwise_binary_between_series_with_deviating_behavior_and(
    lhs, rhs, expected_pandas, expected_snowpark_pandas
):
    snow_ans = try_cast_to_snow_series(lhs) & try_cast_to_snow_series(rhs)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_ans, expected_snowpark_pandas
    )

    # test here pandas to track any version regressions
    native_ans = lhs & rhs
    print(native_ans.index)
    tm.assert_series_equal(native_ans, expected_pandas, check_index_type=False)
