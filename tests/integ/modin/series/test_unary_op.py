#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import math

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

unary_operators = pytest.mark.parametrize("func", [abs, lambda x: -x])


def cast_using_snowflake_rules(func, x):
    if x is None:
        return np.nan
    return func(x)


@unary_operators
@sql_count_checker(query_count=1)
def test_ser_unary_all_pos(func):
    data = [10, 1, 1.5]

    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, func)


@unary_operators
@sql_count_checker(query_count=1)
def test_ser_unary_mixed_dtypes(func):
    data = [100000, math.e, np.float16(13.22), np.int32(32323123)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, func)


@unary_operators
@sql_count_checker(query_count=1)
def test_ser_unary_index(func):
    data = [-100000, math.pi, 3]
    native_ser = native_pd.Series(data=data, index=["a", "b", "c"])
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, func)


@unary_operators
@sql_count_checker(query_count=1)
def test_ser_unary_np_types(func):
    data = [-np.int16(1), 1, 1.5]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, func)


@unary_operators
@pytest.mark.parametrize("value_to_test", [True, False, None])
@sql_count_checker(query_count=1)
def test_ser_unary_np_none_bool(func, value_to_test):
    data = [1.33, -2.33, value_to_test]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, func)


@unary_operators
@pytest.mark.parametrize("data", [[None], [-1, None, 2, True], [1.33, None, False, -3]])
@sql_count_checker(query_count=1)
def test_ser_unary_invalid_in_native_negative(func, data):
    # testing and documenting behaviors that work in SF
    # but not in native pandas.

    expected_data = [cast_using_snowflake_rules(func, x) for x in data]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        func(snow_ser), native_pd.Series(expected_data)
    )

    with pytest.raises(TypeError, match="bad operand type"):
        func(native_ser)


@unary_operators
@pytest.mark.parametrize("data", [[True], [False, True]])
@sql_count_checker(query_count=0)
def test_ser_unary_invalid_in_sf_negative(func, data):
    # testing and documenting behaviors that work in native
    # pandas but not in SF
    snow_ser = pd.Series(data)
    with pytest.raises(
        SnowparkSQLException, match="Invalid argument types for function"
    ):
        func(snow_ser).to_pandas()


@unary_operators
@pytest.mark.parametrize(
    "invalid_value, expected_sf_error",
    [
        ([None, True], "Invalid argument types for function"),
        ([False, True, None], "Invalid argument types for function"),
        ([(3, 3), 2], "Failed to cast variant value"),
        ("bad_str", "is not recognized"),
        ([3, "bad_str"], "Failed to cast variant value"),
        ([3, [2, 3]], "Failed to cast variant value"),
    ],
)
def test_ser_unary_invalid_in_both_native_and_sf_negative(
    func, invalid_value, expected_sf_error
):
    native_ser = native_pd.Series(invalid_value)
    snow_ser = pd.Series(native_ser)

    with pytest.raises(TypeError, match="bad operand type"):
        func(native_ser)

    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException, match=expected_sf_error):
            func(snow_ser).to_pandas()
