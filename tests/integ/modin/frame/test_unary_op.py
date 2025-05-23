#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import math
from operator import neg

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.series.test_unary_op import cast_using_snowflake_rules
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

unary_operators = pytest.mark.parametrize("func", [abs, neg])


@unary_operators
@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "dtype",
    [
        "float",
        param(
            "timedelta64[ns]",
            marks=pytest.mark.xfail(
                strict=True, raises=NotImplementedError, reason="SNOW-1620415"
            ),
        ),
    ],
)
def test_df_unary_all_pos(func, dtype):
    data = [[10, 1, 1.5], [3, 2, 0]]
    eval_snowpark_pandas_result(*create_test_dfs(data, dtype=dtype), func)


@unary_operators
@sql_count_checker(query_count=1)
def test_df_unary_mixed_dtypes(func):
    data = [[-10, 1, 1.5], [100000, math.e, 3], [-100000, math.pi, 1]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, func)


@unary_operators
@pytest.mark.parametrize(
    "index",
    [
        "a",
        ["a", "b"],
        ["c", "a"],
    ],
)
@sql_count_checker(query_count=1)
def test_df_unary_index(func, index):
    data = [[-10, 1, 1.5], [100000, math.e, np.nan], [-100000, math.pi, 1]]
    native_df = native_pd.DataFrame(data, columns=["a", "b", "c"]).set_index(index)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, func)


@unary_operators
@sql_count_checker(query_count=1)
def test_df_unary_np_types(func):
    data = [
        [-np.int16(1), 1, 1.5],
        [100000, math.e, np.float64(32.33)],
        [-np.double(2.5), math.pi, np.int8(3)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, func)


@unary_operators
@pytest.mark.parametrize("value_to_test", [True, False, None])
@sql_count_checker(query_count=1)
def test_df_unary_np_none_bool(func, value_to_test):
    data = [[1.33, 2.33, value_to_test], [3, np.int8(15), np.float16(123.123132)]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, func)


@unary_operators
@pytest.mark.parametrize("data", [[1.33, None, False, -3]])
@sql_count_checker(query_count=1)
def test_df_unary_invalid_in_native_negative(func, data):
    # testing and documenting behaviors that work in SF
    # but not in native pandas.
    input_df = np.transpose([data, [3, -np.int8(15), np.float16(123.123132), 1.33]])

    expected_df = native_pd.DataFrame(
        [[cast_using_snowflake_rules(func, x) for x in row] for row in input_df]
    )
    snow_df = pd.DataFrame(input_df)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        func(snow_df),
        native_pd.DataFrame(expected_df),
    )

    with pytest.raises(TypeError, match="bad operand type"):
        func(input_df)


@unary_operators
@pytest.mark.parametrize("data", [[True, True], [False, False], [False, True]])
@sql_count_checker(query_count=0)
def test_df_unary_invalid_in_sf_negative(func, data):
    # testing and documenting behaviors that work in native
    # pandas but not in SF
    df = [[data[0], -np.float64(23.33)], [data[1], -3]]
    snow_df = pd.DataFrame(df)
    with pytest.raises(
        SnowparkSQLException, match="Invalid argument types for function"
    ):
        func(snow_df).to_pandas()


@unary_operators
@pytest.mark.parametrize(
    "invalid_value, expected_sf_error",
    [
        ([None, False, False], "Invalid argument types for function"),
        ([False, True, None], "Invalid argument types for function"),
        (["string_1", "string_2", "string_3"], "is not recognized"),
        ([3, -np.int8(3), "bad_str"], " Numeric value 'bad_str' is not recognized"),
    ],
)
def test_ser_unary_invalid_in_both_native_and_sf_negative(
    func, invalid_value, expected_sf_error
):

    invalid_df = np.transpose([invalid_value, [-np.float16(23.333), 3, -9]])
    native_df = native_pd.DataFrame(invalid_df)
    snow_df = pd.DataFrame(invalid_df)

    with pytest.raises(TypeError, match="bad operand type"):
        func(native_df)

    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException, match=expected_sf_error):
            func(snow_df).to_pandas()
