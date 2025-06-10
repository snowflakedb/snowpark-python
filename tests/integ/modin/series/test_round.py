#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import math
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

all_decimals = pytest.mark.parametrize("decimals", [-2, -1, 0, 1, 2])
zero_only_decimals = pytest.mark.parametrize("decimals", [0])


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round(decimals):
    data = [10, 1, 1.6]

    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round_mixed_dtypes(decimals):
    data = [100000, math.e, np.float16(13.22), np.int32(32323123)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round_index(decimals):
    data = [-100000, math.pi, 3]
    native_ser = native_pd.Series(data=data, index=["a", "b", "c"])
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round_np_types(decimals):
    data = [-np.int16(1), 1, 1.6]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round_neg_odd_half(decimals):
    data = [-np.double(1.5), -np.double(3.5), -np.double(5.5)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@zero_only_decimals
@sql_count_checker(query_count=1)
def test_ser_round_neg_even_half(decimals):
    data = [-np.double(2.5), -np.double(4.5), -np.double(6.5)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # Series.round(), which we want to test.
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        round(snow_ser, decimals),
        round(native_pd.Series(native_ser), decimals) - 1,
    )


@all_decimals
@sql_count_checker(query_count=1)
def test_ser_round_pos_odd_half(decimals):
    data = [np.double(1.5), np.double(3.5), np.double(5.5)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.round(decimals))


@zero_only_decimals
@sql_count_checker(query_count=1)
def test_ser_round_pos_even_half(decimals):
    data = [np.double(2.5), np.double(4.5), np.double(6.5)]
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # DataFrame.round(), which we want to test.
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        round(snow_ser, decimals),
        round(native_pd.Series(native_ser), decimals) + 1,
    )


@all_decimals
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
def test_ser_round_invalid_in_sf_negative(decimals, invalid_value, expected_sf_error):
    # testing and documenting behaviors that work in native
    # pandas but not in SF
    native_ser = native_pd.Series(invalid_value)
    snow_ser = pd.Series(native_ser)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # DataFrame.round(), which we want to test.
    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException, match=expected_sf_error):
            round(snow_ser, decimals).to_pandas()


@all_decimals
def test_round_timedelta_negative(decimals):
    with SqlCounter(query_count=0):
        with pytest.raises(
            NotImplementedError,
            match=re.escape("round is not yet implemented for Timedelta Type"),
        ):
            eval_snowpark_pandas_result(
                *create_test_series(pd.Timedelta(1)), lambda s: s.round(decimals)
            )
