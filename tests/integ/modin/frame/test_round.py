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
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

all_decimals = pytest.mark.parametrize("decimals", [-2, -1, 0, 1, 2])
zero_only_decimals = pytest.mark.parametrize("decimals", [0])


@pytest.mark.modin_sp_precommit
@all_decimals
@sql_count_checker(query_count=1)
def test_df_round(decimals):
    data = [[10, 1, 1.5], [3, 2, 0]]

    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@sql_count_checker(query_count=1)
def test_df_round_dict_decimals():
    data = [[10, 1, 1.5], [3, 2, 0]]
    decimals = {2: 0}

    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_df_round_mixed_dtypes(decimals):
    data = [[-10, 1, 1.5], [100000, math.e, 3], [-100000, math.pi, 1]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@all_decimals
@pytest.mark.parametrize(
    "index",
    [
        "a",
        ["a", "b"],
        ["c", "a"],
    ],
)
@sql_count_checker(query_count=1)
def test_df_round_index(decimals, index):
    data = [[-10, 1, 1.5], [100000, math.e, np.nan], [-100000, math.pi, 1]]
    native_df = native_pd.DataFrame(data, columns=["a", "b", "c"]).set_index(index)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_df_round_np_types(decimals):
    data = [
        [-np.int16(1), 1, 1.5],
        [100000, math.e, np.float64(32.33)],
        [-np.double(2.6), math.pi, np.int8(3)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@all_decimals
@sql_count_checker(query_count=1)
def test_df_round_neg_odd_half(decimals):
    data = [
        [-np.double(1.5), -np.double(3.5), -np.double(5.5)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@zero_only_decimals
@sql_count_checker(query_count=1)
def test_df_round_neg_even_half(decimals):
    data = [
        [-np.double(2.5), -np.double(4.5), -np.double(6.5)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # DataFrame.round(), which we want to test.
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        round(snow_df, decimals),
        round(native_pd.DataFrame(native_df), decimals) - 1,
    )


@all_decimals
@sql_count_checker(query_count=1)
def test_df_round_pos_odd_half(decimals):
    data = [
        [np.double(1.5), np.double(3.5), np.double(5.5)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.round(decimals))


@zero_only_decimals
@sql_count_checker(query_count=1)
def test_df_round_pos_even_half(decimals):
    data = [
        [np.double(2.5), np.double(4.5), np.double(6.5)],
    ]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # DataFrame.round(), which we want to test.
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        round(snow_df, decimals),
        round(native_pd.DataFrame(native_df), decimals) + 1,
    )


@all_decimals
@pytest.mark.parametrize(
    "invalid_value, expected_sf_error",
    [
        ([None, False, False], "Invalid argument types for function"),
        ([False, True, None], "Invalid argument types for function"),
        (["string_1", "string_2", "string_3"], "is not recognized"),
        ([3, -np.int8(3), "bad_str"], " Numeric value 'bad_str' is not recognized"),
    ],
)
def test_df_round_invalid_in_sf_negative(decimals, invalid_value, expected_sf_error):
    # testing and documenting behaviors that work in native
    # pandas but not in SF
    invalid_df = np.transpose([invalid_value, [-np.float16(23.333), 3, -9]])
    snow_df = pd.DataFrame(invalid_df)

    # TODO(SNOW-1730125): This test is testing the builtin round() instead of
    # DataFrame.round(), which we want to test.
    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException, match=expected_sf_error):
            round(snow_df, decimals).to_pandas()


@sql_count_checker(query_count=0)
def test_df_round_unsupported_series_decimals():
    data = [[10, 1, 1.5], [3, 2, 0]]
    decimals = pd.Series([0, 0, 1])

    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(NotImplementedError):
        snow_df.round(decimals)


@all_decimals
def test_round_timedelta_negative(decimals):
    with SqlCounter(query_count=0):
        with pytest.raises(
            NotImplementedError,
            match=re.escape("round is not yet implemented for Timedelta Type"),
        ):
            eval_snowpark_pandas_result(
                *create_test_dfs([pd.Timedelta(1)]), lambda s: s.round(decimals)
            )
