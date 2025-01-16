#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

from tests.integ.modin.utils import eval_snowpark_pandas_result  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1)
def test_iat_get_default_index(
    key,
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    assert (
        default_index_snowpark_pandas_series.iat[key]
        == default_index_native_series.iat[key]
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_default_index(
    key,
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    def iat_set_helper(series):
        series.iat[key] = 100

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        iat_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1)
def test_iat_get_str_index(
    key,
    str_index_snowpark_pandas_series,
    str_index_native_series,
):
    assert str_index_snowpark_pandas_series.iat[key] == str_index_native_series.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_str_index(
    key,
    str_index_snowpark_pandas_series,
    str_index_native_series,
):
    def iat_set_helper(series):
        series.iat[key] = 100

    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_series,
        str_index_native_series,
        iat_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1)
def test_iat_get_time_index(
    key,
    time_index_snowpark_pandas_series,
    time_index_native_series,
):
    assert (
        time_index_snowpark_pandas_series.iat[key] == time_index_native_series.iat[key]
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_time_index(
    key,
    time_index_snowpark_pandas_series,
    time_index_native_series,
):
    def iat_set_helper(series):
        series.iat[key] = 100

    eval_snowpark_pandas_result(
        time_index_snowpark_pandas_series,
        time_index_native_series,
        iat_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1)
def test_iat_get_multiindex(
    key,
    multiindex_native_int_series,
):
    multiindex_snowpark_int_series = pd.Series(multiindex_native_int_series)
    assert (
        multiindex_snowpark_int_series.iat[key] == multiindex_native_int_series.iat[key]
    )


@pytest.mark.parametrize(
    "key",
    [
        0,
        -7,
        (0,),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_multiindex(
    key,
    multiindex_native_int_series,
):
    def at_set_helper(series):
        series.iat[key] = 100

    multiindex_snowpark_int_series = pd.Series(multiindex_native_int_series)
    eval_snowpark_pandas_result(
        multiindex_snowpark_int_series,
        multiindex_native_int_series,
        at_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key, error",
    [
        ([0, 0], KeyError),
        ({0: 0}, KeyError),
        ("a", IndexError),
        ((0, 0), KeyError),
    ],
)
@sql_count_checker(query_count=0)
def test_iat_neg(
    key,
    error,
    default_index_snowpark_pandas_series,
):
    with pytest.raises(error):
        default_index_snowpark_pandas_series.iat[key]
