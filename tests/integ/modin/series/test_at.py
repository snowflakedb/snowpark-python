#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2, join_count=2)
def test_at_get_default_index(
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    assert (
        default_index_snowpark_pandas_series.at[0] == default_index_native_series.at[0]
    )


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_default_index(
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    def at_set_helper(series):
        series.at[0] = 100

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        at_set_helper,
        inplace=True,
    )


@sql_count_checker(query_count=2, join_count=2)
def test_at_get_str_index(
    str_index_snowpark_pandas_series,
    str_index_native_series,
):
    assert str_index_snowpark_pandas_series.at["b"] == str_index_native_series.at["b"]


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_str_index(
    str_index_snowpark_pandas_series,
    str_index_native_series,
):
    def at_set_helper(series):
        series.at["b"] = 100

    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_series,
        str_index_native_series,
        at_set_helper,
        inplace=True,
    )


@sql_count_checker(query_count=2)
def test_at_get_time_index(
    time_index_snowpark_pandas_series,
    time_index_native_series,
):
    assert (
        time_index_snowpark_pandas_series.at["2023-01-01 00:00:00"]
        == time_index_native_series.at["2023-01-01 00:00:00"]
    )


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_time_index(
    time_index_snowpark_pandas_series,
    time_index_native_series,
):
    def at_set_helper(series):
        series.at["2023-01-01 00:00:00"] = 100

    eval_snowpark_pandas_result(
        time_index_snowpark_pandas_series,
        time_index_native_series,
        at_set_helper,
        inplace=True,
    )


@sql_count_checker(query_count=2)
def test_at_get_multiindex(
    multiindex_native_int_series,
):
    multiindex_snowpark_int_series = pd.Series(multiindex_native_int_series)
    assert (
        multiindex_snowpark_int_series.at[("bar", "one")]
        == multiindex_native_int_series.at[("bar", "one")]
    )


@sql_count_checker(query_count=0)
def test_at_set_multiindex_neg(
    multiindex_native_int_series,
):
    def at_set_helper(series):
        series.at[("bar", "one")] = 100

    multiindex_snowpark_int_series = pd.Series(multiindex_native_int_series)
    with pytest.raises(NotImplementedError):
        at_set_helper(multiindex_snowpark_int_series)


@pytest.mark.parametrize(
    "key",
    [
        [0, 0],
        {0: 0},
        ([0, 0],),
        ({0: 0},),
        ([0], [0]),
        ({0: 0},),
        (0, [0]),
        ("a", "b"),
        (0, "A", 2),
    ],
)
@sql_count_checker(query_count=0)
def test_at_neg(
    key,
    default_index_snowpark_pandas_series,
):
    with pytest.raises(KeyError):
        default_index_snowpark_pandas_series.at[key]
