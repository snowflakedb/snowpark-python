#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "data, expected_query_count",
    [
        ([1, 2, 3, 4, 5, 6], 1),
        ([1, 1, 1, 1, 1], 1),  # value all same
        ([None], 1),
        ([np.nan, np.nan, np.nan], 1),
        ([pd.NaT, pd.NaT, pd.NaT], 1),
        ([6.0, 7.1, np.nan], 1),
        ([None, 1, 2, 3], 1),
        (["abc", None, ("a", "c")], 1),
        (native_pd.Series(["991"] * 7), 1),
    ],
)
def test_series_transpose(data, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        native_series = native_pd.Series(data)
        snow_series = pd.Series(native_series)

        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda series: series.transpose(),
        )


@sql_count_checker(query_count=1)
def test_series_transpose_empty():
    empty_data = []
    native_series = native_pd.Series(empty_data)
    snow_series = pd.Series(empty_data)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.T,
    )


@sql_count_checker(query_count=1)
def test_series_transpose_multiindex():
    data = [1, 2, 3, 4, 5]
    index = [("a", "x"), ("b", "y"), ("c", "z"), ("d", "u"), ("e", "v")]

    native_series = native_pd.Series(data=data, index=index)
    snow_series = pd.Series(data=data, index=index)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.T,
    )


@sql_count_checker(query_count=1)
def test_series_transpose_index_no_names():
    data = [1, 2, 3, 4, 5]
    index = [None, None, None, None, None]

    native_series = native_pd.Series(data=data, index=index)
    snow_series = pd.Series(data=data, index=index)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.T,
    )
