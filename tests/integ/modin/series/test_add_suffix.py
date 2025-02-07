#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

TEST_ADD_SUFFIX_DATA = [
    "_suffix",
    " 0 9 0 1 2 3",
    12345,
    ("tuple data", 12),
    [24, 25, 26, "list!"],
    [[], [1, 2]],
    native_pd.Series(["this", "is", "a", "series"]),
    native_pd.DataFrame({"column1": [67, 68], "column2": [909, 910]}),
    None,
]


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_series_add_suffix(
    suffix, str_index_snowpark_pandas_series, str_index_native_series
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_series,
        str_index_native_series,
        lambda series: series.add_suffix(suffix),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_add_suffix_multiindex(suffix, multiindex_native_int_series):
    eval_snowpark_pandas_result(
        pd.Series(multiindex_native_int_series),
        multiindex_native_int_series,
        lambda df: df.add_suffix(suffix),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_add_suffix_time_column_df(suffix, time_index_series_data):
    series_data, kwargs = time_index_series_data
    snow_series, native_series = create_test_series(series_data, **kwargs)

    # Native pandas time values are of the format `2023-01-01 00:00:00` while Snowflake is `2023-01-01 00:00:00.000`.
    # For easier comparison, add_suffix is called with suffix ".000" for the native pandas df.
    eval_snowpark_pandas_result(
        snow_series,
        native_series.add_suffix(".000"),
        lambda df: df.add_suffix(suffix),
    )


@sql_count_checker(query_count=2)
def test_series_add_suffix_snowpark_pandas_series(
    default_index_snowpark_pandas_series, default_index_native_series
):
    suffix_series = native_pd.Series([1.1, 2.2, 3.3])
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser.add_suffix(
            pd.Series(suffix_series) if isinstance(ser, pd.Series) else suffix_series
        ),
    )


@sql_count_checker(query_count=2)
def test_series_add_suffix_snowpark_pandas_df(
    default_index_snowpark_pandas_series, default_index_native_series
):
    suffix_df = native_pd.DataFrame([["1", "2"], ["3", "4"]], dtype=str)
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser.add_suffix(
            pd.DataFrame(suffix_df) if isinstance(ser, pd.Series) else suffix_df
        ),
    )
