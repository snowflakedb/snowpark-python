#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
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


@sql_count_checker(query_count=1, join_count=0, union_count=0)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_df_add_suffix(
    suffix, default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.add_suffix(suffix),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_df_add_suffix_multiindex(suffix, native_df_with_multiindex_columns):
    eval_snowpark_pandas_result(
        pd.DataFrame(native_df_with_multiindex_columns),
        native_df_with_multiindex_columns,
        lambda df: df.add_suffix(suffix),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("suffix", TEST_ADD_SUFFIX_DATA)
def test_df_add_suffix_time_column_df(
    suffix, time_column_snowpark_pandas_df, time_column_native_df
):
    eval_snowpark_pandas_result(
        time_column_snowpark_pandas_df,
        time_column_native_df,
        lambda df: df.add_suffix(suffix),
    )


@sql_count_checker(query_count=2)
def test_df_add_suffix_snowpark_pandas_series(
    default_index_snowpark_pandas_df, default_index_native_df
):
    suffix_series = native_pd.Series([1.2, 2.5, 3.1])
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.add_suffix(
            pd.Series(suffix_series) if isinstance(df, pd.DataFrame) else suffix_series
        ),
    )


@sql_count_checker(query_count=2)
def test_df_add_prefix_snowpark_pandas_df(
    default_index_snowpark_pandas_df, default_index_native_df
):
    suffix_df = native_pd.DataFrame([["1", "2"], ["3", "4"]], dtype=str)
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.add_suffix(
            pd.DataFrame(suffix_df) if isinstance(df, pd.DataFrame) else suffix_df
        ),
    )
