#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_reorder():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CAB"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_add_elements():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CABDEF"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_remove_elements():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CA"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_add_remove_elements():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(index=list("CADEFG"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_fill_value():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(index=list("CADEFG"), fill_value=-1.0)
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_fill_method(method):
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(index=list("CADEFG"), method=method)
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_ordered_index_unordered_new_index():
    ordered_native_dataframe = native_pd.DataFrame(
        [[5] * 3, [6] * 3, [8] * 3], columns=list("ABC"), index=[5, 6, 8]
    )
    ordered_snow_dataframe = pd.DataFrame(ordered_native_dataframe)
    eval_snowpark_pandas_result(
        ordered_snow_dataframe,
        ordered_native_dataframe,
        lambda df: df.reindex(index=[6, 8, 7], method="ffill"),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_fill_value_with_old_na_values():
    native_df = native_pd.DataFrame(
        [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("ABC")
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(index=list("CEBFGA"), fill_value=-1)
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_fill_method_with_old_na_values(method):
    native_df = native_pd.DataFrame(
        [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("ABC")
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(index=list("CEBFGA"), method=method)
    )
