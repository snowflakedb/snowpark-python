#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
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
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method(method, limit):
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ACE"))
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reindex(index=list("ABCDEFGH"), method=method, limit=limit),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method_pandas_negative(method, limit):
    # The negative is because pandas does not support `limit` parameter
    # if the index and target are not both monotonic, while we do.
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    if limit is not None:
        method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
        with pytest.raises(
            ValueError,
            match=f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
        ):
            native_df.reindex(index=list("CEBFGA"), method=method, limit=limit)

    def perform_reindex(df):
        if isinstance(df, pd.DataFrame):
            return df.reindex(index=list("CADEFG"), method=method, limit=limit)
        else:
            return df.reindex(index=list("ACDEFG"), method=method, limit=limit).reindex(
                list("CADEFG")
            )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_reindex,
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
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_fill_method_with_old_na_values(limit, method):
    native_df = native_pd.DataFrame(
        [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("ACE")
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reindex(index=list("ABCDEFGH"), method=method, limit=limit),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_fill_method_with_old_na_values_pandas_negative(limit, method):
    native_df = native_pd.DataFrame(
        [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("ABC")
    )
    snow_df = pd.DataFrame(native_df)
    if limit is not None:
        method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
        with pytest.raises(
            ValueError,
            match=f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
        ):
            native_df.reindex(index=list("CEBFGA"), method=method, limit=limit)

    def perform_reindex(df):
        if isinstance(df, pd.DataFrame):
            return df.reindex(index=list("CEBFGA"), method=method, limit=limit)
        else:
            return df.reindex(index=list("ABCEFG"), method=method, limit=limit).reindex(
                list("CEBFGA")
            )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_reindex,
    )


@sql_count_checker(query_count=2, join_count=1)
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_datetime_with_fill(limit, method):
    date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    native_df = native_pd.DataFrame(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_df = pd.DataFrame(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    def perform_reindex(df):
        if isinstance(df, pd.DataFrame):
            return df.reindex(
                pd.date_range("12/29/2009", periods=10, freq="D"),
                method=method,
                limit=limit,
            )
        else:
            return df.reindex(
                native_pd.date_range("12/29/2009", periods=10, freq="D"),
                method=method,
                limit=limit,
            )

    eval_snowpark_pandas_result(snow_df, native_df, perform_reindex, check_freq=False)


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_non_overlapping_index():
    native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("EFG"))
    )


@sql_count_checker(query_count=2, join_count=1)
def test_reindex_index_non_overlapping_datetime_index():
    date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    native_df = native_pd.DataFrame(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_df = pd.DataFrame(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    def perform_reindex(df):
        if isinstance(df, pd.DataFrame):
            return df.reindex(
                pd.date_range("12/29/2008", periods=10, freq="D"),
            )
        else:
            return df.reindex(
                native_pd.date_range("12/29/2008", periods=10, freq="D"),
            )

    eval_snowpark_pandas_result(snow_df, native_df, perform_reindex, check_freq=False)


@sql_count_checker(query_count=1)
def test_reindex_index_non_overlapping_different_types_index_negative():
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_df = pd.DataFrame(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    with pytest.raises(SnowparkSQLException, match=".*Timestamp 'A' is not recognized"):
        snow_df.reindex(list("ABC")).to_pandas()
