#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("axis", [0, 1])
def test_reindex_invalid_limit_parameter(axis):
    native_df = native_pd.DataFrame(
        np.arange(9).reshape((3, 3)), index=list("ABC"), columns=list("ABC")
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reindex(axis=axis, labels=list("CAB"), fill_value=1, limit=1),
        expect_exception=True,
        expect_exception_match="limit argument only valid if doing pad, backfill or nearest reindexing",
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


class TestReindexAxis0:
    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_basic_reorder(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CAB"))
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_basic_add_elements(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CABDEF"))
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_basic_remove_elements(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CA"))
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_basic_add_remove_elements(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(index=list("CADEFG"))
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_fill_value(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(index=list("CADEFG"), fill_value=-1.0),
        )

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    def test_reindex_index_fill_method(self, method, limit):
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
    def test_reindex_index_fill_method_pandas_negative(self, method, limit):
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
                return df.reindex(
                    index=list("ACDEFG"), method=method, limit=limit
                ).reindex(list("CADEFG"))

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            perform_reindex,
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_ordered_index_unordered_new_index(self):
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
    def test_reindex_index_fill_value_with_old_na_values(self):
        native_df = native_pd.DataFrame(
            [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(index=list("CEBFGA"), fill_value=-1),
        )

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    @pytest.mark.xfail(reason="Cannot qualify window in fillna.")
    def test_reindex_index_fill_method_with_old_na_values(self, limit, method):
        # Say there are NA values in the data before reindex. reindex is called with
        # ffill as the method, and there is a new NA value in the row following the
        # row with the pre-existing NA value. In this case, the ffilled value should
        # be an NA value (rather than looking to previous rows for a non-NA value).
        # To support this, we would need to have `ignore_nulls=False`, but we would
        # also need to qualify the window for values that were in the original DataFrame
        # as otherwise, if we have multiple new index values that have NA values, we would
        # pick these NA values instead of finding a non-NA value from the original DataFrame.
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
    @pytest.mark.xfail(reason="Cannot qualify window in fillna.")
    def test_reindex_index_fill_method_with_old_na_values_pandas_negative(
        self, limit, method
    ):
        # Say there are NA values in the data before reindex. reindex is called with
        # ffill as the method, and there is a new NA value in the row following the
        # row with the pre-existing NA value. In this case, the ffilled value should
        # be an NA value (rather than looking to previous rows for a non-NA value).
        # To support this, we would need to have `ignore_nulls=False`, but we would
        # also need to qualify the window for values that were in the original DataFrame
        # as otherwise, if we have multiple new index values that have NA values, we would
        # pick these NA values instead of finding a non-NA value from the original DataFrame.
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
                return df.reindex(
                    index=list("ABCEFG"), method=method, limit=limit
                ).reindex(list("CEBFGA"))

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            perform_reindex,
        )

    @sql_count_checker(query_count=2, join_count=1)
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    def test_reindex_index_datetime_with_fill(self, limit, method):
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

        eval_snowpark_pandas_result(
            snow_df, native_df, perform_reindex, check_freq=False
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_reindex_index_non_overlapping_index(self):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABC"))
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("EFG"))
        )

    @sql_count_checker(query_count=2, join_count=1)
    def test_reindex_index_non_overlapping_datetime_index(self):
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

        eval_snowpark_pandas_result(
            snow_df, native_df, perform_reindex, check_freq=False
        )

    @sql_count_checker(query_count=1)
    def test_reindex_index_non_overlapping_different_types_index_negative(self):
        date_index = pd.date_range("1/1/2010", periods=6, freq="D")
        snow_df = pd.DataFrame(
            {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
        )

        with pytest.raises(
            SnowparkSQLException,
            match='Failed to cast variant value "A" to TIMESTAMP_NTZ',
        ):
            snow_df.reindex(list("ABC")).to_pandas()

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize(
        "new_index", [list("ABC"), list("ABCC"), list("ABBC"), list("AABC")]
    )
    def test_reindex_index_duplicate_values(self, new_index):
        native_df = native_pd.DataFrame(np.arange(9).reshape((3, 3)), index=list("ABA"))
        snow_df = pd.DataFrame(native_df)

        with pytest.raises(
            ValueError, match="cannot reindex on an axis with duplicate labels"
        ):
            native_df.reindex(index=new_index)

        dfs_to_concat = []
        for row in new_index:
            if row not in list("AB"):
                dfs_to_concat.append(native_pd.DataFrame([[np.nan] * 3], index=[row]))
            else:
                value = native_df.loc[row]
                if isinstance(value, native_pd.Series):
                    value = native_pd.DataFrame(value).T
                dfs_to_concat.append(value)
        result_native_df = native_pd.concat(dfs_to_concat)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df.reindex(index=new_index), result_native_df
        )


class TestReindexAxis1:
    @sql_count_checker(query_count=1)
    def test_reindex_columns_basic_reorder(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=1, labels=list("CAB"))
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_basic_add_elements(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=1, labels=list("CABDEF"))
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_basic_remove_elements(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=1, labels=list("CA"))
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_basic_add_remove_elements(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(columns=list("CADEFG"))
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_fill_value(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(columns=list("CADEFG"), fill_value=-1.0),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    def test_reindex_columns_fill_method(self, method, limit):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ACE")
        )
        snow_df = pd.DataFrame(native_df)

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(columns=list("ABCDEFGH"), method=method, limit=limit),
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_ordered_columns_unordered_new_columns(self):
        ordered_native_dataframe = native_pd.DataFrame(
            np.array([[5] * 3, [6] * 3, [8] * 3]).T,
            index=list("ABC"),
            columns=[5, 6, 8],
        )
        ordered_snow_dataframe = pd.DataFrame(ordered_native_dataframe)
        eval_snowpark_pandas_result(
            ordered_snow_dataframe,
            ordered_native_dataframe,
            lambda df: df.reindex(columns=[6, 8, 7], method="ffill"),
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_fill_value_with_old_na_values(self):
        native_df = native_pd.DataFrame(
            [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(columns=list("CEBFGA"), fill_value=-1),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    def test_reindex_columns_fill_method_with_old_na_values(self, limit, method):
        native_df = native_pd.DataFrame(
            [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], columns=list("ACE")
        )
        snow_df = pd.DataFrame(native_df)

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(columns=list("ABCDEFGH"), method=method, limit=limit),
        )

    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    def test_reindex_columns_fill_method_with_old_na_values_negative(
        self, limit, method
    ):
        native_df = native_pd.DataFrame(
            [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        if limit is not None:
            method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
            match_str = f"limit argument for '{method_str}' method only well-defined if index and target are monotonic"
            with SqlCounter(query_count=0):
                eval_snowpark_pandas_result(
                    snow_df,
                    native_df,
                    lambda df: df.reindex(
                        columns=list("CEBFGA"), method=method, limit=limit
                    ),
                    expect_exception=True,
                    assert_exception_equal=True,
                    expect_exception_match=match_str,
                    expect_exception_type=ValueError,
                )
        else:
            with SqlCounter(query_count=1):
                eval_snowpark_pandas_result(
                    snow_df,
                    native_df,
                    lambda df: df.reindex(columns=list("CEBFGA"), method=method),
                )

    @sql_count_checker(query_count=5)
    @pytest.mark.parametrize("limit", [None, 1, 2, 100])
    @pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
    def test_reindex_columns_datetime_with_fill(self, limit, method):
        date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
        native_df = native_pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )
        date_index = pd.date_range("1/1/2010", periods=6, freq="D")
        snow_df = pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )

        def perform_reindex(df):
            if isinstance(df, pd.DataFrame):
                return df.reindex(
                    columns=pd.date_range("12/29/2009", periods=10, freq="D"),
                    method=method,
                    limit=limit,
                )
            else:
                return df.reindex(
                    columns=native_pd.date_range("12/29/2009", periods=10, freq="D"),
                    method=method,
                    limit=limit,
                )

        eval_snowpark_pandas_result(
            snow_df, native_df, perform_reindex, check_freq=False
        )

    @sql_count_checker(query_count=1)
    def test_reindex_columns_non_overlapping_columns(self):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABC")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(axis=1, labels=list("EFG"))
        )

    @sql_count_checker(query_count=5)
    def test_reindex_columns_non_overlapping_datetime_columns(self):
        date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
        native_df = native_pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )
        date_index = pd.date_range("1/1/2010", periods=6, freq="D")
        snow_df = pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )

        def perform_reindex(df):
            if isinstance(df, pd.DataFrame):
                return df.reindex(
                    columns=pd.date_range("12/29/2008", periods=10, freq="D"),
                )
            else:
                return df.reindex(
                    columns=native_pd.date_range("12/29/2008", periods=10, freq="D"),
                )

        eval_snowpark_pandas_result(
            snow_df, native_df, perform_reindex, check_freq=False
        )

    @sql_count_checker(query_count=2)
    def test_reindex_columns_non_overlapping_different_types_columns(self):
        date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
        native_df = native_pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )
        date_index = pd.date_range("1/1/2010", periods=6, freq="D")
        snow_df = pd.DataFrame(
            [[100, 101, np.nan, 100, 89, 88]], index=["prices"], columns=date_index
        )

        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.reindex(columns=list("ABCD"))
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "new_columns", [list("ABC"), list("ABCC"), list("ABBC"), list("AABC")]
    )
    def test_reindex_columns_duplicate_values(self, new_columns):
        native_df = native_pd.DataFrame(
            np.arange(9).reshape((3, 3)), columns=list("ABA")
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.reindex(columns=new_columns),
            expect_exception=True,
            expect_exception_match="cannot reindex on an axis with duplicate labels",
            assert_exception_equal=True,
            expect_exception_type=ValueError,
        )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("axis", [0, 1])
def test_reindex_multiindex_negative(axis):
    snow_df = pd.DataFrame(np.arange(9).reshape((3, 3)))
    snow_df = snow_df.set_index([0, 1])

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support `reindex` with MultiIndex",
    ):
        if axis == 0:
            snow_df.reindex(index=[1, 2, 3])
        else:
            snow_df.T.reindex(columns=[1, 2, 3])


@sql_count_checker(query_count=0)
@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_reindex_timedelta_axis_0_negative():
    native_df = native_pd.DataFrame(
        np.arange(9).reshape((3, 3)), index=list("ABC")
    ).astype("timedelta64[ns]")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=0, labels=list("CAB"))
    )


@sql_count_checker(query_count=0)
@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_reindex_timedelta_axis_1_negative():
    native_df = native_pd.DataFrame(
        np.arange(9).reshape((3, 3)), columns=list("ABC")
    ).astype("timedelta64[ns]")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reindex(axis=1, labels=list("CAB"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_index_name():
    native_df = native_pd.DataFrame(
        [[0, 1, 2], [0, 0, 1], [1, 0, 0]],
        index=list("ABC"),
    )
    snow_df = pd.DataFrame(native_df)
    index_with_name = native_pd.Index(list("CAB"), name="weewoo")
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.reindex(index=index_with_name), native_df.reindex(index=index_with_name)
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_index_name_and_df_index_name():
    native_df = native_pd.DataFrame(
        {"X": [1, 2, 3], "Y": [8, 7, 3], "Z": [3, 4, 5]},
        index=native_pd.Index(list("ABC"), name="AAAAA"),
    )
    snow_df = pd.DataFrame(native_df)
    index_with_name = native_pd.Index(list("CAB"), name="weewoo")
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.reindex(index=index_with_name), native_df.reindex(index=index_with_name)
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_lazy_index():
    native_df = native_pd.DataFrame(
        [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, np.nan]], index=list("XYZ")
    )
    snow_df = pd.DataFrame(native_df)
    native_idx = native_pd.Index(list("CAB"))
    lazy_idx = pd.Index(native_idx)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reindex(
            index=native_idx if isinstance(df, native_pd.DataFrame) else lazy_idx
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_str_index_with_tuple_index():
    index = ["A", "B", "C", "D"]
    native_df = native_pd.DataFrame(
        {"one": [200, 200, 404, 404], "two": [200, 200, 404, 404]}, index=index
    )
    snow_df = pd.DataFrame(native_df)
    nat_df = native_df.reindex(index=native_pd.Series(data=[("A", "B"), ("C", "D")]))
    res_df = snow_df.reindex(index=pd.Series(data=[("A", "B"), ("C", "D")]))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(res_df, nat_df)


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_int_index_with_tuple_index():
    native_df = native_pd.DataFrame(
        {"one": [200, 200, 404, 404], "two": [200, 200, 404, 404]}
    )
    snow_df = pd.DataFrame(native_df)
    nat_df = native_df.reindex(index=native_pd.Series(data=[("A", "B"), ("C", "D")]))
    idx = pd.Series(data=[("A", "B"), ("C", "D")])
    res_df = snow_df.reindex(index=idx)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(res_df, nat_df)


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_int_index_with_str_index():
    native_df = native_pd.DataFrame(
        {"one": [200, 200, 404, 404], "two": [200, 200, 404, 404]}
    )
    snow_df = pd.DataFrame(native_df)
    nat_df = native_df.reindex(index=native_pd.Series(data=["A", "C"]))
    idx = pd.Series(data=["A", "C"])
    res_df = snow_df.reindex(index=idx)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(res_df, nat_df)


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_mixed_index_type():
    native_df = native_pd.DataFrame(
        {"one": [200, 300], "two": [200, 200]},
    )
    snow_df = pd.DataFrame(native_df)
    nat_df = native_df.reindex(index=native_pd.Series(data=["A", 1]))
    res_df = snow_df.reindex(index=pd.Series(data=["A", 1]))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(res_df, nat_df)


@sql_count_checker(query_count=3, join_count=1)
def test_reindex_int_timestamp_index_type():
    native_df = native_pd.DataFrame({"prices": [100, 101, np.nan, 100, 89, 88]})
    snow_df = pd.DataFrame(native_df)
    datetime_series = native_pd.Series(
        data=pd.date_range("1/1/2010", periods=6, freq="D")
    )
    nat_df = native_df.reindex(index=datetime_series)
    res_df = snow_df.reindex(index=pd.Series(datetime_series))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(res_df, nat_df)
