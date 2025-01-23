#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def assert_ngroups_equal(snow_res, pd_res):
    assert snow_res.ngroups == pd_res.ngroups


@pytest.mark.parametrize("by", ["a", "b", ["a", "b"]])
@sql_count_checker(query_count=1)
def test_groupby_sort_multiindex_series(native_series_multi_numeric, by):

    snow_ser = pd.Series(native_series_multi_numeric)
    native_ser = native_series_multi_numeric
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.groupby(by=by),
        comparator=assert_ngroups_equal,
    )


@sql_count_checker(query_count=1)
def test_groupby_ngroups_series_nan():
    index = native_pd.Index(["a", "b", "b", "a"])
    index.names = ["grp_col"]
    native_ser = native_pd.Series([390.0, 350.0, np.nan, 20.0], index=index)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.groupby(by="grp_col"),
        comparator=assert_ngroups_equal,
    )


@sql_count_checker(query_count=1)
def test_groupby_ngroups_series_nan_all():
    index = native_pd.Index(["a", "b", "b", "a"])
    index.names = ["grp_col"]

    native_ser = native_pd.Series([np.nan, np.nan, np.nan, np.nan], index=index)

    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.groupby(by="grp_col"),
        comparator=assert_ngroups_equal,
    )


@sql_count_checker(query_count=1)
def test_groupby_ngroups_series():
    index = native_pd.Index(["a", "b", "b", "a"])
    index.names = ["grp_col"]

    native_ser = native_pd.Series(
        [390.0, 350.0, 30.0, 20.0],
        index=index,
    )
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.groupby(by="grp_col"),
        comparator=assert_ngroups_equal,
    )


@pytest.mark.parametrize("by", ["A", ["A", "B"]])
@sql_count_checker(query_count=1)
def test_groupby_ngroups(by):
    native_df = native_pd.DataFrame({"A": list("aabbcccd"), "B": list("xxxxabcx")})
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by),
        comparator=assert_ngroups_equal,
    )


@pytest.mark.parametrize("by", ["c1", ["c1", "c2"], ["c1", "c2", "c1"]])
@sql_count_checker(query_count=1)
def test_groupby_ngroups_nan(by):
    native_df = native_pd.DataFrame(
        {
            "c1": [np.nan, 3, 4, 4, "b"],
            "c2": [1, 2, 3, 4, 5],
            "c3": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by),
        comparator=assert_ngroups_equal,
    )


@pytest.mark.parametrize("by", ["A", ["A", "B"]])
@sql_count_checker(query_count=1)
def test_groupby_ngroups_empty_cols(by):
    native_df = native_pd.DataFrame({"A": [], "B": []})
    snow_df = pd.DataFrame({"A": [], "B": []})

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by),
        comparator=assert_ngroups_equal,
    )


@sql_count_checker(query_count=2)
@pytest.mark.parametrize(
    "level", [0, "B", [1, 1], [1, 0], ["A", "B"], [0, "A"], [-1, 0]]
)
@sql_count_checker(query_count=2)
def test_groupby_ngroups_multiindex(df_multi, level):
    eval_snowpark_pandas_result(
        df_multi,
        df_multi.to_pandas(),
        lambda df: df.groupby(level=level),
        comparator=assert_ngroups_equal,
    )
