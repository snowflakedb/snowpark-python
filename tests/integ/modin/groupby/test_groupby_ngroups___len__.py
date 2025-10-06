#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(
    params=(
        param(lambda groupby: groupby.ngroups, id="ngroups"),
        param(lambda groupby: len(groupby), id="len"),
    )
)
def count_function(request):
    return request.param


@pytest.mark.parametrize("by", ["a", "b", ["a", "b"]])
@sql_count_checker(query_count=1)
def test_series_with_multiindex(native_series_multi_numeric, by, count_function):

    snow_ser = pd.Series(native_series_multi_numeric)
    native_ser = native_series_multi_numeric
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: count_function(ser.groupby(by=by)),
        comparator=int.__eq__,
    )


@sql_count_checker(query_count=1)
def test_series_with_nan(count_function):
    index = native_pd.Index(["a", "b", "b", "a"])
    index.names = ["grp_col"]
    native_ser = native_pd.Series([390.0, 350.0, np.nan, 20.0], index=index)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: count_function(ser.groupby(by="grp_col")),
        comparator=int.__eq__,
    )


@sql_count_checker(query_count=1)
def test_all_nan_series(count_function):
    index = native_pd.Index(["a", "b", "b", "a"])
    index.names = ["grp_col"]

    native_ser = native_pd.Series([np.nan, np.nan, np.nan, np.nan], index=index)

    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: count_function(ser.groupby(by="grp_col")),
        comparator=int.__eq__,
    )


@sql_count_checker(query_count=1)
def test_series_with_single_level_index(count_function):
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
        lambda ser: count_function(ser.groupby(by="grp_col")),
        comparator=int.__eq__,
    )


@pytest.mark.parametrize("by", ["A", ["A", "B"]])
@sql_count_checker(query_count=1)
def test_df(by, count_function):
    native_df = native_pd.DataFrame({"A": list("aabbcccd"), "B": list("xxxxabcx")})
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: count_function(df.groupby(by=by)),
        comparator=int.__eq__,
    )


@pytest.mark.parametrize("by", ["c1", ["c1", "c2"], ["c1", "c2", "c1"]])
@sql_count_checker(query_count=1)
def test_df_with_nan(by, count_function):
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
        lambda df: count_function(df.groupby(by=by)),
        comparator=int.__eq__,
    )


@pytest.mark.parametrize("by", ["A", ["A", "B"]])
@sql_count_checker(query_count=1)
def test_df_with_0_rows(by, count_function):
    native_df = native_pd.DataFrame({"A": [], "B": []})
    snow_df = pd.DataFrame({"A": [], "B": []})

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: count_function(df.groupby(by=by)),
        comparator=int.__eq__,
    )


@sql_count_checker(query_count=2)
@pytest.mark.parametrize(
    "level", [0, "B", [1, 1], [1, 0], ["A", "B"], [0, "A"], [-1, 0]]
)
@sql_count_checker(query_count=2)
def test_df_with_multiindex(df_multi, level, count_function):
    eval_snowpark_pandas_result(
        df_multi,
        df_multi.to_pandas(),
        lambda df: count_function(df.groupby(level=level)),
        comparator=int.__eq__,
    )
