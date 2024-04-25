#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import string

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._typing import Frequency

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.groupby.test_groupby_ngroups import assert_ngroups_equal
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_frame_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)


def getTimeSeriesData(nper=30, freq: Frequency = "B") -> dict[str, native_pd.Series]:
    s = native_pd.Series(
        np.random.default_rng(2).standard_normal(30),
        index=native_pd.date_range(start="2000-01-01", periods=nper, freq=freq),
        name=None,
    )
    return {c: s for c in string.ascii_uppercase[:4]}


def makeTimeDataFrame(nper=30, freq: Frequency = "B") -> native_pd.DataFrame:
    data = getTimeSeriesData(nper, freq)
    return native_pd.DataFrame(data)


@pytest.fixture
def tsframe() -> native_pd.DataFrame:
    return makeTimeDataFrame()[:5]


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize("group_name", ["x", ["x"]])
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_groupby_axis_1(group_name):
    pandas_df = native_pd.DataFrame(
        np.arange(12).reshape(3, 4), index=[0, 1, 0], columns=["a", "b", "a", "b"]
    )
    pandas_df.index.name = "y"
    pandas_df.columns.name = "x"

    snow_df = pd.DataFrame(pandas_df)

    eval_snowpark_pandas_result(
        snow_df, pandas_df, lambda df: df.groupby(axis=1, by=group_name).max()
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize("group_name", ["x", ["x"]])
@sql_count_checker(query_count=16, fallback_count=2, sproc_count=2)
def test_groupby_axis_1_mi(group_name):
    # test on MI column
    iterables = [["bar", "baz", "foo"], ["one", "two"]]
    mi = native_pd.MultiIndex.from_product(iterables=iterables, names=["x", "x1"])
    pandas_df_mi = native_pd.DataFrame(
        np.arange(18).reshape(3, 6), index=[0, 1, 0], columns=mi
    )
    snow_df_mi = pd.DataFrame(pandas_df_mi)
    eval_snowpark_pandas_result(
        snow_df_mi, pandas_df_mi, lambda df: df.groupby(axis=1, by=group_name).sum()
    )
    eval_snowpark_pandas_result(
        snow_df_mi, pandas_df_mi, lambda df: df.groupby(axis=1, level=0).min()
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "by",
    [
        lambda x: x // 3,
        ["col1", lambda x: x // 3],
        ["col1", lambda x: x + 1, lambda x: x % 3, "col2"],
    ],
)
def test_groupby_with_callable_and_array(basic_snowpark_pandas_df, by) -> None:
    pandas_df = basic_snowpark_pandas_df.to_pandas()
    expected_query_count = 8
    if isinstance(by, list):
        expected_query_count = 9
    with SqlCounter(query_count=expected_query_count, fallback_count=1, sproc_count=1):
        eval_snowpark_pandas_result(
            basic_snowpark_pandas_df, pandas_df, lambda df: df.groupby(by).min()
        )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_timeseries_groupby_with_callable(tsframe):
    snow_ts_df = pd.DataFrame(tsframe)
    eval_snowpark_pandas_result(
        snow_ts_df,
        tsframe,
        lambda df: df.groupby(lambda x: x.month).agg(np.percentile, 80, axis=0),
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "agg_func, args",
    [
        (lambda x: np.sum(x), []),  # callable
        ([lambda x: np.sum(x), lambda x: np.max(x)], []),  # list of callable
        (np.percentile, [80]),  # unsupported aggregation function
        (np.quantile, [0.6]),  # unsupported aggregation function
        ({"col2": "max", "col4": lambda x: np.sum(x)}, []),  # dict includes callable
    ],
)
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_groupby_agg_func_unsupported(basic_snowpark_pandas_df, agg_func, args):
    by = "col1"
    pandas_df = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        pandas_df,
        lambda df: df.groupby(by).agg(agg_func, *args),
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "agg_func",
    [
        lambda x: np.sum(x),  # callable
        np.ptp,  # Unsupported aggregation function
    ],
)
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_groupby_agg_func_unsupported_named_agg(basic_snowpark_pandas_df, agg_func):
    by = "col1"
    pandas_df = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        pandas_df,
        lambda df: df.groupby(by).agg(new_col=("col2", agg_func)),
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "agg_func",
    [lambda x: x * 2, np.sin, {"col2": "max", "col4": np.sin}],
)
@sql_count_checker(query_count=4)
def test_groupby_invalid_agg_func_raises(basic_snowpark_pandas_df, agg_func):
    by = "col1"
    with pytest.raises(SnowparkSQLException):
        basic_snowpark_pandas_df.groupby(by=by).aggregate(agg_func)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=10, fallback_count=1, sproc_count=1)
def test_groupby_with_numpy_array(basic_snowpark_pandas_df) -> None:
    by = [1, 1, 4, 2, 2, 4]
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by=by).max(),
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "by_list",
    [[2, 1, 1, 2, 3, 3], [[2, 1, 1, 2, 3, 3], "a"]],
)
@sql_count_checker(query_count=10, fallback_count=1, sproc_count=1)
def test_groupby_series_with_numpy_array(series_multi_numeric, by_list) -> None:
    eval_snowpark_pandas_result(
        series_multi_numeric,
        series_multi_numeric.to_pandas(),
        lambda df: df.groupby(by=by_list).max(),
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@sql_count_checker(query_count=22, fallback_count=2, sproc_count=2)
def test_groupby_with_external_series(basic_snowpark_pandas_df) -> None:
    series_data = [0, 1, 1, 0, 1]
    native_series = native_pd.Series(series_data)
    snowpark_pandas_series = pd.Series(native_series)

    native_df = basic_snowpark_pandas_df.to_pandas()

    result = basic_snowpark_pandas_df.groupby(by=snowpark_pandas_series).sum()
    expected = native_df.groupby(by=native_series).sum()
    # groupby result in snowflake gives a type with precise precision, which
    # continue gives a more precise integer type with to_pandas. Since snowpark python
    # update the to_pandas to convert integer to int64 for default precision, the
    # native_df will have type int64, and the expected result will also have int64
    # with native pandas now. Because dtype are all controlled by snowflake, and
    # checking dtype is not the purpose of this test, we skip the dtype check here.
    assert_frame_equal(result, expected, check_index_type=False, check_dtype=False)

    by_list = ["col1", "col2", snowpark_pandas_series]
    native_by_list = ["col1", "col2", native_series]
    result = basic_snowpark_pandas_df.groupby(by=by_list).sum()
    expected = native_df.groupby(by=native_by_list).sum()
    assert_frame_equal(result, expected, check_index_type=False, check_dtype=False)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "mapper, level",
    [
        ({"foo": 0, "bar": 0, "baz": 1, "qux": 1}, 0),
        ({"one": 0, "two": 0, "three": 1}, 1),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_groupby_level_mapper(mapper, level):
    index = native_pd.MultiIndex(
        levels=[["foo", "bar", "baz", "qux"], ["one", "two", "three"]],
        codes=[[0, 0, 0, 1, 1, 2, 2, 3, 3, 3], [0, 1, 2, 0, 1, 1, 2, 0, 1, 2]],
        names=["first", "second"],
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            np.random.randn(10, 3),
            index=index,
            columns=native_pd.Index(["A", "B", "C"], name="exp"),
        ),
        lambda df: df.groupby(mapper, level=level).sum()
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "grp_agg",
    [
        lambda grp: grp.std(ddof=2),
        lambda grp: grp.var(ddof=3),
    ],
)
@pytest.mark.parametrize("by", ["col1", ["col5", "col1"]])
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_std_var_ddof_unsupported(basic_snowpark_pandas_df, grp_agg, by):
    native_group = basic_snowpark_pandas_df.to_pandas().groupby(by)
    snowpark_pandas_group = basic_snowpark_pandas_df.groupby(by)
    eval_snowpark_pandas_result(snowpark_pandas_group, native_group, grp_agg)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "by", [native_pd.Grouper(key="col1"), ["col5", native_pd.Grouper(key="col1")]]
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_grouper_unsupported(basic_snowpark_pandas_df, by):
    snowpark_pandas_group = basic_snowpark_pandas_df.groupby(by)
    native_group = basic_snowpark_pandas_df.to_pandas().groupby(by)
    eval_snowpark_pandas_result(
        snowpark_pandas_group, native_group, lambda grp: grp.max()
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_groupby_ngroups_axis_1():
    by = "x"
    native_df = native_pd.DataFrame(
        np.arange(12).reshape(3, 4), index=[0, 1, 0], columns=["a", "b", "a", "b"]
    )
    native_df.index.name = "y"
    native_df.columns.name = "x"
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by, axis=1),
        comparator=assert_ngroups_equal,
    )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_groupby_ngroups_axis_1_mi():
    by = "x"
    iterables = [["bar", "baz", "foo"], ["one", "two"]]
    mi = native_pd.MultiIndex.from_product(iterables=iterables, names=["x", "x1"])
    native_df = native_pd.DataFrame(
        np.arange(18).reshape(3, 6), index=[0, 1, 0], columns=mi
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by, axis=1),
        comparator=assert_ngroups_equal,
    )
