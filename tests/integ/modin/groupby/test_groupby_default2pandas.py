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
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


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


@pytest.mark.parametrize("group_name", ["x", ["x"]])
@sql_count_checker(query_count=0)
def test_groupby_axis_1(group_name):
    pandas_df = native_pd.DataFrame(
        np.arange(12).reshape(3, 4), index=[0, 1, 0], columns=["a", "b", "a", "b"]
    )
    pandas_df.index.name = "y"
    pandas_df.columns.name = "x"

    snow_df = pd.DataFrame(pandas_df)

    msg = "GroupBy.max is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.groupby(axis=1, by=group_name).max()


@pytest.mark.parametrize("group_name", ["x", ["x"]])
@sql_count_checker(query_count=0)
def test_groupby_axis_1_mi(group_name):
    # test on MI column
    iterables = [["bar", "baz", "foo"], ["one", "two"]]
    mi = native_pd.MultiIndex.from_product(iterables=iterables, names=["x", "x1"])
    pandas_df_mi = native_pd.DataFrame(
        np.arange(18).reshape(3, 6), index=[0, 1, 0], columns=mi
    )
    snow_df_mi = pd.DataFrame(pandas_df_mi)

    msg = "GroupBy.sum is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df_mi.groupby(axis=1, by=group_name).sum()

    msg = "GroupBy.min is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df_mi.groupby(axis=1, level=0).min()


@pytest.mark.parametrize(
    "by",
    [
        lambda x: x // 3,
        ["col1", lambda x: x // 3],
        ["col1", lambda x: x + 1, lambda x: x % 3, "col2"],
    ],
)
def test_groupby_with_callable_and_array(basic_snowpark_pandas_df, by) -> None:
    msg = "GroupBy.min is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    expected_query_count = 0
    if isinstance(by, list):
        expected_query_count = 1
    with SqlCounter(query_count=expected_query_count):
        with pytest.raises(NotImplementedError, match=msg):
            basic_snowpark_pandas_df.groupby(by).min()


@sql_count_checker(query_count=0)
def test_timeseries_groupby_with_callable(tsframe):
    snow_ts_df = pd.DataFrame(tsframe)
    with pytest.raises(NotImplementedError):
        snow_ts_df.groupby(lambda x: x.month).agg(np.percentile, 80, axis=0)


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
@sql_count_checker(query_count=0)
def test_groupby_agg_func_unsupported(basic_snowpark_pandas_df, agg_func, args):
    by = "col1"
    with pytest.raises(NotImplementedError):
        basic_snowpark_pandas_df.groupby(by).agg(agg_func, *args)


@pytest.mark.parametrize(
    "agg_func",
    [lambda x: x * 2, np.sin, {"col2": "max", "col4": np.sin}],
)
@sql_count_checker(query_count=0)
def test_groupby_invalid_agg_func_raises(basic_snowpark_pandas_df, agg_func):
    by = "col1"
    with pytest.raises(NotImplementedError):
        basic_snowpark_pandas_df.groupby(by=by).aggregate(agg_func)


@sql_count_checker(query_count=1)
def test_groupby_with_numpy_array(basic_snowpark_pandas_df) -> None:
    by = [1, 1, 4, 2, 2, 4]
    msg = "GroupBy.max is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        basic_snowpark_pandas_df.groupby(by=by).max()


@pytest.mark.parametrize(
    "by_list",
    [[2, 1, 1, 2, 3, 3], [[2, 1, 1, 2, 3, 3], "a"]],
)
@sql_count_checker(query_count=1)
def test_groupby_series_with_numpy_array(series_multi_numeric, by_list) -> None:
    msg = "GroupBy.max is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        series_multi_numeric.groupby(by=by_list).max()


def test_groupby_with_external_series(basic_snowpark_pandas_df) -> None:
    series_data = [0, 1, 1, 0, 1]
    native_series = native_pd.Series(series_data)
    snowpark_pandas_series = pd.Series(native_series)

    with SqlCounter(query_count=0):
        msg = "GroupBy.sum is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
        with pytest.raises(NotImplementedError, match=msg):
            basic_snowpark_pandas_df.groupby(by=snowpark_pandas_series).sum()

    with SqlCounter(query_count=1):
        by_list = ["col1", "col2", snowpark_pandas_series]
        msg = "GroupBy.sum is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
        with pytest.raises(NotImplementedError, match=msg):
            basic_snowpark_pandas_df.groupby(by=by_list).sum()


@pytest.mark.parametrize(
    "mapper, level",
    [
        ({"foo": 0, "bar": 0, "baz": 1, "qux": 1}, 0),
        ({"one": 0, "two": 0, "three": 1}, 1),
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_level_mapper(mapper, level):
    index = native_pd.MultiIndex(
        levels=[["foo", "bar", "baz", "qux"], ["one", "two", "three"]],
        codes=[[0, 0, 0, 1, 1, 2, 2, 3, 3, 3], [0, 1, 2, 0, 1, 1, 2, 0, 1, 2]],
        names=["first", "second"],
    )
    snow_df = pd.DataFrame(
        np.random.randn(10, 3),
        index=index,
        columns=native_pd.Index(["A", "B", "C"], name="exp"),
    )
    msg = "GroupBy.sum is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.groupby(mapper, level=level).sum()


@pytest.mark.parametrize(
    "grp_agg, agg_name",
    [
        (lambda grp: grp.std(ddof=2), "std"),
        (lambda grp: grp.var(ddof=3), "var"),
    ],
)
@pytest.mark.parametrize("by", ["col1", ["col5", "col1"]])
@sql_count_checker(query_count=0)
def test_std_var_ddof_unsupported(basic_snowpark_pandas_df, grp_agg, agg_name, by):
    snowpark_pandas_group = basic_snowpark_pandas_df.groupby(by)
    msg = f"GroupBy.{agg_name} is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
    with pytest.raises(NotImplementedError, match=msg):
        grp_agg(snowpark_pandas_group)


@pytest.mark.parametrize(
    "by, query_count",
    [
        (native_pd.Grouper(key="col1"), 0),
        (["col5", native_pd.Grouper(key="col1")], 1),
    ],
)
def test_grouper_unsupported(basic_snowpark_pandas_df, by, query_count):
    with SqlCounter(query_count=query_count):
        snowpark_pandas_group = basic_snowpark_pandas_df.groupby(by)
        msg = "GroupBy.max is not implemented yet for pd.Grouper, if axis == 1, if both by and level are configured, if by contains any non-pandas hashable labels, or for unsupported aggregation parameters."
        with pytest.raises(NotImplementedError, match=msg):
            snowpark_pandas_group.max()


@sql_count_checker(query_count=0)
def test_groupby_ngroups_axis_1():
    by = "x"
    native_df = native_pd.DataFrame(
        np.arange(12).reshape(3, 4), index=[0, 1, 0], columns=["a", "b", "a", "b"]
    )
    native_df.index.name = "y"
    native_df.columns.name = "x"
    snow_df = pd.DataFrame(native_df)

    msg = "GroupBy.ngroups is not implemented yet if axis == 1, both by and level are configured, or if `by` contains any non-pandas hashable labels."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.groupby(by=by, axis=1).ngroups


@sql_count_checker(query_count=0)
def test_groupby_ngroups_axis_1_mi():
    by = "x"
    iterables = [["bar", "baz", "foo"], ["one", "two"]]
    mi = native_pd.MultiIndex.from_product(iterables=iterables, names=["x", "x1"])
    native_df = native_pd.DataFrame(
        np.arange(18).reshape(3, 6), index=[0, 1, 0], columns=mi
    )
    snow_df = pd.DataFrame(native_df)

    msg = "GroupBy.ngroups is not implemented yet if axis == 1, both by and level are configured, or if `by` contains any non-pandas hashable labels."
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.groupby(by=by, axis=1).ngroups
