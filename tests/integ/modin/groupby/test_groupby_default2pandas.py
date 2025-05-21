#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import string

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._typing import Frequency
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    _GROUPBY_UNSUPPORTED_GROUPING_MESSAGE,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN = re.escape(
    f"Snowpark pandas GroupBy.aggregate {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
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


def sensitive_function_name(col: native_pd.Series) -> int:
    return col.sum()


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

    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
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

    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
        snow_df_mi.groupby(axis=1, by=group_name).sum()

    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
        snow_df_mi.groupby(axis=1, level=0).min()


@pytest.mark.parametrize(
    "by",
    [
        lambda x: x // 3,
        ["col1", lambda x: x // 3],
        ["col1", lambda x: x + 1, lambda x: x % 3, "col2"],
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_with_callable_and_array(basic_snowpark_pandas_df, by) -> None:
    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
        basic_snowpark_pandas_df.groupby(by).min()


@sql_count_checker(query_count=0)
def test_timeseries_groupby_with_callable(tsframe):
    snow_ts_df = pd.DataFrame(tsframe)
    with pytest.raises(
        AttributeError, match="'SeriesGroupBy' object has no attribute np.percentile"
    ):
        snow_ts_df.groupby(lambda x: x.month).agg(np.percentile, 80, axis=0)


@sql_count_checker(query_count=0)
def test_groupby_with_numpy_array(basic_snowpark_pandas_df) -> None:
    by = [1, 1, 4, 2, 2, 4]
    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
        basic_snowpark_pandas_df.groupby(by=by).max()


@pytest.mark.parametrize(
    "by_list",
    [[2, 1, 1, 2, 3, 3], [[2, 1, 1, 2, 3, 3], "a"]],
)
@sql_count_checker(query_count=0)
def test_groupby_series_with_numpy_array(native_series_multi_numeric, by_list) -> None:
    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
        pd.Series(native_series_multi_numeric).groupby(by=by_list).max()


def test_groupby_with_external_series(basic_snowpark_pandas_df) -> None:
    series_data = [0, 1, 1, 0, 1]
    native_series = native_pd.Series(series_data)
    snowpark_pandas_series = pd.Series(native_series)

    with SqlCounter(query_count=0):
        with pytest.raises(
            NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
        ):
            basic_snowpark_pandas_df.groupby(by=snowpark_pandas_series).sum()

    with SqlCounter(query_count=0):
        by_list = ["col1", "col2", snowpark_pandas_series]
        with pytest.raises(
            NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
        ):
            basic_snowpark_pandas_df.groupby(by=by_list).sum()


@pytest.mark.parametrize(
    "func, kwargs, error_pattern",
    # note that we expect some functions like `any` to become strings like
    # 'any' in the error message because of preprocessing in the modin API
    # layer in [1]. That's okay.
    # [1] https://github.com/snowflakedb/snowpark-python/blob/7c854cb30df2383042d7899526d5237a44f9fdaf/src/snowflake/snowpark/modin/pandas/utils.py#L633
    [
        param(
            np.argmin,
            {},
            "'SeriesGroupBy' object has no attribute np.argmin",
            id="numpy_aggregation_function",
        ),
        param(
            sensitive_function_name,
            {},
            "'SeriesGroupBy' object has no attribute Callable",
            id="user_defined_function",
        ),
        param(
            [sensitive_function_name, "size"],
            {},
            "'SeriesGroupBy' object has no attribute Callable",
            id="list_with_user_defined_function_and_string",
        ),
        param(
            (sensitive_function_name, "size"),
            {},
            "'SeriesGroupBy' object has no attribute Callable",
            id="tuple_with_user_defined_function_and_string",
        ),
        param(
            {sensitive_function_name, "size"},
            {},
            "'SeriesGroupBy' object has no attribute Callable",
            id="set_with_user_defined_function_and_string",
        ),
        param(
            (all, any, len, list, min, max, set, str, tuple, native_pd.Series.sum),
            {},
            "'SeriesGroupBy' object has no attribute list",
            id="tuple_with_builtins_and_native_pandas_function",
        ),
        param(
            {
                "col2": sensitive_function_name,
                "col3": sum,
                "col4": "size",
                "col5": [np.mean, "size"],
            },
            {},
            "'SeriesGroupBy' object has no attribute Callable",
            id="dict",
        ),
        param(
            None,
            {
                "new_col": ("col2", sensitive_function_name),
                "new_col2": pd.NamedAgg("col3", sum),
            },
            "'SeriesGroupBy' object has no attribute Callable",
            id="named_agg",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_agg_unsupported_function_SNOW_1305464(
    basic_snowpark_pandas_df, func, kwargs, error_pattern
):
    with pytest.raises(AttributeError, match=error_pattern):
        basic_snowpark_pandas_df.groupby("col1").agg(func, **kwargs)


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
    with pytest.raises(
        NotImplementedError, match=AGGREGATE_UNSUPPORTED_GROUPING_ERROR_PATTERN
    ):
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
    with pytest.raises(
        AttributeError, match=f"'SeriesGroupBy' object has no attribute '{agg_name}'"
    ):
        getattr(grp_agg(snowpark_pandas_group), agg_name)()


@sql_count_checker(query_count=0)
def test_groupby_ngroups_axis_1():
    by = "x"
    native_df = native_pd.DataFrame(
        np.arange(12).reshape(3, 4), index=[0, 1, 0], columns=["a", "b", "a", "b"]
    )
    native_df.index.name = "y"
    native_df.columns.name = "x"
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            f"Snowpark pandas GroupBy.ngroups {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
        ),
    ):
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

    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            f"Snowpark pandas GroupBy.ngroups {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
        ),
    ):
        snow_df.groupby(by=by, axis=1).ngroups


@sql_count_checker(query_count=0)
def test_non_callable_func(basic_snowpark_pandas_df):
    # pandas error messages for non-callable aggregation functions are not
    # consistent, so don't check for a match with pandas.
    with pytest.raises(
        ValueError, match=re.escape("aggregation function is not callable")
    ):
        basic_snowpark_pandas_df.groupby("col1").agg([1])
