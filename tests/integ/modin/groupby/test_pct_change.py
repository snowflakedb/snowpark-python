#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import pandas as native_pd
import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.groupby.conftest import multiindex_data
from tests.integ.modin.utils import (
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

BASIC_DATA = [
    ["a", 1, 1.1],
    ["a", 1, 1.2],
    ["a", 2, 1.3],
    ["b", 2, 1.4],
    ["a", 3, 1.5],
    ["b", 4, 1.6],
    ["c", 1, 0.9],
]


@pytest.mark.parametrize("periods", [0, 1, 2, -1])
@pytest.mark.parametrize("fill_method", ["bfill", "ffill"])
@pytest.mark.parametrize("by", [0, [0, 1]])
@sql_count_checker(query_count=1)
def test_df_groupby_pct_change_basic(by, periods, fill_method):
    eval_snowpark_pandas_result(
        *create_test_dfs(BASIC_DATA),
        lambda df: df.groupby(by).pct_change(periods=periods, fill_method=fill_method),
    )


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@sql_count_checker(query_count=1)
def test_df_groupby_pct_change_parameters(dropna, as_index, sort, group_keys):
    # Unlike other GroupBy methods, none of the groupby parameters actually affect for pct_change.
    # This test still verifies that we match the pandas output when they're specified.
    eval_snowpark_pandas_result(
        *create_test_dfs(BASIC_DATA),
        lambda df: df.groupby(
            0, dropna=dropna, as_index=as_index, sort=sort, group_keys=group_keys
        ).pct_change(),
    )


@pytest.mark.parametrize("level", [0, 1])
@sql_count_checker(query_count=1)
def test_df_groupby_pct_change_mi(level):
    # pandas considers NaN != NaN when performing the groupby but Snowpark pandas does not, so any
    # results in rows where the grouping key was NaN will be NaN in native pandas but have a value
    # in Snowpark pandas
    if level == 0:
        col_label = "A"
    else:
        col_label = "B"
    native_df = native_pd.DataFrame(multiindex_data).replace(
        {col_label: {None: "empty"}}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        # Replace all 0s to work around division by zero error in SNOW-1800586
        lambda df: df.set_index(["A", "B"])
        .replace(0, 1)
        .groupby(level=level)
        .pct_change(),
        check_index_type=False,
    )


@pytest.mark.parametrize("name", [None, "named"])
@sql_count_checker(query_count=1)
def test_series_groupby_pct_change_basic(name):
    data = [1, 1, 2, 3, 4]
    index = [0, 0, 1, 1, 0]
    eval_snowpark_pandas_result(
        *create_test_series(data, index=native_pd.Index(index), name=name),
        lambda df: df.groupby(level=0).pct_change(),
    )


@sql_count_checker(query_count=0)
def test_series_groupby_pct_change_division_by_zero_negative():
    data = [0, 0, 0, 1]
    index = [0, 0, 1, 1]
    # SNOW-1800586: pandas creates NaN/inf when dividing by zero, but we raise SnowflakeSQLException
    with pytest.raises(SnowparkSQLException):
        eval_snowpark_pandas_result(
            *create_test_series(data, index=native_pd.Index(index)),
            lambda df: df.groupby(level=0).pct_change(),
        )


@sql_count_checker(query_count=0)
def test_df_groupby_pct_change_nonnumeric_negative():
    # Using non-numeric columns raises a SnowparkSQLException
    snow_series, native_series = create_test_series(
        ["a", "b", "c"], index=native_pd.Index([0, 0, 1])
    )
    with pytest.raises(TypeError):
        native_series.groupby(level=0).pct_change()
    with pytest.raises(SnowparkSQLException):
        snow_series.groupby(level=0).pct_change().to_pandas()


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 1},
        {"freq": 1},
    ],
)
@sql_count_checker(query_count=0)
def test_df_groupby_pct_change_unsupported(params):
    with pytest.raises(NotImplementedError):
        pd.DataFrame(BASIC_DATA).groupby(0).pct_change(**params).to_pandas()
