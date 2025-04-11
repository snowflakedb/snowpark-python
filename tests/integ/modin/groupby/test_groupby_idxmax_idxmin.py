#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.groupby.conftest import multiindex_data
from tests.integ.modin.utils import (
    assert_frame_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "grouping_columns",
    [
        pytest.param(
            "B",
            marks=pytest.mark.xfail(
                reason="SNOW-1270521: `idxmax/idxmin` results in a non-deterministic ordering when tiebreaking values"
            ),
        ),
        ["A", "B"],
    ],
)
@pytest.mark.parametrize("skipna", [False, True])
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@sql_count_checker(query_count=1)
def test_df_groupby_idxmax_idxmin_on_axis_0(
    df_with_multiple_columns, grouping_columns, skipna, func
):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin.
    Here, the DataFrames are grouped by `by` and not `level`.
    """
    eval_snowpark_pandas_result(
        *df_with_multiple_columns,
        lambda df: df.groupby(by=grouping_columns).__getattribute__(func)(
            skipna=skipna, axis=0
        ),
    )


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("level", [0, 1])
@pytest.mark.parametrize("skipna", [False, True])
@sql_count_checker(query_count=0)
def test_df_groupby_idxmax_idxmin_with_multiindex_df(func, level, skipna):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin with a MultiIndex DataFrame.
    Here, the MultiIndex DataFrames are grouped by `level` and not `by`.
    """
    # Create MultiIndex DataFrames.
    df = pd.DataFrame(multiindex_data)
    df = df.set_index(["A", "B"])

    with pytest.raises(
        NotImplementedError,
        match=f"{func} is not yet supported when the index is a MultiIndex.",
    ):
        df.groupby(level=level).__getattribute__(func)(axis=0, skipna=skipna)


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@sql_count_checker(query_count=0)
def test_df_groupby_idxmax_idxmin_on_axis_1_negative(df_with_multiple_columns, func):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin fail when axis=1.
    """
    df = df_with_multiple_columns[0]
    with pytest.raises(
        NotImplementedError,
        match=f"DataFrameGroupBy.{func} with axis=1 is deprecated and will be"
        f" removed in a future version",
    ):
        df.groupby(by="B").__getattribute__(func)(axis=1)


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("numeric_only", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_idxmax_idxmin_with_different_column_dtypes_on_axis_0(
    func, numeric_only
):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin work with columns of different types.
    """
    data = {
        "consumption": ["i", "am", "batman"],
        "consumption2": ["i", "am", "batman"],
        "co2_emissions": [37.2, 19.66, 1712],
    }
    index = ["Pork", "Wheat Products", "Beef"]
    eval_snowpark_pandas_result(
        *create_test_dfs(
            data=data,
            index=index,
        ),
        lambda df: df.groupby("consumption").__getattribute__(func)(
            axis=0, numeric_only=numeric_only
        ),
    )


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@sql_count_checker(query_count=1)
def test_df_groupby_idxmax_idxmin_with_dates_on_axis_0(func):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin work with dates.
    """
    native_df = native_pd.DataFrame(
        data={
            "date_1": ["2000-01-01", "2000-01-01", "2000-01-03"],
            "date_2": ["2000-01-04", "1999-12-18", "2005-01-03"],
            "date_3": ["2001-01-04", "1990-12-18", "2025-01-03"],
            "date_4": ["2010-01-04", "1989-12-18", "2009-01-03"],
        },
        index=[10, 17, 12],
    )
    for col in native_df.columns:
        native_df[col] = native_pd.to_datetime(native_df[col])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by="date_1"), func)(axis=0),
    )


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@sql_count_checker(query_count=0)
def test_df_groupby_idxmax_idxmin_on_groupby_axis_1_unimplemented(func):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin.
    Here, the DataFrames are grouped by `by` and should raise NotImplementedError
    Only testing with idxmax/idxmin(axis=0) since we already raise NotImplementedError when axis=1 is passed.
    """
    # Example from discussion comment:
    # https://github.com/pandas-dev/pandas/issues/51203#issuecomment-1426864317
    def grouper(c):
        if c.startswith("A"):
            return "ACCESS"
        if c.startswith("B"):
            return "BACKHAUL"
        if c.startswith("C"):
            return "CORE"

    items = ["A_10", "A_20", "A_30", "B_10", "B_20", "B_30", "C_10", "C_20", "C_30"]
    costs = np.random.default_rng().uniform(low=1, high=10_000, size=(50, len(items)))
    df = native_pd.DataFrame(costs, columns=items)
    native_res = df.groupby(by=grouper, axis=1).idxmax(axis=0)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas GroupBy.aggregate does not yet support axis == 1, by != None and level != None, or by containing any non-pandas hashable labels.",
    ):
        snow_res = pd.DataFrame(df).groupby(by=grouper, axis=1).idxmax(axis=0)
        assert_frame_equal(native_res, snow_res, check_index_type=False)


@pytest.mark.parametrize("agg_func", ["idxmin", "idxmax"])
@pytest.mark.parametrize("by", ["A", "B"])
@sql_count_checker(query_count=1)
def test_timedelta(agg_func, by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df.groupby(by), agg_func)()
    )
