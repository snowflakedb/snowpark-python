#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
import pytest
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.window.utils import (
    agg_func,
)

data = pytest.mark.parametrize(
    "data",
    [
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]},
        {
            "A": [1, 1, np.nan, 2],
            "B": [1, np.nan, 3, 4],
            "C": [np.nan, np.nan, np.nan, np.nan],
        },
    ],
)
window = pytest.mark.parametrize("window", [1, 2, 3, 4, 6])
min_periods = pytest.mark.parametrize("min_periods", [None, 1, 2])
center = pytest.mark.parametrize("center", [True, False])


@data
@agg_func
@window
@min_periods
@center
def test_rolling_dataframe(data, window, min_periods, center, agg_func):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)
    if min_periods is not None and min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
                test_attrs=False,
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                test_attrs=False,
            )


@sql_count_checker(query_count=1)
def test_groupby_rolling_by_multiple_cols():
    native_df = native_pd.DataFrame(
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(["A", "B"]).rolling(2).sum(),
        test_attrs=False,
    )


@sql_count_checker(query_count=1)
def test_groupby_rolling_dropna_false():
    native_df = native_pd.DataFrame(
        {
            "A": [1, 1, np.nan, np.nan, 2],
            "B": [1, 2, 3, 4, 5],
            "C": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by="A", dropna=False).rolling(2).sum(),
        test_attrs=False,
    )


@sql_count_checker(query_count=0)
def test_groupby_rolling_series_negative():
    date_idx = pd.date_range("1/1/2000", periods=8, freq="min")
    date_idx.names = ["grp_col"]
    snow_ser = pd.Series([1, 1, np.nan, 2])
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "Snowpark pandas does not yet support the method GroupBy.rolling for Series"
        ),
    ):
        snow_ser.groupby(level=0).rolling(2).sum()


@pytest.mark.parametrize(
    "function",
    [
        lambda df: df.groupby("A").rolling(2, on="B").sum(),
        lambda df: df.groupby("A").rolling(2, closed="left").sum(),
        lambda df: df.groupby("A").rolling([1, 2]).sum(),
        lambda df: df.groupby("A").rolling(2, method="table").sum(),
        lambda df: df.groupby("A").rolling(2, win_type="hamming").sum(),
        lambda df: df.groupby("A").rolling(2, axis=1).sum(),
        lambda df: df.groupby("A").rolling(0, min_periods=0).sum(),
        lambda df: df.groupby("A").rolling(0).quantile(),
    ],
)
@sql_count_checker(query_count=0)
def test_rolling_params_unsupported(function):
    snow_df = pd.DataFrame(
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
    )
    with pytest.raises(NotImplementedError):
        function(snow_df)


@sql_count_checker(query_count=1)
def test_groupby_rolling_with_named_index():
    """Test groupby rolling with a named index"""
    native_df = native_pd.DataFrame(
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
    )
    native_df.index.name = "row_id"
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("A").rolling(2).sum(),
        test_attrs=False,
    )


@sql_count_checker(query_count=1)
def test_groupby_rolling_with_multiindex_index():
    index = native_pd.MultiIndex.from_tuples(
        (("A", 1), ("A", 2), ("B", 1), ("B", 2)), names=["letter", "number"]
    )

    native_df = native_pd.DataFrame(
        {
            "group": [1, 1, 2, 2],
            "value1": [1, 2, 3, 4],
            "value2": [0.362, 0.227, 1.267, -0.562],
        },
        index=index,
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("group").rolling(2).sum(),
        test_attrs=False,
    )


@sql_count_checker(query_count=1)
def test_groupby_rolling_with_datetime_index():
    date_index = native_pd.date_range("2023-01-01", periods=8, freq="D")
    date_index.name = "date"

    native_df = native_pd.DataFrame(
        {
            "group": [1, 1, 1, 1, 2, 2, 2, 2],
            "value": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8],
        },
        index=date_index,
    )

    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("group").rolling(3).sum(),
        test_attrs=False,
    )
