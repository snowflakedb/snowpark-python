#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data, index",
    [
        (
            {
                "consumption": [10.51, 103.11, 55.48],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {
                "consumption": [10.51, 103.11, 55.48],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            [10, 17, 12],
        ),
        (
            {"consumption": [10.51, 56, 38.48], "co2_emissions": [10.51, 46, 58.48]},
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {
                "consumption": [58.3, np.nan, np.nan],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {
                "consumption": [np.nan, np.nan, np.nan],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {"consumption": [58.3, np.nan, np.nan], "co2_emissions": [np.nan, 36, 84]},
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {
                "consumption": [58.3, np.nan, np.nan],
                "co2_emissions": [np.nan, np.nan, np.nan],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        ([[1, 2], [3, 4]], [0, 0]),
        ([[1, 2.5]], None),
        ({"A": []}, []),
        ({}, []),
    ],
)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("skipna", [True, False])
def test_idxmax_idxmin_df(data, index, func, axis, skipna):
    if data == {"A": []} and axis == 0:
        pytest.xfail(
            "Snowpark pandas returns a Series with None whereas pandas throws a ValueError"
        )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            data=data,
            index=index,
        ),
        lambda df: getattr(df, func)(axis=axis, skipna=skipna),
        # pandas doesn't propagate attrs if the frame is empty, but Snowpark pandas does.
        test_attrs=len(native_pd.DataFrame(data).index) != 0,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data, index",
    [
        (
            {
                "consumption": ["i", "am", "batman"],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        pytest.param(
            {"consumption": ["GA", "in", 2024], "co2_emissions": [37.2, 19.66, 1712]},
            ["Pork", "Wheat Products", "Beef"],
            marks=pytest.mark.xfail(
                reason="pandas throws TypeError when Snowpark pandas returns result"
            ),
        ),
        pytest.param(
            [[1, pd.Timestamp(3)]],
            None,
            marks=pytest.mark.xfail(
                reason="pandas throws TypeError when Snowpark pandas returns result"
            ),
        ),
    ],
)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("numeric_only", [True, False])
def test_idxmax_idxmin_df_numeric_only_axis_0_different_column_dtypes(
    data, index, func, numeric_only
):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            data=data,
            index=index,
        ),
        lambda df: getattr(df, func)(axis=0, numeric_only=numeric_only),
    )


@pytest.mark.parametrize(
    "data, index",
    [
        (
            {
                "consumption": ["i", "am", "batman"],
                "co2_emissions": [37.2, 19.66, 1712],
            },
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            {"consumption": ["GA", "in", 2024], "co2_emissions": [37.2, 19.66, 1712]},
            ["Pork", "Wheat Products", "Beef"],
        ),
        (
            [[1, pd.Timestamp(3)]],
            None,
        ),
    ],
)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("numeric_only", [True, False])
def test_idxmax_idxmin_df_numeric_only_axis_1_different_column_dtypes(
    data, index, func, numeric_only
):
    native_df = native_pd.DataFrame(data=data, index=index)
    snow_df = pd.DataFrame(native_df)

    if numeric_only:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(df, func)(axis=1, numeric_only=numeric_only),
            )
    else:
        with SqlCounter(query_count=0):
            # pandas raises a TypeError that is type-specific (e.g. "string and float type can't be compared")
            # vs. the Modin frontend returns "reduce operation 'argmax' not allowed for this dtype"
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(df, func)(axis=1, numeric_only=numeric_only),
                expect_exception=True,
                expect_exception_type=TypeError,
                assert_exception_equal=False,
            )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [0, 1])
def test_idxmax_idxmin_with_dates(func, axis):
    native_df = native_pd.DataFrame(
        data={
            "date_1": ["2000-01-01", "2000-01-02", "2000-01-03"],
            "date_2": ["2000-01-04", "1999-12-18", "2005-01-03"],
        },
        index=[10, 17, 12],
    )
    native_df["date_1"] = native_pd.to_datetime(native_df["date_1"])
    native_df["date_2"] = native_pd.to_datetime(native_df["date_2"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, func)(axis=axis),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize(
    "axis",
    [
        0,
        pytest.param(
            1,
            marks=pytest.mark.xfail(
                strict=True, raises=NotImplementedError, reason="SNOW-1653126"
            ),
        ),
    ],
)
def test_idxmax_idxmin_with_timedelta(func, axis):
    native_df = native_pd.DataFrame(
        data={
            "date_1": native_pd.timedelta_range(1, periods=3),
            "date_2": [pd.Timedelta(1), pd.Timedelta(-1), pd.Timedelta(0)],
        },
        index=[10, 17, 12],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, func)(axis=axis),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [0, 1])
def test_idxmax_idxmin_with_strings(func, axis):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            data={
                "string_1": ["i", "am", "batman"],
                "string_2": ["you", "are", "bane"],
            },
            index=[10, 17, 12],
        ),
        lambda df: getattr(df, func)(axis=axis),
    )


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [0, 1])
def test_idxmax_idxmin_empty_df_with_index(func, axis):
    if axis == 0:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                *create_test_dfs(
                    data={},
                    index=["hello"],
                ),
                lambda df: getattr(df, func)(axis=axis),
                # pandas doesn't propagate attrs if the frame is empty, but Snowpark pandas does.
                test_attrs=False,
            )
    else:
        with SqlCounter(query_count=0):
            # pandas raises ValueError("attempt to get argmax of an empty sequence") for
            # an empty dataframe with an index, but in Snowpark pandas we return an empty
            # dataframe. Since an extra check for a valid index adds a query, this behavior is
            # not worth changing.
            pytest.xfail(
                reason="Snowpark pandas returns empty dataframe instead of ValueError"
            )


@sql_count_checker(query_count=0)
@pytest.mark.xfail(
    strict=True, raises=TypeError, reason="Snowpark lit() cannot parse pd.Timestamp"
)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [1])
def test_idxmax_idxmin_crazy_column_names_axis_1(func, axis):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [[1, 2], [3, 4]],
            columns=[pd.Timestamp(1)] * 2,
        ),
        lambda df: getattr(df, func)(axis=axis),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_idxmax_idxmin_multiindex_unsupported(func, axis):
    arrays = [
        ["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"],
        ["one", "two", "one", "two", "one", "two", "one", "two"],
    ]
    eval_snowpark_pandas_result(
        *create_test_dfs(
            np.random.randn(8, 4),
            index=arrays,
        ),
        lambda df: getattr(df, func)(axis=axis),
    )
