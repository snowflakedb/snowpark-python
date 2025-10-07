#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd  # noqa: F401
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


# Test data for basic interpolation scenarios
BASIC_INTERPOLATE_DATA = [
    # Basic linear interpolation
    {
        "data": {"A": [1, np.nan, 3, 4], "B": [np.nan, 2, 3, np.nan]},
        "expected": {"A": [1, 2, 3, 4], "B": [np.nan, 2, 3, np.nan]},
        "id": "basic_linear",
    },
    # Single column with multiple NaNs
    {
        "data": {"A": [1, np.nan, np.nan, 4, 5]},
        "expected": {"A": [1, 2, 3, 4, 5]},
        "id": "multiple_nans",
    },
    # All NaN column
    {
        "data": {"A": [np.nan, np.nan, np.nan], "B": [1, 2, 3]},
        "expected": {"A": [np.nan, np.nan, np.nan], "B": [1, 2, 3]},
        "id": "all_nan_column",
    },
    # No NaN values
    {
        "data": {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8]},
        "expected": {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8]},
        "id": "no_nans",
    },
    # Mixed data types
    {
        "data": {"A": [1, np.nan, 3], "B": ["x", "y", "z"], "C": [1.5, np.nan, 3.5]},
        "expected": {"A": [1, 2, 3], "B": ["x", "y", "z"], "C": [1.5, 2.5, 3.5]},
        "id": "mixed_types",
    },
]


@pytest.mark.parametrize("test_data", BASIC_INTERPOLATE_DATA)
@sql_count_checker(query_count=1)
def test_df_interpolate_basic(test_data):
    """Test basic DataFrame.interpolate functionality with default linear method."""
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data["data"]),
        lambda df: df.interpolate(),
    )


@pytest.mark.parametrize(
    "method",
    ["linear", "pad", "ffill", "bfill", "backfill"],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_methods(method):
    """Test DataFrame.interpolate with different interpolation methods."""
    data = {"A": [1, np.nan, 3, np.nan, 5], "B": [np.nan, 2, np.nan, 4, 5]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(method=method),
    )


@pytest.mark.parametrize(
    "limit",
    [1, 2, 3, None],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_limit(limit):
    """Test DataFrame.interpolate with limit parameter."""
    data = {"A": [1, np.nan, np.nan, np.nan, 5], "B": [np.nan, 2, np.nan, 4, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(limit=limit),
    )


@pytest.mark.parametrize(
    "limit_direction",
    ["forward", "backward", "both"],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_limit_direction(limit_direction):
    """Test DataFrame.interpolate with limit_direction parameter."""
    data = {"A": [1, np.nan, np.nan, np.nan, 5], "B": [np.nan, 2, np.nan, 4, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(limit=2, limit_direction=limit_direction),
    )


@pytest.mark.parametrize(
    "axis",
    [0, 1],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_axis(axis):
    """Test DataFrame.interpolate with different axis parameters."""
    data = {"A": [1, np.nan, 3], "B": [np.nan, 2, np.nan], "C": [1, 2, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(axis=axis),
    )


@pytest.mark.parametrize(
    "inplace",
    [True, False],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_inplace(inplace):
    """Test DataFrame.interpolate with inplace parameter."""
    data = {"A": [1, np.nan, 3], "B": [np.nan, 2, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(inplace=inplace),
        inplace=inplace,
    )


@pytest.mark.parametrize(
    "limit_area",
    ["inside", "outside", None],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_limit_area(limit_area):
    """Test DataFrame.interpolate with limit_area parameter."""
    data = {"A": [1, np.nan, np.nan, 4], "B": [np.nan, 2, 3, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(limit=1, limit_area=limit_area),
    )


@pytest.mark.parametrize(
    "test_data",
    [
        # Empty DataFrame
        {"data": {}, "id": "empty_df"},
        # Single row DataFrame
        {"data": {"A": [1], "B": [2]}, "id": "single_row"},
        # Single column DataFrame
        {"data": {"A": [1, np.nan, 3]}, "id": "single_column"},
        # DataFrame with all NaN values
        {"data": {"A": [np.nan, np.nan], "B": [np.nan, np.nan]}, "id": "all_nans"},
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_edge_cases(test_data):
    """Test DataFrame.interpolate with edge cases."""
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data["data"]),
        lambda df: df.interpolate(),
    )


@pytest.mark.parametrize(
    "test_data",
    [
        # MultiIndex DataFrame
        {
            "data": {"A": [1, np.nan, np.nan, 4]},
            "index": native_pd.MultiIndex.from_arrays(
                [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
            ),
            "id": "multiindex",
        },
        # Custom index DataFrame
        {
            "data": {"A": [1, np.nan, 3], "B": [np.nan, 2, np.nan]},
            "index": ["x", "y", "z"],
            "id": "custom_index",
        },
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_index_types(test_data):
    """Test DataFrame.interpolate with different index types."""
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data["data"], index=test_data.get("index")),
        lambda df: df.interpolate(),
    )


@pytest.mark.parametrize(
    "dtype",
    ["float64", "int64", "float32", "int32"],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_dtypes(dtype):
    """Test DataFrame.interpolate with different numeric data types."""
    data = {"A": [1, np.nan, 3], "B": [np.nan, 2, np.nan]}

    snow_df, native_df = create_test_dfs(data)
    snow_df = snow_df.astype(dtype)
    native_df = native_df.astype(dtype)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.interpolate(),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"method": "linear", "limit": 1},
        {"method": "pad", "limit_direction": "forward"},
        {"method": "bfill", "limit_area": "inside"},
        {"axis": 1, "limit": 2},
        {"method": "linear", "inplace": True},
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_parameter_combinations(kwargs):
    """Test DataFrame.interpolate with various parameter combinations."""
    data = {"A": [1, np.nan, np.nan, 4], "B": [np.nan, 2, 3, np.nan]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(**kwargs),
        inplace=kwargs.get("inplace", False),
    )


@pytest.mark.parametrize(
    "time_precision, null_sparsity",
    [
        # Different time precisions
        ("D", "low"),  # Daily precision, low null sparsity
        ("H", "medium"),  # Hourly precision, medium null sparsity
        ("T", "high"),  # Minute precision, high null sparsity
        ("S", "low"),  # Second precision, low null sparsity
        ("ms", "medium"),  # Millisecond precision, medium null sparsity
        ("us", "high"),  # Microsecond precision, high null sparsity
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_datetime_precision_and_sparsity(time_precision, null_sparsity):
    """Test DataFrame.interpolate with different datetime precisions and null sparsities."""
    # Define null patterns based on sparsity
    null_patterns = {
        "low": [0, 1, 0, 0, 1, 0, 0, 0],  # ~25% nulls
        "medium": [0, 1, 1, 0, 1, 1, 0, 1],  # ~50% nulls
        "high": [1, 0, 1, 1, 0, 1, 1, 1],  # ~75% nulls
    }

    # Create datetime data based on precision
    base_dates = native_pd.date_range("2023-01-01", periods=8, freq=time_precision)

    # Apply null pattern to create missing values
    null_mask = null_patterns[null_sparsity]
    timestamps = [
        base_dates[i] if not null_mask[i] else None for i in range(len(base_dates))
    ]

    data = {
        "timestamp": native_pd.to_datetime(timestamps),
        "value": [1, np.nan, 3, np.nan, 5, np.nan, 7, np.nan],
        "price": [10.5, np.nan, np.nan, 13.0, 14.5, np.nan, np.nan, 18.0],
    }

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(),
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_preserves_non_numeric_columns():
    """Test that DataFrame.interpolate preserves non-numeric columns unchanged."""
    data = {
        "A": [1, np.nan, 3],
        "B": ["x", "y", "z"],
        "C": [True, False, True],
        "D": [np.nan, 2.5, np.nan],
    }

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(),
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_with_datetime_index():
    """Test DataFrame.interpolate with datetime index."""
    dates = native_pd.date_range("2023-01-01", periods=4, freq="D")
    data = {"value": [1, np.nan, np.nan, 4]}

    eval_snowpark_pandas_result(
        *create_test_dfs(data, index=dates),
        lambda df: df.interpolate(),
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_large_dataframe():
    """Test DataFrame.interpolate with a larger DataFrame."""
    np.random.seed(42)
    data = {
        "A": np.random.randn(100),
        "B": np.random.randn(100),
        "C": np.random.randn(100),
    }

    # Introduce some NaN values
    for col in data:
        nan_indices = np.random.choice(100, size=10, replace=False)
        data[col][nan_indices] = np.nan

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(),
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_error_handling():
    """Test DataFrame.interpolate error handling for invalid parameters."""
    data = {"A": [1, np.nan, 3]}
    snow_df, native_df = create_test_dfs(data)

    # Test with invalid method
    with pytest.raises(ValueError):
        snow_df.interpolate(method="invalid_method")

    with pytest.raises(ValueError):
        native_df.interpolate(method="invalid_method")

    # Test with invalid limit_direction
    with pytest.raises(ValueError):
        snow_df.interpolate(limit_direction="invalid_direction")

    with pytest.raises(ValueError):
        native_df.interpolate(limit_direction="invalid_direction")
