#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs

# The aggregation methods whose result in snowflake is compatible with pandas
result_compatible_agg_methods = [
    lambda gr: gr.max(),
    lambda gr: gr.min(),
    lambda gr: gr.sum(),
    lambda gr: gr.count(),
    lambda gr: gr.std(),
    lambda gr: gr.std(ddof=0),
    lambda gr: gr.agg("max"),
    lambda gr: gr.agg(["max", min, np.std]),
]

# the aggregation methods whose result on integer column is decimal with scale > 0 in snowflake,
# but float in pandas, which will have some precision difference.
int_to_decimal_float_agg_methods = [
    lambda gr: gr.mean(),
    lambda gr: gr.median(),
    lambda gr: gr.var(),
    lambda gr: gr.var(ddof=0),
]
all_agg_methods = result_compatible_agg_methods + int_to_decimal_float_agg_methods

# List of groupby aggregate methods that takes min_count as argument
min_count_methods = ["min", "max", "sum"]

# Seeded random number generator.
rng = np.random.default_rng(1234)
multiindex_data = {
    "A": [
        "foo",
        "bar",
        "foo",
        "bar",
        "foo",
        "bar",
        "foo",
        None,
        "foo",
        None,
        "foo",
        "bar",
        "foo",
        None,
        "bar",
        "foo",
    ],
    "B": [
        "one",
        "one",
        "two",
        None,
        "two",
        "two",
        "one",
        None,
        "two",
        "two",
        "one",
        None,
        "one",
        "one",
        "two",
        None,
    ],
    "C": rng.integers(low=-10, high=10, size=16),
    "D": rng.integers(low=-10, high=10, size=16),
}


@pytest.fixture(params=min_count_methods)
def min_count_method(request):
    """Fixture for parametrization of result compatible aggregation methods."""
    return request.param


@pytest.fixture(params=result_compatible_agg_methods)
def result_compatible_agg_method(request):
    """Fixture for parametrization of result compatible aggregation methods."""
    return request.param


@pytest.fixture(params=int_to_decimal_float_agg_methods)
def int_to_decimal_float_agg_method(request):
    """Fixture for parametrization of decimal result methods."""
    return request.param


@pytest.fixture(params=all_agg_methods)
def agg_method(request):
    """Fixture for parametrization of all supported aggregation methods."""
    return request.param


@pytest.fixture(scope="function")
def basic_snowpark_pandas_df():
    return pd.DataFrame(
        {
            "col1": [2, 1, 1, 0, 2, 0],
            "col2": [4, 5, 36, 7, 4, 5],
            "col3": [3.1, 8.0, 12, 10, 4, 1.1],
            "col4": [17, 3, 16, 15, 5, 6],
            "col5": [-1, 3, -1, 3, -2, -1],
        }
    )


@pytest.fixture(scope="function")
def basic_df_data():
    return {
        "col1": [2, 1, 1, 0, 2, 0],
        "col2": [4, 5, 36, 7, 4, 5],
        "col3": [3.1, 8.0, 12, 10, 4, 1.1],
        "col4": [17, 3, 16, 15, 5, 6],
        "col5": [-1, 3, -1, 3, -2, -1],
    }


@pytest.fixture(scope="function")
def basic_snowpark_pandas_df_with_missing_values():
    return pd.DataFrame(
        {
            "col1": [None, 1, 1, 0, None, 0],
            "col2": [4, 5, None, 7, 4, 5],
            "col3": [3.1, 8.0, 12, 10, 4, np.nan],
            "col4": [17, 3, 16, 15, None, None],
            "col5": [None, 3, -1, 3, -2, None],
        }
    )


@pytest.fixture(scope="function")
def df_multi():
    df = pd.DataFrame(
        {
            "A": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"],
            "B": ["one", "one", "two", "three", "two", "two", "one", "three"],
            "C": np.random.randn(8),
            "D": np.random.randn(8),
        }
    )
    df_mi = df.set_index(["A", "B"])
    return df_mi


@pytest.fixture(scope="function")
def native_series_multi_numeric():
    index = native_pd.MultiIndex(
        levels=[[1, 2], [1, 2]],
        codes=[[0, 0, 0, 0, 1, 1], [1, 1, 0, 0, 0, 0]],
        names=["a", "b"],
    )
    return native_pd.Series([0, 1.0, 2.0, 3.0, 4.0, 5.0], index=index)


@pytest.fixture(scope="function")
def series_str():
    index = pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    return pd.Series(["ac", "ea", "be", "ce", "dc"], index=index)


# Seeded random number generator.
rng = np.random.default_rng(1234)
# Number of rows to generate in the large df.
NUM_ROWS = 100


def generate_row_data():
    """
    Helper function to generate random row data.
    """
    return [
        rng.uniform(low=-50, high=50),  # favorite_number
        rng.choice(["red", "blue", "green", "yellow", None]),  # color
        rng.integers(low=-200, high=10),  # random1
        rng.uniform(low=-1000, high=1000),  # random2
        rng.choice([np.nan, np.inf, 3, 4, 5, 6, 7, 8, 9]),  # random3
        rng.binomial(n=100, p=0.005),  # random4
    ]


@pytest.fixture(scope="function")
def large_df_with_na_values():
    """
    Helper function to create a large df with na values.
    It returns both Snowpark pandas and native pandas DataFrames.
    """
    data = [generate_row_data() for _ in range(NUM_ROWS)]
    index = rng.integers(low=-10, high=10, size=NUM_ROWS)
    return create_test_dfs(
        data,
        columns=(
            "favorite_number",
            "color",
            "random1",
            "random2",
            "random3",
            "random4",
        ),
        index=index,
    )


@pytest.fixture(scope="function")
def df_with_multiple_columns():
    """
    Create a Snowpark pandas and native pandas DataFrame with multiple columns.
    """
    return create_test_dfs(
        [
            [None, 1, 87, np.nan, -48, 8],
            [1, 2, 3, -10, -20, -30],
            [1, None, 6, 40, 50, 60],
            [None, None, 7, -80, 90, 100],
            [1, 2, 0, -70, np.nan, -90],
            [1, None, 80, -100, -100, -100],
            [1, 1, 6, 10, 50, 60],
            [1, 2, 1, -10, 10, -10],
            [None, None, 6, -9, 12, 15],
            [1, 1, 16, 32, 64, 128],
            [None, 1, -10, -20, -30],
            [1, 2, 3, 6, 50, 60],
            [1, None, 7, -80, 90, 100],
            [None, None, 7, -70, -80, -90],
            [1, 2, 80, -100, -100, -100],
            [1, None, 80, np.nan, -100, -100],
            [1, 1, 1, -10, 10, -10],
            [1, 2, 6, -9, 12, 15],
            [None, None, 16, 32, 64, 128],
            [1, 1, 16, 3, 2, 1],
        ],
        columns=list("ABCDEF"),
        index=list("cbafjihgedjihgcbafed"),
    )
