#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from string import ascii_lowercase

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401

default_index_df_data = {
    "A": [1, 2, 3, 4, 5, 6, 7],
    "B": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7],
    "C": [True, False, True, False, True, False, True],
    "D": ["a", "b", "c", "d", "e", "f", "g"],
    "E": [
        "2021-01-01",
        "2021-02-01",
        "2021-03-01",
        "2021-04-01",
        "2021-05-01",
        "2021-06-01",
        "2021-07-01",
    ],
    "F": [(1,), (2,), (3,), (4,), (5,), (6,), (7,)],
    "G": [[1], [2], [3], [4], [5], [6], [7]],
}

default_index_df_data_for_multiindex_columns = {
    ("bar", "one"): [1, 2, 3, 4, 5, 6, 7],
    ("bar", "two"): [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7],
    ("baz", "one"): [True, False, True, False, True, False, True],
    ("baz", "two"): ["a", "b", "c", "d", "e", "f", "g"],
    ("foo", "one"): [
        "2021-01-01",
        "2021-02-01",
        "2021-03-01",
        "2021-04-01",
        "2021-05-01",
        "2021-06-01",
        "2021-07-01",
    ],
    ("foo", "two"): [(1,), (2,), (3,), (4,), (5,), (6,), (7,)],
    ("extra", "one"): [[1], [2], [3], [4], [5], [6], [7]],
}

loc_str_index = ["a", "b", "c", "d", "e", "f", "a"]
str_index_native_df_input = native_pd.DataFrame(
    default_index_df_data,
    index=loc_str_index,
).rename(columns={"G": "A"})

date_columns = native_pd.date_range(start="2023-01-01", periods=7, freq="h", tz="UTC")
date_columns_no_tz = date_columns.tz_localize(None)
df_data = np.random.randint(0, 10, size=(7, 7))

arrays_for_multiindex = [
    ["bar", "bar", "baz", "baz", "foo", "foo", "extra"],
    ["one", "two", "one", "two", "one", "two", "one"],
]

simple_data = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]


@pytest.fixture(scope="module")
def score_test_data():
    return {
        "name": ["Alice", "Bob", "Bob"],
        "score": [9.5, 8, 9.5],
        "employed": [False, True, False],
        "kids": [0, 0, 1],
    }


@pytest.fixture(scope="function")
def empty_index_native_pandas_dataframe() -> native_pd.DataFrame:
    # An empty dataframe with only index
    native_df = native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    native_df.set_index("A", inplace=True)
    native_df.drop(columns=native_df.columns, inplace=True)
    return native_df


@pytest.fixture(scope="function")
def default_index_snowpark_pandas_df():
    return pd.DataFrame(default_index_df_data)


@pytest.fixture(scope="function")
def default_index_native_df():
    # native pandas DataFrame with default index
    return native_pd.DataFrame(default_index_df_data)


@pytest.fixture(scope="function")
def multiindex_native():
    tuples = list(zip(*arrays_for_multiindex))
    return native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])


@pytest.fixture(scope="function")
def native_df_with_multiindex_columns(multiindex_native):
    return native_pd.DataFrame(
        default_index_df_data_for_multiindex_columns, columns=multiindex_native
    )


@pytest.fixture(scope="function")
def empty_snowpark_pandas_df():
    return pd.DataFrame()


@pytest.fixture(scope="function")
def str_index_snowpark_pandas_df():

    return pd.DataFrame(
        str_index_native_df_input,
    )


@pytest.fixture(scope="function")
def str_index_native_df():

    return str_index_native_df_input.copy(deep=True)


@pytest.fixture(scope="function")
def time_column_snowpark_pandas_df():
    return pd.DataFrame(df_data, index=loc_str_index, columns=date_columns)


@pytest.fixture(scope="function")
def time_column_native_df():
    return native_pd.DataFrame(df_data, index=loc_str_index, columns=date_columns)


@pytest.fixture(scope="function")
def time_index_snowpark_pandas_df():
    return pd.DataFrame(df_data, index=date_columns_no_tz, columns=date_columns_no_tz)


@pytest.fixture(scope="function")
def time_index_native_df():
    return native_pd.DataFrame(
        df_data, index=date_columns_no_tz, columns=date_columns_no_tz
    )


@pytest.fixture(scope="function")
def date_index_string_column_data():
    kwargs = {
        "index": date_columns_no_tz,
        "columns": list(ascii_lowercase[: df_data.shape[1]]),
    }
    return df_data, kwargs


@pytest.fixture(scope="function")
def float_native_df() -> native_pd.DataFrame:
    """
    Fixture for DataFrame of floats with index of unique strings

    [30 rows x 4 columns]
    """
    return native_pd.DataFrame(
        {f"c{c}": native_pd.Series(np.random.randn(30)) for c in range(4)}
    )


@pytest.fixture(scope="function")
def native_df_1k_1k():
    """
    Fixture for DataFrame of int with index of unique strings

    [1000 rows x 1000 columns]
    """
    return native_pd.DataFrame(
        np.random.randint(-100, 100, size=(1000, 1000)),
        columns=[f"c{i}" for i in range(1000)],
    )


@pytest.fixture(scope="function")
def simple_native_pandas_df():
    return native_pd.DataFrame(simple_data)


@pytest.fixture(scope="function")
def simple_snowpark_pandas_df():
    return pd.DataFrame(simple_data)
