#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401

default_index_series_data = [1, 1.1, True, "a", "2021-01-01", (1,), [1]]
loc_str_index = ["a", "b", "c", "d", "e", "f", "a"]
str_index_native_series_input = native_pd.Series(
    default_index_series_data,
    index=loc_str_index,
)
mixed_type_index = [0, 1.1, 1.5, 2, "a", "b", 4, 5, "c", "abc", -2, -1, True, None]
mixed_type_index_data = [
    1,
    1.1,
    True,
    "a",
    "2021-01-01",
    [14, 13, 12],
    15,
    20,
    19,
    False,
    16,
    "2021-01-01",
    18,
    17,
]

arrays_for_multiindex = [
    ["bar", "bar", "baz", "baz", "foo", "foo", "extra"],
    ["one", "two", "one", "two", "one", "two", "one"],
]

date_columns = native_pd.date_range(start="2023-01-01", periods=7, freq="D", tz="UTC")
date_columns_no_tz = date_columns.tz_localize(None)
series_data = np.random.randint(0, 10, size=(7))


@pytest.fixture(scope="function")
def multiindex_native():
    tuples = list(zip(*arrays_for_multiindex))
    return native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])


@pytest.fixture(scope="function")
def multiindex_native_int_series(multiindex_native):
    return native_pd.Series(list(range(7)), index=multiindex_native)


@pytest.fixture(scope="function")
def default_index_native_int_series():
    return native_pd.Series(list(range(7)), index=loc_str_index)


@pytest.fixture(scope="function")
def default_index_native_series():
    return native_pd.Series(default_index_series_data)


@pytest.fixture(scope="function")
def default_index_snowpark_pandas_series():
    return pd.Series(default_index_series_data)


@pytest.fixture(scope="function")
def default_index_native_int_snowpark_pandas_series():
    return pd.Series(list(range(7)), index=loc_str_index)


@pytest.fixture(scope="function")
def empty_snowpark_pandas_series():
    # Note: For pandas 1.5.3 a future warning gets emitted for pd.Series() which gets type float64 assigned.
    # Yet, in pandas 2.x an empty series gets a more correct type object
    return pd.Series()


@pytest.fixture(scope="function")
def empty_pandas_series():
    # Note: For pandas 1.5.3 a future warning gets emitted for pd.Series() which gets type float64 assigned.
    # Yet, in pandas 2.x an empty series gets a more correct type object
    return native_pd.Series()


@pytest.fixture(scope="function")
def str_index_native_series():
    return str_index_native_series_input.copy(deep=True)


@pytest.fixture(scope="function")
def str_index_snowpark_pandas_series():
    return pd.Series(str_index_native_series_input)


@pytest.fixture(scope="function")
def time_index_snowpark_pandas_series():
    return pd.Series(series_data, index=date_columns_no_tz)


@pytest.fixture(scope="function")
def time_index_native_series():
    return native_pd.Series(series_data, index=date_columns_no_tz)


@pytest.fixture(scope="function")
def mixed_type_index_native_series_mixed_type_index():
    return native_pd.Series(mixed_type_index_data, index=mixed_type_index)


@pytest.fixture(scope="function")
def mixed_type_index_native_series_default_index():
    return native_pd.Series(mixed_type_index_data)


@pytest.fixture(scope="function")
def native_series_with_duplicate_boolean_index():
    return native_pd.Series([3.14, "abc", [1, 2], None], index=[0, 1, False, True])


@pytest.fixture(scope="function")
def time_index_series_data():
    return list(range(7)), {"index": date_columns_no_tz}
