#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_values_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("method", ["any", "all"])
@sql_count_checker(query_count=1)
def test_empty(method):
    # Because empty dataframes are by default object and Snowpark pandas currently falls back
    # for non-int/bool columns, we need to explicitly specify a type here

    # Treat index like any other column in a DataFrame when it comes to types,
    # therefore Snowpark pandas returns a Index(dtype="object") for an empty index
    # whereas pandas returns RangeIndex()
    # This is compatible with behavior for empty dataframe in other tests.
    eval_snowpark_pandas_result(
        pd.Series([], dtype="int64"),
        native_pd.Series([], dtype="int64"),
        lambda df: getattr(df, method)(),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3],
        [0, 0, 1],
    ],
)
@sql_count_checker(query_count=1)
def test_all_int(data):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.all(),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "data",
    [
        [0, 0, 0],
        [0, 1, 2],
    ],
)
@sql_count_checker(query_count=1)
def test_any_int(data):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.any(),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=1)
def test_all_named_index():
    data = [1, 0, 3]
    index_name = ["a", "b", "c"]
    eval_snowpark_pandas_result(
        pd.Series(data, index_name),
        native_pd.Series(data, index_name),
        lambda df: df.all(),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=1)
def test_any_named_index():
    data = [1, 0, 3]
    index_name = ["a", "b", "c"]
    eval_snowpark_pandas_result(
        pd.Series(data, index_name),
        native_pd.Series(data, index_name),
        lambda df: df.any(),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "data",
    [
        [0.1, 0.0, 0.3],
        [0.1, 0.2, 0.3],
        [np.nan, 0.0],
        [0.4, 0.5, np.nan],
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=0)
def test_all_float_not_implemented(data, skipna):
    series = pd.Series(data)
    msg = "Snowpark pandas all API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        series.all(skipna=skipna)


@pytest.mark.parametrize(
    "data",
    [
        [0.1, 0.0, 0.3],
        [0.1, 0.2, 0.3],
        [np.nan, 0.0],
        [0.4, 0.5, np.nan],
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=0)
def test_any_float_not_implemented(data, skipna):
    series = pd.Series(data)
    msg = "Snowpark pandas any API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        series.any(skipna=skipna)


@pytest.mark.parametrize(
    "data",
    [["", "b", "c"], ["d", "e", "f"]],
)
@sql_count_checker(query_count=0)
def test_all_str_not_implemented(data):
    series = pd.Series(data)
    msg = "Snowpark pandas all API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        series.all()


@pytest.mark.parametrize(
    "data",
    [["", "b", "c"], ["d", "e", "f"]],
)
@sql_count_checker(query_count=0)
def test_any_str_not_implemented(data):
    series = pd.Series(data)
    msg = "Snowpark pandas any API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        series.any()
