#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_values_equal, eval_snowpark_pandas_result


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="SNOW-1017231")
@pytest.mark.parametrize("method", ["any", "all"])
@sql_count_checker(query_count=4)
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
@sql_count_checker(query_count=2)
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
@sql_count_checker(query_count=2)
def test_any_int(data):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.any(),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=2)
def test_all_named_index():
    data = [1, 0, 3]
    index_name = ["a", "b", "c"]
    eval_snowpark_pandas_result(
        pd.Series(data, index_name),
        native_pd.Series(data, index_name),
        lambda df: df.all(),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=2)
def test_any_named_index():
    data = [1, 0, 3]
    index_name = ["a", "b", "c"]
    eval_snowpark_pandas_result(
        pd.Series(data, index_name),
        native_pd.Series(data, index_name),
        lambda df: df.any(),
        comparator=assert_values_equal,
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
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
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_all_float_fallback(data, skipna):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.all(skipna=skipna),
        comparator=assert_values_equal,
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
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
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_any_float_fallback(data, skipna):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.any(skipna=skipna),
        comparator=assert_values_equal,
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "data",
    [["", "b", "c"], ["d", "e", "f"]],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_all_str_fallback(data):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.all(),
        comparator=assert_values_equal,
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize(
    "data",
    [["", "b", "c"], ["d", "e", "f"]],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_any_str_fallback(data):
    eval_snowpark_pandas_result(
        pd.Series(data),
        native_pd.Series(data),
        lambda df: df.any(),
        comparator=assert_values_equal,
    )
