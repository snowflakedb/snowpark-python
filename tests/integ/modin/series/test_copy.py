#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def base_series():
    return pd.Series([2, 1], name="a")


@pytest.fixture()
def snow_series(base_series):
    return base_series.copy()


@pytest.mark.parametrize("deep", [None, True, False])
@sql_count_checker(query_count=2)
def test_copy(deep, snow_series):
    native_series = snow_series.to_pandas()
    # Verify copy is same as original
    assert_snowpark_pandas_equal_to_pandas(snow_series.copy(deep=deep), native_series)


@sql_count_checker(query_count=0)
def test_copy_name_on_deep_copy(snow_series):
    copy = snow_series.copy(deep=True)
    copy.name = "b"

    assert snow_series.name == "a"
    assert copy.name == "b"


@sql_count_checker(query_count=0)
def test_copy_name_on_shallow_copy(snow_series):
    copy = snow_series.copy(deep=False)
    copy.name = "b"

    assert snow_series.name == "b"
    assert copy.name == "b"


@pytest.mark.parametrize(
    "operation",
    [
        lambda series: series.sort_values(inplace=True),
        lambda series: series.reset_index(inplace=True, drop=True),
    ],
)
@sql_count_checker(query_count=2)
def test_copy_inplace_operations_on_deep_copy(snow_series, operation):
    native_series = snow_series.to_pandas()
    copy = snow_series.copy(deep=True)
    operation(copy)

    # Verify that 'snow_series' is unchanged.
    assert_snowpark_pandas_equal_to_pandas(snow_series, native_series)


@pytest.mark.parametrize(
    "operation",
    [
        lambda series: series.sort_values(inplace=True),
        lambda series: series.reset_index(inplace=True, drop=True),
    ],
)
@sql_count_checker(query_count=2)
def test_copy_inplace_operations_on_shallow_copy(snow_series, operation):
    with SqlCounter(query_count=2):
        copy = snow_series.copy(deep=False)
        operation(copy)

        # Verify that 'snow_series' is also changed.
        assert_snowpark_pandas_equal_to_pandas(snow_series, copy.to_pandas())


@sql_count_checker(query_count=1)
def test_copy_timedelta():
    native_s = native_pd.Series(
        [
            native_pd.Timedelta("1 days"),
            native_pd.Timedelta("2 days"),
            native_pd.Timedelta("3 days"),
        ]
    )
    eval_snowpark_pandas_result(pd.Series(native_s), native_s, lambda s: s.copy())
