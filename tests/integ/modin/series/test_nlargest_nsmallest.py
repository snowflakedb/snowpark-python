#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture
def snow_series():
    return pd.Series([3, 2, 1, 4, 4])


@pytest.fixture
def native_series(snow_series):
    return snow_series.to_pandas()


@pytest.fixture(params=["nlargest", "nsmallest"])
def method(request):
    return request.param


@pytest.mark.parametrize("n", [0, 1, 2, 4])
@pytest.mark.parametrize("keep", ["first", "last"])
@sql_count_checker(query_count=1)
def test_nlargest_nsmallest(snow_series, native_series, method, n, keep):
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda s: getattr(s, method)(n, keep=keep)
    )


@sql_count_checker(query_count=1)
def test_nlargest_nsmallest_negative_n(snow_series, native_series, method):
    # negative n values results in empty dataframe.
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda s: getattr(s, method)(-1)
    )


@pytest.mark.skip(
    reason="SNOW-1502286: native pandas nlargest output is not consistent between local and CI"
)
@sql_count_checker(query_count=1)
def test_nlargest_nsmallest_large_n(snow_series, native_series, method):
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda s: getattr(s, method)(100)
    )


@sql_count_checker(query_count=0)
def test_nlargest_nsmallest_all_keep_not_implemented(snow_series, method):
    error_msg = (
        f"Snowpark pandas method '{method}' doesn't yet support parameter keep='all'"
    )
    with pytest.raises(NotImplementedError, match=error_msg):
        getattr(snow_series, method)(2, "all")


@sql_count_checker(query_count=0)
def test_nlargest_nsmallest_invalid_keep(snow_series, native_series, method):
    error_msg = 'keep must be either "first", "last" or "all"'
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: getattr(s, method)(2, "second"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=error_msg,
    )


@pytest.mark.parametrize("data", [["a", "c", "b"], [1, "c", 1.0]])
@sql_count_checker(query_count=3)
def test_nlargest_nsmallest_non_numeric_types(method, data):
    snow_s = pd.Series(data)
    native_s = snow_s.to_pandas()
    # Native pandas doesn't support nlargest/nsmallest on string/object types. But
    # snowpark pandas doesn't add any such restriction and allow these types.
    error_msg = f"Cannot use method '{method}' with dtype object"
    with pytest.raises(TypeError, match=error_msg):
        getattr(native_s, method)()
    n = 2
    expected_s = snow_s.sort_values(ascending=(method == "nsmallest")).head(n)
    assert_series_equal(getattr(snow_s, method)(n), expected_s)
