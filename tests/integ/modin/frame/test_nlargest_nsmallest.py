#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture
def snow_df():
    return pd.DataFrame({"A": [3, 2, 1, 4, 4], "B": [1.0, 5.0, None, 1.0, 2.0]})


@pytest.fixture
def native_df(snow_df):
    return snow_df.to_pandas()


@pytest.fixture(params=["nlargest", "nsmallest"])
def method(request):
    return request.param


@pytest.mark.parametrize("n", [0, 1, 2, 4])
@pytest.mark.parametrize("columns", ["A", "B", ["A", "B"], ["B", "A"], []])
@pytest.mark.parametrize("keep", ["first", "last"])
@sql_count_checker(query_count=1)
def test_nlargest_nsmallest(snow_df, native_df, method, n, columns, keep):
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, method)(n, columns=columns, keep=keep),
    )


@sql_count_checker(query_count=1)
def test_nlargest_nsmallest_negative_n(snow_df, native_df, method):
    # negative n values results in empty dataframe.
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df, method)(-1, "A")
    )


@pytest.mark.skip(
    reason="SNOW-1502286: native pandas nlargest output is not consistent between local and CI"
)
@sql_count_checker(query_count=1)
def test_nlargest_nsmallest_large_n(snow_df, native_df, method):
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df, method)(100, "A")
    )


@sql_count_checker(query_count=1)
def test_nlargest_nsmallest_overlapping_index_name(snow_df, native_df, method):
    snow_df = snow_df.rename_axis("A")
    native_df = native_df.rename_axis("A")
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df, method)(2, "A")
    )


@sql_count_checker(query_count=0)
def test_nlargest_nsmallest_all_keep_not_implemented(snow_df, method):
    error_msg = (
        f"Snowpark pandas method '{method}' doesn't yet support parameter keep='all'"
    )
    with pytest.raises(NotImplementedError, match=error_msg):
        getattr(snow_df, method)(2, "A", "all")


@sql_count_checker(query_count=0)
def test_nlargest_nsmallest_invalid_keep(snow_df, native_df, method):
    error_msg = 'keep must be either "first", "last" or "all"'
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, method)(2, "A", "second"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=error_msg,
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("columns", ["C", None, ["A", "C"]])
def test_nlargest_nsmallest_invalid_label(snow_df, native_df, method, columns):
    error_msg = "None" if columns is None else "C"
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, method)(2, columns),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match=error_msg,
    )


@sql_count_checker(query_count=0)
def test_nlargest_nsmallest_duplicate_labels(snow_df, method):
    snow_df.columns = ["A", "A"]
    # Native pandas raises "AttributeError: 'DataFrame' object has no attribute 'dtype'"
    # Snowpark pandas raises more helpful error
    error_msg = "The column label 'A' is not unique."
    with pytest.raises(ValueError, match=error_msg):
        getattr(snow_df, method)(2, "A")


@pytest.mark.parametrize("data", [["a", "c", "b"], [1, "c", 1.0]])
@sql_count_checker(query_count=3)
def test_nlargest_nsmallest_non_numeric_types(method, data):
    snow_df = pd.DataFrame({"A": data})
    native_df = snow_df.to_pandas()
    # Native pandas doesn't support nlargest/nsmallest on string/object types. But
    # snowpark pandas doesn't add any such restriction and allow these types.
    error_msg = (
        f"Column 'A' has dtype object, cannot use method '{method}' with this dtype"
    )
    with pytest.raises(TypeError, match=error_msg):
        getattr(native_df, method)(2, "A")
    n = 2
    expected_df = snow_df.sort_values("A", ascending=(method == "nsmallest")).head(n)
    assert_frame_equal(getattr(snow_df, method)(n, "A"), expected_df)


@pytest.mark.parametrize("n", [1, 2, 4])
@pytest.mark.parametrize("columns", ["A", "B", ["A", "B"], ["B", "A"]])
@pytest.mark.parametrize("keep", ["first", "last"])
@sql_count_checker(query_count=1)
def test_time_delta_nlargest_nsmallest(method, n, columns, keep):
    native_df = native_pd.DataFrame(
        {"A": [3, 2, 1, 4, 4], "B": [1, 2, 3, 4, 5]}
    ).astype("timedelta64[ns]")
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, method)(n, columns=columns, keep=keep),
    )
