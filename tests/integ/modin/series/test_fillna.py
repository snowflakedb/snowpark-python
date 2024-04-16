#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.fixture(scope="function")
def test_fillna_series():
    return native_pd.Series([np.nan, 2, np.nan, 0], list("bacd"))


@pytest.fixture(scope="function")
def test_fillna_series_2():
    return native_pd.Series([np.nan, 2, np.nan, 0], list("abcd"))


@pytest.fixture(scope="function")
def test_fillna_df():
    return native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABCD"),
        index=[1, np.nan, 2, np.nan],
    )


@pytest.fixture(scope="function")
def test_fillna_series_dup():
    return native_pd.Series([np.nan, 2, np.nan, 0], list("aacd"))  # duplicated index


@sql_count_checker(query_count=0)
def test_fillna_for_both_value_and_method_None_negative():
    native_ser = native_pd.Series()
    snow_ser = pd.Series()

    # Check error when `value` and `method` are both `None`.
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.fillna(),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_match="Must specify a fill 'value' or 'method'.",
        expect_exception_type=ValueError,
    )

    # Check error when `value` and `method` are both *not* `None`.
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.fillna(value=1, method="ffill"),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot specify both 'value' and 'method'.",
    )


@sql_count_checker(query_count=0)
def test_fillna_invalid_method_negative():
    native_ser = native_pd.Series()
    snow_ser = pd.Series()

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.fillna(method="invalid_method"),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_type=ValueError,
        expect_exception_match=r"Invalid fill method. Expecting pad \(ffill\) or backfill \(bfill\)\. Got invalid_method",
    )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@sql_count_checker(query_count=1)
def test_fillna_method(test_fillna_series, method):
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series),
        test_fillna_series,
        lambda s: s.fillna(method=method),
    )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3] + [np.nan] * 1000 + [4, 5, 6] + [np.nan],
        [np.nan] * 1000 + [4, 5, 6] + [np.nan] + [1, 2, 3],
    ],
    ids=["ends_with_nan", "starts_with_nan"],
)
@sql_count_checker(query_count=6)
def test_fillna_method_long_series(method, data):
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.fillna(method=method),
    )


@sql_count_checker(query_count=1)
def test_value_scalar(test_fillna_series):
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series),
        test_fillna_series,
        lambda s: s.fillna(1),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_value_series(test_fillna_series, test_fillna_series_2):
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series),
        test_fillna_series,
        lambda s: s.fillna(pd.Series(test_fillna_series_2))
        if isinstance(s, pd.Series)
        else s.fillna(test_fillna_series_2),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_value_dict(test_fillna_series, test_fillna_series_2):
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series),
        test_fillna_series,
        lambda s: s.fillna(test_fillna_series_2.to_dict()),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_value_series_dup(test_fillna_series_dup, test_fillna_series_2):
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series_dup),
        test_fillna_series_dup,
        lambda s: s.fillna(pd.Series(test_fillna_series_2))
        if isinstance(s, pd.Series)
        else s.fillna(test_fillna_series_2),
    )


@sql_count_checker(query_count=0)
def test_argument_negative(test_fillna_series, test_fillna_df):
    # df is not allowed as values for series fillna
    eval_snowpark_pandas_result(
        pd.Series(test_fillna_series),
        test_fillna_series,
        lambda s: s.fillna(pd.DataFrame(test_fillna_df))
        if isinstance(s, pd.Series)
        else s.fillna(test_fillna_df),
        expect_exception=True,
        expect_exception_match='"value" parameter must be a scalar, dict or Series',
        expect_exception_type=TypeError,
        assert_exception_equal=False,
    )
