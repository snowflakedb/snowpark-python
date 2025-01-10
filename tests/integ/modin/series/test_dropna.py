#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "sample, expected_query_count",
    (
        ([1.0, 2.0, np.nan], 1),
        ([np.nan, 2, pd.NaT, "", None, "I stay"], 1),
    ),
)
def test_basic(sample, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda s: s.dropna(),
        )


@pytest.mark.parametrize("sample", ([1.0, 2.0, np.nan],))
@sql_count_checker(query_count=2)
def test_inplace(sample):
    s = pd.Series(sample)
    s2 = s.dropna()
    s.dropna(inplace=True)
    assert_series_equal(s, s2)


@pytest.mark.parametrize("sample", ([1.0, 2.0, np.nan],))
@sql_count_checker(query_count=1)
def test_unused_arguments(sample):
    eval_snowpark_pandas_result(
        pd.Series(sample),
        native_pd.Series(sample),
        lambda s: s.dropna(axis="columns"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="No axis named columns for object type Series",
    )

    eval_snowpark_pandas_result(
        pd.Series(sample),
        native_pd.Series(sample),
        lambda s: s.dropna(how="any"),
    )


@pytest.mark.parametrize(
    "ser",
    [
        native_pd.Series([1, None, 3, 4, None, 6, None, 8, None]),
    ],
)
@sql_count_checker(query_count=1)
def test_dropna_iloc(ser):
    # Check that dropna() generates a new index correctly for iloc.
    native_res = ser.dropna()
    snowpark_res = pd.Series(ser).dropna()
    indices = range(native_res.count())
    assert_snowpark_pandas_equal_to_pandas(
        snowpark_res.iloc[indices], native_res.iloc[indices]
    )


@pytest.mark.parametrize(
    "ser",
    [
        native_pd.Series([None, "string 1", "string 2", None, None, "string 3", None]),
        native_pd.Series(
            [
                999,
                -10,
                None,
                3.14,
                np.nan,
                88888,
                -1,
                None,
                np.nan,
                None,
                0,
                10,
                None,
            ]
        ),
        native_pd.Series([np.nan, None, 0]),
    ],
)
@sql_count_checker(query_count=1)
def test_dropna_sort_values(ser):
    # Test data that does not start with a row_position_column due to sorting.
    eval_snowpark_pandas_result(
        pd.Series(ser).sort_values(),
        ser.sort_values(),
        lambda s: s.dropna(),
    )
