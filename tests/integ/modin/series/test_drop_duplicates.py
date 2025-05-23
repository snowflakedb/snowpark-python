#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("keep", ["first", "last", False])
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates(keep):
    pandas_ser = native_pd.Series(["a", "b", "b", "c", "a"], name="name")
    snow_ser = pd.Series(pandas_ser)

    assert_series_equal(
        snow_ser.drop_duplicates(keep=keep),
        pandas_ser.drop_duplicates(keep=keep),
        check_dtype=False,
        check_index_type=False,
    )


@pytest.mark.parametrize("keep", ["first", "last", False])
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_on_empty_series(keep):
    pandas_ser = native_pd.Series([], name="name")
    snow_ser = pd.Series(pandas_ser)

    assert_series_equal(
        snow_ser.drop_duplicates(keep=keep),
        pandas_ser.drop_duplicates(keep=keep),
        check_dtype=False,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "keep, expected",
    [
        ("first", native_pd.Series([np.nan, 3], index=[0, 1])),
        ("last", native_pd.Series([3, np.nan], index=[2, 4])),
        (False, native_pd.Series([])),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_nan_none(keep, expected):
    # Note that Snowpark pandas treats np.nan and None the same
    ser = pd.Series([np.nan, 3, 3, None, np.nan], dtype=object)

    result = ser.drop_duplicates(keep=keep)
    assert_series_equal(
        result,
        expected,
        check_dtype=False,
        check_index_type=False,
    )


@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_post_sort_values():
    pandas_ser = native_pd.Series(["a", "b", "b", "c", "a"], name="name")
    snow_ser = pd.Series(pandas_ser)

    assert_series_equal(
        snow_ser.sort_values(kind="stable").drop_duplicates(),
        pandas_ser.sort_values(kind="stable").drop_duplicates(),
        check_dtype=False,
        check_index_type=False,
    )


@sql_count_checker(query_count=0, join_count=0)
def test_drop_duplicates_invalid_keep():
    snow_ser = pd.Series(["a", "b", "b", "c", "a"], name="name")
    with pytest.raises(
        ValueError, match='keep must be either "first", "last" or False'
    ):
        snow_ser.drop_duplicates(keep="invalid")
