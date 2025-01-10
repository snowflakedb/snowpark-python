#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def test_series_take():
    # TODO SNOW-933835 rewrite iloc will optimize SqlCounter
    ser = pd.Series([-1, 5, 6, 2, 4])

    actual = ser.take([1, 3, 4])
    expected = pd.Series([5, 2, 4], index=[1, 3, 4])
    with SqlCounter(query_count=2, join_count=2):
        assert_series_equal(actual, expected)

    actual = ser.take([-1, 3, 4])
    expected = pd.Series([4, 2, 4], index=[4, 3, 4])
    with SqlCounter(query_count=2, join_count=2):
        assert_series_equal(actual, expected)

    # Out-of-bounds testing - valid because .iloc is used in backend.
    actual = ser.take([1, 10])
    expected = pd.Series([5], index=[1])
    with SqlCounter(query_count=2, join_count=2):
        assert_series_equal(actual, expected)

    actual = ser.take([2, 5])
    expected = pd.Series([6], index=[2])
    with SqlCounter(query_count=2, join_count=2):
        assert_series_equal(actual, expected)


@sql_count_checker(query_count=0)
def test_series_take_axis_1():
    ser = pd.Series([-1, 5, 6, 2, 4])
    with pytest.raises(ValueError, match="No axis named 1 for object type Series"):
        ser.take([0, 1, 3], axis=1)
