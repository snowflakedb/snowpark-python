#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import assert_series_equal


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
