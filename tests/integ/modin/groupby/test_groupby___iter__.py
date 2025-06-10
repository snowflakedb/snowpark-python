#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "by, expected_query_count, expected_join_count",
    [
        ("a", 3, 4),
        (["a"], 3, 4),
        (["a", "b"], 4, 6),
    ],
)
def test_frame_groupby___iter__(by, expected_query_count, expected_join_count):
    data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
    native_df = native_pd.DataFrame(data, columns=["a", "b", "c"])
    snow_df = pd.DataFrame(native_df)

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        native_result = list(iter(native_df.groupby(by=by)))
        snow_result = list(iter(snow_df.groupby(by=by)))

        # check that sizes are identical
        assert len(snow_result) == len(native_result)
        for i in range(len(snow_result)):
            # check that corresponding keys are identical
            assert snow_result[i][0] == native_result[i][0]
            # check that corresponding frames are identical
            assert_frame_equal(snow_result[i][1], native_result[i][1])


@sql_count_checker(query_count=3, join_count=4)
def test_series_groupby___iter__():
    lst = ["a", "a", "b"]
    native_ser = native_pd.Series([1, 2, 3], index=lst)
    snow_ser = pd.Series(native_ser)

    native_result = list(iter(native_ser.groupby(level=0)))
    snow_result = list(iter(snow_ser.groupby(level=0)))

    # check that sizes are identical
    assert len(snow_result) == len(native_result)
    for i in range(len(snow_result)):
        # check that corresponding keys are identical
        assert snow_result[i][0] == native_result[i][0]
        # check that corresponding series are identical
        assert_series_equal(snow_result[i][1], native_result[i][1])
