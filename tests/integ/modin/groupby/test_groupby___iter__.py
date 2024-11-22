#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=3, join_count=4)
@pytest.mark.parametrize("by", ["a", ["a"]])
def test_frame_groupby___iter__(by):
    data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
    native_df = native_pd.DataFrame(data, columns=["a", "b", "c"])
    snow_df = pd.DataFrame(native_df)

    native_result = []
    for x, y in native_df.groupby(by=by):
        native_result.append([x, y])

    snow_result = []
    for x, y in snow_df.groupby(by=by):
        snow_result.append([x, y])

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

    native_result = []
    for x, y in native_ser.groupby(level=0):
        native_result.append([x, y])

    snow_result = []
    for x, y in snow_ser.groupby(level=0):
        snow_result.append([x, y])

    # check that sizes are identical
    assert len(snow_result) == len(native_result)
    for i in range(len(snow_result)):
        # check that corresponding keys are identical
        assert snow_result[i][0] == native_result[i][0]
        # check that corresponding series are identical
        assert_series_equal(snow_result[i][1], native_result[i][1])
