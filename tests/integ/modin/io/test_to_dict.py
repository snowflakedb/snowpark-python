#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections import OrderedDict, defaultdict

import modin.pandas as pd
import pandas as native_pd
import pytest
from numpy.testing import assert_equal
from pandas.testing import assert_series_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

DICT_CLASSES = [dict, OrderedDict, defaultdict(int), defaultdict(list)]


@pytest.mark.parametrize("into", DICT_CLASSES)
@pytest.mark.parametrize(
    "index",
    [
        ["a", "b", "c", "d"],
        pd.MultiIndex.from_arrays([[1, 1, 2, 2], ["row1", "row2", "row1", "row2"]]),
    ],
)
@sql_count_checker(query_count=1)
def test_series_to_dict(into, index):
    native_series = native_pd.Series([1, 2, None, 4], index=index)
    snow_series = pd.Series(native_series)
    native_result = native_series.to_dict(into)
    snow_result = snow_series.to_dict(into)
    assert type(native_result) == type(snow_result)
    # numpy.testing.assert_equal can handle nan equality
    assert_equal(native_result, snow_result)


@pytest.mark.parametrize(
    "orient", ["dict", "list", "series", "split", "tight", "records", "index"]
)
@pytest.mark.parametrize("into", DICT_CLASSES)
@pytest.mark.parametrize(
    "index", [["row1", "row2"], pd.MultiIndex.from_arrays([[1, 2], ["row1", "row2"]])]
)
@sql_count_checker(query_count=1)
def test_dataframe_to_dict(orient, into, index):
    native_df = native_pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [0.5, None],
            "col3": native_pd.timedelta_range("1 hour", periods=2),
        },
        index=index,
    )
    snow_df = pd.DataFrame(native_df)
    native_result = native_df.to_dict(orient, into)
    snow_result = snow_df.to_dict(orient, into)
    assert type(native_result) == type(snow_result)
    if orient == "series":
        assert native_result.keys() == snow_result.keys()
        for s1, s2 in zip(native_result.values(), snow_result.values()):
            assert_series_equal(s1, s2, check_dtype=False, check_index_type=False)
    else:
        # numpy.testing.assert_equal can handle nan equality
        assert_equal(native_result, snow_result)
