#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
)


@sql_count_checker(query_count=2)
def test_datetime_index_construction():
    # create from native pandas datetime index.
    index = native_pd.DatetimeIndex(["2021-01-01", "2021-01-02", "2021-01-03"])
    snow_index = pd.Index(index)
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from query compiler with timestamp type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=index)
    snow_index = df.index
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from snowpark pandas datetime index.
    snow_index = pd.Index(pd.DatetimeIndex([123]))
    assert isinstance(snow_index, pd.DatetimeIndex)


@pytest.mark.skip(reason="SNOW-1616989: Fix datetime index construction from int")
@sql_count_checker(query_count=1)
def test_datetime_index_construction_from_int():
    snow_index = pd.DatetimeIndex(pd.Index([1, 2, 3]))
    native_index = native_pd.DatetimeIndex(native_pd.Index([1, 2, 3]))
    assert_index_equal(snow_index, native_index)


@sql_count_checker(query_count=0)
def test_datetime_index_construction_negative():
    # Try to create datatime index query compiler with int type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    msg = "DatetimeIndex can only be created from a query compiler with TimestampType"
    with pytest.raises(ValueError, match=msg):
        pd.DatetimeIndex(df._query_compiler)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "kwargs",
    [
        {"freq": "M"},
        {"tz": "UTC"},
        {"normalize": True},
        {"closed": "left"},
        {"ambiguous": "infer"},
        {"dayfirst": True},
        {"yearfirst": True},
        {"dtype": "int"},
        {"copy": True},
        {"name": "abc"},
    ],
)
def test_non_default_args(kwargs):
    idx = pd.DatetimeIndex(["2014-01-01 10:00:00"])

    name = list(kwargs.keys())[0]
    value = list(kwargs.values())[0]
    msg = f"Non-default argument '{name}={value}' when constructing Index with query compiler"
    with pytest.raises(AssertionError, match=msg):
        pd.DatetimeIndex(data=idx._query_compiler, **kwargs)


@sql_count_checker(query_count=6)
def test_index_parent():
    """
    Check whether the parent field in Index is updated properly.
    """
    native_idx1 = native_pd.DatetimeIndex(["2024-01-01"], name="id1")
    native_idx2 = native_pd.DatetimeIndex(["2024-01-01", "2024-01-02"], name="id2")

    # DataFrame case.
    df = pd.DataFrame({"A": [1]}, index=native_idx1)
    snow_idx = df.index
    assert_frame_equal(snow_idx._parent, df)
    assert_index_equal(snow_idx, native_idx1)

    # Series case.
    s = pd.Series([1, 2], index=native_idx2, name="zyx")
    snow_idx = s.index
    assert_series_equal(snow_idx._parent, s)
    assert_index_equal(snow_idx, native_idx2)
