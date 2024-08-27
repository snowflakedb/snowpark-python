#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@sql_count_checker(query_count=3)
def test_timedelta_index_construction():
    # create from native pandas timedelta index.
    index = native_pd.TimedeltaIndex(["1 days", "2 days", "3 days"])
    snow_index = pd.Index(index)
    assert isinstance(snow_index, pd.TimedeltaIndex)

    # create from query compiler with timedelta type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=index)
    snow_index = df.index
    assert isinstance(snow_index, pd.TimedeltaIndex)

    # create from snowpark pandas timedelta index.
    snow_index = pd.Index(pd.TimedeltaIndex([123]))
    assert isinstance(snow_index, pd.TimedeltaIndex)

    # create by subtracting datetime index from another.
    date_range1 = pd.date_range("2000-01-01", periods=10, freq="h")
    date_range2 = pd.date_range("2001-05-01", periods=10, freq="h")
    snow_index = date_range2 - date_range1
    assert isinstance(snow_index, pd.TimedeltaIndex)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "kwargs",
    [
        {"unit": "ns"},
        {"freq": "M"},
        {"dtype": "int"},
        {"copy": True},
        {"name": "abc"},
    ],
)
def test_non_default_args(kwargs):
    idx = pd.TimedeltaIndex(["1 days"])

    name = list(kwargs.keys())[0]
    value = list(kwargs.values())[0]
    msg = f"Non-default argument '{name}={value}' when constructing Index with query compiler"
    with pytest.raises(AssertionError, match=msg):
        pd.TimedeltaIndex(query_compiler=idx._query_compiler, **kwargs)


@pytest.mark.parametrize(
    "property", ["days", "seconds", "microseconds", "nanoseconds", "inferred_freq"]
)
@sql_count_checker(query_count=0)
def test_property_not_implemented(property):
    snow_index = pd.TimedeltaIndex(["1 days", "2 days"])
    msg = f"Snowpark pandas does not yet support the property TimedeltaIndex.{property}"
    with pytest.raises(NotImplementedError, match=msg):
        getattr(snow_index, property)
