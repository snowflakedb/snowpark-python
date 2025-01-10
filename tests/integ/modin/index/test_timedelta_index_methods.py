#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

TIMEDELTA_INDEX_DATA = [
    "0ns",
    "1d",
    "1h",
    "5h",
    "9h",
    "60s",
    "1s",
    "800ms",
    "900ms",
    "5us",
    "6ns",
    "1ns",
    "1d 3s",
    "9m 15s 8us",
    None,
]


@sql_count_checker(query_count=0)
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


@pytest.mark.parametrize("property", ["components", "inferred_freq"])
@sql_count_checker(query_count=0)
def test_property_not_implemented(property):
    snow_index = pd.TimedeltaIndex(["1 days", "2 days"])
    msg = f"Snowpark pandas does not yet support the property TimedeltaIndex.{property}"
    with pytest.raises(NotImplementedError, match=msg):
        getattr(snow_index, property)


@pytest.mark.parametrize("attr", ["days", "seconds", "microseconds", "nanoseconds"])
@sql_count_checker(query_count=1)
def test_timedelta_index_properties(attr):
    native_index = native_pd.TimedeltaIndex(TIMEDELTA_INDEX_DATA)
    snow_index = pd.Index(native_index)
    assert_index_equal(
        getattr(snow_index, attr), getattr(native_index, attr), exact=False
    )


@pytest.mark.parametrize("method", ["round", "floor", "ceil"])
@pytest.mark.parametrize(
    "freq", ["ns", "us", "ms", "s", "min", "h", "d", "0s", "3s", pd.Timedelta(0)]
)
@sql_count_checker(query_count=1)
def test_timedelta_floor_ceil_round(method, freq):
    native_index = native_pd.TimedeltaIndex(TIMEDELTA_INDEX_DATA)
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index, native_index, lambda x: getattr(x, method)(freq)
    )


@pytest.mark.parametrize("method", ["round", "floor", "ceil"])
@pytest.mark.parametrize(
    "freq", ["nano", "millis", "second", "minutes", "hour", "days", "month", "year"]
)
@sql_count_checker(query_count=0)
def test_timedelta_floor_ceil_round_negative(method, freq):
    native_index = native_pd.TimedeltaIndex(TIMEDELTA_INDEX_DATA)
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda x: getattr(x, method)(freq),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=f"Invalid frequency: {freq}",
    )


@sql_count_checker(query_count=1)
def test_timedelta_total_seconds():
    native_index = native_pd.TimedeltaIndex(TIMEDELTA_INDEX_DATA)
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(snow_index, native_index, lambda x: x.total_seconds())


@pytest.mark.parametrize("skipna", [True, False])
@pytest.mark.parametrize("data", [[1, 2, 3], [1, 2, 3, None], [None], []])
@sql_count_checker(query_count=1)
def test_timedelta_index_mean(skipna, data):
    native_index = native_pd.TimedeltaIndex(data)
    snow_index = pd.Index(native_index)
    native_result = native_index.mean(skipna=skipna)
    snow_result = snow_index.mean(skipna=skipna)
    # Special check for NaN because Nan != Nan.
    if pd.isna(native_result):
        assert pd.isna(snow_result)
    else:
        assert snow_result == native_result


@sql_count_checker(query_count=0)
def test_timedelta_index_mean_invalid_axis():
    native_index = native_pd.TimedeltaIndex([1, 2, 3])
    snow_index = pd.Index(native_index)
    with pytest.raises(IndexError, match="tuple index out of range"):
        native_index.mean(axis=1)
    # Snowpark pandas raises ValueError instead of IndexError.
    with pytest.raises(ValueError, match="axis should be 0 for TimedeltaIndex.mean"):
        snow_index.mean(axis=1).to_pandas()
