#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

""" test to_timedelta function"""
import re
from datetime import timedelta

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
    VALID_PANDAS_TIMEDELTA_ABBREVS,
)
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_series_equal,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

TIMEDELTA_DATA = [
    "1W",
    "1 W",
    "1d",
    "2 D",
    "3day",
    "4 day",
    "5days",
    "6 days 06:05:01.00003",
    "1h",
    "2 H",
    "3hr",
    "4 hour",
    "5hours",
    "1m",
    "2 T",
    "3min",
    "4 minute",
    "5minutes",
    "1s",
    "2 S",
    "3sec",
    "4 second",
    "5seconds",
    "1ms",
    "2 L",
    "3milli",
    "4 millisecond",
    "5milliseconds",
    "1us",
    "2 U",
    "3micro",
    "4 microsecond",
    "5microseconds",
    "1ns",
    "2 N",
    "3nano",
    "4 nanosecond",
    "5nanoseconds",
    "1 day 3 millis",
    "6 hours 4 nanos",
    "4 days 00:01:02.000000009",
    "02:01:03",
    567,
    123.456,
    timedelta(days=1, hours=2, minutes=3, seconds=4, milliseconds=5, microseconds=6),
]


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("arg", TIMEDELTA_DATA)
def test_to_timedelta_scalar(arg):
    assert native_pd.to_timedelta(arg) == pd.to_timedelta(arg)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("arg", [None, np.nan, "nan", "", "NaT", np.timedelta64("NaT")])
def test_to_timedelta_na(arg):
    assert pd.isna(native_pd.to_timedelta(arg))
    assert pd.isna(pd.to_timedelta(arg))


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data", [TIMEDELTA_DATA, np.array(TIMEDELTA_DATA), tuple(TIMEDELTA_DATA)]
)
def test_to_timedelta_listlike(data):
    assert_index_equal(pd.to_timedelta(data), native_pd.to_timedelta(data))


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("unit", VALID_PANDAS_TIMEDELTA_ABBREVS.keys())
def test_to_timedelta_series(unit):
    native_series = native_pd.Series([1, 2, 3])
    snow_series = pd.Series([1, 2, 3])
    expected = native_pd.to_timedelta(native_series, unit=unit)

    # native series
    assert_series_equal(pd.to_timedelta(native_series, unit=unit), expected)
    # lazy series
    assert_series_equal(pd.to_timedelta(snow_series, unit=unit), expected)


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("unit", VALID_PANDAS_TIMEDELTA_ABBREVS.keys())
def test_to_timedelta_units(unit):
    native_index = native_pd.Index([1, 2, 3])
    snow_index = pd.Index([1, 2, 3])
    expected = native_pd.to_timedelta(native_index, unit=unit)

    # native index
    assert_index_equal(pd.to_timedelta(native_index, unit=unit), expected)
    # lazy index
    assert_index_equal(pd.to_timedelta(snow_index, unit=unit), expected)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("unit", ["year", "month", "week"])
def test_to_timedelta_invalid_unit(unit):
    data = [1, 2, 3]
    eval_snowpark_pandas_result(
        "snow",
        "native",
        lambda x: native_pd.to_timedelta(data, unit=unit)
        if x == "native"
        else pd.to_timedelta(data, unit=unit),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=f"invalid unit abbreviation: {unit}",
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "native_index",
    [
        native_pd.Index([1, 3, 4]),
        native_pd.Index([1.4, 3.0, 45.9]),
        native_pd.TimedeltaIndex(["1 min", "2 seconds", "3 millis"]),
    ],
)
def test_to_timedelta_dtypes(native_index):
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda x: native_pd.to_timedelta(x, unit="nanos")
        if isinstance(x, native_pd.Index)
        else pd.to_timedelta(x, unit="nanos"),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "native_index",
    [native_pd.Index([True, False]), native_pd.date_range("20210101", periods=3)],
)
def test_to_timedelta_invalid_dtype(native_index):
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda x: native_pd.to_timedelta(x)
        if isinstance(x, native_pd.Index)
        else pd.to_timedelta(x),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match=re.escape(
            f"dtype {native_index.dtype} cannot be converted to timedelta64[ns]"
        ),
    )


@sql_count_checker(query_count=0)
def test_to_timedelta_string_dtype_not_implemented():
    data = pd.Index(["1 days", "2 days", "3 days"])
    msg = "Snowpark pandas method pd.to_timedelta does not yet support conversion from string type"
    with pytest.raises(NotImplementedError, match=msg):
        pd.to_timedelta(data)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("errors", ["ignore", "coerce"])
def test_to_timedelta_errors_not_implemented(errors):
    data = pd.Index([1, 2, 3])
    msg = "Snowpark pandas method pd.to_timedelta does not yet support the 'errors' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        pd.to_timedelta(data, errors=errors)
