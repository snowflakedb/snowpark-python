#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(params=["date_range", "bdate_range"])
def date_range_func(request):
    """
    utc keyword to pass to to_datetime.
    """
    return request.param


@pytest.mark.parametrize(
    "kwargs",
    [
        # Specify start and end, with the default daily frequency.
        {"start": "1/1/2018", "end": "1/08/2018", "name": "test"},
        # Specify start and periods, the number of periods (days).
        {"start": "1/1/2018", "periods": 8},
        # Specify end and periods, the number of periods (days).
        {"end": "1/1/2018", "periods": 8},
        # Specify frequency to hour.
        {"start": "1/1/2018", "end": "1/08/2018", "freq": pd.offsets.Hour(2)},
        # Specify frequency to minute.
        {"start": "1/1/2018", "end": "1/02/2018", "freq": "min"},
        # Specify frequency to second.
        {"start": "1/1/2018", "end": "1/02/2018", "freq": "s"},
        # Specify frequency to millisecond.
        {
            "start": "1/1/2018 00:00:00.001",
            "end": "1/1/2018 00:00:01.00123",
            "freq": "ms",
        },
        # Specify frequency to microsecond.
        {
            "start": "1/1/2018 00:00:00.001",
            "end": "1/1/2018 00:00:00.10123",
            "freq": "us",
        },
        # Specify frequency to nanosecond.
        {
            "start": "1/1/2018 00:00:00.001",
            "end": "1/1/2018 00:00:00.00123",
            "freq": "ns",
        },
        # Specify freq to None
        {
            "start": "2017-01-01",
            "periods": 19,
            "freq": None,
        },
        # Specify freq and periods to None
        {
            "start": "1/1/2018 00:00:00.001",
            "end": "1/1/2018 00:00:00.00123",
            "periods": None,
            "freq": None,
        },
    ],
)
@sql_count_checker(query_count=1)
def test_regular_range(kwargs, date_range_func):
    if date_range_func == "bdate_range" and kwargs.get("freq", None) is None:
        kwargs["freq"] = "D"
    assert_snowpark_pandas_equal_to_pandas(
        getattr(pd, date_range_func)(**kwargs),
        getattr(native_pd, date_range_func)(**kwargs),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "start": "1/1/2018 00:00:00.001",
            "end": "2/1/2019 00:00:00.10123",
            "freq": "3ME",
        },
        {
            "end": "3/1/2018 00:00:00.001",  # start < end
            "start": "4/1/2019 00:00:00.10123",
            "freq": "3ME",
        },
        {
            "start": "2/29/2024",
            "periods": 5,
            "freq": "ME",
        },
        {
            "start": "2/29/2024",
            "periods": 5,
            "freq": "BME",
        },
        {
            "start": "6/15/2018",
            "periods": 5,
            "freq": "MS",
        },
        {
            "start": "6/15/2018",
            "periods": 5,
            "freq": "BMS",
        },
        {
            "start": "7/15/2018",
            "periods": 5,
            "freq": "W",
        },
        {
            "start": "8/15/2018",
            "periods": 5,
            "freq": "QS",
        },
        {
            "start": "8/15/2018",
            "periods": 5,
            "freq": "BQS",
        },
        {
            "start": "9/15/2018",
            "periods": 5,
            "freq": "QE",
        },
        {
            "start": "9/15/2018",
            "periods": 5,
            "freq": "BQE",
        },
        {
            "end": "10/10/2018",
            "periods": 5,
            "freq": "YS",
        },
        {
            "end": "10/10/2018",
            "periods": 5,
            "freq": "BYS",
        },
        {
            "end": "11/10/2018",
            "periods": 5,
            "freq": "Y",
        },
        {
            "end": "11/10/2018",
            "periods": 5,
            "freq": "BY",
        },
        {
            "end": "12/10/2018",
            "periods": 5,
            "freq": "YE",
        },
        {
            "end": "12/10/2018",
            "periods": 5,
            "freq": "BYE",
        },
    ],
)
@sql_count_checker(query_count=1)
def test_irregular_range(kwargs, date_range_func):
    assert_snowpark_pandas_equal_to_pandas(
        getattr(pd, date_range_func)(**kwargs),
        getattr(native_pd, date_range_func)(**kwargs),
    )


@pytest.mark.parametrize(
    "freq",
    [
        "C",  # custom business day frequency
        "SMS",  # semi-month start frequency (1st and 15th)
        "CBMS",  # custom business month start frequency
        "bh",  # business hour frequency
        "cbh",  # custom business hour frequency
    ],
)
@sql_count_checker(query_count=0)
def test_irregular_range_not_implemented(freq, date_range_func):
    with pytest.raises(NotImplementedError):
        getattr(pd, date_range_func)(start="1/1/2018", periods=5, freq=freq)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1, 2, 5, 13])
@pytest.mark.parametrize(
    "inclusive",
    [
        "both",
        "left",
        "right",
        "neither",
    ],
)
def test_without_freq(periods, inclusive):
    kwargs = {
        "start": "2018-04-24",
        "end": "2018-04-27",
        "periods": periods,
        "inclusive": inclusive,
    }
    assert_snowpark_pandas_equal_to_pandas(
        pd.date_range(**kwargs), native_pd.date_range(**kwargs)
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start": "2017-01-01", "end": "2017-01-04"},
        {"start": "1/1/2018", "periods": 5},
        {"end": "1/1/2018", "periods": 5},
    ],
)
@pytest.mark.parametrize(
    "inclusive",
    [
        "both",
        "left",
        "right",
        "neither",
    ],
)
@sql_count_checker(query_count=1)
def test_inclusive(kwargs, inclusive):
    kwargs.update({"inclusive": inclusive})
    assert_snowpark_pandas_equal_to_pandas(
        pd.date_range(**kwargs), native_pd.date_range(**kwargs)
    )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        [
            {"periods": 5},
            "Of the four parameters: start, end, periods, and freq, exactly three must be specified",
        ],
        [
            {"start": pd.NaT, "end": "2010", "freq": "H"},
            "Neither `start` nor `end` can be NaT",
        ],
        [
            {"start": "2018-04-24", "end": "2018-04-27", "periods": -1},
            "Number of samples, -1, must be non-negative.",
        ],
    ],
)
@sql_count_checker(query_count=0)
def test_value_error_negative(kwargs, match):
    with pytest.raises(ValueError, match=match):
        native_pd.date_range(**kwargs)

    with pytest.raises(ValueError, match=match):
        pd.date_range(**kwargs).to_pandas()


@sql_count_checker(query_count=0)
def test_bdate_range_negative():
    with pytest.raises(TypeError):
        pd.bdate_range(freq=None)

    with pytest.raises(NotImplementedError):
        pd.bdate_range(holidays="set")


@pytest.mark.parametrize(
    "start, end, periods, tz, tz_offset",
    [
        ["2018-04-24", None, 10, "Asia/Tokyo", "UTC+09:00"],
        ["2018-04-24", None, 10, "UTC", "UTC"],
        [
            native_pd.to_datetime("1/1/2018").tz_localize("Europe/Berlin"),
            native_pd.to_datetime("1/08/2018").tz_localize("Europe/Berlin"),
            None,
            None,
            "UTC+01:00",
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_tz(date_range_func, start, end, periods, tz, tz_offset):
    kwargs = {
        "start": start,
        "end": end,
        "periods": periods,
        "tz": tz,
    }
    # TODO: SNOW-1707640 fix this bug: bdate_range returns less data points than expected
    if date_range_func == "bdate_range" and kwargs.get("freq", None) is None:
        kwargs["freq"] = "D"
    assert_snowpark_pandas_equal_to_pandas(
        getattr(pd, date_range_func)(**kwargs),
        # convert expected index with tz offset
        getattr(native_pd, date_range_func)(**kwargs).tz_convert(tz_offset),
    )
