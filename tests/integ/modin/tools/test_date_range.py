#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


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
def test_regular_range(kwargs):
    assert_snowpark_pandas_equal_to_pandas(
        pd.date_range(**kwargs), native_pd.Series(native_pd.date_range(**kwargs))
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
            "start": "6/15/2018",
            "periods": 5,
            "freq": "MS",
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
            "start": "9/15/2018",
            "periods": 5,
            "freq": "QE",
        },
        {
            "end": "10/10/2018",
            "periods": 5,
            "freq": "YS",
        },
        {
            "end": "11/10/2018",
            "periods": 5,
            "freq": "Y",
        },
        {
            "end": "12/10/2018",
            "periods": 5,
            "freq": "YE",
        },
    ],
)
@sql_count_checker(query_count=1)
def test_irregular_range(kwargs):
    assert_snowpark_pandas_equal_to_pandas(
        pd.date_range(**kwargs), native_pd.Series(native_pd.date_range(**kwargs))
    )


@pytest.mark.parametrize(
    "freq",
    [
        "B",  # business day frequency
        "C",  # custom business day frequency
        "SMS",  # semi-month start frequency (1st and 15th)
        "BMS",  # business month start frequency
        "CBMS",  # custom business month start frequency
        "BQS",  # business quarter start frequency
        "BYS",  # business year start frequency
        "bh",  # business hour frequency
        "cbh",  # custom business hour frequency
    ],
)
@sql_count_checker(query_count=0)
def test_irregular_range_not_implemented(freq):
    with pytest.raises(NotImplementedError):
        pd.date_range(start="1/1/2018", periods=5, freq=freq)


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
        pd.date_range(**kwargs), native_pd.Series(native_pd.date_range(**kwargs))
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
        pd.date_range(**kwargs), native_pd.Series(native_pd.date_range(**kwargs))
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


@pytest.mark.parametrize(
    "kwargs",
    [
        # Specify tz to set the timezone. TODO: SNOW-879476 support tz with other tz APIs
        {"start": "1/1/2018", "periods": 5, "tz": "Asia/Tokyo"},
    ],
)
@sql_count_checker(query_count=0)
def test_not_supported(kwargs):
    with pytest.raises(NotImplementedError):
        pd.date_range(**kwargs).to_pandas()
