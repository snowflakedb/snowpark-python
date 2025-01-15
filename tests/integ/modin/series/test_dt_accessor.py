#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
import pytz

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import IS_WINDOWS

dt_properties = pytest.mark.parametrize(
    "property_name",
    [
        "date",
        "time",
        "hour",
        "minute",
        "second",
        "microsecond",
        "nanosecond",
        "year",
        "month",
        "day",
        "quarter",
        "is_month_start",
        "is_month_end",
        "is_quarter_start",
        "is_quarter_end",
        "is_year_start",
        "is_year_end",
        "is_leap_year",
        "days_in_month",
        "daysinmonth",
    ],
)


timezones = pytest.mark.parametrize(
    "tz",
    [
        None,
        # Use a subset of pytz.common_timezones containing a few timezones in each
        *[
            param_for_one_tz
            for tz in [
                "Africa/Abidjan",
                "Africa/Timbuktu",
                "America/Adak",
                "America/Yellowknife",
                "Antarctica/Casey",
                "Asia/Dhaka",
                "Asia/Manila",
                "Asia/Shanghai",
                "Atlantic/Stanley",
                "Australia/Sydney",
                "Canada/Pacific",
                "Europe/Chisinau",
                "Europe/Luxembourg",
                "Indian/Christmas",
                "Pacific/Chatham",
                "Pacific/Wake",
                "US/Arizona",
                "US/Central",
                "US/Eastern",
                "US/Hawaii",
                "US/Mountain",
                "US/Pacific",
                "UTC",
            ]
            for param_for_one_tz in (
                pytz.timezone(tz),
                tz,
            )
        ],
    ],
)


@pytest.fixture
def day_of_week_or_year_data() -> native_pd.Series:
    return native_pd.Series(
        [
            pd.Timestamp(
                year=2017,
                month=1,
                day=1,
                hour=10,
                minute=59,
                second=59,
                microsecond=5959,
            ),
            pd.Timestamp(year=2000, month=2, day=1),
            pd.NaT,
            pd.Timestamp(year=2024, month=7, day=29),
        ],
    )


@pytest.fixture
def set_week_start(request):
    original_start = (
        pd.session.sql("SHOW PARAMETERS LIKE 'WEEK_START'").collect()[0].value
    )
    pd.session.connection.cursor().execute(
        f"ALTER SESSION SET WEEK_START = {request.param};"
    )
    yield
    pd.session.connection.cursor().execute(
        f"ALTER SESSION SET WEEK_START = {original_start};"
    )


@pytest.mark.parametrize(
    "datetime_index_value",
    [
        ["2014-04-04 23:56", "2014-07-18 21:24", "2015-11-22 22:14"],
        ["04/04/2014", "07/18/2013", "11/22/2015"],
        ["2014-04-04 23:56", pd.NaT, "2014-07-18 21:24", "2015-11-22 22:14", pd.NaT],
        [
            pd.Timestamp(2017, 1, 1, 12),
            pd.Timestamp(2018, 2, 1, 10),
            pd.Timestamp(2000, 2, 1, 10),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_date(datetime_index_value):
    native_ser = native_pd.Series(native_pd.DatetimeIndex(datetime_index_value))
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.dt.date)


@pytest.mark.parametrize(
    "datetime_index_value",
    [
        ["2014-04-04 23:56:20", "2014-07-18 21:24:30", "2015-11-22 22:14:40"],
        ["04/04/2014", "07/18/2013", "11/22/2015"],
        ["2014-04-04 23:56", pd.NaT, "2014-07-18 21:24", "2015-11-22 22:14", pd.NaT],
        [
            pd.Timestamp(2017, 1, 1, 12),
            pd.Timestamp(2018, 2, 1, 10),
            pd.Timestamp(2000, 2, 1, 10),
        ],
    ],
)
@pytest.mark.parametrize("func", ["floor", "ceil", "round"])
@pytest.mark.parametrize(
    "freq",
    [
        "1d",
        "2d",
        "1h",
        "2h",
        "1min",
        "2min",
        "1s",
        "2s",
    ],
)
def test_floor_ceil_round(datetime_index_value, func, freq):
    native_ser = native_pd.Series(native_pd.DatetimeIndex(datetime_index_value))
    snow_ser = pd.Series(native_ser)
    if func == "round" and "s" in freq:
        with SqlCounter(query_count=0):
            with pytest.raises(NotImplementedError):
                snow_ser.dt.round(freq=freq)
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_ser, native_ser, lambda ser: getattr(ser.dt, func)(freq)
            )


@pytest.mark.parametrize("func", ["floor", "ceil", "round"])
@pytest.mark.parametrize(
    "freq, ambiguous, nonexistent",
    [
        ("1w", "raise", "raise"),
        ("1h", "infer", "raise"),
        ("1h", "NaT", "raise"),
        ("1h", np.array([True, True, False]), "raise"),
        ("1h", "raise", "shift_forward"),
        ("1h", "raise", "shift_backward"),
        ("1h", "raise", "NaT"),
        ("1h", "raise", pd.Timedelta("1h")),
        ("1w", "infer", "shift_forward"),
    ],
)
@sql_count_checker(query_count=0)
def test_floor_ceil_round_negative(func, freq, ambiguous, nonexistent):
    datetime_index_value = [
        "2014-04-04 23:56",
        pd.NaT,
        "2014-07-18 21:24",
        "2015-11-22 22:14",
        pd.NaT,
    ]
    native_ser = native_pd.Series(native_pd.DatetimeIndex(datetime_index_value))
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        getattr(snow_ser.dt, func)(
            freq=freq, ambiguous=ambiguous, nonexistent=nonexistent
        )


@sql_count_checker(query_count=1)
def test_normalize():
    date_range = native_pd.date_range(start="2021-01-01", periods=5, freq="7h")
    native_ser = native_pd.Series(date_range)
    native_ser.iloc[2] = native_pd.NaT
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.dt.normalize(),
    )


@sql_count_checker(query_count=2)
@timezones
def test_tz_convert(tz):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series(datetime_index)
    assert str(native_ser.dtype.tz) == "US/Eastern"
    snow_ser = pd.Series(native_ser)
    # This is a great example to show the current limit of Snowpark pandas timezone, it only preserves the timezone
    # offset and the timezone will be gone. So in this case, Snowpark pandas does not know the timezone is "US/Eastern"
    # so it will treat it as a multi timezone offset column which results a dtype as "object".
    assert snow_ser.dtype == "object"
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.dt.tz_convert(tz),
    )


@sql_count_checker(query_count=0)
def test_tz_convert_negative():
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        snow_ser.dt.tz_convert(tz="UTC+09:00")


@sql_count_checker(query_count=1)
@timezones
def test_tz_localize(tz):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.dt.tz_localize(tz),
    )


@pytest.mark.parametrize(
    "tz, ambiguous, nonexistent",
    [
        (None, "infer", "raise"),
        (None, "NaT", "raise"),
        (None, np.array([True, True, False]), "raise"),
        (None, "raise", "shift_forward"),
        (None, "raise", "shift_backward"),
        (None, "raise", "NaT"),
        (None, "raise", pd.Timedelta("1h")),
        (None, "infer", "shift_forward"),
        ("UTC+09:00", "raise", "raise"),
    ],
)
@sql_count_checker(query_count=0)
def test_tz_localize_negative(tz, ambiguous, nonexistent):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        snow_ser.dt.tz_localize(tz=tz, ambiguous=ambiguous, nonexistent=nonexistent)


@pytest.mark.parametrize("name", [None, "hello"])
def test_isocalendar(name):
    with SqlCounter(query_count=1):
        date_range = native_pd.date_range("2020-05-01", periods=5, freq="4D")
        native_ser = native_pd.Series(date_range, name=name)
        snow_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda ser: ser.dt.isocalendar()
        )
    with SqlCounter(query_count=1):
        native_ser = native_pd.to_datetime(
            native_pd.Series(["2010-01-01", None], name=name)
        )
        snow_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda ser: ser.dt.isocalendar()
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("property", ["dayofyear", "day_of_year"])
def test_day_of_year(property, day_of_week_or_year_data):
    eval_snowpark_pandas_result(
        *create_test_series(day_of_week_or_year_data),
        lambda s: getattr(s.dt, property),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("property", ["dayofweek", "day_of_week", "weekday"])
@pytest.mark.parametrize(
    "set_week_start",
    # Test different WEEK_START values because WEEK_START changes the DAYOFWEEK
    # in Snowflake.
    list(range(8)),
    indirect=True,
)
def test_day_of_week(property, day_of_week_or_year_data, set_week_start):
    eval_snowpark_pandas_result(
        *create_test_series(day_of_week_or_year_data),
        lambda s: getattr(s.dt, property),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("method", ["day_name", "month_name"])
def test_day_month_name(method):
    date_range = native_pd.date_range("2020-05-01", periods=5, freq="17D")
    native_ser = native_pd.Series(date_range)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: getattr(s.dt, method)(),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("method", ["day_name", "month_name"])
def test_day_month_name_negative(method):
    date_range = native_pd.date_range("2020-05-01", periods=5, freq="17D")
    native_ser = native_pd.Series(date_range)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        getattr(snow_ser.dt, method)(locale="pt_BR.utf8")


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "property",
    [
        "is_month_start",
        "is_month_end",
        "is_quarter_start",
        "is_quarter_end",
        "is_year_start",
        "is_year_end",
    ],
)
def test_is_x_start_end(property):
    # Create a series containing the first and last dates of each month
    #  in a normal year and a leap year.
    date_range = native_pd.date_range("2023-01-01", periods=731, freq="1D")
    native_ser = native_pd.Series(date_range)
    native_ser = native_ser[native_ser.dt.is_month_start | native_ser.dt.is_month_end]
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: getattr(s.dt, property),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "property",
    [
        "days_in_month",
        "daysinmonth",
    ],
)
def test_days_in_month(property):
    # Create a series containing one date in each month
    #  within both a normal year and a leap year.
    date_range = native_pd.date_range("2023-01-01", periods=25, freq="1M")
    native_ser = native_pd.Series(date_range)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: getattr(s.dt, property),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "date_format",
    [
        "a%d-%m-%Y-%H-%M-%S-%f-%j-%X-%%b",
        "%d-%m-%Y-%H-%M-%S-%f-%j-%X-%%b",
        "a%d-%m-%Y-%H-%M-%S-%f-%j-%X-%%",
        "%d-%m-%Y-%H-%M-%S-%f-%j-%X-%%",
        "%%%M",
        "%%M",
        "abc%",
    ],
)
def test_strftime(date_format):
    if IS_WINDOWS and date_format == "abc%":
        pytest.skip(
            "Windows test shows that native pandas leaves the input value unchanged when date_format='abc%'"
        )

    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.dt.strftime(date_format=date_format),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "date_format",
    [
        "%a",
        "%A",
        "%w",
        "%b",
        "%B",
        "%y",
        "%I",
        "%p",
        "%z",
        "%Z",
        "%U",
        "%W",
        "%c",
        "%x",
    ],
)
def test_strftime_neg(date_format):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        snow_ser.dt.strftime(date_format=date_format)


@dt_properties
@sql_count_checker(query_count=1)
def test_dt_property_with_tz(property_name):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.dt, property_name)
    )


@dt_properties
@pytest.mark.parametrize(
    "freq", ["d", "h", "min", "s", "y", "m", "D", "3m", "ms", "us", "ns"]
)
@sql_count_checker(query_count=1)
def test_dt_properties(property_name, freq):
    native_ser = native_pd.Series(
        native_pd.date_range(start="2021-01-01", periods=5, freq=freq),
        index=[2, 6, 7, 8, 11],
        name="test",
    )
    native_ser.iloc[2] = native_pd.NaT
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.dt, property_name)
    )


@pytest.mark.parametrize(
    "property_name", ["days", "seconds", "microseconds", "nanoseconds"]
)
@sql_count_checker(query_count=1)
def test_dt_timedelta_properties(property_name):
    native_ser = native_pd.Series(
        native_pd.TimedeltaIndex(
            [
                "1d",
                "1h",
                "60s",
                "1s",
                "800ms",
                "5us",
                "6ns",
                "1d 3s",
                "9m 15s 8us",
                None,
            ]
        ),
        index=[2, 6, 7, 8, 11, 16, 17, 20, 25, 27],
        name="test",
    )
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.dt, property_name)
    )


@pytest.mark.parametrize(
    "data, data_type",
    [
        ([1, 2, 3, 4, 5], "int"),
        ([1.1, 2.0, None, 4.0, 5.3], "float"),
        (["a", "b", "c", "dd"], None),
        (
            [
                datetime.date(2019, 12, 4),
                datetime.date(2019, 12, 5),
                datetime.date(2019, 12, 6),
            ],
            None,
        ),
        (
            [
                datetime.time(11, 12, 13),
                datetime.time(12, 21, 5),
                datetime.time(5, 2, 6),
            ],
            None,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_dt_invalid_dtypes(data, data_type):
    native_ser = native_pd.Series(data)
    if data_type:
        native_ser.astype(data_type)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.dt,
        expect_exception=True,
        expect_exception_match="Can only use .dt accessor with datetimelike values",
    )


@pytest.mark.parametrize(
    "data, data_type, property_name",
    [
        (
            [
                datetime.datetime(2019, 12, 4, 11, 12, 13),
                datetime.datetime(2019, 12, 5, 12, 21, 5),
                datetime.datetime(2019, 12, 6, 5, 2, 6),
            ],
            None,
            "seconds",
        ),
        (
            [
                datetime.timedelta(11, 12, 13),
                datetime.timedelta(12, 21, 5),
                datetime.timedelta(5, 2, 6),
            ],
            None,
            "second",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_dt_invalid_dtype_property_combo(data, data_type, property_name):
    native_ser = native_pd.Series(data)
    if data_type:
        native_ser.astype(data_type)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: getattr(ser.dt, property_name),
        expect_exception=True,
        expect_exception_match="object has no attribute",
    )


@sql_count_checker(query_count=1)
def test_dt_total_seconds():
    data = [
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
    native_ser = native_pd.Series(native_pd.TimedeltaIndex(data))
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda x: x.dt.total_seconds())


@sql_count_checker(query_count=0)
def test_timedelta_total_seconds_type_error():
    native_ser = native_pd.Series(native_pd.DatetimeIndex(["2024-01-01"]))
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda x: x.dt.total_seconds(),
        expect_exception=True,
        expect_exception_type=AttributeError,
        expect_exception_match="'DatetimeProperties' object has no attribute 'total_seconds'",
    )
