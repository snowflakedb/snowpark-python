#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result

dt_properties = pytest.mark.parametrize(
    "property_name",
    ["date", "hour", "minute", "second", "year", "month", "day", "quarter"],
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


@pytest.mark.modin_sp_precommit
def test_isocalendar():
    with SqlCounter(query_count=1):
        date_range = native_pd.date_range("2020-05-01", periods=5, freq="4D")
        native_ser = native_pd.Series(date_range)
        snow_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda ser: ser.dt.isocalendar()
        )
    with SqlCounter(query_count=1):
        native_ser = native_pd.to_datetime(native_pd.Series(["2010-01-01", None]))
        snow_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda ser: ser.dt.isocalendar()
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("property", ["dayofyear", "day_of_year"])
def test_day_of_year(property, day_of_week_or_year_data):
    eval_snowpark_pandas_result(
        *create_test_series(day_of_week_or_year_data),
        lambda df: getattr(df.dt, property),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("property", ["dayofweek", "day_of_week"])
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
        lambda df: getattr(df.dt, property),
    )


@dt_properties
@sql_count_checker(query_count=1)
def test_dt_property_with_tz(property_name):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56",
            "2014-07-18 21:24",
            "2015-11-22 22:14",
            "2015-11-23",
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
@pytest.mark.parametrize("freq", ["d", "h", "min", "s", "y", "m", "D", "3m"])
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
