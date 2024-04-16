#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
from typing import Callable
from unittest import mock

import numpy as np
import pandas as native_pd
import pytest
from pandas import DatetimeTZDtype

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark.modin.pandas import DataFrame, Series
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.types import TimestampType


@pytest.fixture(scope="function")
def mock_query_compiler_for_dt_series() -> SnowflakeQueryCompiler:
    mock_internal_frame = mock.create_autospec(InternalFrame)
    mock_internal_frame.data_columns_index = native_pd.Index(["A"], name="B")
    mock_internal_frame.data_column_snowflake_quoted_identifiers = ['"A"']
    mock_internal_frame.quoted_identifier_to_snowflake_type.return_value = {
        '"A"': TimestampType()
    }
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return fake_query_compiler


@mock.patch(
    "snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.DateTimeDefault.register"
)
@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda s: s.dt.time, "time"),
        (lambda s: s.dt.timetz, "timetz"),
        (lambda s: s.dt.microsecond, "microsecond"),
        (lambda s: s.dt.nanosecond, "nanosecond"),
        (lambda s: s.dt.week, "week"),
        (lambda s: s.dt.weekofyear, "weekofyear"),
        (lambda s: s.dt.dayofweek, "dayofweek"),
        (lambda s: s.dt.weekday, "weekday"),
        (lambda s: s.dt.dayofyear, "dayofyear"),
        (lambda s: s.dt.is_month_start, "is_month_start"),
        (lambda s: s.dt.is_month_end, "is_month_end"),
        (lambda s: s.dt.is_quarter_start, "is_quarter_start"),
        (lambda s: s.dt.is_quarter_end, "is_quarter_end"),
        (lambda s: s.dt.is_year_start, "is_year_start"),
        (lambda s: s.dt.is_year_end, "is_year_end"),
        (lambda s: s.dt.is_leap_year, "is_leap_year"),
        (lambda s: s.dt.daysinmonth, "daysinmonth"),
        (lambda s: s.dt.days_in_month, "days_in_month"),
        (lambda s: s.dt.to_period(), "to_period"),
        (lambda s: s.dt.tz_localize(tz="UTC"), "tz_localize"),
        (lambda s: s.dt.tz_convert(tz="UTC"), "tz_convert"),
        (lambda s: s.dt.normalize(), "normalize"),
        (lambda s: s.dt.strftime(date_format="YY/MM/DD"), "strftime"),
        (lambda s: s.dt.round(freq="1D"), "round"),
        (lambda s: s.dt.floor(freq="1D"), "floor"),
        (lambda s: s.dt.ceil(freq="1D"), "ceil"),
        (lambda s: s.dt.month_name(), "month_name"),
        (lambda s: s.dt.day_name(), "day_name"),
        (lambda s: s.dt.total_seconds(), "total_seconds"),
        (lambda s: s.dt.seconds, "seconds"),
        (lambda s: s.dt.days, "days"),
        (lambda s: s.dt.microseconds, "microseconds"),
        (lambda s: s.dt.nanoseconds, "nanoseconds"),
        (lambda s: s.dt.qyear, "qyear"),
        (lambda s: s.dt.start_time, "start_time"),
        (lambda s: s.dt.end_time, "end_time"),
        (lambda s: s.dt.to_timestamp(), "to_timestamp"),
    ],
)
def test_dt_methods(
    mock_datetime_register, func, func_name, mock_query_compiler_for_dt_series
):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = mock_query_compiler_for_dt_series
    mock_datetime_register.return_value = return_callable
    res = func(mock_series)
    mock_datetime_register.assert_called_once()
    assert isinstance(res, Series), func_name
    assert res._query_compiler == mock_query_compiler_for_dt_series, func_name


@mock.patch(
    "snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.DateTimeDefault.register"
)
def test_dt_components(mock_datetime_register, mock_query_compiler_for_dt_series):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = mock_query_compiler_for_dt_series
    mock_datetime_register.return_value = return_callable
    res = mock_series.dt.components
    mock_datetime_register.assert_called_once()
    assert isinstance(res, DataFrame)
    assert res._query_compiler == mock_query_compiler_for_dt_series


@mock.patch(
    "snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.DateTimeDefault.register"
)
def test_dt_to_pytimedelta(mock_datetime_register, mock_query_compiler_for_dt_series):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    result_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    result_array = np.array(
        [
            [
                datetime.timedelta(0),
                datetime.timedelta(days=1),
                datetime.timedelta(days=2),
                datetime.timedelta(days=3),
            ],
            [0, 1, 2, 3],
        ],
        dtype=object,
    )
    result_query_compiler.to_numpy.return_value = result_array

    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = result_query_compiler
    mock_datetime_register.return_value = return_callable
    res = mock_series.dt.to_pytimedelta()
    assert res.tolist() == np.array([datetime.timedelta(0), 0]).tolist()


@mock.patch(
    "snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.DateTimeDefault.register"
)
def test_dt_to_pydatetime(mock_datetime_register, mock_query_compiler_for_dt_series):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    result_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    result_query_compiler.columnarize.return_value = result_query_compiler
    result_array = np.array(
        [datetime.datetime(2018, 3, 10, 0, 0), datetime.datetime(2018, 3, 11, 0, 0)],
        dtype=object,
    )
    result_query_compiler.to_numpy.return_value = result_array

    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = result_query_compiler
    mock_datetime_register.return_value = return_callable
    res = mock_series.dt.to_pydatetime()
    assert res.tolist() == result_array.tolist()


def test_dt_tz():
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler
    time_type = DatetimeTZDtype(tz="UTC")
    mock_query_compiler.dtypes = native_pd.Series([time_type])
    mock_series = Series(query_compiler=mock_query_compiler)

    res = mock_series.dt.tz
    assert res == time_type.tz


@mock.patch(
    "snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.DateTimeDefault.register"
)
def test_dt_freq(mock_datetime_register, mock_query_compiler_for_dt_series):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    result_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    result_query_compiler.to_pandas.return_value = native_pd.DataFrame(["D"])

    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = result_query_compiler
    mock_datetime_register.return_value = return_callable
    res = mock_series.dt.freq
    assert res == "D"
