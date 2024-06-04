#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
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


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda s: s.dt.time, "time"),
        (lambda s: s.dt.timetz, "timetz"),
        (lambda s: s.dt.microsecond, "microsecond"),
        (lambda s: s.dt.nanosecond, "nanosecond"),
        (lambda s: s.dt.weekday, "weekday"),
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
        (lambda s: s.dt.components(), "components"),
        (lambda s: s.dt.to_pytimedelta(), "to_pytimedelta"),
        (lambda s: s.dt.to_pydatetime(), "to_pydatetime"),
    ],
)
def test_dt_methods(func, func_name, mock_query_compiler_for_dt_series):
    mock_series = pd.Series(query_compiler=mock_query_compiler_for_dt_series)
    msg = f"Snowpark pandas doesn't yet support the (method|property) 'Series.dt.{func_name}'"
    with pytest.raises(NotImplementedError, match=msg):
        func(mock_series)
