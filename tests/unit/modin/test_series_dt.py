#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
    mock_internal_frame.get_snowflake_type.return_value = [TimestampType()]
    mock_internal_frame.quoted_identifier_to_snowflake_type.return_value = {
        '"A"': TimestampType()
    }
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return fake_query_compiler


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda s: s.dt.timetz, "timetz"),
        (lambda s: s.dt.to_period(), "to_period"),
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
