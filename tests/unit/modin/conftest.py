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
from snowflake.snowpark.types import StringType

from modin.config import AutoSwitchBackend

# Disable automatic backend selection for hybrid execution by default.
AutoSwitchBackend.disable()


@pytest.fixture(scope="function", autouse=True)
def mock_session():
    # Temporary fix for SNOW-2132871
    # Modin QC code tries to find an active session even with auto-switching disabled
    with mock.patch("snowflake.snowpark.context.get_active_session"):
        yield


@pytest.fixture(scope="function")
def mock_single_col_query_compiler() -> SnowflakeQueryCompiler:
    mock_internal_frame = mock.create_autospec(InternalFrame)
    mock_internal_frame.data_columns_index = native_pd.Index(["A"], name="B")
    mock_internal_frame.data_column_snowflake_quoted_identifiers = ['"A"']
    mock_internal_frame.snowflake_quoted_identifier_to_snowpark_pandas_type = {
        '"A"': None
    }
    mock_internal_frame.get_snowflake_type.return_value = [StringType()]
    mock_internal_frame.quoted_identifier_to_snowflake_type.return_value = {
        '"A"': StringType()
    }
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return fake_query_compiler


@pytest.fixture(scope="function")
def mock_series(mock_single_col_query_compiler) -> pd.Series:
    fake_query_compiler = mock_single_col_query_compiler
    fake_series = pd.Series(query_compiler=fake_query_compiler)

    return fake_series


@pytest.fixture(scope="function")
def mock_dataframe(mock_single_col_query_compiler) -> pd.DataFrame:
    fake_query_compiler = mock_single_col_query_compiler
    fake_dataframe = pd.DataFrame(query_compiler=fake_query_compiler)

    return fake_dataframe


@pytest.fixture(scope="function")
def mock_dataframe_multicolumns() -> pd.DataFrame:
    mock_internal_frame = mock.create_autospec(InternalFrame)
    mock_internal_frame.data_columns_index = native_pd.Index(
        ["A", "B", "C"], name="cols"
    )
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return pd.DataFrame(query_compiler=fake_query_compiler)
