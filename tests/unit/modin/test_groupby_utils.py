#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.groupby_utils import (
    check_is_groupby_supported_by_snowflake,
    is_groupby_value_label_like,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)


def create_series_query_compiler() -> SnowflakeQueryCompiler:
    mock_internal_frame = mock.create_autospec(InternalFrame)
    mock_internal_frame.data_columns_index = native_pd.Index(["A"], name="B")
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return fake_query_compiler


def create_df_query_compiler() -> SnowflakeQueryCompiler:
    mock_internal_frame = mock.create_autospec(InternalFrame)
    mock_internal_frame.data_columns_index = native_pd.Index(["B", "C"], name=None)
    fake_query_compiler = SnowflakeQueryCompiler(mock_internal_frame)

    return fake_query_compiler


@pytest.mark.parametrize(
    "by, expected_result",
    [
        ("col", True),  # hashable
        (["col1", "col2"], True),  # list of hashable
        (
            pd.Series(query_compiler=create_series_query_compiler()),
            False,
        ),  # SnowflakeQueryCompiler
        (
            pd.Series(query_compiler=create_series_query_compiler()),
            False,
        ),  # SnowSeries
        (lambda x: x + 1, False),  # Callable
        (
            [
                "col1",
                pd.Series(query_compiler=create_series_query_compiler()),
                "col2",
            ],
            False,
        ),
        ([lambda x: x // 3, "col1", "col2"], False),
        (["col1", [1, 2, 3]], False),
        (["col1", None, "col2"], True),
    ],
)
def test_check_groupby_snowflake_execution_by_args(by, expected_result):
    can_be_distributed = check_is_groupby_supported_by_snowflake(
        by=by, level=None, axis=0
    )
    assert can_be_distributed == expected_result


def test_check_groupby_snowflake_execution_by_args_axis_1():
    can_be_distributed = check_is_groupby_supported_by_snowflake(
        by="col1", level=None, axis=1
    )
    assert not can_be_distributed


@pytest.mark.parametrize(
    "val, expected_result",
    [
        ("col", True),  # hashable
        (("col1", "col2"), True),  # hashable
        (lambda x: x + 1, False),  # callable
        ([1, 2, 3], False),  # list like
        (3, True),  # scalar
        (np.array([1, 2, 3]), False),
        (None, True),
        (pd.Series(query_compiler=create_series_query_compiler()), False),
        ({"col1": 0, "col2": 1}, False),  # map
        (pd.Grouper(level=1), False),  # grouper
    ],
)
def test_is_groupby_value_label_like(val, expected_result):
    assert is_groupby_value_label_like(val) == expected_result
