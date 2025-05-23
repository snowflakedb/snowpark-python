#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock, patch

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import (
    create_empty_native_pandas_frame,
    try_convert_builtin_func_to_str,
    validate_and_try_convert_agg_func_arg_func_to_str,
)

# Tests for modin.plugin.extensions.utils


@pytest.mark.parametrize("index_names", [["C"], ["C", "D"], [None]])
@patch(
    "snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler.SnowflakeQueryCompiler"
)
def test_create_empty_frame_from_dataframe(mock_query_compiler, index_names):
    columns = ["A", "B"]
    mock_query_compiler.columns = columns
    mock_query_compiler.get_index_names = MagicMock(return_value=index_names)
    snow_df = pd.DataFrame(query_compiler=mock_query_compiler)
    native_df = create_empty_native_pandas_frame(snow_df)

    # Verify columns
    assert native_df.columns.tolist() == columns
    # Verify index type
    if len(index_names) > 1:
        assert isinstance(native_df.index, native_pd.MultiIndex)
    else:
        assert isinstance(native_df.index, native_pd.Index)
    # Verify index names
    assert native_df.index.names == index_names


@pytest.mark.parametrize("index_names", [["C"], ["C", "D"], [None]])
@patch(
    "snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler.SnowflakeQueryCompiler"
)
def test_create_empty_frame_from_series(mock_query_compiler, index_names):
    name = "A"
    mock_query_compiler.columns = [name]
    mock_query_compiler.get_index_names = MagicMock(return_value=index_names)
    mock_query_compiler.columnarize = MagicMock(return_value=mock_query_compiler)
    snow_series = pd.Series(query_compiler=mock_query_compiler)
    native_df = create_empty_native_pandas_frame(snow_series)

    # Verify columns
    assert native_df.columns.tolist() == [name]
    # Verify index type
    if len(index_names) > 1:
        assert isinstance(native_df.index, native_pd.MultiIndex)
    else:
        assert isinstance(native_df.index, native_pd.Index)
    # Verify index names
    assert native_df.index.names == index_names


@pytest.mark.parametrize(
    "agg_func, expected_res",
    [
        (max, "max"),
        (("count", np.max, min), ["count", np.max, "min"]),
        ([max, "min"], ["max", "min"]),
        ([np.max, np.percentile, min], [np.max, np.percentile, "min"]),
        ([sum, max, min], ["sum", "max", "min"]),
    ],
)
def test_try_convert_builtin_func_to_str(mock_dataframe, agg_func, expected_res):
    result = try_convert_builtin_func_to_str(agg_func, mock_dataframe)
    assert result == expected_res


@pytest.mark.parametrize(
    "agg_func, axis, expected_res",
    [
        (max, 0, "max"),
        (max, 1, "max"),
        (("count", np.max, min), 0, ["count", np.max, "min"]),
        (
            {"A": max, "B": [np.max, "count"], "C": [sum]},
            0,
            {"A": "max", "B": [np.max, "count"], "C": ["sum"]},
        ),
    ],
)
def test_validate_and_try_convert_agg_func_arg_func_to_str(
    mock_dataframe_multicolumns, agg_func, axis, expected_res
):
    result = validate_and_try_convert_agg_func_arg_func_to_str(
        agg_func,
        mock_dataframe_multicolumns,
        allow_duplication=True,
        axis=axis,
    )
    assert result == expected_res
