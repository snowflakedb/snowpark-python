#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest
from unittest import mock

from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.debug_utils import (
    DataFrameTraceNode,
    _format_source_location,
)
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto


curr_file_path = os.path.dirname(os.path.abspath(__file__))
test_file_path = os.path.join(
    os.path.dirname(curr_file_path),
    "resources",
    "test_df_debug_dir",
    "sample_read_file.txt",
)


@pytest.mark.parametrize(
    "start_line, end_line, start_column, end_column, expected, minor_py_version",
    [
        (1, 2, 2, 5, "34567890\n23456", 12),
        (1, 0, 1, 1, "1234567890", 9),
        (2, 4, 0, 20, "2345678901\n3456789012\n4567890123", 12),
        (1, 1, 0, 5, "12345", 11),
    ],
)
def test_read_file(
    mock_session,
    start_line,
    end_line,
    start_column,
    end_column,
    expected,
    minor_py_version,
):
    mock_ast_batch = AstBatch(mock_session)
    stmt = mock_ast_batch.bind()

    stmt_cache = {1: stmt}

    LineageNode = DataFrameTraceNode(1, stmt_cache)
    with mock.patch("sys.version_info", (3, minor_py_version)):
        result = LineageNode._read_file(
            filename=str(test_file_path),
            start_line=start_line,
            end_line=end_line,
            start_column=start_column,
            end_column=end_column,
        )
        assert result == expected


@pytest.mark.parametrize(
    "start_line, end_line, start_column, end_column, expected",
    [
        (0, 0, 0, 0, "test_debug_utils.py: line 0"),
        (1000, 1500, 50, 100, "test_debug_utils.py: lines 1000-1500"),
        (42, 42, 1, 20, "test_debug_utils.py: line 42"),
        (20, 25, 15, 15, "test_debug_utils.py: lines 20-25"),
    ],
)
def test_format_source_location(
    start_line, end_line, start_column, end_column, expected
):
    from snowflake.snowpark._internal.ast.utils import __STRING_INTERNING_MAP__

    __STRING_INTERNING_MAP__["test_debug_utils.py"] = 1
    src = proto.SrcPosition()
    src.file = 1
    src.start_line = start_line
    src.end_line = end_line
    src.start_column = start_column
    src.end_column = end_column

    result = _format_source_location(src)
    assert result == expected

    src = None
    assert _format_source_location(src) == ""
