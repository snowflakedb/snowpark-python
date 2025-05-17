#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.debug_utils import DataFrameTraceNode


curr_file_path = os.path.dirname(os.path.abspath(__file__))
test_file_path = os.path.join(
    os.path.dirname(curr_file_path),
    "resources",
    "test_df_debug_dir",
    "sample_read_file.txt",
)


@pytest.mark.parametrize(
    "start_line, end_line, start_column, end_column, expected",
    [
        (1, 2, 2, 5, "34567890\n23456"),
        (1, 0, 1, 1, "1234567890"),
        (2, 4, 0, 20, "2345678901\n3456789012\n4567890123"),
        (1, 1, 0, 5, "12345"),
    ],
)
def test_read_file(
    mock_session, start_line, end_line, start_column, end_column, expected
):
    mock_ast_batch = AstBatch(mock_session)
    stmt = mock_ast_batch.bind()

    stmt_cache = {1: stmt}

    LineageNode = DataFrameTraceNode(1, stmt_cache)
    result = LineageNode._read_file(
        filename=str(test_file_path),
        start_line=start_line,
        end_line=end_line,
        start_column=start_column,
        end_column=end_column,
    )
    assert result == expected
