#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import tempfile
from unittest import mock

import pytest

from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.debug_utils import (
    DataFrameTraceNode,
    QueryProfiler,
    _format_source_location,
    get_python_source_from_sql_error,
    resolve_failed_query_index,
)
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    BatchInsertQuery,
    PlanQueryType,
    Query,
    QueryLineInterval,
    SnowflakePlan,
)
from snowflake.snowpark._internal.utils import get_plan_from_line_numbers
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto


curr_file_path = os.path.dirname(os.path.abspath(__file__))
test_file_path = os.path.join(
    os.path.dirname(curr_file_path),
    "resources",
    "test_debug_utils_dir",
    "sample_read_file.txt",
)
test_write_file_path = os.path.join(
    os.path.dirname(curr_file_path),
    "resources",
    "test_debug_utils_dir",
    "sample_write_file.txt",
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


@pytest.mark.parametrize(
    "message, expected_output",
    [
        ("Test message for stdout", "Test message for stdout\n"),
        ("", "\n"),
        ("Multi\nline message", "Multi\nline message\n"),
    ],
)
def test_write_output_to_stdout(message, expected_output):
    mock_session = mock.MagicMock()
    profiler = QueryProfiler(mock_session)
    with mock.patch("sys.stdout") as mock_stdout:
        profiler._write_output(message)
        mock_stdout.write.assert_called_once_with(expected_output)


@pytest.mark.parametrize(
    "message, expected_content",
    [
        ("Test message for file", "Test message for file\n"),
        ("", "\n"),
        ("Multi\nline message", "Multi\nline message\n"),
        ("Message with special chars: @#$%", "Message with special chars: @#$%\n"),
    ],
)
def test_write_output_to_file(message, expected_content):
    mock_session = mock.MagicMock()
    open(test_write_file_path, "w").close()
    profiler = QueryProfiler(mock_session, test_write_file_path)
    profiler._write_output(message)
    profiler.close()
    with open(test_write_file_path, encoding="utf-8") as test_file:
        actual_content = test_file.read()
        assert actual_content == expected_content


def test_build_operator_tree():
    mock_session = mock.MagicMock()
    profiler = QueryProfiler(mock_session)
    operators_data = [
        {
            "OPERATOR_ID": 1,
            "OPERATOR_TYPE": "TableScan",
            "INPUT_ROWS": 1000,
            "OUTPUT_ROWS": 1000,
            "ROW_MULTIPLE": 1.0,
            "OVERALL_PERCENTAGE": 50.0,
            "OPERATOR_ATTRIBUTES": "table=test_table",
            "PARENT_OPERATORS": None,
        },
        {
            "OPERATOR_ID": 2,
            "OPERATOR_TYPE": "Filter",
            "INPUT_ROWS": 1000,
            "OUTPUT_ROWS": 500,
            "ROW_MULTIPLE": 0.5,
            "OVERALL_PERCENTAGE": 30.0,
            "OPERATOR_ATTRIBUTES": "condition=col1>10",
            "PARENT_OPERATORS": "[1]",
        },
    ]

    nodes, children, root_nodes = profiler.build_operator_tree(operators_data)
    assert len(nodes) == 2
    assert nodes[1]["id"] == 1
    assert nodes[1]["type"] == "TableScan"
    assert nodes[1]["input_rows"] == 1000
    assert nodes[1]["output_rows"] == 1000
    assert nodes[1]["row_multiple"] == 1.0
    assert nodes[1]["exec_time"] == 50.0
    assert nodes[1]["attributes"] == "table=test_table"

    assert nodes[2]["id"] == 2
    assert nodes[2]["type"] == "Filter"
    assert nodes[2]["input_rows"] == 1000
    assert nodes[2]["output_rows"] == 500

    assert len(children) == 2
    assert children[1] == [2]
    assert children[2] == []

    assert root_nodes == {1}


def test_build_operator_tree_with_default_values():
    mock_session = mock.MagicMock()
    profiler = QueryProfiler(mock_session)
    operators_data = [{}]

    nodes, _, _ = profiler.build_operator_tree(operators_data)

    assert nodes[0]["type"] == "N/A"
    assert nodes[0]["input_rows"] == 0
    assert nodes[0]["output_rows"] == 0
    assert nodes[0]["row_multiple"] == 0
    assert nodes[0]["exec_time"] == 0
    assert nodes[0]["attributes"] == "N/A"


def test_print_operator_tree_single_node():
    mock_session = mock.MagicMock()
    nodes = {
        1: {
            "id": 1,
            "type": "TableScan",
            "input_rows": 1000,
            "output_rows": 1000,
            "row_multiple": 1.0,
            "exec_time": 100.0,
        }
    }
    children = {1: []}

    # Create temporary file and get its path
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.print_operator_tree(nodes, children, 1)
        profiler.close()

        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            expected_content = (
                "└── [1] TableScan (In: 1,000, Out: 1,000, Mult: 1.00, Time: 100.00%)\n"
            )
            assert content == expected_content
    finally:
        os.unlink(temp_path)


def test_print_operator_tree_with_children():
    mock_session = mock.MagicMock()
    nodes = {
        1: {
            "id": 1,
            "type": "TableScan",
            "input_rows": 1000,
            "output_rows": 1000,
            "row_multiple": 1.0,
            "exec_time": 50.0,
        },
        2: {
            "id": 2,
            "type": "Filter",
            "input_rows": 1000,
            "output_rows": 500,
            "row_multiple": 0.5,
            "exec_time": 30.0,
        },
    }
    children = {1: [2], 2: []}

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.print_operator_tree(nodes, children, 1)
        profiler.close()

        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            expected_content = (
                "└── [1] TableScan (In: 1,000, Out: 1,000, Mult: 1.00, Time: 50.00%)\n"
                "    └── [2] Filter (In: 1,000, Out: 500, Mult: 0.50, Time: 30.00%)\n"
            )
            assert content == expected_content
    finally:
        os.unlink(temp_path)


def test_print_operator_tree_multiple_children():
    """Test printing a tree with multiple children."""
    mock_session = mock.MagicMock()
    nodes = {
        1: {
            "id": 1,
            "type": "Join",
            "input_rows": 2000,
            "output_rows": 1500,
            "row_multiple": 0.75,
            "exec_time": 60.0,
        },
        2: {
            "id": 2,
            "type": "TableScan",
            "input_rows": 1000,
            "output_rows": 1000,
            "row_multiple": 1.0,
            "exec_time": 20.0,
        },
        3: {
            "id": 3,
            "type": "TableScan",
            "input_rows": 1000,
            "output_rows": 1000,
            "row_multiple": 1.0,
            "exec_time": 20.0,
        },
    }
    children = {1: [2, 3], 2: [], 3: []}

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.print_operator_tree(nodes, children, 1)
        profiler.close()

        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            expected_content = (
                "└── [1] Join (In: 2,000, Out: 1,500, Mult: 0.75, Time: 60.00%)\n"
                "    ├── [2] TableScan (In: 1,000, Out: 1,000, Mult: 1.00, Time: 20.00%)\n"
                "    └── [3] TableScan (In: 1,000, Out: 1,000, Mult: 1.00, Time: 20.00%)\n"
            )
            assert content == expected_content
    finally:
        os.unlink(temp_path)


def test_print_operator_tree_three_levels():
    mock_session = mock.MagicMock()
    nodes = {
        1: {
            "id": 1,
            "type": "Join",
            "input_rows": 3000,
            "output_rows": 2000,
            "row_multiple": 0.67,
            "exec_time": 40.0,
        },
        2: {
            "id": 2,
            "type": "TableScan",
            "input_rows": 1000,
            "output_rows": 1000,
            "row_multiple": 1.0,
            "exec_time": 15.0,
        },
        3: {
            "id": 3,
            "type": "Filter",
            "input_rows": 2000,
            "output_rows": 1500,
            "row_multiple": 0.75,
            "exec_time": 25.0,
        },
        4: {
            "id": 4,
            "type": "TableScan",
            "input_rows": 2000,
            "output_rows": 2000,
            "row_multiple": 1.0,
            "exec_time": 20.0,
        },
        5: {
            "id": 5,
            "type": "Sort",
            "input_rows": 1500,
            "output_rows": 1500,
            "row_multiple": 1.0,
            "exec_time": 10.0,
        },
    }
    children = {1: [2, 3], 2: [], 3: [4], 4: [5], 5: []}

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.print_operator_tree(nodes, children, 1)
        profiler.close()

        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            expected_content = (
                "└── [1] Join (In: 3,000, Out: 2,000, Mult: 0.67, Time: 40.00%)\n"
                "    ├── [2] TableScan (In: 1,000, Out: 1,000, Mult: 1.00, Time: 15.00%)\n"
                "    └── [3] Filter (In: 2,000, Out: 1,500, Mult: 0.75, Time: 25.00%)\n"
                "        └── [4] TableScan (In: 2,000, Out: 2,000, Mult: 1.00, Time: 20.00%)\n"
                "            └── [5] Sort (In: 1,500, Out: 1,500, Mult: 1.00, Time: 10.00%)\n"
            )
            assert content == expected_content
    finally:
        os.unlink(temp_path)


@pytest.mark.parametrize(
    "prefix, is_last, expected_content",
    [
        (
            "  ",
            False,
            "  ├── [1] Filter (In: 500, Out: 250, Mult: 0.50, Time: 25.00%)\n",
        ),
        (
            "    ",
            True,
            "    └── [1] Filter (In: 500, Out: 250, Mult: 0.50, Time: 25.00%)\n",
        ),
        ("", False, "├── [1] Filter (In: 500, Out: 250, Mult: 0.50, Time: 25.00%)\n"),
        ("", True, "└── [1] Filter (In: 500, Out: 250, Mult: 0.50, Time: 25.00%)\n"),
    ],
)
def test_print_operator_tree_with_prefix(prefix, is_last, expected_content):
    mock_session = mock.MagicMock()
    nodes = {
        1: {
            "id": 1,
            "type": "Filter",
            "input_rows": 500,
            "output_rows": 250,
            "row_multiple": 0.5,
            "exec_time": 25.0,
        }
    }
    children = {1: []}

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.print_operator_tree(nodes, children, 1, prefix=prefix, is_last=is_last)
        profiler.close()

        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            assert content == expected_content
    finally:
        os.unlink(temp_path)


def test_print_operator_tree_to_stdout():
    mock_session = mock.MagicMock()
    # Create profiler without file path to write to stdout
    profiler = QueryProfiler(mock_session)
    nodes = {
        1: {
            "id": 1,
            "type": "TableScan",
            "input_rows": 100,
            "output_rows": 100,
            "row_multiple": 1.0,
            "exec_time": 10.0,
        }
    }
    children = {1: []}

    with mock.patch("sys.stdout") as mock_stdout:
        profiler.print_operator_tree(nodes, children, 1)
        expected_output = (
            "└── [1] TableScan (In: 100, Out: 100, Mult: 1.00, Time: 10.00%)\n"
        )
        mock_stdout.write.assert_called_once_with(expected_output)


def test_profile_query_with_mock_data():
    mock_session = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_session._conn._conn.cursor.return_value = mock_cursor

    # Mock the query info result (for fetchone)
    mock_cursor.fetchone.return_value = (
        "SELECT * FROM test",
        5000,
        "2023-01-01 10:00:00",
        "2023-01-01 10:00:05",
    )

    # Mock the operator stats result (for fetchall)
    mock_cursor.fetchall.return_value = [
        (1, "TableScan", "table=test_table", 1000, 1000, 1.0, 50.0),
        (2, "Filter", "condition=col1>10", 1000, 500, 0.5, 30.0),
    ]
    mock_cursor.description = [
        ("OPERATOR_ID",),
        ("OPERATOR_TYPE",),
        ("OPERATOR_ATTRIBUTES",),
        ("INPUT_ROWS",),
        ("OUTPUT_ROWS",),
        ("ROW_MULTIPLE",),
        ("OVERALL_PERCENTAGE",),
    ]

    query_id = "test_query_123"
    profiler = QueryProfiler(mock_session)

    with mock.patch("sys.stdout") as mock_stdout:
        profiler.profile_query(query_id)
        expected_info_query = f"""
            SELECT
                query_text,
                total_elapsed_time,
                start_time,
                end_time
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE query_id = '{query_id}'
            LIMIT 1
        """
        expected_stats_query = f"""
            SELECT
                operator_id,
                operator_type,
                operator_attributes,
                operator_statistics:input_rows::number as input_rows,
                operator_statistics:output_rows::number as output_rows,
                CASE
                    WHEN operator_statistics:input_rows::number > 0
                    THEN operator_statistics:output_rows::number / operator_statistics:input_rows::number
                    ELSE NULL
                END as row_multiple,
                execution_time_breakdown:overall_percentage::number as overall_percentage
            FROM TABLE(get_query_operator_stats('{query_id}'))
            ORDER BY step_id, operator_id
            """
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        assert calls[0][0][0] == expected_info_query
        assert calls[1][0][0] == expected_stats_query
        assert mock_stdout.write.call_count > 0
        output_calls = [call.args[0] for call in mock_stdout.write.call_args_list]
        header_found = any(
            "=== Analyzing Query test_query_123 ===" in call for call in output_calls
        )
        assert header_found


def test_profile_query_output_to_file():
    """Test profile_query function writing output to a file."""
    mock_session = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_session._conn._conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (
        "SELECT * FROM test",
        3000,
        "2023-01-01 09:00:00",
        "2023-01-01 09:00:03",
    )
    mock_cursor.fetchall.return_value = [
        (1, "TableScan", "table=test_table", 1000, 1000, 1.0, 50.0),
    ]
    mock_cursor.description = [
        ("OPERATOR_ID",),
        ("OPERATOR_TYPE",),
        ("OPERATOR_ATTRIBUTES",),
        ("INPUT_ROWS",),
        ("OUTPUT_ROWS",),
        ("ROW_MULTIPLE",),
        ("OVERALL_PERCENTAGE",),
    ]

    query_id = "test_query_456"

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.profile_query(query_id)
        profiler.close()
        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            assert "=== Analyzing Query test_query_456 ===" in content
            assert "QUERY OPERATOR TREE" in content
            assert "DETAILED OPERATOR STATISTICS" in content
            assert "TableScan" in content
    finally:
        os.unlink(temp_path)


def test_profile_query_empty_results():
    """Test profile_query function with empty database results."""
    mock_session = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_session._conn._conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (
        "SELECT * FROM empty",
        1000,
        "2023-01-01 08:00:00",
        "2023-01-01 08:00:01",
    )
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [
        ("OPERATOR_ID",),
        ("OPERATOR_TYPE",),
        ("OPERATOR_ATTRIBUTES",),
        ("INPUT_ROWS",),
        ("OUTPUT_ROWS",),
        ("ROW_MULTIPLE",),
        ("OVERALL_PERCENTAGE",),
    ]

    query_id = "empty_query_789"
    profiler = QueryProfiler(mock_session)
    with mock.patch("sys.stdout") as mock_stdout:
        profiler.profile_query(query_id)
        output_calls = [call.args[0] for call in mock_stdout.write.call_args_list]
        header_found = any(
            "=== Analyzing Query empty_query_789 ===" in call for call in output_calls
        )
        assert header_found
        tree_header_found = any("QUERY OPERATOR TREE" in call for call in output_calls)
        assert not tree_header_found


def test_profile_query_with_complex_attributes():
    """Test profile_query function with complex operator attributes containing newlines."""
    mock_session = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_session._conn._conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (
        "SELECT * FROM complex",
        2000,
        "2023-01-01 07:00:00",
        "2023-01-01 07:00:02",
    )
    complex_attrs = "join_condition=table1.id=table2.id\ntype=inner\nrows=1000"
    mock_cursor.fetchall.return_value = [
        (1, "Join", complex_attrs, 2000, 1500, 0.75, 40.0),
    ]
    mock_cursor.description = [
        ("OPERATOR_ID",),
        ("OPERATOR_TYPE",),
        ("OPERATOR_ATTRIBUTES",),
        ("INPUT_ROWS",),
        ("OUTPUT_ROWS",),
        ("ROW_MULTIPLE",),
        ("OVERALL_PERCENTAGE",),
    ]

    query_id = "complex_query_202"

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt", encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name

    try:
        profiler = QueryProfiler(mock_session, temp_path)
        profiler.profile_query(query_id)
        profiler.close()
        with open(temp_path, encoding="utf-8") as test_file:
            content = test_file.read()
            lines = content.split("\n")
            detail_section = False
            for line in lines:
                if "DETAILED OPERATOR STATISTICS" in line:
                    detail_section = True
                elif detail_section and "Join" in line:
                    assert (
                        "join_condition=table1.id=table2.id type=inner rows=1000"
                        in line
                    )
                    break
    finally:
        os.unlink(temp_path)


def _error_with_failed_query(failed_query=None, is_last=None):
    error = Exception("fake error")
    if failed_query is not None:
        error._snowpark_failed_query = failed_query
        error._snowpark_failed_query_is_last = is_last
    return error


def test_resolve_failed_query_index():
    queries = [Query("pre 0"), Query("pre 1"), Query("main")]
    top_plan = mock.Mock()
    top_plan.queries = queries

    # Untagged errors, and errors from the last query, keep using the last query.
    assert resolve_failed_query_index(top_plan, Exception("x")) == -1
    assert (
        resolve_failed_query_index(top_plan, _error_with_failed_query(queries[2], True))
        == -1
    )
    # An earlier query is found by equality, also when it is a compiled copy.
    assert (
        resolve_failed_query_index(
            top_plan, _error_with_failed_query(queries[0], False)
        )
        == 0
    )
    copied = Query("pre 1", query_id_place_holder=queries[1].query_id_place_holder)
    assert (
        resolve_failed_query_index(top_plan, _error_with_failed_query(copied, False))
        == 1
    )
    # A query that is not part of the plan (e.g. rewritten by the plan compiler).
    assert (
        resolve_failed_query_index(
            top_plan, _error_with_failed_query(Query("rewritten"), False)
        )
        is None
    )


def test_get_python_source_from_sql_error_query_index():
    error_msg = "SQL compilation error: error line 3 at position 0\ninvalid identifier"
    top_plan = mock.Mock()
    with mock.patch(
        "snowflake.snowpark._internal.utils.get_plan_from_line_numbers"
    ) as mocked, mock.patch(
        "snowflake.snowpark._internal.debug_utils._extract_source_locations_from_plan",
        return_value=["file.py: line 7"],
    ):
        # Unknown failing query: no mapping is attempted.
        assert get_python_source_from_sql_error(top_plan, error_msg, None) == ""
        mocked.assert_not_called()

        # Default maps against the last query, as before.
        assert "file.py: line 7" in get_python_source_from_sql_error(
            top_plan, error_msg
        )
        mocked.assert_called_with(top_plan, 2, -1)

        assert "file.py: line 7" in get_python_source_from_sql_error(
            top_plan, error_msg, 0
        )
        mocked.assert_called_with(top_plan, 2, 0)


def _make_mock_plan(plan_uuid, queries_intervals, children=(), df_ast_ids=None):
    """Build a mock SnowflakePlan whose i-th query has the i-th list of intervals."""
    plan = mock.Mock(spec=SnowflakePlan)
    plan.uuid = plan_uuid
    plan.children_plan_nodes = list(children)
    plan.queries = []
    for intervals in queries_intervals:
        query = mock.Mock()
        query.query_line_intervals = intervals
        plan.queries.append(query)
    plan.df_ast_ids = df_ast_ids
    return plan


def test_get_plan_from_line_numbers_query_index():
    # The top-level plan runs a pre-action (owned by a grandchild) before its main query.
    pre_action_owner = _make_mock_plan(
        "owner", [[QueryLineInterval(0, 2, "owner")]], df_ast_ids=[1]
    )
    middle = _make_mock_plan(
        "middle",
        [[QueryLineInterval(0, 0, "middle"), QueryLineInterval(1, 3, "owner")]],
        children=[pre_action_owner],
    )
    top = _make_mock_plan(
        "top",
        [
            [
                QueryLineInterval(0, 9, "owner")
            ],  # pre-action: whole query owned by "owner"
            [QueryLineInterval(0, 1, "top"), QueryLineInterval(2, 5, "middle")],
        ],
        children=[middle],
        df_ast_ids=[1, 2],
    )

    # Default reads the last query; this is the behavior before the query_index param.
    assert get_plan_from_line_numbers(top, 0).uuid == "top"
    assert get_plan_from_line_numbers(top, 2).uuid == "middle"
    assert get_plan_from_line_numbers(top, 0, -1).uuid == "top"
    # A non-negative index of the last query behaves like the default.
    assert get_plan_from_line_numbers(top, 2, 1).uuid == "middle"
    # Line 9 only exists in the pre-action; the last query can't resolve it.
    with pytest.raises(ValueError, match="does not fall within any interval"):
        get_plan_from_line_numbers(top, 9)

    # query_index=0 reads the pre-action and finds its owner even though it is not a
    # direct child of the top-level plan.
    result = get_plan_from_line_numbers(top, 9, 0)
    assert result is pre_action_owner
    assert result.df_ast_ids == [1]
    assert get_plan_from_line_numbers(top, 0, 0) is pre_action_owner
    assert get_plan_from_line_numbers(top, 0, -2) is pre_action_owner


def test_get_plan_from_line_numbers_query_index_inherits_ast_ids():
    # The owner of the pre-action has no df ast ids; it inherits its closest ancestor's.
    owner = _make_mock_plan("owner", [[QueryLineInterval(0, 0, "owner")]])
    middle = _make_mock_plan(
        "middle", [[QueryLineInterval(0, 0, "owner")]], [owner], df_ast_ids=[7]
    )
    top = _make_mock_plan(
        "top",
        [[QueryLineInterval(0, 0, "owner")], [QueryLineInterval(0, 0, "middle")]],
        [middle],
        df_ast_ids=[7, 8],
    )
    result = get_plan_from_line_numbers(top, 0, 0)
    assert result is owner
    assert result.df_ast_ids == [7]


def test_get_plan_from_line_numbers_query_index_errors():
    top = _make_mock_plan(
        "top",
        [[QueryLineInterval(0, 3, "unknown")], [QueryLineInterval(0, 3, "top")]],
    )
    # Index out of range.
    with pytest.raises(ValueError, match="out of range"):
        get_plan_from_line_numbers(top, 0, 2)
    with pytest.raises(ValueError, match="out of range"):
        get_plan_from_line_numbers(top, 0, -3)
    # Line not in the selected query.
    with pytest.raises(ValueError, match="does not fall within any interval"):
        get_plan_from_line_numbers(top, 4, 0)
    # The owner of the interval is not in the plan tree.
    with pytest.raises(ValueError, match="does not fall within any interval"):
        get_plan_from_line_numbers(top, 0, 0)


def test_get_plan_from_line_numbers_children_use_last_query():
    # query_index applies to the top-level plan only: a child with several queries is
    # still searched through its last query.
    child = _make_mock_plan(
        "child",
        [[QueryLineInterval(0, 5, "wrong")], [QueryLineInterval(0, 5, "child")]],
    )
    top = _make_mock_plan(
        "top",
        [[QueryLineInterval(0, 0, "top")], [QueryLineInterval(0, 5, "child")]],
        [child],
    )
    assert get_plan_from_line_numbers(top, 3).uuid == "child"
    assert get_plan_from_line_numbers(top, 0, 0).uuid == "top"


def _fake_plan_for_execute(mock_server_connection, queries):
    fake_session = mock.create_autospec(Session)
    fake_session._collect_snowflake_plan_telemetry_at_critical_path = False
    fake_session._last_canceled_id = 0
    fake_session._conn = mock_server_connection
    fake_session._join_alias_fix = False
    return SnowflakePlan(
        queries=queries, schema_query="fake schema query", session=fake_session
    )


@pytest.mark.parametrize("failing_index", [0, 1, 2])
def test_execute_queries_tags_failing_query(mock_server_connection, failing_index):
    queries = [Query("q0"), Query("q1"), Query("q2")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    post_action = Query("drop q")

    def run_query(sql, *args, **kwargs):
        if sql == f"q{failing_index}":
            raise ProgrammingError(msg="fake error")
        return {"sfqid": f"id_{sql}"}

    with mock.patch.object(
        mock_server_connection, "run_query", side_effect=run_query
    ) as mocked_run_query, mock.patch(
        "snowflake.snowpark._internal.server_connection.get_new_description"
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {
                    PlanQueryType.QUERIES: queries,
                    PlanQueryType.POST_ACTIONS: [post_action],
                },
                action_id=1,
            )
    assert exc_info.value._snowpark_failed_query is queries[failing_index]
    assert exc_info.value._snowpark_failed_query_is_last is (failing_index == 2)
    # post actions still run after the failure
    assert mocked_run_query.call_args_list[-1].args[0] == "drop q"


def test_execute_queries_tags_failing_batch_insert(mock_server_connection):
    queries = [Query("create"), BatchInsertQuery("insert", []), Query("select")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    with mock.patch.object(
        mock_server_connection, "run_query", return_value={"sfqid": "id"}
    ), mock.patch.object(
        mock_server_connection,
        "run_batch_insert",
        side_effect=ProgrammingError(msg="fake error"),
    ), mock.patch(
        "snowflake.snowpark._internal.server_connection.get_new_description"
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {PlanQueryType.QUERIES: queries, PlanQueryType.POST_ACTIONS: []},
                action_id=1,
            )
    assert exc_info.value._snowpark_failed_query is queries[1]
    assert exc_info.value._snowpark_failed_query_is_last is False


def test_execute_queries_does_not_tag_async_multi_statement(mock_server_connection):
    # All queries are sent as one multi-statement request, so the failing one is unknown.
    queries = [Query("q0"), Query("q1")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    with mock.patch.object(
        mock_server_connection,
        "run_query",
        side_effect=ProgrammingError(msg="fake error"),
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {PlanQueryType.QUERIES: queries, PlanQueryType.POST_ACTIONS: []},
                action_id=1,
                block=False,
            )
    assert not hasattr(exc_info.value, "_snowpark_failed_query")
