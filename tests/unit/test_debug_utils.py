#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import tempfile
import types
from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.analyzer_utils import cte_statement
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    CteSegment,
    PlanQueryType,
    Query,
    QueryLineInterval,
    SnowflakePlan,
)
from snowflake.snowpark._internal.df_trace import SrcPosition, TraceNode
from snowflake.snowpark._internal.utils import get_plan_from_pre_query_line_number
from snowflake.snowpark._internal.debug_utils import (
    DataFrameTraceNode,
    QueryProfiler,
    _format_source_location,
    _get_clause_trace_node,
    _get_own_trace_nodes,
    _resolve_error_line,
    _select_clause_spans,
    get_python_source_from_sql_error,
)


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
    LineageNode = DataFrameTraceNode(TraceNode(SrcPosition("file.py", 1)))
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
    src = SrcPosition(
        filename="test_debug_utils.py",
        start_line=start_line,
        start_column=start_column,
        end_line=end_line,
        end_column=end_column,
    )

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


def _plan_with_query(sql, intervals, uuid="top", schema_query=None):
    plan = mock.Mock(spec=SnowflakePlan)
    plan.uuid = uuid
    plan.source_plan = None
    plan.children_plan_nodes = []
    plan.schema_query = schema_query
    plan.queries = [Query(sql, query_line_intervals=intervals)]
    plan.df_trace_nodes = None
    plan._last_execution_queries = None
    return plan


def test_resolve_error_line_uses_the_plan_for_its_own_sql():
    plan = _plan_with_query(" SELECT 1\nFROM t", [QueryLineInterval(0, 1, "top")])
    # no executed sql known, the same sql (it is stripped before it is sent), the schema query
    assert _resolve_error_line(plan, 1, 0, None) == (plan, 1)
    assert _resolve_error_line(plan, 1, 0, "SELECT 1\nFROM t") == (plan, 1)
    plan.schema_query = "SELECT 1\nFROM t LIMIT 0"
    assert _resolve_error_line(plan, 1, 0, "SELECT 1\nFROM t LIMIT 0") == (plan, 1)


def test_resolve_error_line_fills_in_placeholders():
    # e.g. session.sql("show ..."): the id of the SHOW query is filled in when it runs
    plan = _plan_with_query(
        "SELECT *\nFROM (SELECT * FROM TABLE(RESULT_SCAN('query_id_place_holder_Ab12')))",
        [QueryLineInterval(0, 1, "top")],
    )
    executed = "SELECT *\nFROM (SELECT * FROM TABLE(RESULT_SCAN('01c83c55-0a1c-4195-000c-a91555b7ea9f')))"
    assert _resolve_error_line(plan, 1, 0, executed) == (plan, 1)
    # another query is still not mapped with these intervals
    assert _resolve_error_line(plan, 1, 0, "SELECT *\nFROM other") is None
    # e.g. a reader: the name of its temp file format is filled in when it runs
    plan = _plan_with_query(
        'SELECT $1\nFROM @s/a.csv( FILE_FORMAT  => \'"DB"."PUBLIC".temp_name_placeholder_x1\')',
        [QueryLineInterval(0, 1, "top")],
    )
    executed = (
        "SELECT $1\nFROM @s/a.csv( FILE_FORMAT  => 'SNOWPARK_TEMP_FILE_FORMAT_J6N7')"
    )
    assert _resolve_error_line(plan, 1, 0, executed) == (plan, 1)


def test_resolve_error_line_skips_unknown_rewritten_sql():
    plan = _plan_with_query("SELECT 1\nFROM t", [QueryLineInterval(0, 1, "top")])
    # e.g. a query of large query breakdown: never map it with these intervals
    assert _resolve_error_line(plan, 1, 0, "CREATE TEMP TABLE x AS SELECT 1") is None
    plan._last_execution_queries = {
        PlanQueryType.QUERIES: [Query("CREATE TEMP TABLE x AS SELECT 1")]
    }
    assert _resolve_error_line(plan, 0, 0, "CREATE TEMP TABLE x AS SELECT 1") is None


def test_resolve_error_line_maps_through_cte_segments():
    definition = "SELECT 1 AS A\nFROM t"
    body = "(\nSELECT A\nFROM CTE\nWHERE B > 0\n)"
    definition_plan = _plan_with_query(definition, [], uuid="definition")
    body_plan = _plan_with_query(body, [], uuid="body")
    prefix = cte_statement([definition], ["CTE"])
    compiled_sql = prefix + body
    start = len("WITH CTE AS (")
    compiled = Query(
        compiled_sql,
        cte_segments=[
            CteSegment(start, start + len(definition), definition_plan),
            CteSegment(len(prefix), len(compiled_sql), body_plan),
        ],
    )
    top = _plan_with_query(
        "(\nSELECT A\nFROM (SELECT 1 AS A\nFROM t)\nWHERE B > 0\n)", []
    )
    top._last_execution_queries = {PlanQueryType.QUERIES: [compiled]}
    lines = compiled_sql.split("\n")

    # "FROM t" is the second line of the definition
    line = next(i for i, text in enumerate(lines) if text.startswith("FROM t"))
    assert _resolve_error_line(top, line, 0, compiled_sql) == (definition_plan, 1)
    # "WHERE B > 0" is the fourth line of the body
    line = next(i for i, text in enumerate(lines) if text.startswith("WHERE"))
    assert _resolve_error_line(top, line, 6, compiled_sql) == (body_plan, 3)
    # a line with the end of the definition and the start of the body: the position decides
    line = next(i for i, text in enumerate(lines) if text.startswith("FROM t"))
    assert _resolve_error_line(top, line, 0, compiled_sql) == (definition_plan, 1)
    assert _resolve_error_line(top, line, len(lines[line]) - 1, compiled_sql) == (
        body_plan,
        0,
    )
    assert _resolve_error_line(top, 0, start, compiled_sql) == (definition_plan, 0)
    # a line past the end is not mapped
    assert _resolve_error_line(top, len(lines) + 3, 0, compiled_sql) is None


def test_resolve_error_line_single_line_query():
    definition = "SELECT 1 AS A FROM t"
    body = "SELECT A FROM CTE WHERE B > 0"
    definition_plan = _plan_with_query(definition, [], uuid="definition")
    body_plan = _plan_with_query(body, [], uuid="body")
    prefix = cte_statement([definition], ["CTE"])
    compiled_sql = prefix + body
    start = len("WITH CTE AS (")
    compiled = Query(
        compiled_sql,
        cte_segments=[
            CteSegment(start, start + len(definition), definition_plan),
            CteSegment(len(prefix), len(compiled_sql), body_plan),
        ],
    )
    top = _plan_with_query("SELECT A FROM (SELECT 1 AS A FROM t) WHERE B > 0", [])
    top._last_execution_queries = {PlanQueryType.QUERIES: [compiled]}
    assert _resolve_error_line(top, 0, compiled_sql.index("FROM t"), compiled_sql) == (
        definition_plan,
        0,
    )
    assert _resolve_error_line(top, 0, compiled_sql.index("WHERE"), compiled_sql) == (
        body_plan,
        0,
    )


def test_python_source_from_sql_error_is_absent_for_unknown_sql():
    plan = _plan_with_query("SELECT 1\nFROM t", [QueryLineInterval(0, 1, "top")])
    message = (
        "SQL compilation error: error line 2 at position 0\ninvalid identifier 'X'"
    )
    assert get_python_source_from_sql_error(plan, message, "SELECT 2\nFROM u") == ""


def _trace_node(line):
    return TraceNode(SrcPosition("f.py", line, 0, line, 0))


def _plan(trace_nodes, children=(), sql="SELECT 1", schema_query=None):
    return mock.MagicMock(
        df_trace_nodes=trace_nodes,
        children_plan_nodes=list(children),
        queries=[mock.MagicMock(sql=sql)],
        schema_query=schema_query,
        _last_execution_queries=None,
    )


def test_get_own_trace_nodes_leaves_out_child_nodes():
    source, child_op, own_op = _trace_node(1), _trace_node(2), _trace_node(3)
    child = _plan([source, child_op])
    plan = _plan([source, child_op, own_op], children=[child])
    assert _get_own_trace_nodes(plan) == [own_op]


def test_get_own_trace_nodes_with_several_children():
    left, right, join = _trace_node(1), _trace_node(2), _trace_node(3)
    plan = _plan([left, right, join], children=[_plan([left]), _plan([right])])
    assert _get_own_trace_nodes(plan) == [join]


def test_get_own_trace_nodes_keeps_nodes_copied_to_select_sql():
    # the nodes of a SelectSQL child (e.g. the RESULT_SCAN of a SHOW query) are copied
    # down from the plan, not inherited from the child
    from snowflake.snowpark._internal.analyzer.select_statement import SelectSQL

    source, op = _trace_node(1), _trace_node(2)
    select_sql = mock.MagicMock(spec=SelectSQL, df_trace_nodes=[op])
    plan = _plan([source, op], children=[select_sql])
    assert _get_own_trace_nodes(plan) == [source, op]


def test_get_own_trace_nodes_falls_back_to_all_nodes():
    # a plan that only re-wraps its child has no operation of its own
    nodes = [_trace_node(1), _trace_node(2)]
    plan = _plan(list(nodes), children=[_plan(list(nodes))])
    assert _get_own_trace_nodes(plan) == nodes
    # a leaf plan keeps all its nodes
    assert _get_own_trace_nodes(_plan(list(nodes))) == nodes


@pytest.mark.parametrize(
    "executed_sql, expected_lines",
    [
        # the failed query is the sql of the plan: only its own operation
        ("SELECT 1", [3]),
        (" SELECT 1\n", [3]),
        # unknown query, or the schema query of the plan: all the operations
        (None, [1, 2, 3]),
        ("SELECT 0", [1, 2, 3]),
        # a query that is not one of the plan: not mapped
        ("SELECT 2", []),
    ],
)
def test_get_python_source_from_sql_error_own_operations(executed_sql, expected_lines):
    source, child_op, own_op = _trace_node(1), _trace_node(2), _trace_node(3)
    matched = _plan([source, child_op, own_op], children=[_plan([source, child_op])])
    top_plan = _plan(
        [source, child_op, own_op], sql="SELECT 1", schema_query="SELECT 0"
    )
    with mock.patch(
        "snowflake.snowpark._internal.utils.get_plan_from_line_numbers",
        return_value=matched,
    ) as get_plan:
        message = get_python_source_from_sql_error(
            top_plan,
            "SQL compilation error: error line 2 at position 4\ninvalid identifier",
            executed_sql,
        )
    assert [
        int(line) for line in re.findall(r"f\.py: line (\d+)", message)
    ] == expected_lines
    if expected_lines:
        get_plan.assert_called_once_with(top_plan, 1)


_FLATTENED_SQL = 'SELECT\n    "A",\n    UPPER(b, a) AS "C"\nFROM (\n    SELECT 1\n)\nWHERE\n    ("A" > 0)'


def _cached_statement(sql, projection_slots=(), where_slots=(), order_slots=()):
    from snowflake.snowpark._internal.analyzer.binary_expression import And
    from snowflake.snowpark._internal.analyzer.expression import UnresolvedAttribute
    from snowflake.snowpark._internal.analyzer.select_statement import SelectStatement

    class CachedStatement(SelectStatement):
        def __getattribute__(self, name):
            if name in {
                "sql_query",
                "analyzer",
                "_generate_sql",
                "projection_in_str",
                "commented_sql",
                "from_",
            }:
                raise AssertionError(f"Diagnostics must not access {name}")
            return super().__getattribute__(name)

    # A real SelectStatement with only the diagnostic contract populated. No session or
    # analyzer is needed to inspect already-cached SQL.
    statement = object.__new__(CachedStatement)
    statement._sql_query = sql
    statement.projection = [UnresolvedAttribute("p") for _ in projection_slots]
    statement.order_by = [UnresolvedAttribute("o") for _ in order_slots]
    predicates = [UnresolvedAttribute("w") for _ in where_slots]
    statement.where = predicates[0] if predicates else None
    for predicate in predicates[1:]:
        statement.where = And(statement.where, predicate)
    statement._projection_nodes = (statement.projection, list(projection_slots))
    statement._order_by_nodes = (statement.order_by, list(order_slots))
    statement._where_nodes = (statement.where, list(zip(predicates, where_slots)))
    return statement


def _flattened_plan(nodes, plan_sql=_FLATTENED_SQL):
    """A plan of a flattened SELECT statement whose projection and WHERE conjunct were written
    by ``nodes`` (projection "A", projection UPPER, WHERE conjunct)."""
    statement = _cached_statement(_FLATTENED_SQL, nodes[:2], [nodes[2]])
    plan = _plan(list(nodes), sql=plan_sql)
    plan.source_plan = statement
    return plan


@pytest.mark.parametrize(
    "line_number, position, expected",
    [
        (1, 4, 0),  # the "A" column
        (2, 10, 1),  # inside UPPER(...)
        (7, 4, 2),  # the WHERE conjunct
        (1, 0, None),  # the indentation before "A" is no part
        (4, 4, None),  # a line of the FROM subquery
        (2, 100, None),  # a position past the end of the line
        (9, 0, None),  # a line past the end of the sql
    ],
)
def test_get_clause_trace_node(line_number, position, expected):
    nodes = [_trace_node(1), _trace_node(2), _trace_node(3)]
    plan = _flattened_plan(nodes)
    lines = _FLATTENED_SQL.split("\n")
    executed_line = lines[line_number] if line_number < len(lines) else ""
    result = _get_clause_trace_node(plan, position, executed_line)
    assert result is (nodes[expected] if expected is not None else None)


@pytest.mark.parametrize(
    "expression",
    [
        '"FROM, WHERE" AS "ORDER BY"',
        'COALESCE(\'a,b FROM\', F(1, 2)) AS "a"",b"',
        "'it''s, WHERE ORDER BY'",
        r"'it\'s, FROM'",
        "$$a,b ' WHERE ( ORDER BY$$",
        "[1, [2, 3], F(4, 5)]",
        "(SELECT x FROM t WHERE x IN (1, 2))",
        "F(a /* , FROM WHERE */ , b) -- , ORDER BY\n",
        "COUNT(*)",
        "a * b",
    ],
)
def test_cached_clause_spans_opaque_expressions(expression):
    first, second = _trace_node(1), _trace_node(2)
    sql = f"SELECT {expression}, bad FROM t"
    statement = _cached_statement(sql, [first, second])
    before = dict(statement.__dict__)
    spans, _, _ = _select_clause_spans(statement)
    assert spans[-1] == (sql.index("bad"), sql.index("bad") + 3, second)
    assert spans[0][2] is first
    assert statement.__dict__ == before
    plan = _plan([first, second], sql=sql)
    plan.source_plan = statement
    line = sql.splitlines()[-1]
    assert _get_clause_trace_node(plan, line.index("bad"), line) is second


def test_cached_clause_spans_conjunctions_and_order_keys():
    nodes = [_trace_node(line) for line in range(1, 7)]
    sql = (
        "SELECT DISTINCT F(a, b) FROM t WHERE "
        "(((a = 'AND') AND (b IN (1, 2))) AND (c > 0)) "
        "ORDER BY F(a, b) DESC NULLS LAST, [c, d] ASC LIMIT 3 OFFSET 1"
    )
    statement = _cached_statement(sql, nodes[:1], nodes[1:4], nodes[4:])
    spans, from_start, from_end = _select_clause_spans(statement)
    assert sql[from_start:from_end] == " t"
    expected = [
        "F(a, b)",
        "F(a, b) DESC NULLS LAST",
        "[c, d] ASC",
        "(a = 'AND')",
        "(b IN (1, 2))",
        "(c > 0)",
    ]
    assert [sql[start:end] for start, end, _ in spans] == expected
    plan = _plan(nodes, sql=sql)
    plan.source_plan = statement
    for text, owner in zip(expected, [nodes[index] for index in [0, 4, 5, 1, 2, 3]]):
        assert _get_clause_trace_node(plan, sql.index(text), sql) is owner
    for text in ["SELECT", "WHERE", "ORDER BY", "LIMIT", "OFFSET"]:
        assert _get_clause_trace_node(plan, sql.index(text), sql) is None


def test_cached_clause_spans_recorded_and_is_atomic():
    from snowflake.snowpark._internal.analyzer.binary_expression import And
    from snowflake.snowpark._internal.analyzer.expression import UnresolvedAttribute

    first, second = _trace_node(1), _trace_node(2)
    sql = "SELECT * FROM t WHERE ((a AND b) AND (c > 0))"
    statement = _cached_statement(sql, [], [first, second])
    atom = And(UnresolvedAttribute("a"), UnresolvedAttribute("b"))
    statement.where.left = atom
    statement._where_nodes[1][0] = (atom, first)
    spans, _, _ = _select_clause_spans(statement)
    assert [(sql[start:end], owner) for start, end, owner in spans] == [
        ("(a AND b)", first),
        ("(c > 0)", second),
    ]


@pytest.mark.parametrize("conflict", [True, False])
def test_cached_clause_spans_reused_predicate(conflict):
    from snowflake.snowpark._internal.analyzer.binary_expression import And

    first, second = _trace_node(1), _trace_node(2)
    statement = _cached_statement("SELECT * FROM t WHERE (a AND a)", [], [first])
    predicate = statement.where
    statement.where = And(predicate, predicate)
    statement._where_nodes = (
        statement.where,
        [
            (predicate, first),
            (predicate, second if conflict else first),
            (predicate, first),
        ],
    )
    spans, _, _ = _select_clause_spans(statement)
    assert [owner for _, _, owner in spans] == ([] if conflict else [first, first])


@pytest.mark.parametrize("slot", [None, object(), "not a trace node"])
def test_cached_clause_spans_unknown_slots(slot):
    statement = _cached_statement(
        "SELECT a FROM t WHERE a ORDER BY a", [slot], [slot], [slot]
    )
    assert _select_clause_spans(statement)[0] == []


@pytest.mark.parametrize("clause", ["projection", "where", "order_by"])
def test_cached_clause_spans_replaced_clause(clause):
    from snowflake.snowpark._internal.analyzer.expression import UnresolvedAttribute

    nodes = [_trace_node(line) for line in range(1, 4)]
    statement = _cached_statement(
        "SELECT a FROM t WHERE a ORDER BY a", nodes[:1], nodes[1:2], nodes[2:]
    )
    replacement = UnresolvedAttribute("a")
    setattr(statement, clause, replacement if clause == "where" else [replacement])
    spans, _, _ = _select_clause_spans(statement)
    replaced_owner = nodes[["projection", "where", "order_by"].index(clause)]
    assert len(spans) == 2
    assert all(owner is not replaced_owner for _, _, owner in spans)


@pytest.mark.parametrize(
    "sql",
    [
        None,
        "",
        "SELECT a",
        "SELECT a FROM",
        "SELECT FROM t",
        "SELECT a, FROM t",
        "SELECT a, b FROM t",
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT a FROM t WHERE a",
        "SELECT a FROM t ORDER BY a",
        "SELECT (a] FROM t",
        "SELECT (a FROM t",
        "SELECT a) FROM t",
        "SELECT 'unclosed FROM t",
        'SELECT "unclosed FROM t',
        "SELECT $$unclosed FROM t",
        "SELECT a /* unclosed FROM t",
        "SELECT a /* nested /* comment */ FROM t",
        "SELECT {a: 1} FROM t",
        "SELECT a FROM t; SELECT b FROM t",
        "SELECT a FROM t GROUP BY a",
        "SELECT a FROM t QUALIFY a = 1",
        "SELECT a FROM t UNION SELECT a FROM u",
        "SELECT TOP 1 a FROM t",
        "SELECT a FROM t LIMIT",
        "SELECT a FROM t LIMIT 1 unexpected",
        "SELECT a FROM t ORDER a",
        "SELECT a FROM t OFFSET 1 LIMIT 2",
        "WITH c AS (SELECT a FROM t) SELECT a FROM c",
    ],
)
def test_cached_clause_spans_rejects_unknown_or_malformed_sql(sql):
    statement = _cached_statement(sql, [_trace_node(1)])
    assert _select_clause_spans(statement) is None


@pytest.mark.parametrize("wrapped", [False, True])
def test_cached_clause_spans_rejects_expanded_star(wrapped):
    from snowflake.snowpark._internal.analyzer.expression import Star
    from snowflake.snowpark._internal.analyzer.unary_expression import UnresolvedAlias

    statement = _cached_statement("SELECT a FROM t", [_trace_node(1)])
    star = Star([])
    statement.projection[0] = UnresolvedAlias(star) if wrapped else star
    assert _select_clause_spans(statement) is None


@pytest.mark.parametrize(
    "where_sql",
    [
        "a AND b",
        "(a OR b)",
        "(a AND b AND c)",
        "(a) AND (b)",
        "(AND b)",
        "(a BETWEEN b AND c)",
        "(a OR b AND c)",
    ],
)
def test_cached_clause_spans_rejects_unknown_and_layout(where_sql):
    statement = _cached_statement(
        f"SELECT * FROM t WHERE {where_sql}", [], [_trace_node(1), _trace_node(2)]
    )
    assert _select_clause_spans(statement) is None


def test_cached_clause_spans_missing_cache_or_unknown_predicate():
    node = _trace_node(1)
    statement = _cached_statement("SELECT * FROM t WHERE a", [], [node])
    del statement._sql_query
    assert _select_clause_spans(statement) is None
    statement._sql_query = "SELECT * FROM t WHERE a"
    statement._where_nodes = (statement.where, [])
    assert _select_clause_spans(statement) is None
    statement._sql_query = "SELECT a FROM t WHERE a"
    assert _select_clause_spans(statement) is None


def test_cached_clause_spans_rejects_order_count_and_slot_count_mismatch():
    statement = _cached_statement(
        "SELECT a FROM t ORDER BY a, b", [_trace_node(1)], [], [_trace_node(2)]
    )
    assert _select_clause_spans(statement) is None
    statement._sql_query = "SELECT a FROM t ORDER BY a"
    statement._projection_nodes[1].append(_trace_node(3))
    assert _select_clause_spans(statement) is None


@pytest.mark.parametrize("inside_from", [False, True])
def test_get_clause_trace_node_ambiguous_lines(inside_from):
    first, second = _trace_node(1), _trace_node(2)
    sql = "SELECT\n a\nFROM t\nORDER BY\n a"
    if inside_from:
        sql = "SELECT\n a\nFROM (SELECT\n a\nFROM t)\nORDER BY b"
    statement = _cached_statement(sql, [first], [], [second])
    plan = _plan([first, second], sql=sql)
    plan.source_plan = statement
    assert _get_clause_trace_node(plan, 1, " a") is None


def test_get_clause_trace_node_cte_style_from_and_leading_whitespace():
    first, second = _trace_node(1), _trace_node(2)
    sql = " SELECT a FROM (SELECT a FROM t) WHERE b"
    statement = _cached_statement(sql, [first], [second])
    plan_sql = " SELECT a FROM CTE_1 WHERE b"
    plan = _plan([first, second], sql=plan_sql)
    plan.source_plan = statement
    executed = plan_sql.lstrip()
    assert _get_clause_trace_node(plan, executed.index("a"), executed) is first
    assert _get_clause_trace_node(plan, executed.index("b"), executed) is second
    assert _get_clause_trace_node(plan, executed.index("CTE_1"), executed) is None
    # A WITH prefix does not share this statement's coordinate system.
    compiled = "WITH CTE_1 AS (SELECT a FROM t) " + executed
    assert _get_clause_trace_node(plan, compiled.index("WHERE") + 6, compiled) is None


def test_get_clause_trace_node_needs_the_executed_line():
    nodes = [_trace_node(1), _trace_node(2), _trace_node(3)]
    plan = _flattened_plan(nodes)
    assert _get_clause_trace_node(plan, 10, "    UPPER(c) AS X") is None


def test_get_clause_trace_node_with_a_differently_laid_out_from_clause():
    # the sql of the plan has the uuid comments of the FROM subquery removed, which may leave
    # other line breaks in it; the parts outside it are the same
    nodes = [_trace_node(1), _trace_node(2), _trace_node(3)]
    plan_sql = _FLATTENED_SQL.replace(
        "FROM (\n    SELECT 1\n)", "FROM (\n\n    SELECT 1\n\n)"
    )
    plan = _flattened_plan(nodes, plan_sql=plan_sql)
    assert _get_clause_trace_node(plan, 4, '    ("A" > 0)') is nodes[2]
    # Parse the plan's cached layout directly, without requiring matching prefixes/suffixes.
    plan = _flattened_plan(nodes, plan_sql=_FLATTENED_SQL.replace('"C"', '"D"'))
    assert _get_clause_trace_node(plan, 10, '    UPPER(b, a) AS "D"') is nodes[1]
    # Missing statement SQL must not trigger SQL generation or hide cached plan SQL.
    plan = _flattened_plan(nodes)
    plan.source_plan._sql_query = None
    assert _get_clause_trace_node(plan, 10, '    UPPER(b, a) AS "C"') is nodes[1]
    # not a SELECT statement
    plan.source_plan = mock.MagicMock()
    assert _get_clause_trace_node(plan, 10, '    UPPER(b, a) AS "C"') is None


@pytest.mark.parametrize("multiline", [False, True])
@pytest.mark.parametrize("missing_cache", ["none", "absent"])
def test_get_clause_trace_node_uses_plan_without_statement_cache(
    multiline, missing_cache
):
    nodes = [_trace_node(line) for line in range(1, 4)]
    separator = "\n" if multiline else " "
    sql = separator.join(
        [" SELECT bad_projection", "FROM t", "WHERE bad_filter", "ORDER BY bad_sort"]
    )
    statement = _cached_statement(None, nodes[:1], nodes[1:2], nodes[2:])
    if missing_cache == "absent":
        del statement._sql_query
    before = dict(statement.__dict__)
    plan = _plan(nodes, sql=sql)
    plan.source_plan = statement
    spans, _, _ = _select_clause_spans(statement, sql)
    assert len(spans) == 3
    for text, node in zip(["bad_projection", "bad_filter", "bad_sort"], nodes):
        line = next(line for line in sql.lstrip().split("\n") if text in line)
        assert _get_clause_trace_node(plan, line.index(text), line) is node
    assert statement.__dict__ == before


@pytest.mark.parametrize("clause", ["projection", "where", "order_by"])
def test_get_clause_trace_node_uses_commented_cache_with_compiled_with(clause):
    nodes = [_trace_node(line) for line in range(1, 4)]
    uuid_comment = "-- 12345678-1234-1234-1234-123456789abc"
    body = (
        "SELECT\n bad_projection\nFROM CTE_1\nWHERE\n bad_filter\nORDER BY\n bad_sort"
    )
    compiled = "WITH CTE_1 AS (SELECT a, b FROM t)\n" + body
    statement = _cached_statement(None, nodes[:1], nodes[1:2], nodes[2:])
    statement._commented_sql = body.replace(
        "CTE_1", f"(\n{uuid_comment}\nSELECT a, b FROM t\n{uuid_comment}\n)"
    )
    before = dict(statement.__dict__)
    plan = _plan_with_query(
        compiled, [QueryLineInterval(0, compiled.count("\n"), "top")]
    )
    plan.source_plan = statement
    plan.df_trace_nodes = nodes
    index = ["projection", "where", "order_by"].index(clause)
    text = ["bad_projection", "bad_filter", "bad_sort"][index]
    line_number = compiled.split("\n").index(" " + text)
    executed_line = compiled.split("\n")[line_number]
    assert _get_clause_trace_node(plan, 1, executed_line) is nodes[index]
    error = f"SQL compilation error: error line {line_number + 1} at position 1\ninvalid identifier"
    assert get_python_source_from_sql_error(plan, error, compiled) == (
        f"\nSQL compilation error corresponds to Python source at f.py: line {index + 1}.\n"
    )
    assert statement._sql_query is None
    assert statement.__dict__ == before


@pytest.mark.parametrize("layout", ["duplicate", "from", "conflict", "unknown"])
def test_commented_cache_ambiguous_layout_keeps_broader_mapping(layout):
    first, second = _trace_node(1), _trace_node(2)
    sql = "SELECT\n bad\nFROM t\nORDER BY good"
    uuid_comment = "-- 12345678-1234-1234-1234-123456789abc"
    layouts = {
        "duplicate": "SELECT\n bad\nFROM t\nORDER BY\n bad",
        "from": "SELECT good\nFROM (SELECT\n bad\nFROM t)\nORDER BY good",
        "conflict": "SELECT good\nFROM t\nORDER BY\n bad",
        "unknown": "SELECT good\nFROM t\nORDER BY\n bad",
    }
    statement = _cached_statement(None, [first], [], [second])
    statement._commented_sql = layouts[layout].replace(
        "FROM t", f"FROM t {uuid_comment}"
    )
    if layout == "duplicate":
        # Duplicate matching lines are ambiguous even when their owners agree.
        statement._order_by_nodes = (statement.order_by, [first])
    elif layout == "unknown":
        statement._order_by_nodes = (statement.order_by, [None])
    before = dict(statement.__dict__)
    plan = _plan_with_query(sql, [QueryLineInterval(0, 4, "top")])
    plan.source_plan = statement
    plan.df_trace_nodes = [first, second]
    assert _get_clause_trace_node(plan, 1, " bad") is None
    message = get_python_source_from_sql_error(
        plan,
        "SQL compilation error: error line 2 at position 1\ninvalid identifier",
        sql,
    )
    # Exact executed-query coordinates disambiguate the plan layout; unrelated
    # cache layouts are only consulted when those coordinates are unavailable.
    assert re.findall(r"f\.py: line (\d+)", message) == ["1"]
    assert statement.__dict__ == before


def test_get_python_source_from_sql_error_reports_inherited_lifted_sort():
    source, sort, select = (_trace_node(line) for line in range(1, 4))
    sql = 'SELECT\n ("A" + 1) AS "A"\nFROM (\n SELECT "A" FROM t\n)\nORDER BY\n "A" ASC NULLS FIRST'
    child = _plan([source, sort], sql='SELECT "A" FROM t')
    plan = _plan_with_query(sql, [QueryLineInterval(0, 6, "top")])
    plan.children_plan_nodes = [child]
    plan.df_trace_nodes = [source, sort, select]
    plan.source_plan = _cached_statement(sql, [select], [], [sort])
    assert _get_own_trace_nodes(plan) == [select]
    assert _get_clause_trace_node(plan, 1, sql.split("\n")[-1]) is sort
    message = get_python_source_from_sql_error(
        plan,
        "SQL compilation error: error line 7 at position 1\ninvalid identifier",
        sql,
    )
    assert message == (
        "\nSQL compilation error corresponds to Python source at f.py: line 2.\n"
    )


def test_get_clause_trace_node_uses_nested_singleline_statement_cache():
    first, second = _trace_node(1), _trace_node(2)
    statement_sql = " SELECT bad FROM (SELECT 1) WHERE predicate"
    plan_sql = statement_sql.replace("(SELECT 1)", "(\nSELECT 1\n)")
    statement = _cached_statement(statement_sql, [first], [second])
    plan = _plan([first, second], sql=plan_sql)
    plan.source_plan = statement
    for text, owner in [("bad", first), ("predicate", second)]:
        assert (
            _get_clause_trace_node(plan, statement_sql.index(text), statement_sql)
            is owner
        )
    statement._sql_query = None
    assert (
        _get_clause_trace_node(plan, statement_sql.index("bad"), statement_sql) is None
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "other_layout", ["agree", "conflict", "from", "duplicate", "unknown", "malformed"]
)
def test_get_clause_trace_node_requires_unambiguous_agreeing_layouts(
    reverse, other_layout
):
    first, second = _trace_node(1), _trace_node(2)
    sql = "SELECT\n bad\nFROM t\nORDER BY good"
    alternatives = {
        "agree": "SELECT\n bad\nFROM (SELECT 1)\nORDER BY good",
        "conflict": "SELECT good\nFROM t\nORDER BY\n bad",
        "from": "SELECT good\nFROM (SELECT\n bad\nFROM t)\nORDER BY good",
        "duplicate": "SELECT\n bad\nFROM t\nORDER BY\n bad",
        "unknown": "SELECT good\nFROM t\nORDER BY good -- comment\n bad",
        "malformed": "SELECT\n bad\nFROM t\nORDER good",
    }
    plan_sql, statement_sql = sql, alternatives[other_layout]
    if reverse:
        plan_sql, statement_sql = statement_sql, plan_sql
    statement = _cached_statement(statement_sql, [first], [], [second])
    if other_layout == "duplicate":
        # Repeated matching lines are ambiguous even if both have the same owner.
        statement._order_by_nodes = (statement.order_by, [first])
    if other_layout == "unknown":
        statement._order_by_nodes = (statement.order_by, [None])
    plan = _plan([first, second], sql=plan_sql)
    plan.source_plan = statement
    assert _get_clause_trace_node(plan, 1, " bad") is (
        first if other_layout == "agree" else None
    )


@pytest.mark.parametrize("statement_cached", [False, True])
def test_get_python_source_from_sql_error_flattened_statement(statement_cached):
    nodes = [_trace_node(1), _trace_node(2), _trace_node(3)]
    plan = _flattened_plan(nodes)
    if not statement_cached:
        plan.source_plan._sql_query = None
    message = "SQL compilation error: error line 3 at position 10\ntoo many arguments"
    with mock.patch(
        "snowflake.snowpark._internal.utils.get_plan_from_line_numbers",
        return_value=plan,
    ):
        assert get_python_source_from_sql_error(plan, message, _FLATTENED_SQL) == (
            "\nSQL compilation error corresponds to Python source at f.py: line 2.\n"
        )
        # the operation is not one of the plan's: all of them are reported
        plan.df_trace_nodes = [nodes[0], nodes[2]]
        assert "line 2" not in get_python_source_from_sql_error(
            plan, message, _FLATTENED_SQL
        )
        # the schema query of the plan: no part of the statement is known
        plan.df_trace_nodes = list(nodes)
        plan.schema_query = "SELECT 0"
        assert (
            get_python_source_from_sql_error(plan, message, "SELECT 0").count("f.py")
            == 3
        )


def test_resolve_error_line_maps_a_query_before_the_last_through_its_own_intervals():
    # e.g. session.sql(<non-select>): the statement runs before the RESULT_SCAN query
    owner = _plan_with_query("SELECT 1", [QueryLineInterval(0, 0, "owner")], "owner")
    top = _plan_with_query("SELECT *\nFROM x", [QueryLineInterval(0, 1, "top")])
    top.children_plan_nodes = [owner]
    pre_action_sql = "CREATE TEMP TABLE t AS\nSELECT\nbad"
    pre_action = Query(
        pre_action_sql, query_line_intervals=[QueryLineInterval(0, 2, "owner")]
    )
    last = Query("SELECT *\nFROM x")
    top._last_execution_queries = {PlanQueryType.QUERIES: [pre_action, last]}
    # line 2 only exists in the pre-action
    assert _resolve_error_line(top, 2, 0, pre_action_sql) == (owner, None)
    # the owner of the line is not part of the plan (e.g. large query breakdown)
    pre_action.query_line_intervals = [QueryLineInterval(0, 2, "not_in_plan")]
    assert _resolve_error_line(top, 2, 0, pre_action_sql) is None
    # the last compiled query is never mapped through its own intervals
    top._last_execution_queries = {PlanQueryType.QUERIES: [last, pre_action]}
    assert _resolve_error_line(top, 2, 0, pre_action_sql) is None


def _plan_node(uuid, children=(), df_trace_nodes=None, wrapped_plan=None):
    # Strings stand in for the df trace nodes.
    return types.SimpleNamespace(
        uuid=uuid,
        children_plan_nodes=list(children),
        df_trace_nodes=df_trace_nodes,
        _snowflake_plan=wrapped_plan,
    )


def _pre_query(*intervals):
    query = Query("pre-action")
    query.query_line_intervals = [QueryLineInterval(*i) for i in intervals]
    return query


@pytest.mark.parametrize(
    "line_number, owner_uuid, expected_uuid, expected_trace_nodes",
    [
        # owned by the top-level plan itself
        (0, "top", "top", ["sql", "filter"]),
        # owned by a node several levels down that has its own trace nodes
        (0, "leaf", "leaf", ["leaf"]),
        # owned by a node without trace nodes: inherits the closest ancestor's
        (0, "middle_no_nodes", "middle_no_nodes", ["sql", "filter"]),
        # owned by the snowflake plan wrapped by a selectable (large local relation)
        (0, "wrapped", "wrapper", ["sql", "filter"]),
        # owner not in the tree (e.g. a large query breakdown partition)
        (0, "not_in_tree", None, None),
        # line not in the query's intervals
        (5, "top", None, None),
    ],
)
def test_get_plan_from_pre_query_line_number(
    line_number, owner_uuid, expected_uuid, expected_trace_nodes
):
    leaf = _plan_node("leaf", df_trace_nodes=["leaf"])
    middle_no_nodes = _plan_node("middle_no_nodes", children=[leaf])
    wrapper = _plan_node("wrapper", wrapped_plan=_plan_node("wrapped"))
    top = _plan_node(
        "top", children=[middle_no_nodes, wrapper], df_trace_nodes=["sql", "filter"]
    )
    result = get_plan_from_pre_query_line_number(
        top, _pre_query((0, 3, owner_uuid)), line_number
    )
    if expected_uuid is None:
        assert result is None
    else:
        assert result.uuid == expected_uuid
        assert result.df_trace_nodes == expected_trace_nodes
