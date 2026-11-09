#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
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
    _resolve_error_line,
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
