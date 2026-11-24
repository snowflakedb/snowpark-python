#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import os
import re
import tempfile
import types
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
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
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    SaveMode,
    SnowflakeCreateTable,
    TableCreationSource,
    WithQueryBlock,
)
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    get_snowflake_plan_queries,
    resolve_and_update_snowflake_plan,
)
from snowflake.snowpark._internal import debug_utils
from snowflake.snowpark._internal.df_trace import SrcPosition, TraceNode
from snowflake.snowpark._internal.utils import get_plan_from_pre_query_line_number
from snowflake.snowpark._internal.debug_utils import (
    DataFrameTraceNode,
    QueryProfiler,
    _format_source_location,
    _get_cached_plan_from_line_number,
    _get_own_trace_nodes,
    _resolve_error_line,
    get_line_less_error_context,
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
    assert _resolve_error_line(plan, 1, 0, None) == (plan, 1, False)
    assert _resolve_error_line(plan, 1, 0, "SELECT 1\nFROM t") == (plan, 1, False)
    plan.schema_query = "SELECT 1\nFROM t LIMIT 0"
    assert _resolve_error_line(plan, 1, 0, "SELECT 1\nFROM t LIMIT 0") == (
        plan,
        1,
        False,
    )


def test_resolve_error_line_fills_in_placeholders():
    # e.g. session.sql("show ..."): the id of the SHOW query is filled in when it runs
    plan = _plan_with_query(
        "SELECT *\nFROM (SELECT * FROM TABLE(RESULT_SCAN('query_id_place_holder_Ab12')))",
        [QueryLineInterval(0, 1, "top")],
    )
    executed = "SELECT *\nFROM (SELECT * FROM TABLE(RESULT_SCAN('01c83c55-0a1c-4195-000c-a91555b7ea9f')))"
    assert _resolve_error_line(plan, 1, 0, executed) == (plan, 1, False)
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
    assert _resolve_error_line(plan, 1, 0, executed) == (plan, 1, False)


@pytest.mark.parametrize("cached_only", [False, True])
@pytest.mark.parametrize("decoy_first", [False, True])
def test_error_line_routing_uses_only_the_matched_segment(
    cached_only, decoy_first, monkeypatch
):
    owner = _plan_with_query("SELECT bad", [QueryLineInterval(0, 0, "owner")], "owner")
    owner.df_trace_nodes = [_trace_node(10)]
    top = _plan_with_query("SELECT original", [])
    query = Query(
        "SELECT bad",
        cte_segments=[CteSegment(0, len("SELECT bad"), owner, cached_only=cached_only)],
    )
    decoy = Query(
        "SELECT unrelated",
        cte_segments=[
            CteSegment(0, len("SELECT unrelated"), owner, cached_only=not cached_only)
        ],
    )
    top._last_execution_queries = {
        PlanQueryType.QUERIES: [decoy, query] if decoy_first else [query, decoy]
    }
    assert _resolve_error_line(top, 0, 7, query.sql) == (owner, 0, cached_only)
    cached = mock.Mock(wraps=_get_cached_plan_from_line_number)
    legacy = mock.Mock(return_value=owner)
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_utils._get_cached_plan_from_line_number",
        cached,
    )
    monkeypatch.setattr(
        "snowflake.snowpark._internal.utils.get_plan_from_line_numbers", legacy
    )
    message = get_python_source_from_sql_error(
        top, "SQL compilation error: error line 1 at position 7", query.sql
    )
    assert re.findall(r"f\.py: line (\d+)", message) == ["10"]
    selected, unselected = (cached, legacy) if cached_only else (legacy, cached)
    selected.assert_called_once_with(owner, 0)
    unselected.assert_not_called()


def test_resolve_error_line_skips_unknown_rewritten_sql():
    plan = _plan_with_query("SELECT 1\nFROM t", [QueryLineInterval(0, 1, "top")])
    # e.g. a query of large query breakdown: never map it with these intervals
    assert _resolve_error_line(plan, 1, 0, "CREATE TEMP TABLE x AS SELECT 1") is None
    plan._last_execution_queries = {
        PlanQueryType.QUERIES: [Query("CREATE TEMP TABLE x AS SELECT 1")]
    }
    assert _resolve_error_line(plan, 0, 0, "CREATE TEMP TABLE x AS SELECT 1") is None


@pytest.mark.parametrize("trace_enabled", [False, True])
def test_reresolved_query_intervals_keep_the_existing_owner(
    mock_session, monkeypatch, trace_enabled
):
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_config.enabled",
        lambda session, name: trace_enabled,
    )
    from snowflake.snowpark._internal.analyzer.unary_plan_node import Project

    node = SnowflakePlan(
        [Query("SELECT 0")], None, source_plan=Project([], None), session=mock_session
    )
    resolved = SnowflakePlan(
        [Query("SELECT bad\nFROM child")], None, session=mock_session
    )
    intervals = [
        QueryLineInterval(0, 0, resolved.uuid),
        QueryLineInterval(1, 1, "child"),
    ]
    resolved.queries[-1].query_line_intervals = intervals
    original_query = resolved.queries[-1]
    generator = mock.Mock()
    generator.resolve.return_value = resolved
    resolve_and_update_snowflake_plan(node, generator)
    assert resolved.queries[-1] is original_query
    assert original_query.query_line_intervals == intervals
    assert node.queries[-1].sql == original_query.sql
    if trace_enabled:
        assert node.queries[-1] is not original_query
        assert node.queries[-1].query_line_intervals == [
            QueryLineInterval(0, 0, node.uuid),
            QueryLineInterval(1, 1, "child"),
        ]
        node.df_trace_nodes = [_trace_node(42)]
        assert "f.py: line 42" in get_python_source_from_sql_error(
            node,
            "SQL compilation error: error line 1 at position 7",
            node.queries[-1].sql,
        )
    else:
        assert node.queries[-1] is original_query


@pytest.fixture
def offline_compiler_session(mock_server_connection, request):
    # Real Session/analyzer/compiler; only the connector and transaction probe are mocked.
    mock_server_connection._conn.database = "DB"
    mock_server_connection._conn.schema = "SCHEMA"
    mock_server_connection._thread_safe_session_enabled = getattr(
        request, "param", False
    )
    session = Session(mock_server_connection)
    session._query_compilation_stage_enabled = True
    session._cte_optimization_enabled = False
    session._large_query_breakdown_enabled = True
    session._large_query_breakdown_complexity_bounds = (30, 80)
    session.debug.set(trace_sql_errors_to_dataframe=True)
    try:
        with mock.patch(
            "snowflake.snowpark._internal.compiler.large_query_breakdown.is_active_transaction",
            return_value=False,
        ):
            yield session
    finally:
        session._enable_multiline_queries()
        session.close()


@pytest.mark.parametrize(
    "build_multiline, compile_multiline",
    [(False, True), (True, False), (True, True), (False, False)],
)
@pytest.mark.parametrize("toggle_before_parent", [False, True])
@pytest.mark.parametrize("literal_newline", [False, True])
def test_lqb_cached_layout_mapping(
    offline_compiler_session,
    build_multiline,
    compile_multiline,
    literal_newline,
    toggle_before_parent,
):
    session = offline_compiler_session
    (
        session._enable_multiline_queries
        if build_multiline
        else session._disable_multiline_queries
    )()

    def tag(df, line):
        df._plan.df_trace_nodes = [_trace_node(line)]
        df._select_statement.df_trace_nodes = df._plan.df_trace_nodes
        return df

    sql = "select 1 as A, 'first\nlast' as S" if literal_newline else "select 1 as A"
    df = tag(session.sql(sql).filter(col("bad") > 0), 10)
    if toggle_before_parent:
        (
            session._enable_multiline_queries
            if compile_multiline
            else session._disable_multiline_queries
        )()
    for index in range(7):
        df = tag(df.union_all(df.filter(col("A") > index)).sort("A"), 100 + index)
    (
        session._enable_multiline_queries
        if compile_multiline
        else session._disable_multiline_queries
    )()
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    # Actual LQB, not compiler fallback or synthetic partitions.
    assert len(queries) == 6
    query = queries[0]
    assert query.sql.lstrip().startswith("CREATE")
    sql = query.sql.strip()
    offset = sql.index("BAD")
    line = sql.count("\n", 0, offset) + 1
    position = offset - sql.rfind("\n", 0, offset) - 1
    message = get_python_source_from_sql_error(
        df._plan,
        f"SQL compilation error: error line {line} at position {position}\ninvalid identifier 'BAD'",
        query.sql,
    )
    assert query.cte_segments is not None
    if build_multiline and compile_multiline:
        assert re.findall(r"f\.py: line (\d+)", message) == ["10"]
    else:
        # Single-line SQL of the filter is cached in the union/sort SQL with other
        # line breaks than its intervals. Never blame the union/sort for it.
        assert message == ""


@pytest.mark.parametrize("simplifier", [False, True])
@pytest.mark.parametrize("late_error", [False, True])
@pytest.mark.parametrize(
    "build_multiline, compile_multiline",
    [(False, True), (True, False), (True, True), (False, False)],
)
def test_lqb_real_compiler_cached_only_diagnostics(
    offline_compiler_session, simplifier, late_error, build_multiline, compile_multiline
):
    from contextlib import ExitStack
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
        SelectStatement,
        SetStatement,
    )

    session = offline_compiler_session
    session.sql_simplifier_enabled = simplifier
    (
        session._enable_multiline_queries
        if build_multiline
        else session._disable_multiline_queries
    )()

    def tag(df, line):
        df._plan.df_trace_nodes = [_trace_node(line)]
        if df._select_statement is not None:
            df._select_statement.df_trace_nodes = df._plan.df_trace_nodes
        return df

    df = tag(session.sql("SELECT 1 AS A"), 1)
    if not late_error:
        df = tag(df.filter(col("BAD") > 0), 10)
    for index in range(7):
        if late_error and index == 6:
            df = tag(df.filter(col("BAD") > 0), 10)
        df = tag(df.union_all(df.filter(col("A") > index)).sort("A"), 100 + index)
    (
        session._enable_multiline_queries
        if compile_multiline
        else session._disable_multiline_queries
    )()
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) > 1
    bad_index, query = next(
        (index, query) for index, query in enumerate(queries) if '"BAD"' in query.sql
    )
    if simplifier and late_error:
        assert len(queries) == 5 and bad_index == 4
        assert all('"BAD"' not in query.sql for query in queries[:4])
    elif not late_error:
        assert bad_index == 0
    sql = query.sql.strip()
    offset = sql.index('"BAD"')
    error = f"SQL compilation error: error line {sql.count(chr(10), 0, offset) + 1} at position {offset - sql.rfind(chr(10), 0, offset) - 1}"
    with ExitStack() as stack:
        for cls, name in (
            (Selectable, "get_snowflake_plan"),
            (SelectStatement, "_generate_sql"),
            (SetStatement, "_generate_sql"),
            (QueryGenerator, "resolve"),
        ):
            stack.enter_context(
                mock.patch.object(
                    cls,
                    name,
                    side_effect=AssertionError("diagnostics must not regenerate"),
                )
            )
        for cls, name in (
            (SelectStatement, "schema_query"),
            (SetStatement, "schema_query"),
            (SelectStatement, "sql_query"),
            (SetStatement, "sql_query"),
            (SnowflakePlan, "attributes"),
        ):
            stack.enter_context(
                mock.patch.object(
                    cls,
                    name,
                    new_callable=mock.PropertyMock,
                    side_effect=AssertionError("diagnostics must use cached state"),
                )
            )
        message = get_python_source_from_sql_error(df._plan, error, query.sql)
    if simplifier and compile_multiline and (build_multiline or late_error):
        # A late filter is compiled into the last query in multiline mode.
        assert re.findall(r"f\.py: line (\d+)", message) == ["10"]
    else:
        # Non-simplifier set operations have no child UUID intervals, even in
        # multiline mode, and single-line SQL of a child is not at the lines of
        # its interval. Never blame the union/sort for an operand's error.
        assert message == ""


@pytest.mark.parametrize("offline_compiler_session", [True], indirect=True)
@pytest.mark.parametrize("simplifier", [False, True])
@pytest.mark.parametrize("replacement", ["empty", "different_sql", "same_sql"])
def test_cached_only_routing_survives_execution_cache_replacement(
    offline_compiler_session, simplifier, replacement, monkeypatch, request
):
    session = offline_compiler_session
    assert session._conn._thread_safe_session_enabled is True
    session.sql_simplifier_enabled = simplifier
    session._enable_multiline_queries()

    def tag(df, line):
        df._plan.df_trace_nodes = [_trace_node(line)]
        if df._select_statement is not None:
            df._select_statement.df_trace_nodes = df._plan.df_trace_nodes
        return df

    df = tag(session.sql("SELECT 1 AS A").filter(col("BAD") > 0), 10)
    for index in range(7):
        df = tag(df.union_all(df.filter(col("A") > index)).sort("A"), 100 + index)
    top = df._plan
    compiled = top.execution_queries
    queries = compiled[PlanQueryType.QUERIES]
    assert len(queries) > 1
    query = queries[0]
    assert query.cte_segments and query.cte_segments[0].cached_only
    sql = query.sql.strip()
    offset = sql.index('"BAD"')
    error = f"SQL compilation error: error line {sql.count(chr(10), 0, offset) + 1} at position {offset - sql.rfind(chr(10), 0, offset) - 1}"

    other_sql = query.sql if replacement == "same_sql" else "SELECT unrelated"
    other = SnowflakePlan([Query(other_sql)], None, session=session)
    other.df_trace_nodes = [_trace_node(999)]
    replacement_queries = {
        PlanQueryType.QUERIES: []
        if replacement == "empty"
        else [
            Query(
                other_sql,
                cte_segments=[CteSegment(0, len(other_sql), other, cached_only=False)],
            )
        ]
    }
    # Guard actual traversal, not just its result: diagnostics must never lazily
    # resolve compiler nodes, even when precise attribution is unavailable.
    request.getfixturevalue("cached_plan_no_resolution")
    legacy = mock.Mock(side_effect=AssertionError("must keep cached-only routing"))
    monkeypatch.setattr(
        "snowflake.snowpark._internal.utils.get_plan_from_line_numbers", legacy
    )
    cached = mock.Mock(wraps=_get_cached_plan_from_line_number)
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_utils._get_cached_plan_from_line_number",
        cached,
    )
    barrier = Barrier(2, timeout=10)
    snapshots = []

    def resolve_then_pause(*args):
        resolved = _resolve_error_line(*args)
        snapshots.append(resolved)
        barrier.wait()
        barrier.wait()
        return resolved

    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_utils._resolve_error_line",
        resolve_then_pause,
    )

    def replace_execution_cache():
        barrier.wait()
        top._remember_execution_queries(replacement_queries)
        barrier.wait()

    with ThreadPoolExecutor(max_workers=2) as executor:
        diagnostic = executor.submit(get_python_source_from_sql_error, top, error, sql)
        writer = executor.submit(replace_execution_cache)
        writer.result(timeout=15)
        message = diagnostic.result(timeout=15)

    assert top._last_execution_queries is replacement_queries
    assert snapshots[0] is not None
    assert snapshots[0].cached_only is True
    assert snapshots[0].plan is query.cte_segments[0].plan
    cached.assert_called_once_with(snapshots[0].plan, snapshots[0].line_number)
    legacy.assert_not_called()
    if simplifier:
        assert re.findall(r"f\.py: line (\d+)", message) == ["10"]
    else:
        # Missing child ownership on non-simplifier set operations must remain
        # unattributed, not fall back to a union/sort or the other execution.
        assert message == ""


def test_retained_diagnostics_are_invalidated_on_update(offline_compiler_session):
    from snowflake.snowpark._internal.compiler.utils import update_resolvable_node

    session = offline_compiler_session
    original = session.sql("SELECT 1 AS A").filter(col("A") > 0)._select_statement
    copied = copy.deepcopy(original)
    assert copied._sql_error_plan is original._snowflake_plan
    update_resolvable_node(copied, QueryGenerator(session))
    assert copied._sql_error_plan is None


@pytest.mark.parametrize("missing_cache", [False, True])
def test_uncached_set_diagnostics_abstain_on_unproven_layout(
    offline_compiler_session, missing_cache
):
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SetOperand,
        SetStatement,
    )
    from snowflake.snowpark._internal.debug_utils import (
        _get_cached_plan_from_line_number,
    )

    session = offline_compiler_session
    session._enable_multiline_queries()
    left = session.sql("SELECT 1 AS A").filter(col("BAD") > 0)._select_statement
    right = session.sql("SELECT 2 AS A")._select_statement
    node = SetStatement(
        SetOperand(left), SetOperand(right, " UNION ALL "), analyzer=session._analyzer
    )
    sql = node.sql_query
    assert node._snowflake_plan is None
    line = sql.count("\n", 0, sql.index('"BAD"'))
    assert _get_cached_plan_from_line_number(node, line) is not None
    if missing_cache:
        left._snowflake_plan = None
        left._sql_query = None
    else:
        node._sql_query = "unexpected prefix" + sql
    with mock.patch.object(
        SetStatement, "_generate_sql", side_effect=AssertionError("no regeneration")
    ):
        assert _get_cached_plan_from_line_number(node, line) is None


@pytest.fixture
def cached_plan_no_resolution(monkeypatch):
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
        SelectSQL,
        SelectStatement,
        SetStatement,
    )

    guards = []
    for cls, names in (
        (Selectable, ("snowflake_plan",)),
        (SelectSQL, ("sql_query", "schema_query")),
        (SelectStatement, ("sql_query", "schema_query")),
        (SetStatement, ("sql_query", "schema_query")),
        (SnowflakePlan, ("attributes",)),
    ):
        for name in names:
            guard = mock.Mock(
                side_effect=AssertionError("diagnostics must use cached state")
            )
            # A real property only fires on instances. Before Python 3.12,
            # Mock(spec=cls) reads class attributes, which calls a PropertyMock.
            monkeypatch.setattr(cls, name, property(lambda _, g=guard: g()))
            guards.append(guard)
    for cls, name in (
        (Selectable, "get_snowflake_plan"),
        (SelectStatement, "_generate_sql"),
        (SetStatement, "_generate_sql"),
        (QueryGenerator, "resolve"),
    ):
        guard = mock.Mock(side_effect=AssertionError("no diagnostic resolution"))
        monkeypatch.setattr(cls, name, guard)
        guards.append(guard)
    yield
    for guard in guards:
        guard.assert_not_called()


@pytest.fixture
def cached_set_statement(mock_session, cached_plan_no_resolution):
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SelectSQL,
        SetOperand,
        SetStatement,
    )

    left = SelectSQL("SELECT bad\nFROM left_table", analyzer=mock_session._analyzer)
    right = SelectSQL("SELECT bad\nFROM right_table", analyzer=mock_session._analyzer)
    for child, uuid in ((left, "left"), (right, "right")):
        child._snowflake_plan = _plan_with_query(
            child._sql_query, [QueryLineInterval(0, 1, uuid)], uuid=uuid
        )
        child._sql_error_plan = None
    node = SetStatement(
        SetOperand(left),
        SetOperand(right, " UNION ALL "),
        analyzer=mock_session._analyzer,
    )
    node._sql_error_plan = None
    # Seed the actual multiline renderer layout without invoking any lazy property.
    node._sql_query = (
        "(\nSELECT bad\nFROM left_table\n) UNION ALL (\nSELECT bad\nFROM right_table\n)"
    )
    return node, left, right


@pytest.mark.parametrize("cache_attribute", ["_snowflake_plan", "_sql_error_plan"])
@pytest.mark.parametrize("line, operand_index", [(1, 1), (2, 1), (4, 2), (5, 2)])
def test_cached_set_diagnostics_select_exact_operand(
    cached_set_statement, cache_attribute, line, operand_index
):
    node, left, right = cached_set_statement
    expected = cached_set_statement[operand_index]._snowflake_plan
    for child in (left, right):
        cached = child._snowflake_plan
        child._snowflake_plan = None
        setattr(child, cache_attribute, cached)
        # A resolved/retained plan is authoritative, not the selectable's stale SQL.
        child._sql_query = "SELECT stale"
    assert _get_cached_plan_from_line_number(node, line) is expected


@pytest.mark.parametrize("line", [-1, 0, 3, 6, 7])
def test_cached_set_diagnostics_do_not_attribute_wrapper_lines(
    cached_set_statement, line
):
    node, _, _ = cached_set_statement
    assert _get_cached_plan_from_line_number(node, line) is None


@pytest.mark.parametrize("line, operand_index", [(1, 1), (4, 2)])
def test_cached_set_diagnostics_ignore_first_operand_operator(
    cached_set_statement, line, operand_index
):
    # union then except nests the union in a set whose first operand keeps
    # " UNION ", which _generate_sql does not render.
    node, left, right = cached_set_statement
    node.set_operands[0].operator = " UNION "
    assert (
        _get_cached_plan_from_line_number(node, line)
        is (left, right)[operand_index - 1]._snowflake_plan
    )


@pytest.mark.parametrize(
    "invalid_cache",
    [
        "missing_set_sql",
        "missing_child_sql",
        "opening_delimiter",
        "operator",
        "child_sql",
        "closing_delimiter",
        "trailing_sql",
    ],
)
def test_cached_set_diagnostics_validate_entire_layout(
    cached_set_statement, invalid_cache
):
    node, _, right = cached_set_statement
    if invalid_cache == "missing_set_sql":
        node._sql_query = None
    elif invalid_cache == "missing_child_sql":
        right._snowflake_plan = None
        right._sql_query = None
    else:
        old, new = {
            "opening_delimiter": (" UNION ALL (\n", " UNION ALL ( "),
            "operator": (" UNION ALL ", " INTERSECT "),
            "child_sql": ("FROM right_table", "FROM stale_table"),
            "closing_delimiter": ("FROM right_table\n)", "FROM right_table)"),
            "trailing_sql": ("FROM right_table\n)", "FROM right_table\n) trailing"),
        }[invalid_cache]
        node._sql_query = node._sql_query.replace(old, new)
    # Even a valid first operand cannot be attributed if a later operand disagrees.
    assert _get_cached_plan_from_line_number(node, 1) is None


@pytest.mark.parametrize("line", [1, 4])
def test_cached_set_diagnostics_reject_uncached_nonset_operand(
    cached_set_statement, line
):
    node, _, right = cached_set_statement
    right._snowflake_plan = None
    assert _get_cached_plan_from_line_number(node, line) is (
        cached_set_statement[1]._snowflake_plan if line == 1 else None
    )


def test_cached_set_diagnostics_descend_nested_cached_sql(
    cached_set_statement, mock_session
):
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SetOperand,
        SetStatement,
    )

    inner, left, right = cached_set_statement
    outer = SetStatement(
        SetOperand(inner),
        SetOperand(right, " UNION ALL "),
        analyzer=mock_session._analyzer,
    )
    outer._sql_error_plan = None
    outer._sql_query = f"(\n{inner._sql_query}\n) UNION ALL (\n{right._sql_query}\n)"
    assert _get_cached_plan_from_line_number(outer, 2) is left._snowflake_plan
    assert _get_cached_plan_from_line_number(outer, 5) is right._snowflake_plan
    assert _get_cached_plan_from_line_number(outer, 9) is right._snowflake_plan


@pytest.mark.parametrize("cache_attribute", ["_snowflake_plan", "_sql_error_plan"])
def test_cached_set_diagnostics_prefer_cached_plan(
    cached_set_statement, cache_attribute
):
    node, left, _ = cached_set_statement
    expected = left._snowflake_plan
    setattr(node, cache_attribute, expected)
    node._sql_query = None
    if cache_attribute == "_snowflake_plan":
        node._sql_error_plan = _plan_with_query("SELECT stale", [], uuid="stale")
    assert _get_cached_plan_from_line_number(node, 0) is expected


@pytest.mark.parametrize(
    "metadata",
    [
        "no_intervals",
        "outside_interval",
        "unknown_uuid",
        "source_uuid",
        "child_uuid",
        "child_sql_differs",
        "child_sql_shifted",
        "uncached_child",
        "set_operation",
    ],
)
def test_cached_plan_diagnostics_require_known_interval_owner(
    cached_set_statement, metadata
):
    from snowflake.snowpark._internal.analyzer.binary_plan_node import Union

    _, left, right = cached_set_statement
    child = left._snowflake_plan
    plan = _plan_with_query(
        "SELECT parent\nSELECT bad\nFROM left_table",
        [QueryLineInterval(1, 2, child.uuid)],
    )
    plan.children_plan_nodes = [right._snowflake_plan, left]
    left._uuid = child.uuid
    expected = None
    line_number = 2
    if metadata == "no_intervals":
        plan.queries[-1].query_line_intervals = []
    elif metadata == "outside_interval":
        plan.queries[-1].query_line_intervals = [QueryLineInterval(0, 0, plan.uuid)]
    elif metadata == "unknown_uuid":
        plan.queries[-1].query_line_intervals = [QueryLineInterval(1, 2, "missing")]
    elif metadata == "source_uuid":
        plan.source_plan = left
        expected = plan
    elif metadata == "child_uuid":
        # The parent's line 2 must become the child's line 1, not line 2.
        expected = child
    elif metadata == "child_sql_differs":
        # The line is in the child's interval, but the parent's lines 1-2 are
        # not the cached SQL of the child: no attribution.
        plan.queries[-1].sql = "SELECT parent\nSELECT bad\nFROM other_table"
    elif metadata == "child_sql_shifted":
        # The child SQL is verbatim at lines 1-2, but its interval says 0-1.
        plan.queries[-1].query_line_intervals = [QueryLineInterval(0, 1, child.uuid)]
        line_number = 1
    elif metadata == "uncached_child":
        # Descending into a child without cached SQL would need SQL generation:
        # no attribution.
        left._snowflake_plan = None
        left._sql_query = None
    else:
        plan.source_plan = Union(child, right._snowflake_plan, is_all=True)
        plan.queries[-1].query_line_intervals = [QueryLineInterval(1, 2, plan.uuid)]
    assert _get_cached_plan_from_line_number(plan, line_number) is expected


def test_lqb_nonsimplifier_unary_partition_is_precise(offline_compiler_session):
    session = offline_compiler_session
    session.sql_simplifier_enabled = False
    session._enable_multiline_queries()
    df = session.sql("SELECT 1 AS A").filter(col("BAD") > 0)
    df._plan.df_trace_nodes = [_trace_node(10)]
    for index in range(12):
        df = df.filter(col("A") > index).sort("A")
        df._plan.df_trace_nodes = [_trace_node(100 + index)]
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) == 2
    sql = queries[0].sql.strip()
    offset = sql.index('"BAD"')
    line = sql.count("\n", 0, offset) + 1
    position = offset - sql.rfind("\n", 0, offset) - 1
    message = get_python_source_from_sql_error(
        df._plan,
        f"SQL compilation error: error line {line} at position {position}",
        queries[0].sql,
    )
    assert re.findall(r"f\.py: line (\d+)", message) == ["10"]


@pytest.mark.parametrize("clone", [copy.copy, copy.deepcopy])
@pytest.mark.parametrize("cte", [False, True])
@pytest.mark.parametrize("simplifier", [False, True])
def test_plan_copy_preserves_child_intervals(
    offline_compiler_session, clone, cte, simplifier
):
    from snowflake.snowpark._internal.debug_utils import (
        _get_cached_plan_from_line_number,
    )

    session = offline_compiler_session
    session.sql_simplifier_enabled = simplifier
    session._enable_multiline_queries()
    child = session.sql("SELECT 1 AS A").filter(col("BAD") > 0)
    child._plan.df_trace_nodes = [_trace_node(10)]
    if child._select_statement is not None:
        child._select_statement.df_trace_nodes = child._plan.df_trace_nodes
    original = child.sort("A")._plan
    session._cte_optimization_enabled = cte
    intervals = list(original.queries[-1].query_line_intervals)
    copied = clone(original)
    assert original.queries[-1].query_line_intervals == intervals
    assert copied.queries[-1] is not original.queries[-1]
    assert copied.queries[-1].sql == original.queries[-1].sql
    assert copied.queries[-1].params == original.queries[-1].params
    assert len(copied.queries[-1].query_line_intervals) == len(intervals)
    sql = copied.queries[-1].sql
    line = sql.count("\n", 0, sql.index('"BAD"'))
    owner = _get_cached_plan_from_line_number(copied, line)
    assert owner is not None
    assert owner.df_trace_nodes[0].src.start_line == 10
    assert any(
        interval.uuid == copied.uuid
        for interval in copied.queries[-1].query_line_intervals
    )


@pytest.mark.parametrize("clone", [copy.copy, copy.deepcopy])
def test_query_copy_preserves_line_intervals(clone):
    from snowflake.snowpark._internal.analyzer.snowflake_plan import BatchInsertQuery

    intervals = [QueryLineInterval(0, 0, "owner")]
    queries = [
        Query("SELECT 1", query_line_intervals=intervals),
        BatchInsertQuery("INSERT INTO T VALUES (?)"),
    ]
    queries[1].query_line_intervals = intervals
    for query in queries:
        copied = clone(query)
        assert type(copied) is type(query)
        assert copied.sql == query.sql
        assert copied.query_line_intervals == intervals


def test_lqb_retained_projection_layout_maps_to_its_owner(offline_compiler_session):
    from snowflake.snowpark._internal.analyzer.select_statement import SelectStatement

    session = offline_compiler_session
    session._enable_multiline_queries()
    child = session.sql("SELECT 1 AS A")._select_statement
    selectable = SelectStatement(
        projection=[col("A")._expression, col("A").alias("B")._expression],
        from_=child,
        analyzer=session._analyzer,
    )
    # Expressions are cached in single-line mode, but both SQL strings and
    # the SnowflakePlan are first generated after switching back to multiline.
    session._disable_multiline_queries()
    assert "\n" not in selectable.projection_in_str
    session._enable_multiline_queries()
    for copied in (selectable, copy.copy(selectable), copy.deepcopy(selectable)):
        plan = copied.get_snowflake_plan(skip_schema_query=True)
        lines = plan.queries[-1].sql.split("\n")
        # The intervals of the select are counted on its own SQL, so the lines
        # of the retained projection belong to the select.
        projection_line = next(i for i, line in enumerate(lines) if '"B"' in line)
        assert _get_cached_plan_from_line_number(plan, projection_line) is plan
        # The child is rendered verbatim, but has no cached plan to descend into.
        child_line = lines.index("SELECT 1 AS A")
        assert _get_cached_plan_from_line_number(plan, child_line) is None


@pytest.mark.parametrize("build_multiline", [False, True])
@pytest.mark.parametrize("multiline", [False, True])
@pytest.mark.parametrize("scoped", [False, True])
def test_lqb_ctas_tracing_preserves_sql_bytes(
    offline_compiler_session, build_multiline, multiline, scoped
):
    session = offline_compiler_session
    session._use_scoped_temp_objects = scoped
    queries = []
    for trace in (False, True):
        session.debug.set(trace_sql_errors_to_dataframe=trace)
        (
            session._enable_multiline_queries
            if build_multiline
            else session._disable_multiline_queries
        )()
        child = session.sql("SELECT ? AS A", params=[9])._plan
        (
            session._enable_multiline_queries
            if multiline
            else session._disable_multiline_queries
        )()
        plan = SnowflakeCreateTable(
            ["T"],
            None,
            SaveMode.ERROR_IF_EXISTS,
            child,
            table_type="temp",
            creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
        )
        generated = QueryGenerator(session).generate_queries([plan])
        assert len(generated[PlanQueryType.QUERIES]) == 1
        queries.append(generated[PlanQueryType.QUERIES][0])
    assert queries[0].sql.encode() == queries[1].sql.encode()
    assert queries[0].params == queries[1].params == [9]
    assert queries[0].is_ddl_on_temp_object is queries[1].is_ddl_on_temp_object is True
    assert ("SCOPED" in queries[0].sql) == scoped


@pytest.mark.parametrize("multiline", [False, True])
@pytest.mark.parametrize("trace_enabled", [False, True])
@pytest.mark.parametrize("plan_count", [1, 2])
def test_generate_queries_does_not_annotate_input_plans(
    mock_session, monkeypatch, trace_enabled, plan_count, multiline
):
    mock_session._generate_multiline_queries = multiline
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_config.enabled",
        lambda session, name: trace_enabled,
    )
    plans = [
        SnowflakePlan(
            [Query(f"SELECT {index}")],
            None,
            session=mock_session,
        )
        for index in range(plan_count)
    ]
    generator = QueryGenerator(mock_session)
    monkeypatch.setattr(generator, "resolve", lambda plan: plan)
    original_queries = [plan.queries for plan in plans]
    original_last_queries = [plan.queries[-1] for plan in plans]
    for _ in range(2):
        queries = generator.generate_queries(plans)[PlanQueryType.QUERIES]
        for plan, query, original_list, original in zip(
            plans, queries, original_queries, original_last_queries
        ):
            assert plan.queries is original_list
            assert plan.queries[-1] is original
            assert original.cte_segments is None
            assert query == original
            if trace_enabled and plan_count > 1:
                assert query is not original
                assert query.cte_segments == [
                    CteSegment(0, len(query.sql), plan, cached_only=True)
                ]
                assert query.query_line_intervals == original.query_line_intervals
            else:
                assert query.cte_segments is None


@pytest.mark.parametrize("multiline", [False, True])
def test_generate_queries_keeps_existing_segment_owners(
    mock_session, monkeypatch, multiline
):
    mock_session._generate_multiline_queries = multiline
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_config.enabled", lambda session, name: True
    )
    child = SnowflakePlan([Query("SELECT 1")], None, session=mock_session)
    segments = [CteSegment(0, len(child.queries[-1].sql), child)]
    plan = SnowflakePlan(
        [Query("SELECT 1", cte_segments=segments)], None, session=mock_session
    )
    other = SnowflakePlan([Query("SELECT 2")], None, session=mock_session)
    generator = QueryGenerator(mock_session)
    monkeypatch.setattr(generator, "resolve", lambda plan: plan)
    query = generator.generate_queries([plan, other])[PlanQueryType.QUERIES][0]
    # The owners and ranges are kept; the segments of the split plan are only read
    # from cached SQL. The plan-owned query and segments are not changed.
    assert query is not plan.queries[-1]
    assert [(s.start, s.end, s.plan, s.cached_only) for s in query.cte_segments] == [
        (0, len(child.queries[-1].sql), child, True)
    ]
    assert plan.queries[-1].cte_segments is segments
    assert not segments[0].cached_only


@pytest.mark.parametrize("new_line", [" ", "\n"])
@pytest.mark.parametrize("clone", [copy.copy, copy.deepcopy])
def test_generated_segments_belong_to_cloned_plan(
    mock_session, monkeypatch, clone, new_line
):
    mock_session._generate_multiline_queries = True
    monkeypatch.setattr(
        "snowflake.snowpark._internal.analyzer.analyzer_utils.NEW_LINE", new_line
    )
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_config.enabled", lambda session, name: True
    )
    # Segments are attached regardless of the global formatter state: the
    # diagnosis checks the layout of the cached SQL.
    plan = SnowflakePlan([Query("SELECT 1")], None, session=mock_session)
    other = SnowflakePlan([Query("SELECT 2")], None, session=mock_session)
    generator = QueryGenerator(mock_session)
    monkeypatch.setattr(generator, "resolve", lambda plan: plan)
    original = generator.generate_queries([plan, other])[PlanQueryType.QUERIES][0]
    copied_plan = clone(plan)
    copied = generator.generate_queries([copied_plan, other])[PlanQueryType.QUERIES][0]
    assert original.cte_segments[0].plan is plan
    assert copied.cte_segments[0].plan is copied_plan
    assert copied_plan.queries[-1].cte_segments is None
    assert plan.queries[-1].query_line_intervals == [QueryLineInterval(0, 0, plan.uuid)]
    assert copied_plan.queries[-1].query_line_intervals == [
        QueryLineInterval(0, 0, copied_plan.uuid)
    ]


@pytest.mark.parametrize("trace_enabled", [False, True])
@pytest.mark.parametrize("clone", [copy.copy, copy.deepcopy])
def test_cte_generation_does_not_mutate_input_plan(
    mock_session, monkeypatch, trace_enabled, clone
):
    monkeypatch.setattr(
        "snowflake.snowpark._internal.debug_config.enabled",
        lambda session, name: trace_enabled,
    )
    definition = SnowflakePlan(
        [Query("SELECT ? AS A", params=[1])], None, session=mock_session
    )
    block = WithQueryBlock("CTE", definition)
    pre_query = Query("SELECT 0")
    body = Query("SELECT * FROM CTE WHERE A > ?", params=[0])
    plan = SnowflakePlan(
        [pre_query, body], None, session=mock_session, referenced_ctes={block: 1}
    )
    original_queries = plan.queries
    original_intervals = body.query_line_intervals
    prefix = cte_statement([definition.queries[-1].sql], ["CTE"])
    for iteration in range(3):
        current_plan = plan if iteration < 2 else clone(plan)
        queries = get_snowflake_plan_queries(
            current_plan, {"CTE": definition.queries[-1]}, {"CTE": definition}
        )[PlanQueryType.QUERIES]
        assert queries is not current_plan.queries
        assert queries[0] is current_plan.queries[0]
        compiled = queries[-1]
        assert compiled is not current_plan.queries[-1]
        assert compiled.sql == prefix + "SELECT * FROM CTE WHERE A > ?"
        assert compiled.params == [1, 0]
        assert current_plan.queries[-1].sql == "SELECT * FROM CTE WHERE A > ?"
        assert current_plan.queries[-1].params == [0]
        assert current_plan.queries[-1].cte_segments is None
        if trace_enabled:
            assert compiled.cte_segments[0].plan is definition
            assert compiled.cte_segments[-1].plan is current_plan
            assert compiled.cte_segments[-1].start == len(prefix)
            assert compiled.cte_segments[-1].end == len(compiled.sql)
        else:
            assert compiled.cte_segments is None
    assert plan.queries is original_queries
    assert body.query_line_intervals is original_intervals
    assert definition.queries[-1].sql == "SELECT ? AS A"
    assert definition.queries[-1].params == [1]


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
    assert _resolve_error_line(top, line, 0, compiled_sql) == (
        definition_plan,
        1,
        False,
    )
    # "WHERE B > 0" is the fourth line of the body
    line = next(i for i, text in enumerate(lines) if text.startswith("WHERE"))
    assert _resolve_error_line(top, line, 6, compiled_sql) == (body_plan, 3, False)
    # a line with the end of the definition and the start of the body: the position decides
    line = next(i for i, text in enumerate(lines) if text.startswith("FROM t"))
    assert _resolve_error_line(top, line, 0, compiled_sql) == (
        definition_plan,
        1,
        False,
    )
    assert _resolve_error_line(top, line, len(lines[line]) - 1, compiled_sql) == (
        body_plan,
        0,
        False,
    )
    assert _resolve_error_line(top, 0, start, compiled_sql) == (
        definition_plan,
        0,
        False,
    )
    # The WITH wrapper before either segment and a line past the end are not mapped.
    assert _resolve_error_line(top, 0, 0, compiled_sql) is None
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
        False,
    )
    assert _resolve_error_line(top, 0, compiled_sql.index("WHERE"), compiled_sql) == (
        body_plan,
        0,
        False,
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
    assert _resolve_error_line(top, 2, 0, pre_action_sql) == (owner, None, False)
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


def _line_less_error(message="Invalid identifier TYPE", errno=2004):
    from snowflake.connector.errors import ProgrammingError

    return ProgrammingError(msg=f"SQL compilation error:\n{message}", errno=errno)


def _fake_plan(line, children=(), sql=None, queries=None, inherit=True):
    """A plan whose own operation is at ``line`` (no own operation if None); it inherits the
    trace nodes of its children like a real plan, unless ``inherit`` is False."""
    trace_nodes = []
    if inherit:
        for child in children:
            trace_nodes.extend(child.df_trace_nodes)
    if line is not None:
        trace_nodes.append(TraceNode(SrcPosition("f.py", line, 0, line, 0)))
    return types.SimpleNamespace(
        children_plan_nodes=list(children),
        df_trace_nodes=trace_nodes,
        queries=queries or [Query(sql or f"select {line}")],
        post_actions=[],
        _last_execution_queries=None,
        schema_query=None,
    )


def _line_less_context(top, failing_sql, other_error_sql=()):
    describes = []

    def describe(sql, params):
        describes.append(sql)
        if sql in other_error_sql:
            raise _line_less_error("Object 'T' does not exist or not authorized.", 2003)
        if sql in failing_sql:
            raise _line_less_error()

    top.session = types.SimpleNamespace(_get_result_attributes=describe)
    return (
        get_line_less_error_context(top, _line_less_error(), top.queries[-1].sql),
        describes,
    )


def test_line_less_error_reports_the_failing_child():
    source = _fake_plan(1, sql="source")
    join = _fake_plan(2, [source], sql="join")
    top = _fake_plan(3, [join], sql="top")
    context, describes = _line_less_context(top, {"join", "top"})
    assert (
        context
        == "\nSQL compilation error corresponds to Python source at f.py: line 2.\n"
    )
    assert describes == ["join", "source"]


def test_line_less_error_reports_the_top_plan():
    left, right = _fake_plan(1, sql="left"), _fake_plan(2, sql="right")
    top = _fake_plan(3, [left, right], sql="top")
    context, _ = _line_less_context(top, {"top"})
    assert (
        context
        == "\nSQL compilation error corresponds to Python source at f.py: line 3.\n"
    )


def test_line_less_error_reports_every_failing_branch():
    left = _fake_plan(1, [_fake_plan(None, sql="l0")], sql="left")
    right = _fake_plan(2, [_fake_plan(None, sql="r0")], sql="right")
    top = _fake_plan(3, [left, right], sql="top")
    context, _ = _line_less_context(top, {"left", "right", "top"})
    assert "f.py: line 1" in context and "f.py: line 2" in context
    assert "f.py: line 3" not in context


@pytest.mark.parametrize("call_id", [None, 122, 123])
def test_line_less_error_wrapper_without_operation_uses_its_parent(call_id):
    # An empty trace is internal only with provenance from the owning public call.
    source = _fake_plan(1, sql="source")
    wrapper = _fake_plan(None, [source], sql="wrapper", inherit=False)
    top = _fake_plan(2, [wrapper], sql="top", inherit=False)
    top.df_trace_nodes[-1].trace_call_id = 123
    wrapper._df_trace_call_id = call_id
    context, _ = _line_less_context(top, {"wrapper", "top"})
    assert context == (
        "\nSQL compilation error corresponds to Python source at f.py: line 2.\n"
        if call_id == 123
        else ""
    )


@pytest.mark.parametrize(
    "child",
    [
        # another error
        _fake_plan(1, sql="other"),
        # needs queries that run before it
        _fake_plan(
            1,
            queries=[
                types.SimpleNamespace(sql="create temp table"),
                types.SimpleNamespace(sql="child"),
            ],
        ),
        # refers to an object created at execution time
        _fake_plan(1, sql="select * from temp_name_placeholder_abc"),
    ],
    ids=["other_error", "pre_queries", "placeholder"],
)
def test_line_less_error_not_verified(child):
    top = _fake_plan(2, [child], sql="top")
    context, _ = _line_less_context(top, {"top"}, other_error_sql={"other"})
    assert context == ""


def test_line_less_error_plan_without_own_operation():
    # the plan only re-wraps its child: the operation that generated its SQL is unknown
    child = _fake_plan(1, sql="child")
    rewrap = _fake_plan(None, [child], sql="rewrap")
    top = _fake_plan(None, [rewrap], sql="top")
    context, _ = _line_less_context(top, {"rewrap", "top"})
    assert context == ""


def test_line_less_error_describe_limit(monkeypatch):
    monkeypatch.setattr(debug_utils, "_MAX_LINE_LESS_ERROR_DESCRIBES", 1)
    top = _fake_plan(3, [_fake_plan(1, sql="a"), _fake_plan(2, sql="b")], sql="top")
    context, describes = _line_less_context(top, {"top"})
    assert context == ""
    assert describes == ["a"]


@pytest.fixture
def line_less_session(mock_server_connection):
    from snowflake.snowpark import Session
    from snowflake.snowpark._internal import debug_config

    session = mock.create_autospec(Session)
    session._conn = mock_server_connection
    session._join_alias_fix = False
    session._cte_optimization_enabled = False
    session._query_compilation_stage_enabled = False
    session._collect_snowflake_plan_telemetry_at_critical_path = False
    session._last_canceled_id = 0
    session._generate_new_action_id.return_value = 1
    session._get_result_attributes = types.MethodType(
        Session._get_result_attributes, session
    )
    session._debug = debug_config.DebugConfig(session)
    session._debug.enable_trace_sql_errors_to_dataframe()
    yield session
    session._debug.set()


@pytest.mark.parametrize("child_fails", [False, True])
def test_line_less_error_describe_preserves_bindings(line_less_session, child_fails):
    child = _fake_plan(10, queries=[Query("select ? as ID", params=[7])])
    top = _fake_plan(20, [child], sql="top")
    top.session = line_less_session
    cursor = line_less_session._conn._cursor

    def describe(sql, params=None):
        assert sql == "select ? as ID"
        assert params == [7]
        if child_fails:
            raise _line_less_error()
        return []

    cursor._describe_internal.side_effect = describe
    context = get_line_less_error_context(top, _line_less_error(), "top")
    assert f"f.py: line {10 if child_fails else 20}" in context
    cursor._describe_internal.assert_called_once_with("select ? as ID", params=[7])


@pytest.mark.parametrize("matching_child_error", [False, True])
@pytest.mark.parametrize(
    "layout",
    [
        "main",
        "pre_action",
        "multiquery_main",
        "missing",
        "unknown",
        "schema",
        "compiled_pre_action",
        "compiled_main",
        "compiled_with",
        "compiled_unknown",
        "duplicate_pre_action",
        "duplicate_post_action",
        "post_action",
        "placeholder",
    ],
)
def test_line_less_error_wrapper_checks_failed_query(
    line_less_session, matching_child_error, layout
):
    from snowflake.snowpark.exceptions import SnowparkSQLException

    session = line_less_session
    server = session._conn
    source = SnowflakePlan([Query("source SQL")], "source schema", session=session)
    source.df_trace_nodes = [_trace_node(10)]
    top = SnowflakePlan([Query("final SQL")], "top schema", session=session)
    top.source_plan = types.SimpleNamespace(children=[source])
    top.df_trace_nodes = source.df_trace_nodes + [_trace_node(20)]
    queries = list(top.queries)
    post_actions = []
    executed_sql = "final SQL"
    if layout in ("pre_action", "multiquery_main", "duplicate_pre_action"):
        pre_sql = "final SQL" if layout == "duplicate_pre_action" else "pre-action SQL"
        top.queries.insert(0, Query(pre_sql))
        queries = list(top.queries)
        executed_sql = "final SQL" if layout == "multiquery_main" else pre_sql
    elif layout == "missing":
        executed_sql = None
    elif layout == "unknown":
        executed_sql = "unknown SQL"
    elif layout == "schema":
        executed_sql = top.schema_query
    elif layout in ("compiled_pre_action", "compiled_main"):
        queries.insert(0, Query("compiled pre-action SQL"))
        if layout == "compiled_pre_action":
            executed_sql = queries[0].sql
    elif layout == "compiled_with":
        queries = [Query("WITH c AS (source SQL) SELECT * FROM c")]
        executed_sql = queries[0].sql
    elif layout == "compiled_unknown":
        queries = [Query("unrelated compiled SQL")]
    elif layout in ("post_action", "duplicate_post_action"):
        post_actions = [
            Query(
                "final SQL" if layout == "duplicate_post_action" else "post-action SQL"
            )
        ]
        top.post_actions = post_actions
        executed_sql = post_actions[0].sql
    elif layout == "placeholder":
        top.queries = queries = [Query("select * from temp_name_placeholder_abc")]
        executed_sql = ' select * from "DB"."SC".SNOWPARK_TEMP_TABLE_123 \n'
    execution = {
        PlanQueryType.QUERIES: queries,
        PlanQueryType.POST_ACTIONS: post_actions,
    }
    error = _line_less_error()
    error.query = executed_sql

    def run_query(sql, *args, **kwargs):
        if layout == "multiquery_main" and sql == "pre-action SQL":
            return {"sfqid": "pre-action-id", "data": []}
        raise error

    server.run_query = mock.Mock(side_effect=run_query)
    server._cursor._describe_internal.return_value = []
    if matching_child_error:
        server._cursor._describe_internal.side_effect = _line_less_error()

    def execution_queries(plan):
        # Match the compiler cache populated by the actual property.
        plan._last_execution_queries = execution
        return execution

    with mock.patch.object(
        SnowflakePlan, "execution_queries", property(execution_queries)
    ):
        with pytest.raises(SnowparkSQLException) as exc:
            server.get_result_set(top)
    assert exc.value.query == executed_sql
    if layout in ("main", "placeholder"):
        assert (
            f"f.py: line {10 if matching_child_error else 20}"
            in exc.value.debug_context
        )
        server._cursor._describe_internal.assert_called_once_with(
            "source SQL", params=[]
        )
    else:
        assert not exc.value.debug_context
        server._cursor._describe_internal.assert_not_called()
    if layout in ("pre_action", "duplicate_pre_action", "compiled_pre_action"):
        assert server.run_query.call_count == 1
        assert server.run_query.call_args.args[0] == queries[0].sql


@pytest.mark.parametrize("describe_error_type", ["operational", "value"])
@pytest.mark.parametrize("trace_sql_errors", [False, True])
@pytest.mark.parametrize("trace_on_error", [False, True])
def test_line_less_error_wrapper_preserves_lineage_on_describe_failure(
    line_less_session, describe_error_type, trace_sql_errors, trace_on_error, caplog
):
    from snowflake.connector.errors import OperationalError
    from snowflake.snowpark._internal.analyzer import snowflake_plan
    from snowflake.snowpark.exceptions import SnowparkSQLException

    session = line_less_session
    session._debug.set(
        trace_sql_errors_to_dataframe=trace_sql_errors,
        dataframe_trace_on_error=trace_on_error,
    )
    child = SnowflakePlan([Query("select 1")], "select 1", session=session)
    child.df_trace_nodes = [_trace_node(10)]
    top = SnowflakePlan([Query("select 2")], "select 2", session=session)
    top.source_plan = types.SimpleNamespace(children=[child])
    top.df_trace_nodes = child.df_trace_nodes + [_trace_node(20)]
    original = _line_less_error()
    original.query = "select 2"
    original.sfqid = "original-query-id"
    original.sqlstate = "42000"
    original_metadata = {
        name: getattr(original, name)
        for name in ("query", "msg", "raw_msg", "errno", "sqlstate", "sfqid", "args")
    }
    describe_error = (
        OperationalError(msg="Connection timed out", errno=250003)
        if describe_error_type == "operational"
        else ValueError("diagnostic conversion failed")
    )
    cursor = session._conn._cursor
    cursor._describe_internal.side_effect = describe_error

    @SnowflakePlan.Decorator.wrap_exception
    def fail(plan):
        raise original

    with (
        mock.patch.object(
            snowflake_plan,
            "get_line_less_error_context",
            wraps=snowflake_plan.get_line_less_error_context,
        ) as line_less_context,
        mock.patch.object(
            snowflake_plan,
            "get_df_transform_trace_message",
            wraps=snowflake_plan.get_df_transform_trace_message,
        ) as lineage,
        caplog.at_level("DEBUG", logger=snowflake_plan.__name__),
        pytest.raises(SnowparkSQLException) as exc,
    ):
        fail(top)

    error = exc.value
    assert error.conn_error is original
    assert error.error_code == "1304"
    assert error.query == original_metadata["query"]
    assert error.message == original_metadata["msg"]
    assert error.raw_message == original_metadata["raw_msg"]
    assert error.sql_error_code == original_metadata["errno"]
    assert error.sfqid == original_metadata["sfqid"]
    for name, value in original_metadata.items():
        assert getattr(original, name) == value
    context = error.debug_context or ""
    assert (
        "Trace of the most recent dataframe operations" in context
    ) == trace_on_error
    assert "SQL compilation error corresponds to Python" not in context
    assert str(describe_error) not in str(error)
    if trace_sql_errors:
        line_less_context.assert_called_once_with(top, original, original.query)
        cursor._describe_internal.assert_called_once_with("select 1", params=[])
        assert "Error when getting the line-less SQL error context" in caplog.text
    else:
        line_less_context.assert_not_called()
        cursor._describe_internal.assert_not_called()
        assert "Error when getting the line-less SQL error context" not in caplog.text
    if trace_on_error:
        lineage.assert_called_once_with(top.df_trace_nodes[-1])
    else:
        lineage.assert_not_called()


@pytest.mark.parametrize("simplifier", [False, True])
@pytest.mark.parametrize("tracing", ["late", "gap", "continuous"])
@pytest.mark.parametrize("operation", ["union", "join"])
def test_line_less_error_real_plan_trace_ownership(
    mock_server_connection, simplifier, tracing, operation
):
    from snowflake.snowpark import Session
    from snowflake.snowpark._internal.analyzer import snowflake_plan
    from snowflake.snowpark.functions import col
    from snowflake.snowpark.exceptions import SnowparkSQLException

    server = mock_server_connection
    with Session(server) as session:
        session.sql_simplifier_enabled = simplifier
        session._cte_optimization_enabled = False
        session._ast_enabled = False
        session.debug.set(
            trace_sql_errors_to_dataframe=tracing != "late",
            dataframe_trace_on_error=tracing != "late",
        )
        left = session.create_dataframe([[1]], schema=["A"])
        right = session.create_dataframe([[1, 2]], schema=["A", "B"])
        if tracing == "gap":
            session.debug.set(
                trace_sql_errors_to_dataframe=False, dataframe_trace_on_error=False
            )
        bad = left.union_all(right) if operation == "union" else left.join(right, "B")
        session.debug.set(
            trace_sql_errors_to_dataframe=True, dataframe_trace_on_error=True
        )
        result = bad.filter(col("A") > 0)
        top = result._plan
        message = (
            "invalid number of result columns for set operator input branches"
            if operation == "union"
            else "Invalid identifier B"
        )
        original = _line_less_error(message)
        original.sfqid = "original-query-id"
        original.sqlstate = "42000"

        def fail_query(sql, *args, **kwargs):
            original.query = sql
            raise original

        def describe(sql, params=None):
            if ("UNION ALL" if operation == "union" else "JOIN") in sql:
                raise _line_less_error(message)
            return []

        cursor = server._cursor
        with (
            mock.patch.object(server, "run_query", side_effect=fail_query) as execute,
            mock.patch.object(
                cursor, "_describe_internal", side_effect=describe
            ) as probes,
            mock.patch.object(
                snowflake_plan,
                "get_line_less_error_context",
                wraps=snowflake_plan.get_line_less_error_context,
            ) as diagnostic,
            pytest.raises(SnowparkSQLException) as exc,
        ):
            result.collect()
        error = exc.value
        assert error.conn_error is original
        assert error.query == original.query == top.queries[-1].sql
        assert error.sfqid == original.sfqid == "original-query-id"
        assert error.sql_error_code == original.errno
        assert error.message == original.msg
        assert error.raw_message == original.raw_msg
        assert original.sqlstate == "42000"
        execute.assert_called_once()
        diagnostic.assert_called_once_with(top, original, original.query)
        assert top._last_execution_queries is not None
        assert 0 < probes.call_count <= debug_utils._MAX_LINE_LESS_ERROR_DESCRIBES
        context = error.debug_context
        assert "Trace of the most recent dataframe operations" in context
        assert 'bad.filter(col("A") > 0)' in context
        if tracing == "continuous":
            attribution = context.split(
                "Trace of the most recent dataframe operations"
            )[0]
            assert "SQL compilation error corresponds to Python source" in attribution
            assert _format_source_location(bad._trace_node.src) in attribution
            if operation != "union" or not simplifier:
                assert (
                    _format_source_location(result._trace_node.src) not in attribution
                )
        else:
            assert bad._trace_node is None
            assert "SQL compilation error corresponds to Python" not in context
        session.debug.set()


@pytest.mark.parametrize("copy_plan", [copy.copy, copy.deepcopy])
def test_line_less_error_trace_call_provenance_survives_wrapping_and_copying(
    mock_server_connection, copy_plan
):
    from snowflake.snowpark import Session
    from snowflake.snowpark._internal import df_trace
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SelectSnowflakePlan,
    )
    from snowflake.snowpark._internal.utils import publicapi

    with Session(mock_server_connection) as session:
        session._ast_enabled = False
        session.debug.enable_trace_sql_errors_to_dataframe()
        traced = session.create_dataframe([[1]], schema=["A"])
        token = traced._trace_node.trace_call_id
        assert token is not None
        assert traced._plan._df_trace_call_id == token
        for plan in (traced._plan, traced._select_statement):
            assert copy_plan(plan)._df_trace_call_id == plan._df_trace_call_id

        session.debug.disable_trace_sql_errors_to_dataframe()
        # Another session can keep the public API wrapper active while this one is off.
        with mock.patch.object(df_trace, "tracing_flags_on", return_value=True):
            untraced = session.create_dataframe([[2]], schema=["A"])
        assert untraced._plan._df_trace_call_id is None
        session.debug.enable_trace_sql_errors_to_dataframe()

        @publicapi
        def rewrap():
            assert df_trace.current_trace_call() != token
            return SelectSnowflakePlan(untraced._plan, analyzer=session._analyzer)

        # Creating a wrapper now must not relabel an older untraced plan as internal.
        wrapped = rewrap()
        assert wrapped._df_trace_call_id is None
        assert copy_plan(wrapped)._df_trace_call_id is None
        copied_selectable = copy_plan(untraced._select_statement)
        assert copied_selectable.snowflake_plan._df_trace_call_id is None
        assert df_trace.current_trace_call() is None

        @publicapi
        def fail():
            assert df_trace.current_trace_call() is not None
            raise ValueError("test failure")

        with pytest.raises(ValueError, match="test failure"):
            fail()
        assert df_trace.current_trace_call() is None
        session.debug.set()


@pytest.mark.parametrize("executed_sql", [None, "", "unknown SQL"])
def test_line_less_error_without_execution_cache_requires_provenance(executed_sql):
    top = _fake_plan(1, sql="top")
    top.session = mock.Mock()
    assert get_line_less_error_context(top, _line_less_error(), executed_sql) == ""
    top.session._get_result_attributes.assert_not_called()


@pytest.mark.parametrize("main_result", ["same_error", "different_error", "success"])
@pytest.mark.parametrize("budget", [0, 1, 2])
def test_line_less_schema_error_verifies_main_query(
    line_less_session, monkeypatch, main_result, budget
):
    from snowflake.snowpark.exceptions import SnowparkSQLException

    monkeypatch.setattr(debug_utils, "_MAX_LINE_LESS_ERROR_DESCRIBES", budget)
    child = SnowflakePlan(
        [Query("child SQL")], "child schema", session=line_less_session
    )
    child.df_trace_nodes = [_trace_node(10)]
    top = SnowflakePlan(
        [Query("select ? as ID", params=[7])], "schema SQL", session=line_less_session
    )
    top.source_plan = types.SimpleNamespace(children=[child])
    top.df_trace_nodes = child.df_trace_nodes + [_trace_node(20)]
    error = _line_less_error()
    error.query = "schema SQL"

    @SnowflakePlan.Decorator.wrap_exception
    def fail_schema(plan):
        raise error

    def describe(sql, params):
        if sql == "select ? as ID":
            assert params == [7]
            if main_result == "same_error":
                raise _line_less_error()
            if main_result == "different_error":
                raise _line_less_error("different error")
        else:
            assert sql == "child SQL"
        return []

    cursor = line_less_session._conn._cursor
    cursor._describe_internal.side_effect = describe
    with pytest.raises(SnowparkSQLException) as exc:
        fail_schema(top)
    if main_result == "same_error" and budget == 2:
        assert "f.py: line 20" in exc.value.debug_context
        assert "f.py: line 10" not in exc.value.debug_context
    else:
        assert not exc.value.debug_context
    expected_describes = min(budget, 2 if main_result == "same_error" else 1)
    assert cursor._describe_internal.call_count == expected_describes


@pytest.mark.parametrize("child_count", [32, 33])
def test_line_less_error_describe_budget_and_listeners(line_less_session, child_count):
    from snowflake.snowpark.query_history import QueryHistory

    session = line_less_session
    server = session._conn
    included = QueryHistory(session, include_describe=True, include_error=True)
    excluded = QueryHistory(session, include_describe=False, include_error=True)
    server.add_query_listener(included)
    server.add_query_listener(excluded)
    server._cursor._describe_internal.return_value = []
    top = _fake_plan(100, [_fake_plan(index + 1) for index in range(child_count)])
    top.session = session
    context = get_line_less_error_context(top, _line_less_error(), top.queries[0].sql)
    assert bool(context) == (child_count == 32)
    assert server._cursor._describe_internal.call_count == 32
    assert len(included.queries) == 32
    assert not excluded.queries
    included.queries.clear()
    server._cursor._describe_internal.side_effect = _line_less_error()
    top = _fake_plan(20, [_fake_plan(10)])
    top.session = session
    assert "f.py: line 10" in get_line_less_error_context(
        top, _line_less_error(), top.queries[0].sql
    )
    # Failed describes do not notify listeners, even with include_error=True.
    assert not included.queries
    assert not excluded.queries


@pytest.mark.parametrize("kind", ["with_query", "plain", "cte_partition"])
def test_annotate_breakdown_query_copies_plan_owned_queries(kind):
    plan = mock.MagicMock()
    plan.referenced_ctes = [mock.MagicMock()] if kind == "cte_partition" else []
    interval = QueryLineInterval(0, 0, "uuid")
    last = Query("CREATE TABLE T AS SELECT 1")
    last.query_line_intervals = [interval]
    segments = None
    if kind == "with_query":
        segments = [CteSegment(0, 3, plan), CteSegment(3, 6, plan)]
        last.cte_segments = segments
    first = Query("SELECT 0")
    owned = [first, last]
    plan_queries = {PlanQueryType.QUERIES: owned}
    QueryGenerator._annotate_breakdown_query(plan, plan_queries)
    queries = plan_queries[PlanQueryType.QUERIES]
    assert queries is not owned and queries[0] is first and queries[1] is not last
    # The plan-owned objects are not changed.
    assert owned == [first, last] and last.query_line_intervals == [interval]
    assert last.cte_segments is segments
    if segments is not None:
        assert not any(segment.cached_only for segment in segments)
    annotated = queries[1]
    assert annotated.sql == last.sql
    if kind == "with_query":
        assert [
            (s.start, s.end, s.plan, s.cached_only) for s in annotated.cte_segments
        ] == [
            (0, 3, plan, True),
            (3, 6, plan, True),
        ]
        assert annotated.query_line_intervals == [interval]
    elif kind == "plain":
        assert [
            (s.start, s.end, s.plan, s.cached_only) for s in annotated.cte_segments
        ] == [(0, len(last.sql), plan, True)]
        assert annotated.query_line_intervals == [interval]
    else:
        assert annotated.cte_segments is None
        assert annotated.query_line_intervals == []
