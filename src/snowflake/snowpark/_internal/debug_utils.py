#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
import os
import sys
from typing import Dict, List, Optional
import itertools
import re
from typing import TYPE_CHECKING
from snowflake.snowpark._internal.ast.batch import get_dependent_bind_ids
from snowflake.snowpark._internal.ast.utils import __STRING_INTERNING_MAP__
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.ast.utils import extract_src_from_expr

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

UNKNOWN_FILE = "__UNKNOWN_FILE__"
SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH = (
    "SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH"
)


class DataFrameTraceNode:
    """A node representing a dataframe operation in the DAG that represents the lineage of a DataFrame."""

    def __init__(self, batch_id: int, stmt_cache: Dict[int, proto.Stmt]) -> None:
        self.batch_id = batch_id
        self.stmt_cache = stmt_cache

    @cached_property
    def children(self) -> set[int]:
        """Returns the batch_ids of the children of this node."""
        return get_dependent_bind_ids(self.stmt_cache[self.batch_id])

    def get_src(self) -> Optional[proto.SrcPosition]:
        """The source Stmt of the DataFrame described by the batch_id."""
        stmt = self.stmt_cache[self.batch_id]
        api_call = stmt.bind.expr.WhichOneof("variant")
        return (
            getattr(stmt.bind.expr, api_call).src
            if api_call and getattr(stmt.bind.expr, api_call).HasField("src")
            else None
        )

    def _read_file(
        self, filename, start_line, end_line, start_column, end_column
    ) -> str:
        """Read the relevant code snippets of where the DataFrame was created. The filename given here
        must have read permissions for the executing user."""
        with open(filename) as f:
            code_lines = []
            if sys.version_info >= (3, 11):
                # Skip to start_line and read only the required lines
                lines = itertools.islice(f, start_line - 1, end_line)
                code_lines = list(lines)
                if start_line == end_line:
                    code_lines[0] = code_lines[0][start_column:end_column]
                else:
                    code_lines[0] = code_lines[0][start_column:]
                    code_lines[-1] = code_lines[-1][:end_column]
            else:
                # For python 3.9/3.10, we do not extract the end line from the source code
                # so we just read the start line and return.
                for line in itertools.islice(f, start_line - 1, start_line):
                    code_lines.append(line)

            code_lines = [line.rstrip() for line in code_lines]
            return "\n".join(code_lines)

    @cached_property
    def source_id(self) -> str:
        """Unique identifier of the location of the DataFrame creation in the source code."""
        src = self.get_src()
        if src is None:  # pragma: no cover
            return ""

        fileno = src.file
        start_line = src.start_line
        start_column = src.start_column
        end_line = src.end_line
        end_column = src.end_column
        return f"{fileno}:{start_line}:{start_column}-{end_line}:{end_column}"

    def get_source_snippet(self) -> str:
        """Read the source file and extract the snippet where the dataframe is created."""
        src = self.get_src()
        if src is None:  # pragma: no cover
            return "No source"

        # get the latest mapping of fileno to filename
        _fileno_to_filename_map = {v: k for k, v in __STRING_INTERNING_MAP__.items()}
        fileno = src.file
        filename = _fileno_to_filename_map.get(fileno, UNKNOWN_FILE)

        start_line = src.start_line
        end_line = src.end_line
        start_column = src.start_column
        end_column = src.end_column

        # Build the code identifier to find the operations where the DataFrame was created
        if sys.version_info >= (3, 11):
            code_identifier = (
                f"{filename}|{start_line}:{start_column}-{end_line}:{end_column}"
            )
        else:
            code_identifier = f"{filename}|{start_line}"

        if filename != UNKNOWN_FILE and os.access(filename, os.R_OK):
            # If the file is readable, read the code snippet
            code = self._read_file(
                filename, start_line, end_line, start_column, end_column
            )
            return f"{code_identifier}: {code}"
        return code_identifier  # pragma: no cover


def _get_df_transform_trace(
    batch_id: int,
    stmt_cache: Dict[int, proto.Stmt],
) -> List[DataFrameTraceNode]:
    """Helper function to get the transform trace of the dataframe involved in the exception.
    It gathers the lineage in the following way:

    1. Start by creating a DataFrameTraceNode for the given batch_id.
    2. We use BFS to traverse the lineage using the node created in 1. as the first layer.
    3. During each iteration, we check if the node's source_id has been visited. If not,
        we add it to the visited set and append its source format to the trace. This step
        is needed to avoid source_id added multiple times in lineage due to loops.
    4. We then explore the next layer by adding the children of the current node to the
        next layer. We check if the child ID has been visited and if not, we add it to the
        visited set and append the DataFrameTraceNode for it to the next layer.
    5. We repeat this process until there are no more nodes to explore.

    Args:
        batch_id: The batch ID of the dataframe involved in the exception.
        stmt_cache: The statement cache of the session.

    Returns:
        A list of DataFrameTraceNode objects representing the transform trace of the dataframe.
    """
    visited_batch_id = set()
    visited_source_id = set()

    visited_batch_id.add(batch_id)
    curr = [DataFrameTraceNode(batch_id, stmt_cache)]
    lineage = []

    while curr:
        next: List[DataFrameTraceNode] = []
        for node in curr:
            # tracing updates
            source_id = node.source_id
            if source_id not in visited_source_id:
                visited_source_id.add(source_id)
                lineage.append(node)

            # explore next layer
            for child_id in node.children:
                if child_id in visited_batch_id:
                    continue
                visited_batch_id.add(child_id)
                next.append(DataFrameTraceNode(child_id, stmt_cache))

        curr = next

    return lineage


def get_df_transform_trace_message(
    df_ast_id: int, stmt_cache: Dict[int, proto.Stmt]
) -> str:
    """Get the transform trace message for the dataframe involved in the exception.

    Args:
        df_ast_id: The AST ID of the dataframe involved in the exception.
        stmt_cache: The statement cache of the session.

    Returns:
        A string representing the transform trace message, empty if the dataframe is not found.
    """
    df_transform_trace_nodes = _get_df_transform_trace(df_ast_id, stmt_cache)
    if len(df_transform_trace_nodes) == 0:  # pragma: no cover
        return ""

    df_transform_trace_length = len(df_transform_trace_nodes)
    show_trace_length = int(
        os.environ.get(SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH, 5)
    )

    debug_info_lines = [
        f"Trace of the most recent dataframe operations associated with the error (total {df_transform_trace_length}):\n",
    ]
    for node in df_transform_trace_nodes[:show_trace_length]:
        debug_info_lines.append(node.get_source_snippet())
    if df_transform_trace_length > show_trace_length:
        debug_info_lines.append(
            f"... and {df_transform_trace_length - show_trace_length} more.\nYou can increase "
            f"the lineage length by setting {SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH} "
            "environment variable."
        )
    return "\n".join(debug_info_lines)


def _format_source_location(src: Optional[proto.SrcPosition]) -> str:
    """Helper function to format source location information."""
    if src is None:
        return ""

    from snowflake.snowpark._internal.ast.utils import __STRING_INTERNING_MAP__

    filename_map = {v: k for k, v in __STRING_INTERNING_MAP__.items()}
    # if we cannot find the file, we use a placeholder
    filename = filename_map.get(src.file, "UNKNOWN_FILE")
    lines_info = f"{filename}: line {src.start_line}"
    if src.end_line > src.start_line:
        lines_info = f"{filename}: lines {src.start_line}-{src.end_line}"
    return lines_info


def get_python_source_from_sql_error(top_plan: "SnowflakePlan", error_msg: str) -> str:
    """
    Extract SQL error line number and map it back to Python source code. We use the
    helper function get_plan_from_line_numbers to get the plan from the line number
    found in the SQL compilation error message. We then extract the source lines
    and columns using the ast_id associated with the plan. We return a message with
    the affected Python lines if found, otherwise an empty string.

    Args:
        plan: The top level SnowflakePlan object that contains the SQL compilation error.
        error_msg: The error message from the SQL compilation error.

    Returns:
        Error message with the affected Python lines numbers if found, otherwise an empty string.
    """
    sql_compilation_error_regex = re.compile(
        r""".*SQL compilation error:\s*error line (\d+) at position (\d+).*""",
    )
    match = sql_compilation_error_regex.match(error_msg)
    if not match:
        return ""

    sql_line_number = int(match.group(1)) - 1

    from snowflake.snowpark._internal.utils import (
        get_plan_from_line_numbers,
    )

    plan = get_plan_from_line_numbers(top_plan, sql_line_number)
    source_locations = []
    found_locations = set()
    if plan.df_ast_ids is not None:
        for ast_id in plan.df_ast_ids:
            bind_stmt = plan.session._ast_batch._bind_stmt_cache.get(ast_id)
            if bind_stmt is not None:
                src = extract_src_from_expr(bind_stmt.bind.expr)
                location = _format_source_location(src)
                if location != "" and location not in found_locations:
                    found_locations.add(location)
                    source_locations.append(location)
    if source_locations:
        if len(source_locations) == 1:
            return f"\nSQL compilation error corresponds to Python source at {source_locations[0]}.\n"
        else:
            locations_str = "\n  - ".join(source_locations)
            return f"\nSQL compilation error corresponds to Python sources at:\n  - {locations_str}\n"
    return ""
