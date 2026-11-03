#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Debug output of the error experience (lineage trace, SQL error to Python source mapping and
object context) computed from the lightweight DataFrame trace instead of the AST.

It mirrors the functions of ``debug_utils`` that read the AST; ``wrap_exception`` does not use it yet.
"""

from functools import cached_property
import os
import sys
from typing import List, Optional
import itertools
import re
from typing import TYPE_CHECKING
from snowflake.snowpark._internal.df_trace import SrcPosition, TraceNode

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

UNKNOWN_FILE = "__UNKNOWN_FILE__"
SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH = (
    "SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH"
)


class DataFrameTraceNode:
    """A node representing a dataframe operation in the DAG that represents the lineage of a DataFrame."""

    def __init__(self, trace_node: TraceNode) -> None:
        self.trace_node = trace_node

    @cached_property
    def children(self) -> List[TraceNode]:
        """Returns the trace nodes of the dataframes this node was built from."""
        return self.trace_node.parents

    def get_src(self) -> Optional[SrcPosition]:
        """The source position of the user code that created the DataFrame."""
        return self.trace_node.src

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
                # For python 3.10, we do not extract the end line from the source code
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

        return self.trace_node.source_id

    def get_source_snippet(self) -> str:
        """Read the source file and extract the snippet where the dataframe is created."""
        src = self.get_src()
        if src is None:  # pragma: no cover
            return "No source"

        filename = src.filename or UNKNOWN_FILE

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
    root: TraceNode,
) -> List[DataFrameTraceNode]:
    """Helper function to get the transform trace of the dataframe involved in the exception.
    It gathers the lineage in the following way:

    1. Start by creating a DataFrameTraceNode for the given trace node.
    2. We use BFS to traverse the lineage using the node created in 1. as the first layer.
    3. During each iteration, we check if the node's source_id has been visited. If not,
        we add it to the visited set and append its source format to the trace. This step
        is needed to avoid source_id added multiple times in lineage due to loops.
    4. We then explore the next layer by adding the parents of the current node to the
        next layer. We check if the parent has been visited and if not, we add it to the
        visited set and append the DataFrameTraceNode for it to the next layer.
    5. We repeat this process until there are no more nodes to explore.

    Args:
        root: The trace node of the dataframe involved in the exception.

    Returns:
        A list of DataFrameTraceNode objects representing the transform trace of the dataframe.
    """
    visited_nodes = {id(root)}
    visited_source_id = set()

    curr = [DataFrameTraceNode(root)]
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
            for child in node.children:
                if id(child) in visited_nodes:
                    continue
                visited_nodes.add(id(child))
                next.append(DataFrameTraceNode(child))

        curr = next

    return lineage


def get_df_transform_trace_message(root: TraceNode) -> str:
    """Get the transform trace message for the dataframe involved in the exception.

    Args:
        root: The trace node of the dataframe involved in the exception.

    Returns:
        A string representing the transform trace message, empty if the dataframe is not found.
    """
    df_transform_trace_nodes = _get_df_transform_trace(root)
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


def _format_source_location(src: Optional[SrcPosition]) -> str:
    """Helper function to format source location information."""
    if src is None:
        return ""

    # if we cannot find the file, we use a placeholder
    filename = src.filename or "UNKNOWN_FILE"
    lines_info = f"{filename}: line {src.start_line}"
    if src.end_line > src.start_line:
        lines_info = f"{filename}: lines {src.start_line}-{src.end_line}"
    return lines_info


def _extract_source_locations_from_plan(plan: "SnowflakePlan") -> List[str]:
    """
    Extract source locations from a SnowflakePlan's AST IDs.

    Args:
        plan: The SnowflakePlan object to extract source locations from

    Returns:
        List of unique source location strings (e.g., "file.py: line 42")
    """
    source_locations = []
    found_locations = set()

    if plan.df_trace_nodes is not None:
        for trace_node in plan.df_trace_nodes:
            location = _format_source_location(trace_node.src)
            if location and location not in found_locations:
                found_locations.add(location)
                source_locations.append(location)

    return source_locations


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
    source_locations = _extract_source_locations_from_plan(plan)

    if source_locations:
        if len(source_locations) == 1:
            return f"\nSQL compilation error corresponds to Python source at {source_locations[0]}.\n"
        else:
            locations_str = "\n  - ".join(source_locations)
            return f"\nSQL compilation error corresponds to Python sources at:\n  - {locations_str}\n"
    return ""


def get_missing_object_context(top_plan: "SnowflakePlan", error_msg: str) -> str:
    """
    Extract Python source location context for missing object errors.

    Args:
        top_plan (SnowflakePlan): The top-level SnowflakePlan object that contains the SQL
            compilation error.
        error_msg (str): The raw error message from the SQL compilation error. Expected to contain
            the pattern "Object 'name' does not exist or not authorized".

    Returns:
        Error message with the affected Python lines numbers if found, otherwise an empty string.

    """
    sql_compilation_error_regex = re.compile(
        r""".*SQL compilation error:\s*Object '([^']+)' does not exist or not authorized.*""",
    )
    match = sql_compilation_error_regex.match(error_msg)
    if not match:
        return ""

    # Extract the object name from the error message
    object_name = match.group(1)

    # For missing object errors, the compilation error is at the top level
    # (with query like select * from non_existent_table), so we use the top plan directly
    found_locations = {}
    if top_plan.df_trace_nodes is not None:
        for trace_node in top_plan.df_trace_nodes:
            location = _format_source_location(trace_node.src)
            if location != "":
                found_locations[location] = None
    if found_locations:
        if len(found_locations) == 1:
            return f"\nMissing object '{object_name}' corresponds to Python source at {list(found_locations.keys())[0]}.\n"
        else:
            locations_str = "\n  - ".join(list(found_locations.keys()))
            return f"\nMissing object '{object_name}' corresponds to Python sources at:\n  - {locations_str}\n"
    return ""


def get_existing_object_context(top_plan: "SnowflakePlan", error_msg: str) -> str:
    """
    Extract table/object name from error messages like 'Object "TABLE" already exists'
    and return information about where that table was referenced in the Python source code.

    Args:
        top_plan: The top level SnowflakePlan object that contains the error.
        error_msg: The error message containing the object/table name.

    Returns:
        Error message with the Python source locations where the table was referenced,
        otherwise an empty string.
    """
    sql_compilation_error_regex = re.compile(
        r""".*SQL compilation error:\s*Object '([^']+)' already exists.*""",
    )
    match = sql_compilation_error_regex.match(error_msg)
    if not match:
        return ""

    object_name = match.group(1)

    def normalize_sql_identifier(name: str) -> str:
        """Normalize SQL identifier by removing quotes and converting to uppercase."""
        return name.strip('"').strip("'").upper()

    def object_name_match(extracted_name: str, error_object_name: str) -> bool:
        """Check if two object names match exactly, accounting for schema prefixes."""
        extracted_norm = normalize_sql_identifier(extracted_name)
        error_norm = normalize_sql_identifier(error_object_name)
        return error_norm in extracted_norm or extracted_norm in error_norm

    def sql_contains_object_creation(sql_query: str, target_object: str) -> bool:
        """Check if SQL query contains a CREATE statement for the target object."""
        query_upper = sql_query.upper()
        target_upper = normalize_sql_identifier(target_object)

        # Extract just the table name from the qualified name
        # target_object could be "DB.SCHEMA.TABLE" or just "TABLE"
        table_name_parts = target_upper.split(".")
        table_name = table_name_parts[-1]  # Get the last part (table name)

        # Pattern to match SQL identifiers that can be quoted or unquoted
        # Matches: identifier, "identifier", 'identifier'
        identifier_pattern = r'(?:["\']?[^"\'\s.]+["\']?)'

        create_patterns = [
            # Simple table name: CREATE TABLE table_name
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?['\"]?{re.escape(table_name)}['\"]?\b",
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?['\"]?{re.escape(table_name)}['\"]?\b",
            # Schema-qualified: CREATE TABLE schema.table_name or "schema".table_name
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{identifier_pattern}\.{re.escape(table_name)}\b",
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?{identifier_pattern}\.{re.escape(table_name)}\b",
            # Database-qualified: CREATE TABLE database.schema.table_name or "database"."schema".table_name
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{identifier_pattern}\.{identifier_pattern}\.{re.escape(table_name)}\b",
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?{identifier_pattern}\.{identifier_pattern}\.{re.escape(table_name)}\b",
        ]

        for pattern in create_patterns:
            if re.search(pattern, query_upper):
                return True
        return False

    registry = getattr(top_plan.session, "_df_trace_registry", None)
    if registry is None:
        return ""

    for creation in registry.items():
        if creation.kind in ("table", "view"):
            # case when we create an object by using .save_as_table(), create_temp_view(),
            # create_or_replace_view() or create_or_replace_temp_view()
            if creation.name_or_query and object_name_match(
                creation.name_or_query, object_name
            ):
                location = _format_source_location(creation.src)
                if location:
                    return f"\nObject '{object_name}' was first referenced at {location}.\n"
        elif creation.kind == "sql":
            # case when we create an object by using session.sql()
            if sql_contains_object_creation(creation.name_or_query, object_name):
                location = _format_source_location(creation.src)
                if location:
                    return f"\nObject '{object_name}' was first referenced at {location}.\n"

    return ""
