#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
import os
import sys
from typing import Dict, List, Optional, Set, Tuple
import itertools
import re
from typing import TYPE_CHECKING
import snowflake.snowpark
from snowflake.snowpark._internal.df_trace import SrcPosition, TraceNode
from ast import literal_eval

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


def _get_own_trace_nodes(plan: "SnowflakePlan") -> List[TraceNode]:
    """
    Return the trace nodes of the DataFrame operations that produced this plan's
    own SQL text, leaving out the ones it inherited from the subqueries it wraps.

    A plan copies the trace nodes of its child plans when it wraps them, so its
    list also holds every upstream operation. The SQL lines of those child plans
    are mapped to the child plans themselves, so their nodes are dropped here.
    The nodes of a ``SelectSQL`` child are not inherited but copied down from the
    plan (``SnowflakePlan.add_df_trace_node``), so they are kept.
    If nothing is left (e.g. the plan only re-wraps its child), the full list is
    returned.
    """
    from snowflake.snowpark._internal.analyzer.select_statement import SelectSQL

    trace_nodes = plan.df_trace_nodes or []
    inherited_ids = set()
    for child in plan.children_plan_nodes:
        if isinstance(child, SelectSQL):
            continue
        for node in getattr(child, "df_trace_nodes", None) or []:
            inherited_ids.add(id(node))
    own_nodes = [node for node in trace_nodes if id(node) not in inherited_ids]
    return own_nodes or trace_nodes


def _extract_source_locations_from_plan(
    plan: "SnowflakePlan", own_only: bool = False
) -> List[str]:
    """
    Extract source locations from the trace nodes of a SnowflakePlan.

    Args:
        plan: The SnowflakePlan object to extract source locations from
        own_only: Only use the trace nodes of the operations that produced the plan's
            own SQL text (see ``_get_own_trace_nodes``).

    Returns:
        List of unique source location strings (e.g., "file.py: line 42")
    """
    source_locations = []
    found_locations = set()

    if plan.df_trace_nodes is not None:
        trace_nodes = _get_own_trace_nodes(plan) if own_only else plan.df_trace_nodes
        for trace_node in trace_nodes:
            location = _format_source_location(trace_node.src)
            if location and location not in found_locations:
                found_locations.add(location)
                source_locations.append(location)

    return source_locations


_PLACEHOLDER = re.compile(
    r'(?:"[^"]*"\.){0,2}temp_name_placeholder_[A-Za-z0-9]+|query_id_place_holder_[A-Za-z0-9]+'
)


def _same_query(executed: str, sql: str) -> bool:
    """Whether ``executed`` is ``sql`` as it was sent: stripped, and with the names of the temp
    objects and the ids of the queries it depends on (e.g. ``RESULT_SCAN`` of a ``SHOW`` query) in
    place of their placeholders."""
    sql = sql.strip()
    if executed == sql:
        return True
    parts = _PLACEHOLDER.split(sql)
    if len(parts) == 1:
        return False
    pattern = r"[0-9A-Za-z_$.\"-]+".join(re.escape(part) for part in parts)
    return re.fullmatch(pattern, executed) is not None


def _resolve_error_line(
    top_plan: "SnowflakePlan",
    sql_line_number: int,
    position: int,
    executed_sql: Optional[str],
) -> Optional[Tuple["SnowflakePlan", Optional[int]]]:
    """Returns the plan whose line intervals describe the line of the executed sql that has the
    error, and the line number relative to the sql of that plan, or None if the line cannot be
    mapped reliably. The line number is None when the returned plan is the one responsible for
    the line.

    The line intervals of ``top_plan`` describe the sql it generated itself. When the executed sql
    is different, because the plan compiler rewrote it (a WITH clause for CTE optimization, or the
    query was split by large query breakdown), they must not be used: a line of the compiled sql
    is mapped through the part of the compiled query it is in instead. A query that runs before
    the last one (e.g. the statement of ``session.sql(<non-select>)``) is mapped through its own
    line intervals, and an error of a query whose parts are unknown is not mapped.
    """
    if executed_sql is None:
        return top_plan, sql_line_number
    # The sql is stripped before it is sent.
    executed = executed_sql.strip()
    if _same_query(executed, top_plan.queries[-1].sql) or (
        top_plan.schema_query is not None
        and _same_query(executed, top_plan.schema_query)
    ):
        return top_plan, sql_line_number

    compiled = top_plan._last_execution_queries or {}
    compiled_query = None
    for queries in compiled.values():
        for query in queries:
            if _same_query(executed, query.sql):
                compiled_query = query
                break
        if compiled_query is not None:
            break
    if compiled_query is None:
        return None
    compiled_sql, segments = compiled_query.sql, compiled_query.cte_segments
    if not segments:
        # A query that runs before the last one, e.g. a pre-action: its line intervals are
        # owned by a node of the plan. Queries generated by large query breakdown are not, so
        # they are not mapped.
        from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
        from snowflake.snowpark._internal.utils import (
            get_plan_from_pre_query_line_number,
        )

        main_queries = compiled.get(PlanQueryType.QUERIES, [])
        if not any(query is compiled_query for query in main_queries[:-1]):
            return None
        owner = get_plan_from_pre_query_line_number(
            top_plan, compiled_query, sql_line_number
        )
        return None if owner is None else (owner, None)

    # The lines of the compiled sql: filling in query ids does not change the line breaks.
    lines = compiled_sql.strip().split("\n")
    if sql_line_number >= len(lines):
        return None
    leading = len(compiled_sql) - len(compiled_sql.lstrip())
    offset = leading + sum(len(line) + 1 for line in lines[:sql_line_number]) + position
    for segment in segments:
        if segment.start <= offset < segment.end:
            local_line = compiled_sql.count("\n", segment.start, offset)
            return segment.plan, local_line
    return None


def get_python_source_from_sql_error(
    top_plan: "SnowflakePlan", error_msg: str, executed_sql: Optional[str] = None
) -> str:
    """
    Extract SQL error line number and map it back to Python source code. We use the
    helper function get_plan_from_line_numbers to get the plan from the line number
    found in the SQL compilation error message. We then extract the source lines
    and columns using the trace nodes associated with the plan. We return a message with
    the affected Python lines if found, otherwise an empty string.

    Args:
        plan: The top level SnowflakePlan object that contains the SQL compilation error.
        error_msg: The error message from the SQL compilation error.
        executed_sql: The sql that failed, if known. When it differs from the sql of the plan
            (the plan compiler rewrote it), the line is mapped through the compiled query.
            When the line is a line of the sql of a plan, only the operations that
            produced it are reported.

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
    resolved = _resolve_error_line(
        top_plan, sql_line_number, int(match.group(2)), executed_sql
    )
    if resolved is None:
        return ""
    plan_root, line_number = resolved
    # The line intervals describe the sql of the plan. A line of another query of the plan
    # (its schema query) only identifies the plan, so all of its operations are reported.
    own_only = executed_sql is not None and (
        plan_root is not top_plan
        or _same_query(executed_sql.strip(), top_plan.queries[-1].sql)
    )

    from snowflake.snowpark._internal.utils import (
        get_plan_from_line_numbers,
    )

    plan = (
        plan_root
        if line_number is None
        else get_plan_from_line_numbers(plan_root, line_number)
    )
    source_locations = _extract_source_locations_from_plan(plan, own_only=own_only)

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


class QueryProfiler:
    """
    A class for profiling Snowflake queries and analyzing operator statistics.
    It can generate tree visualizations and output tables of operator statistics.
    """

    def __init__(
        self, session: "snowflake.snowpark.Session", output_file: Optional[str] = None
    ) -> None:
        self.session = session
        if output_file:
            self.file_handle = open(output_file, "a", encoding="utf-8")
        else:
            self.file_handle = None

    def _get_node_info(self, row: Dict) -> Dict:
        parent_operators = row.get("PARENT_OPERATORS")
        parent_operators = (
            str(parent_operators) if parent_operators is not None else None
        )
        node_info = {
            "id": row.get("OPERATOR_ID") or 0,
            "parent_operators": parent_operators,
            "type": row.get("OPERATOR_TYPE") or "N/A",
            "input_rows": row.get("INPUT_ROWS") or 0,
            "output_rows": row.get("OUTPUT_ROWS") or 0,
            "row_multiple": row.get("ROW_MULTIPLE") or 0,
            "exec_time": row.get("OVERALL_PERCENTAGE") or 0,
            "attributes": row.get("OPERATOR_ATTRIBUTES") or "N/A",
        }
        return node_info

    def build_operator_tree(self, operators_data: List[Dict]) -> Tuple[Dict, Dict, Set]:
        """
        Build a tree structure from raw operator data for query profiling.

        Args:
            operators_data (List[Dict]): A list of dictionaries containing operator statistics.
            The keys include operator id, operator type, parent operators, input rows, output rows,
            row multiple, overall percentage, and operator attributes.

        Returns:
            Tuple[Dict, Dict, Set]: A tuple containing:
                - nodes (Dict[int, Dict]): Dictionary mapping operator IDs to node information
                - children (Dict[int, List[int]]): Dictionary mapping operator IDs to lists of child operator IDs
                - root_nodes (Set[int]): Set of operator IDs that are root nodes (have no parents)

        """

        nodes = {}
        children = {}
        root_nodes = set()
        for row in operators_data:
            node_info = self._get_node_info(row)

            nodes[node_info["id"]] = node_info
            children[node_info["id"]] = []

            if node_info["parent_operators"] is None:
                root_nodes.add(node_info["id"])
            else:
                # parse parent_operators, which is a string like "[1, 2, 3]" to a list
                x = literal_eval(node_info["parent_operators"])
                for parent_id in x:
                    if parent_id not in children:
                        children[parent_id] = []
                    children[parent_id].append(node_info["id"])

        return nodes, children, root_nodes

    def _write_output(self, message: str) -> None:
        """Helper function to write output to either console or file."""
        if self.file_handle:
            self.file_handle.write(message + "\n")
        else:
            sys.stdout.write(message + "\n")

    def close(self) -> None:
        """Close the file handle if it exists."""
        if self.file_handle:
            self.file_handle.close()

    def print_operator_tree(
        self,
        nodes: Dict[int, Dict],
        children: Dict[int, List[int]],
        node_id: int,
        prefix: str = "",
        is_last: bool = True,
    ) -> None:
        """
        Print a visual tree representation of query operators with their statistics.

        Args:
            nodes (Dict[int, Dict]): Dictionary mapping operator IDs to node information.
            children (Dict[int, List[int]]): Dictionary mapping operator IDs to lists of child operator IDs.
            node_id (int): The ID of the current operator node to print.
            prefix (str, optional): String prefix for tree formatting (used for indentation).
                Defaults to "".
            is_last (bool, optional): Whether this node is the last child of its parent.
                Used for proper tree connector formatting. Defaults to True.

        Returns:
            None: This function writes output to a file or prints and doesn't return a value.

        """
        node = nodes[node_id]

        connector = "└── " if is_last else "├── "

        node_info = (
            f"[{node['id']}] {node['type']} "
            f"(In: {node['input_rows']:,}, Out: {node['output_rows']:,}, "
            f"Mult: {float(node['row_multiple']):.2f}, Time: {float(node['exec_time']):.2f}%)"
        )

        self._write_output(f"{prefix}{connector}{node_info}")

        extension = "    " if is_last else "│   "
        new_prefix = prefix + extension

        child_list = children.get(node_id, [])
        for i, child_id in enumerate(child_list):
            is_last_child = i == len(child_list) - 1
            self.print_operator_tree(
                nodes, children, child_id, new_prefix, is_last_child
            )

    def print_describe_queries(self, describe_queries: List[Tuple[str, float]]) -> None:
        """
        Prints sql queries and time taken for descrisbe queries
        """
        self._write_output(f"\n{'='*80}")
        self._write_output("DESCRIBE QUERY INFORMATION")
        self._write_output(f"{'='*80}")
        for query, time in describe_queries:
            self._write_output(f"Query: {query}")
            self._write_output(f"Time: {time:.3f} seconds\n")

    def profile_query(
        self,
        query_id: str,
        verbose: bool = False,
    ) -> None:
        """
        Profile a query and save the results to a file.

        Args:
            query_id: The query ID to profile
            verbose: Whether to print the full query text

        Returns:
            None - output either to the console or to the file specified by output_file
        """
        execution_time_ms = None
        sql_text = "N/A"
        start_time = None
        end_time = None
        query_info_sql = f"""
            SELECT
                query_text,
                total_elapsed_time,
                start_time,
                end_time
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE query_id = '{query_id}'
            LIMIT 1
        """

        cursor = self.session._conn._conn.cursor()
        cursor.execute(query_info_sql)
        result = cursor.fetchone()

        if result:
            sql_text = result[0] if result[0] else "N/A"
            execution_time_ms = result[1] if result[1] is not None else None
            start_time = result[2] if result[2] else None
            end_time = result[3] if result[3] else None

        stats_query = f"""
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
        stats_connection = self.session._conn._conn.cursor()
        stats_connection.execute(stats_query)
        raw_results = stats_connection.fetchall()

        column_names = [desc[0] for desc in stats_connection.description]
        stats_result = [dict(zip(column_names, row)) for row in raw_results]

        nodes, children, root_nodes = self.build_operator_tree(stats_result)

        self._write_output(f"\n=== Analyzing Query {query_id} ===")

        self._write_output(f"\n{'='*80}")
        self._write_output("QUERY EXECUTION INFORMATION")
        self._write_output(f"{'='*80}")

        if execution_time_ms is not None:
            self._write_output(
                f"Query Execution Time: {float(execution_time_ms)/1000:.3f} seconds"
            )
        else:
            self._write_output("Query Execution Time: N/A")

        if start_time and end_time:
            self._write_output(f"Query Start Time: {start_time}")
            self._write_output(f"Query End Time: {end_time}")

        self._write_output("\nQuery Text:")
        formatted_sql = (
            str(sql_text).strip() if str(sql_text) != "N/A" else str(sql_text)
        )
        if len(formatted_sql) > 500 and not verbose:
            self._write_output(f"{formatted_sql[:500]}...")
            self._write_output(
                "(Query truncated for display - set verbose = True to see full output)"
            )
        else:
            self._write_output(formatted_sql)

        if len(stats_result) > 0:
            self._write_output(f"\n{'='*80}")
            self._write_output("QUERY OPERATOR TREE")
            self._write_output(f"{'='*80}")

            root_list = sorted(list(root_nodes))
            for i, root_id in enumerate(root_list):
                is_last_root = i == len(root_list) - 1
                self.print_operator_tree(nodes, children, root_id, "", is_last_root)

            self._write_output(f"\n{'='*160}")
            self._write_output("DETAILED OPERATOR STATISTICS")
            self._write_output(f"{'='*160}")
            self._write_output(
                f"{'Operator':<15} {'Type':<15} {'Input Rows':<12} {'Output Rows':<12} {'Row Multiple':<12} {'Overall %':<12} {'Attributes':<50}",
            )
            self._write_output(f"{'='*160}")

            for row in stats_result:
                node_info = self._get_node_info(row)
                operator_attrs = (
                    node_info["attributes"].replace("\n", " ").replace("  ", " ")
                )

                self._write_output(
                    f"{node_info['id']:<15} {node_info['type']:<15} {node_info['input_rows']:<12} {node_info['output_rows']:<12} {float(node_info['row_multiple']):<12.2f} {node_info['exec_time']:<12} {operator_attrs:<50}",
                )

            self._write_output(f"{'='*160}")
