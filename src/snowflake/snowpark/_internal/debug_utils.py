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
from snowflake.snowpark._internal.ast.batch import get_dependent_bind_ids
from snowflake.snowpark._internal.ast.utils import __STRING_INTERNING_MAP__
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from ast import literal_eval
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

    if plan.df_ast_ids is not None:
        for ast_id in plan.df_ast_ids:
            bind_stmt = plan.session._ast_batch._bind_stmt_cache.get(ast_id)
            if bind_stmt is not None:
                src = extract_src_from_expr(bind_stmt.bind.expr)
                location = _format_source_location(src)
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
    if top_plan.df_ast_ids is not None:
        for ast_id in top_plan.df_ast_ids:
            bind_stmt = top_plan.session._ast_batch._bind_stmt_cache.get(ast_id)
            if bind_stmt is not None:
                src = extract_src_from_expr(bind_stmt.bind.expr)
                location = _format_source_location(src)
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

    bind_stmt_cache = top_plan.session._ast_batch._bind_stmt_cache

    for _, stmt in bind_stmt_cache.items():
        if hasattr(stmt, "bind") and hasattr(stmt.bind, "expr"):
            expr = stmt.bind.expr

            # case when we create an object by using .save_as_table()
            if expr.HasField("write_table"):
                write_table_expr = expr.write_table
                expr_object_name = None
                if write_table_expr.table_name.HasField(
                    "name"
                ) and write_table_expr.table_name.name.HasField("name_flat"):
                    expr_object_name = write_table_expr.table_name.name.name_flat.name
                elif write_table_expr.table_name.HasField(
                    "name"
                ) and write_table_expr.table_name.name.HasField("name_structured"):
                    expr_object_name = (
                        write_table_expr.table_name.name.name_structured.name[-1]
                    )

                if expr_object_name and object_name_match(
                    expr_object_name, object_name
                ):
                    location = _format_source_location(write_table_expr.src)
                    if location:
                        return f"\nObject '{object_name}' was first referenced at {location}.\n"

            # case when we create an object by using session.sql()
            elif expr.HasField("sql"):
                sql_expr = expr.sql
                if sql_contains_object_creation(sql_expr.query, object_name):
                    location = _format_source_location(sql_expr.src)
                    if location:
                        return f"\nObject '{object_name}' was first referenced at {location}.\n"

            # case when we create a view by using create_temp_view(), create_or_replace_view(), create_or_replace_temp_view()
            elif expr.HasField("dataframe_create_or_replace_view"):
                create_view_expr = expr.dataframe_create_or_replace_view
                expr_object_name = None
                if create_view_expr.name.HasField(
                    "name"
                ) and create_view_expr.name.name.HasField("name_flat"):
                    expr_object_name = create_view_expr.name.name.name_flat.name
                elif create_view_expr.name.HasField(
                    "name"
                ) and create_view_expr.name.name.HasField("name_structured"):
                    expr_object_name = create_view_expr.name.name.name_structured.name[
                        -1
                    ]
                if expr_object_name and object_name_match(
                    expr_object_name, object_name
                ):
                    location = _format_source_location(create_view_expr.src)
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
