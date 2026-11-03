#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Lightweight tracking of the Python source lines that create DataFrames.

This is the data source of the debug features of
``snowflake.snowpark.context.configure_development_features`` (lineage trace, SQL error to
Python line mapping and object context). It records, for each DataFrame operation, the source
position of the user code that made the call and the DataFrames it was derived from. It does not
depend on the Snowpark AST.
"""

import functools
import inspect
import itertools
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

SNOWPARK_LIB_PATH = Path(__file__).parent.parent.resolve()

# Same switch as the AST implementation uses in its tests.
SRC_POSITION_TEST_MODE = False

_MAX_OBJECT_REGISTRY_SIZE = 10000


@dataclass(frozen=True)
class SrcPosition:
    """Where in the user's code a DataFrame operation was called.

    Columns are only recorded on Python 3.11+, like in the AST implementation.
    """

    filename: str
    start_line: int
    start_column: int = 0
    end_line: int = 0
    end_column: int = 0


_node_ids = itertools.count(1)
_node_ids_lock = threading.Lock()


def _next_node_id() -> int:
    with _node_ids_lock:
        return next(_node_ids)


@dataclass(eq=False)
class TraceNode:
    """A DataFrame operation (or other API object) and the nodes it was derived from."""

    src: Optional[SrcPosition]
    parents: List["TraceNode"] = field(default_factory=list)
    kind: str = "dataframe"
    node_id: int = field(default_factory=_next_node_id)

    def __deepcopy__(self, memo) -> "TraceNode":
        # Trace nodes are shared, not duplicated, when plans are deep-copied.
        return self

    def __copy__(self) -> "TraceNode":
        return self

    @property
    def source_id(self) -> str:
        """Unique identifier of the location of the operation in the source code."""
        if self.src is None:
            return ""
        s = self.src
        return (
            f"{s.filename}:{s.start_line}:{s.start_column}-{s.end_line}:{s.end_column}"
        )


def capture_user_src_position(
    caller_frame_depth: Optional[int] = None,
) -> Optional[SrcPosition]:
    """Returns the position of the code of interest in the user's code.

    The frame walk is the one of ``ast.utils.with_src_position``: it steps out of the Snowpark
    package (or back ``caller_frame_depth`` frames) from the caller's caller. Returns None if the
    frame cannot be determined.
    """
    frame = inspect.currentframe()
    try:
        if frame is None:
            return None
        if caller_frame_depth is None:
            frame = frame.f_back.f_back  # type: ignore[union-attr]
            while (
                frame is not None
                and SNOWPARK_LIB_PATH in Path(frame.f_code.co_filename).parents
            ):
                frame = frame.f_back
        else:
            depth = 0
            while frame is not None and depth < caller_frame_depth:
                frame = frame.f_back
                depth += 1
        if frame is None:
            return None

        code = frame.f_code
        filename = (
            code.co_filename if not SRC_POSITION_TEST_MODE else "SRC_POSITION_TEST_MODE"
        )
        start_line = frame.f_lineno
        start_column = end_line = end_column = 0
        if sys.version_info >= (3, 11) and frame.f_lasti >= 0:
            # The same positions inspect.getframeinfo reports, without reading the source file.
            pos = next(itertools.islice(code.co_positions(), frame.f_lasti // 2, None))
            if pos[0] is not None:
                start_line = pos[0]
            if pos[1] is not None:
                end_line = pos[1]
            if pos[2] is not None:
                start_column = pos[2]
            if pos[3] is not None:
                end_column = pos[3]
        return SrcPosition(filename, start_line, start_column, end_line, end_column)
    finally:
        del frame


def is_tracing_enabled(session=None) -> bool:
    """Whether a debug feature that needs the DataFrame trace is on.

    The trace is not supported for local testing sessions.
    """
    from snowflake.snowpark import context

    if not (
        context._enable_dataframe_trace_on_error
        or context._enable_trace_sql_errors_to_dataframe
    ):
        return False
    if session is not None:
        from snowflake.snowpark.mock._connection import MockServerConnection

        if isinstance(getattr(session, "_conn", None), MockServerConnection):
            return False
    return True


@dataclass(frozen=True)
class ObjectCreation:
    """A call that may create a Snowflake object, kept for the 'already exists' context."""

    kind: str  # "table", "view" or "sql"
    name_or_query: str
    src: Optional[SrcPosition]


class ObjectCreationRegistry:
    """Bounded, ordered record of the object creating calls of a session."""

    def __init__(self, max_size: int = _MAX_OBJECT_REGISTRY_SIZE) -> None:
        self._max_size = max_size
        self._items: List[ObjectCreation] = []
        self._lock = threading.Lock()

    def add(self, kind: str, name_or_query: str, src: Optional[SrcPosition]) -> None:
        with self._lock:
            if len(self._items) >= self._max_size:
                del self._items[0]
            self._items.append(ObjectCreation(kind, name_or_query, src))

    def items(self) -> List[ObjectCreation]:
        with self._lock:
            return list(self._items)


def _object_name(name) -> str:
    """A table or view name as the 'already exists' context compares it: a flat name as given, the
    last part of a structured name."""
    if isinstance(name, str):
        return name
    try:
        return list(name)[-1]
    except Exception:  # pragma: no cover
        return str(name)


def record_object_creation(session, kind: str, name_or_query) -> None:
    """Remembers a call that may create a Snowflake object, if tracing is enabled."""
    if not is_tracing_enabled(session):
        return
    registry = getattr(session, "_df_trace_registry", None)
    if registry is None:
        return
    value = name_or_query if kind == "sql" else _object_name(name_or_query)
    registry.add(kind, value, capture_user_src_position())


_MAX_ANCESTOR_SEARCH = 2000
_MAX_PLAN_DEPTH = 100000


def _is_ancestor(candidate: TraceNode, node: TraceNode) -> bool:
    """Whether ``candidate`` is reachable from ``node`` through parent links."""
    seen = set()
    stack = list(node.parents)
    while stack and len(seen) < _MAX_ANCESTOR_SEARCH:
        cur = stack.pop()
        if cur is candidate:
            return True
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        stack.extend(cur.parents)
    return False


def add_parent(node: TraceNode, parent: Optional[TraceNode]) -> None:
    """Adds ``parent`` as an input of ``node``. Inputs are kept in creation order."""
    if parent is None or parent is node or any(p is parent for p in node.parents):
        return
    node.parents.append(parent)
    node.parents.sort(key=lambda n: n.node_id)


def traces_inputs(*argument_names: str):
    """Decorator for dataframe methods that take other dataframes as arguments.

    The trace node of the returned dataframe gets an input for each of the named arguments that
    is a dataframe, like the AST records a reference to every dataframe an operation uses.
    """

    def decorator(func):
        signature = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            node = getattr(result, "_trace_node", None)
            if node is not None:
                bound = signature.bind_partial(*args, **kwargs).arguments
                for name in argument_names:
                    add_parent(node, getattr(bound.get(name), "_trace_node", None))
            return result

        return wrapper

    return decorator


def collect_input_nodes(*plans) -> List[TraceNode]:
    """Finds the trace nodes of the DataFrames that the given plans were built from.

    For each plan the most recent trace node it carries is taken. Plans without a trace node are
    searched through their children until the nearest trace nodes are found. Nodes that are
    ancestors of other found nodes are dropped, so that only the direct inputs remain.
    """
    found: List[TraceNode] = []
    visited = set()
    stack = [p for p in reversed(plans) if p is not None]
    steps = 0
    while stack and steps < _MAX_PLAN_DEPTH:
        steps += 1
        plan = stack.pop()
        if id(plan) in visited:
            continue
        visited.add(id(plan))
        nodes = getattr(plan, "df_trace_nodes", None)
        if nodes:
            if nodes[-1] not in found:
                found.append(nodes[-1])
            continue
        try:
            children = plan.children_plan_nodes
        except Exception:  # pragma: no cover
            children = None
        if children:
            stack.extend(reversed(children))
    result = []
    for cand in found:
        if not any(o is not cand and _is_ancestor(cand, o) for o in found):
            result.append(cand)
    # Visit the inputs in the order they were created, like the order of their statements.
    result.sort(key=lambda n: n.node_id)
    return result


def registration_trace_node(kind: str) -> Optional[TraceNode]:
    """A node for the registration of a UDF, UDTF or UDAF, if tracing is enabled."""
    if not tracing_flags_on():
        return None
    return TraceNode(capture_user_src_position(), [], kind=kind)


# ----------------------------------------------------------------------------- public api calls
# State of the public API calls of the current thread, used while tracing is enabled.
_call_state = threading.local()

_MAX_EXPRESSION_NODES = 10000


_context_module = None


def tracing_flags_on() -> bool:
    """Fast check of the tracing flags for the public API wrapper."""
    global _context_module
    if _context_module is None:
        from snowflake.snowpark import context

        _context_module = context
    return (
        _context_module._enable_dataframe_trace_on_error
        or _context_module._enable_trace_sql_errors_to_dataframe
    )


def public_call_depth() -> int:
    return getattr(_call_state, "depth", 0)


def register_created_dataframe(df) -> None:
    """Remembers a dataframe created during the current outermost public API call."""
    created = getattr(_call_state, "created", None)
    if created is not None:
        created.append(df)


_attribute_names_by_class: dict = {}


def _attribute_names(expr) -> tuple:
    """The attribute names of an expression class, read once per class.

    ``vars()`` creates a ``__dict__`` for every instance it is called on (Python 3.11+ keeps
    instance attributes inline), which would keep extra memory on each scanned expression.
    """
    cls = type(expr)
    names = _attribute_names_by_class.get(cls)
    if names is None:
        names = tuple(vars(expr))
        _attribute_names_by_class[cls] = names
    return names


def _expression_inputs(expression) -> List[TraceNode]:
    """Trace nodes referenced by an expression: dataframes used as subqueries and functions
    (UDF, UDTF, UDAF) that carry the node of their registration."""
    from snowflake.snowpark._internal.analyzer.expression import (
        Expression,
        ScalarSubquery,
    )

    found: List[TraceNode] = []
    seen = set()
    stack = [expression]
    while stack and len(seen) < _MAX_EXPRESSION_NODES:
        expr = stack.pop()
        if id(expr) in seen:
            continue
        seen.add(id(expr))
        node = getattr(expr, "_df_trace_node", None)
        if node is not None:
            found.append(node)
        if isinstance(expr, ScalarSubquery):
            found.extend(collect_input_nodes(expr.plan))
            continue
        pending = [getattr(expr, name, None) for name in _attribute_names(expr)]
        while pending:
            value = pending.pop()
            if isinstance(value, Expression):
                stack.append(value)
            elif isinstance(value, (list, tuple)):
                pending.extend(
                    value
                )  # e.g. the (condition, value) branches of CASE WHEN
            elif isinstance(value, dict):
                pending.extend(value.values())
    return found


def _argument_inputs(args, kwargs) -> List[TraceNode]:
    """Trace nodes referenced by the column arguments of a public API call."""
    from snowflake.snowpark.column import Column
    from snowflake.snowpark.table_function import TableFunctionCall

    found: List[TraceNode] = []
    values = list(args) + list(kwargs.values())
    for value in values:
        items = value if isinstance(value, (list, tuple)) else [value]
        if isinstance(value, dict):
            items = list(value.keys()) + list(value.values())
        for item in items:
            # Only columns and table function calls: getattr on a DataFrame resolves column names.
            if isinstance(item, Column):
                found.extend(_expression_inputs(item._expression))
            elif isinstance(item, TableFunctionCall):
                node = getattr(item, "_df_trace_node", None)
                if node is not None:
                    found.append(node)
    return found


def note_trace_seed(container) -> None:
    """Remembers a plan whose trace list was seeded while its dataframe was being created."""
    seeds = getattr(_call_state, "seeds", None)
    if seeds is not None:
        seeds.append((container, container.df_trace_nodes))


def _has_user_input(node: TraceNode) -> bool:
    """Whether the node was derived from an operation at another source position."""
    seen, stack = set(), list(node.parents)
    while stack and len(seen) < _MAX_ANCESTOR_SEARCH:
        cur = stack.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if cur.source_id != node.source_id:
            return True
        stack.extend(cur.parents)
    return False


def _undo_source_seeds(seeds, returned) -> None:
    """Undoes the trace lists seeded while a source api created its dataframe.

    A source api (create_dataframe, table, sql, readers) gets its AST id after the dataframe is
    created, so the AST never seeds the plans of its construction. Seeds of calls that derive from
    other dataframes stay, and so do seeds of calls that raised: they carry the context of
    construction-time errors.
    """
    if not seeds or any(_has_user_input(df._trace_node) for df in returned):
        return
    for container, seeded in seeds:
        if container.df_trace_nodes is seeded:
            container.df_trace_nodes = None


def _returned_dataframes(result) -> list:
    items = result if isinstance(result, (list, tuple)) else [result]
    return [item for item in items if getattr(item, "_trace_node", None) is not None]


def _strip_internal_nodes(created, returned) -> None:
    """Removes the nodes of the dataframes an API call created internally from the plans.

    Only the dataframes returned to the user are operations of the call; the AST never gave the
    internal ones an id, so their nodes must not appear in the plans' lists either. The lineage
    graph keeps them (their position is the one of the call).
    """
    returned_nodes = {id(df._trace_node) for df in returned}
    internal = {
        id(df._trace_node)
        for df in created
        if df._trace_node is not None and id(df._trace_node) not in returned_nodes
    }
    if not internal:
        return
    for df in created:
        for container in (df._plan, df._select_statement):
            nodes = getattr(container, "df_trace_nodes", None)
            if nodes:
                nodes[:] = [n for n in nodes if id(n) not in internal]
                if not nodes:
                    container.df_trace_nodes = None


def trace_public_call(func, args, kwargs):
    """Runs a public API call while tracing is enabled.

    For the outermost call it links the returned dataframes to the dataframes and functions used
    in its column arguments, and removes the nodes of internal dataframes from the plans.
    """
    depth = public_call_depth()
    _call_state.depth = depth + 1
    if depth == 0:
        _call_state.created = []
        _call_state.seeds = []
    try:
        result = func(*args, **kwargs)
    except BaseException:
        if depth == 0:
            _call_state.created = None
            _call_state.seeds = None
        raise
    finally:
        _call_state.depth = depth
    if depth == 0:
        created, _call_state.created = _call_state.created, None
        seeds, _call_state.seeds = _call_state.seeds, None
        returned = _returned_dataframes(result)
        if returned:
            inputs = _argument_inputs(args, kwargs)
            for df in returned:
                for node in inputs:
                    if node is not df._trace_node:
                        add_parent(df._trace_node, node)
            _strip_internal_nodes(created or [], returned)
            _undo_source_seeds(seeds, returned)
    return result
