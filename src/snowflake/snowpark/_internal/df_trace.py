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


def is_tracing_enabled() -> bool:
    """Whether a debug feature that needs the DataFrame trace is on."""
    from snowflake.snowpark import context

    return bool(
        context._enable_dataframe_trace_on_error
        or context._enable_trace_sql_errors_to_dataframe
    )


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
    if not is_tracing_enabled():
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
