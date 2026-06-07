#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Except,
    Intersect,
    Join,
    SetOperation,
    Union,
)
from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    FunctionExpression,
    NamedFunctionExpression,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Cast
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LeafNode,
    Limit,
    LogicalPlan,
    Range,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    Distinct,
    Filter,
    Pivot,
    Project,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.analyzer.window_expression import WindowExpression

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

PLAN_FINGERPRINT_VERSION = 1
MAX_FINGERPRINT_ENTRIES = 256

_FUNCTION_ALIASES = {
    "sum_": "sum",
    "count_": "count",
    "avg_": "avg",
    "min_": "min",
    "max_": "max",
}


@dataclass(frozen=True)
class PlanFingerprint:
    plan_operators: list[str]
    plan_functions_ordered: list[str]
    plan_cast_types_ordered: list[str]
    plan_is_redacted: bool
    plan_fingerprint_version: int = PLAN_FINGERPRINT_VERSION

    def snowpark_fp_skeleton(self) -> str:
        payload = (
            " ".join(self.plan_operators)
            + "|F:"
            + ",".join(self.plan_functions_ordered)
            + "|C:"
            + ",".join(self.plan_cast_types_ordered)
        )
        if self.plan_is_redacted and not self.plan_operators:
            payload += f"|V:{self.plan_fingerprint_version}"
        return hashlib.md5(payload.encode()).hexdigest()


def _normalize_function_name(name: str) -> str:
    lowered = name.lower()
    return _FUNCTION_ALIASES.get(lowered, lowered)


def _append_limited(values: list[str], value: str, redacted: list[bool]) -> None:
    if len(values) >= MAX_FINGERPRINT_ENTRIES:
        redacted[0] = True
        return
    values.append(value)


def _operator_token(node: LogicalPlan) -> str | None:
    if isinstance(node, LeafNode):
        return "read"
    if isinstance(node, Range):
        return "read"
    if isinstance(node, Filter):
        return "filter"
    if isinstance(node, Join):
        return "join"
    if isinstance(node, Aggregate):
        return "aggregate"
    if isinstance(node, Project):
        return "project"
    if isinstance(node, Sort):
        return "sort"
    if isinstance(node, Limit):
        return "limit"
    if isinstance(node, Distinct):
        return "distinct"
    if isinstance(node, Union):
        return "union"
    if isinstance(node, Intersect):
        return "intersect"
    if isinstance(node, Except):
        return "except"
    if isinstance(node, SetOperation):
        return "union"
    if isinstance(node, Pivot):
        return "pivot"
    if isinstance(node, Unpivot):
        return "unpivot"
    if isinstance(node, Sample):
        return "sample"
    return None


def _walk_expression(
    expr: Expression | None,
    operators: list[str],
    functions: list[str],
    casts: list[str],
    redacted: list[bool],
) -> None:
    if expr is None or redacted[0]:
        return

    if isinstance(expr, WindowExpression):
        _append_limited(operators, "window", redacted)
        _walk_expression(expr.window_function, operators, functions, casts, redacted)
        _walk_expression(expr.window_spec, operators, functions, casts, redacted)
        return

    if isinstance(expr, FunctionExpression):
        _append_limited(functions, _normalize_function_name(expr.name), redacted)
    elif isinstance(expr, NamedFunctionExpression):
        _append_limited(functions, _normalize_function_name(expr.name), redacted)
    elif isinstance(expr, Cast):
        _append_limited(casts, str(expr.to), redacted)

    for child in getattr(expr, "children", None) or []:
        _walk_expression(child, operators, functions, casts, redacted)

    window_function = getattr(expr, "window_function", None)
    if window_function is not None:
        _walk_expression(window_function, operators, functions, casts, redacted)
    window_spec = getattr(expr, "window_spec", None)
    if window_spec is not None:
        _walk_expression(window_spec, operators, functions, casts, redacted)

    for child_expr in getattr(expr, "expressions", None) or []:
        _walk_expression(child_expr, operators, functions, casts, redacted)


def _collect_node_expressions(
    node: LogicalPlan,
    operators: list[str],
    functions: list[str],
    casts: list[str],
    redacted: list[bool],
) -> None:
    if isinstance(node, Aggregate):
        for expr in node.grouping_expressions + node.aggregate_expressions:
            _walk_expression(expr, operators, functions, casts, redacted)
    elif isinstance(node, Filter):
        _walk_expression(node.condition, operators, functions, casts, redacted)
    elif isinstance(node, Project):
        for expr in node.project_list:
            _walk_expression(expr, operators, functions, casts, redacted)
    elif isinstance(node, Join):
        _walk_expression(node.join_condition, operators, functions, casts, redacted)
        _walk_expression(node.match_condition, operators, functions, casts, redacted)
    elif isinstance(node, Sort):
        for order in node.order:
            _walk_expression(order, operators, functions, casts, redacted)


def _visit_logical_plan(
    node: LogicalPlan,
    operators: list[str],
    functions: list[str],
    casts: list[str],
    redacted: list[bool],
) -> None:
    if redacted[0]:
        return
    for child in node.children:
        _visit_logical_plan(child, operators, functions, casts, redacted)
    token = _operator_token(node)
    if token is not None:
        _append_limited(operators, token, redacted)
    _collect_node_expressions(node, operators, functions, casts, redacted)


def extract_plan_fingerprint_from_logical_plan(
    root: LogicalPlan | None,
) -> PlanFingerprint:
    operators: list[str] = []
    functions: list[str] = []
    casts: list[str] = []
    redacted = [False]
    if root is not None:
        _visit_logical_plan(root, operators, functions, casts, redacted)
    return PlanFingerprint(
        plan_operators=operators,
        plan_functions_ordered=functions,
        plan_cast_types_ordered=casts,
        plan_is_redacted=redacted[0],
    )


def extract_plan_fingerprint(plan: SnowflakePlan) -> PlanFingerprint:
    return extract_plan_fingerprint_from_logical_plan(plan.source_plan)
