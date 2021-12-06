from typing import Any, List, Optional

from snowflake.snowpark._internal.plans.logical.logical_plan import (
    BinaryNode,
    LeafNode,
    LogicalPlan as LogicalPlan,
    UnaryNode,
)
from snowflake.snowpark._internal.sp_expressions import (
    Expression as Expression,
    SortOrder as SortOrder,
)

class Join(BinaryNode):
    left: Any
    right: Any
    join_type: Any
    condition: Any
    hint: Any
    children: Any
    def __init__(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        join_type: JoinType,
        condition: Optional[Expression],
        hint: JoinHint = ...,
    ) -> None: ...
    @property
    def sql(self): ...

class Range(LeafNode):
    start: Any
    end: Any
    step: Any
    num_slices: Any
    def __init__(self, start, end, step, num_slices: int = ...) -> None: ...

class Aggregate(UnaryNode):
    grouping_expressions: Any
    aggregate_expressions: Any
    child: Any
    def __init__(self, grouping_expressions, aggregate_expressions, child) -> None: ...

class Pivot(UnaryNode):
    pivot_column: Any
    pivot_values: Any
    aggregates: Any
    child: Any
    def __init__(
        self,
        pivot_column: Expression,
        pivot_values: List[Expression],
        aggregates: List[Expression],
        child: LogicalPlan,
    ) -> None: ...

class Sort(UnaryNode):
    order: Any
    is_global: Any
    child: Any
    def __init__(
        self, order: List[SortOrder], is_global: bool, child: LogicalPlan
    ) -> None: ...

class SetOperation(BinaryNode):
    left: Any
    right: Any
    def __init__(self, left: LogicalPlan, right: LogicalPlan) -> None: ...

class Intersect(SetOperation):
    children: Any
    def __init__(self, left: LogicalPlan, right: LogicalPlan) -> None: ...
    def node_name(self): ...
    @property
    def sql(self): ...

class Union(SetOperation):
    is_all: Any
    children: Any
    def __init__(self, left: LogicalPlan, right: LogicalPlan, is_all: bool) -> None: ...
    def node_name(self): ...
    @property
    def sql(self): ...

class Except(SetOperation):
    children: Any
    def __init__(self, left: LogicalPlan, right: LogicalPlan) -> None: ...
    def node_name(self): ...
    @property
    def sql(self): ...
