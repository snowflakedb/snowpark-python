#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder


class SpecialFrameBoundary(Expression):
    sql: str

    def __init__(self) -> None:
        super().__init__()

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT


class UnboundedPreceding(SpecialFrameBoundary):
    sql = "UNBOUNDED PRECEDING"


class UnboundedFollowing(SpecialFrameBoundary):
    sql = "UNBOUNDED FOLLOWING"


class CurrentRow(SpecialFrameBoundary):
    sql = "CURRENT ROW"


class FrameType:
    sql: str


class RowFrame(FrameType):
    sql = "ROWS"


class RangeFrame(FrameType):
    sql = "RANGE"


class WindowFrame(Expression):
    def __init__(self) -> None:
        super().__init__()


class UnspecifiedFrame(WindowFrame):
    pass


class SpecifiedWindowFrame(WindowFrame):
    def __init__(
        self, frame_type: FrameType, lower: Expression, upper: Expression
    ) -> None:
        super().__init__()
        self.frame_type = frame_type
        self.lower = lower
        self.upper = upper

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.lower, self.upper)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # frame_type BETWEEN lower AND upper
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.lower.cumulative_node_complexity,
            self.upper.cumulative_node_complexity,
        )


class WindowSpecDefinition(Expression):
    def __init__(
        self,
        partition_spec: List[Expression],
        order_spec: List[SortOrder],
        frame_spec: WindowFrame,
    ) -> None:
        super().__init__()
        self.partition_spec = partition_spec
        self.order_spec = order_spec
        self.frame_spec = frame_spec

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(
            *self.partition_spec, *self.order_spec, self.frame_spec
        )

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # partition_spec order_by_spec frame_spec
        complexity = self.frame_spec.cumulative_node_complexity
        complexity = (
            sum_node_complexities(
                complexity,
                {PlanNodeCategory.PARTITION_BY: 1},
                *(expr.cumulative_node_complexity for expr in self.partition_spec),
            )
            if self.partition_spec
            else complexity
        )
        complexity = (
            sum_node_complexities(
                complexity,
                {PlanNodeCategory.ORDER_BY: 1},
                *(expr.cumulative_node_complexity for expr in self.order_spec),
            )
            if self.order_spec
            else complexity
        )
        return complexity


class WindowExpression(Expression):
    def __init__(
        self, window_function: Expression, window_spec: WindowSpecDefinition
    ) -> None:
        super().__init__()
        self.window_function = window_function
        self.window_spec = window_spec

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.window_function, self.window_spec)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.WINDOW

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # window_function OVER ( window_spec )
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.window_function.cumulative_node_complexity,
            self.window_spec.cumulative_node_complexity,
        )


class RankRelatedFunctionExpression(Expression):
    sql: str

    def __init__(
        self,
        expr: Expression,
        offset: int,
        default: Optional[Expression],
        ignore_nulls: bool,
    ) -> None:
        super().__init__()
        self.expr = expr
        self.offset = offset
        self.default = default
        self.ignore_nulls = ignore_nulls

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, self.default)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # for func_name
        complexity = {PlanNodeCategory.FUNCTION: 1}
        # for offset
        complexity = (
            sum_node_complexities(complexity, {PlanNodeCategory.LITERAL: 1})
            if self.offset
            else complexity
        )

        # for ignore nulls
        complexity = (
            sum_node_complexities(complexity, {PlanNodeCategory.LOW_IMPACT: 1})
            if self.ignore_nulls
            else complexity
        )
        # func_name (expr [, offset] [, default]) [IGNORE NULLS]
        complexity = sum_node_complexities(
            complexity, self.expr.cumulative_node_complexity
        )
        complexity = (
            sum_node_complexities(complexity, self.default.cumulative_node_complexity)
            if self.default
            else complexity
        )
        return complexity


class Lag(RankRelatedFunctionExpression):
    sql = "LAG"


class Lead(RankRelatedFunctionExpression):
    sql = "LEAD"


class LastValue(RankRelatedFunctionExpression):
    sql = "LAST_VALUE"


class FirstValue(RankRelatedFunctionExpression):
    sql = "FIRST_VALUE"
