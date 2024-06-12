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

    def calculate_cumulative_node_complexity(self) -> Dict[str, int]:
        # frame_type BETWEEN lower AND upper
        return sum_node_complexities(
            self.individual_node_complexity,
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
    def individual_node_complexity(self) -> Dict[str, int]:
        score = {}
        score = (
            sum_node_complexities(score, {PlanNodeCategory.PARTITION_BY.value: 1})
            if self.partition_spec
            else score
        )
        score = (
            sum_node_complexities(score, {PlanNodeCategory.ORDER_BY.value: 1})
            if self.order_spec
            else score
        )
        return score

    def calculate_cumulative_node_complexity(self) -> Dict[str, int]:
        # partition_spec order_by_spec frame_spec
        return sum_node_complexities(
            self.individual_node_complexity,
            self.frame_spec.cumulative_node_complexity,
            *(expr.cumulative_node_complexity for expr in self.partition_spec),
            *(expr.cumulative_node_complexity for expr in self.order_spec),
        )


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

    def calculate_cumulative_node_complexity(self) -> Dict[str, int]:
        # window_function OVER ( window_spec )
        return sum_node_complexities(
            self.window_function.cumulative_node_complexity,
            self.window_spec.cumulative_node_complexity,
            self.individual_node_complexity,
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
    def individual_node_complexity(self) -> Dict[str, int]:
        # for func_name
        score = {PlanNodeCategory.FUNCTION.value: 1}
        # for offset
        score = (
            sum_node_complexities(score, {PlanNodeCategory.LITERAL.value: 1})
            if self.offset
            else score
        )

        # for ignore nulls
        score = (
            sum_node_complexities(score, {PlanNodeCategory.LOW_IMPACT.value: 1})
            if self.ignore_nulls
            else score
        )
        return score

    def calculate_cumulative_node_complexity(self) -> Dict[str, int]:
        # func_name (expr [, offset] [, default]) [IGNORE NULLS]
        score = sum_node_complexities(
            self.individual_node_complexity, self.expr.cumulative_node_complexity
        )
        score = (
            sum_node_complexities(score, self.default.cumulative_node_complexity)
            if self.default
            else score
        )
        return score


class Lag(RankRelatedFunctionExpression):
    sql = "LAG"


class Lead(RankRelatedFunctionExpression):
    sql = "LEAD"


class LastValue(RankRelatedFunctionExpression):
    sql = "LAST_VALUE"


class FirstValue(RankRelatedFunctionExpression):
    sql = "FIRST_VALUE"
