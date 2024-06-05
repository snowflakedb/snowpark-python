#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
from typing import AbstractSet, List, Optional

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.materialization_utils import (
    PlanNodeCategory,
    Counter,
)
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder


class SpecialFrameBoundary(Expression):
    sql: str

    def __init__(self) -> None:
        super().__init__()

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({PlanNodeCategory.OTHERS.value: 1})


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
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({PlanNodeCategory.OTHERS.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # frame_type BETWEEN lower AND upper
        return (
            self.individual_complexity_stat
            + self.lower.cumulative_complexity_stat
            + self.upper.cumulative_complexity_stat
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
    def individual_complexity_stat(self) -> Counter[str]:
        estimate = Counter()
        estimate += (
            Counter({PlanNodeCategory.PARTITION_BY.value: 1})
            if self.partition_spec
            else Counter()
        )
        estimate += (
            Counter({PlanNodeCategory.ORDER_BY.value: 1})
            if self.order_spec
            else Counter()
        )
        return estimate

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # partition_spec order_by_spec frame_spec
        return (
            self.individual_complexity_stat
            + sum(
                (expr.cumulative_complexity_stat for expr in self.partition_spec),
                Counter(),
            )
            + sum(
                (expr.cumulative_complexity_stat for expr in self.order_spec), Counter()
            )
            + self.frame_spec.cumulative_complexity_stat
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
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({PlanNodeCategory.WINDOW.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # window_function OVER ( window_spec )
        return (
            self.window_function.cumulative_complexity_stat
            + self.window_spec.cumulative_complexity_stat
            + self.individual_complexity_stat
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
    def individual_complexity_stat(self) -> Counter[str]:
        # for func_name
        estimate = Counter({PlanNodeCategory.FUNCTION.value: 1})
        # for offset
        estimate += (
            Counter({PlanNodeCategory.LITERAL.value: 1}) if self.offset else Counter()
        )
        # for ignore nulls
        estimate += (
            Counter({PlanNodeCategory.OTHERS.value: 1})
            if self.ignore_nulls
            else Counter()
        )
        return estimate

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # func_name (expr [, offset] [, default]) [IGNORE NULLS]
        estimate = (
            self.individual_complexity_stat + self.expr.cumulative_complexity_stat
        )
        estimate += (
            self.default.cumulative_complexity_stat if self.default else Counter()
        )
        return estimate


class Lag(RankRelatedFunctionExpression):
    sql = "LAG"


class Lead(RankRelatedFunctionExpression):
    sql = "LEAD"


class LastValue(RankRelatedFunctionExpression):
    sql = "LAST_VALUE"


class FirstValue(RankRelatedFunctionExpression):
    sql = "FIRST_VALUE"
