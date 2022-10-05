#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Optional, Set

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder


class SpecialFrameBoundary(Expression):
    sql: str

    def __init__(self) -> None:
        super().__init__()


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

    def dependent_column_names(self) -> Optional[Set[str]]:
        return derive_dependent_columns(self.lower, self.upper)


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

    def dependent_column_names(self) -> Optional[Set[str]]:
        return derive_dependent_columns(
            *self.partition_spec, *self.order_spec, self.frame_spec
        )


class WindowExpression(Expression):
    def __init__(
        self, window_function: Expression, window_spec: WindowSpecDefinition
    ) -> None:
        super().__init__()
        self.window_function = window_function
        self.window_spec = window_spec

    def dependent_column_names(self) -> Optional[Set[str]]:
        return derive_dependent_columns(self.window_function, self.window_spec)


class RankRelatedFunctionExpression(Expression):
    sql: str

    def __init__(
        self, expr: Expression, offset: int, default: Expression, ignore_nulls: bool
    ) -> None:
        super().__init__()
        self.expr = expr
        self.offset = offset
        self.default = default
        self.ignore_nulls = ignore_nulls

    def dependent_column_names(self) -> Optional[Set[str]]:
        return derive_dependent_columns(self.expr, self.default)


class Lag(RankRelatedFunctionExpression):
    sql = "LAG"


class Lead(RankRelatedFunctionExpression):
    sql = "LEAD"


class LastValue(RankRelatedFunctionExpression):
    sql = "LAST_VALUE"


class FirstValue(RankRelatedFunctionExpression):
    sql = "FIRST_VALUE"
