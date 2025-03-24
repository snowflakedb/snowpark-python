#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    NamedExpression,
    derive_dependent_columns,
    derive_dependent_columns_with_duplication,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark.types import DataType


class UnaryExpression(Expression):
    sql_operator: str
    operator_first: bool

    def __init__(self, child: Expression) -> None:
        super().__init__()
        self.child = child
        self.nullable = child.nullable
        self.children = [child]
        self.datatype = self.child.datatype

    def __str__(self):
        return (
            f"{self.sql_operator} {self.child}"
            if self.operator_first
            else f"{self.child} {self.sql_operator}"
        )

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.child)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.child)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT


class Cast(UnaryExpression):
    sql_operator = "CAST"
    operator_first = True

    def __init__(
        self,
        child: Expression,
        to: DataType,
        try_: bool = False,
        is_rename: bool = False,
        is_add: bool = False,
    ) -> None:
        super().__init__(child)
        self.to = to
        self.try_ = try_
        self.is_rename = is_rename
        self.is_add = is_add


class UnaryMinus(UnaryExpression):
    sql_operator = "-"
    operator_first = True


class IsNull(UnaryExpression):
    sql_operator = "IS NULL"
    operator_first = False


class IsNotNull(UnaryExpression):
    sql_operator = "IS NOT NULL"
    operator_first = False


class IsNaN(UnaryExpression):
    sql_operator = "= 'NAN'"
    operator_first = False


class Not(UnaryExpression):
    sql_operator = "NOT"
    operator_first = True


class Alias(UnaryExpression, NamedExpression):
    sql_operator = "AS"
    operator_first = False

    def __init__(self, child: Expression, name: str) -> None:
        super().__init__(child)
        self.name = name

    def __str__(self):
        return f"{self.child} {self.sql_operator} {self.name}"

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # do not add additional complexity for alias
        return {}


class _InternalAlias(Alias):
    pass


class UnresolvedAlias(UnaryExpression, NamedExpression):
    sql_operator = "AS"
    operator_first = False

    def __init__(self, child: Expression) -> None:
        super().__init__(child)
        self.name = child.sql

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # this is a wrapper around child
        return {}
