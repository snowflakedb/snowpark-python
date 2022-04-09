#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.analyzer.expression import Expression, NamedExpression
from snowflake.snowpark.types import DataType


class UnaryExpression(Expression):
    sql_operator: str
    operator_first: bool

    def __init__(self, child: Expression):
        super().__init__()
        self.child = child
        self.nullable = child.nullable
        self.children = [child]
        self.datatype = self.child.datatype

    def children(self) -> List[Expression]:
        return self.children

    def __repr__(self):
        return (
            f"{self.sql_operator} {self.child}"
            if self.operator_first
            else f"{self.child} {self.sql_operator}"
        )


class Cast(UnaryExpression):
    sql_operator = "CAST"
    operator_first = True

    def __init__(self, child: Expression, to: DataType, try_: bool = False):
        super().__init__(child)
        self.to = to
        self.try_ = try_


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
    sql_operator = "= 'NaN'"
    operator_first = False


class Not(UnaryExpression):
    sql_operator = "NOT"
    operator_first = True


class Alias(UnaryExpression, NamedExpression):
    sql_operator = "AS"
    operator_first = False

    def __init__(self, child: Expression, name: str):
        super().__init__(child)
        self.name = name

    def __repr__(self):
        return f"{self.child} {self.sql_operator} {self.name}"


class UnresolvedAlias(UnaryExpression, NamedExpression):
    sql_operator = "AS"
    operator_first = False

    def __init__(self, child: Expression):
        super().__init__(child)
        self.name = child.sql
