#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional, Set

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)


class BinaryExpression(Expression):
    sql_operator: str

    def __init__(self, left: Expression, right: Expression) -> None:
        super().__init__()
        self.left = left
        self.right = right
        self.children = [self.left, self.right]
        self.nullable = self.left.nullable or self.right.nullable

    def __str__(self):
        return f"{self.left} {self.sql_operator} {self.right}"

    def dependent_column_names(self) -> Optional[Set[str]]:
        return derive_dependent_columns(self.left, self.right)


class BinaryArithmeticExpression(BinaryExpression):
    pass


class EqualTo(BinaryArithmeticExpression):
    sql_operator = "="


class NotEqualTo(BinaryArithmeticExpression):
    sql_operator = "!="


class GreaterThan(BinaryArithmeticExpression):
    sql_operator = ">"


class LessThan(BinaryArithmeticExpression):
    sql_operator = "<"


class GreaterThanOrEqual(BinaryArithmeticExpression):
    sql_operator = ">="


class LessThanOrEqual(BinaryArithmeticExpression):
    sql_operator = "<="


class EqualNullSafe(BinaryExpression):
    sql_operator = "EQUAL_NULL"


class And(BinaryArithmeticExpression):
    sql_operator = "AND"


class Or(BinaryArithmeticExpression):
    sql_operator = "OR"


class Add(BinaryArithmeticExpression):
    sql_operator = "+"


class Subtract(BinaryArithmeticExpression):
    sql_operator = "-"


class Multiply(BinaryArithmeticExpression):
    sql_operator = "*"


class Divide(BinaryArithmeticExpression):
    sql_operator = "/"


class Remainder(BinaryArithmeticExpression):
    sql_operator = "%"


class Pow(BinaryExpression):
    sql_operator = "POWER"


class BitwiseAnd(BinaryExpression):
    sql_operator = "BITAND"


class BitwiseOr(BinaryExpression):
    sql_operator = "BITOR"


class BitwiseXor(BinaryExpression):
    sql_operator = "BITXOR"
