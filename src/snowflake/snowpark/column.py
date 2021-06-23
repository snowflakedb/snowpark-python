#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from typing import Optional

from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.internal.sp_expressions import (
    Add as SPAdd,
    Alias as SPAlias,
    And as SPAnd,
    Ascending as SPAscending,
    BitwiseAnd as SPBitwiseAnd,
    BitwiseOr as SPBitwiseOr,
    BitwiseXor as SPBitwiseXor,
    Cast as SPCast,
    Descending as SPDescending,
    Divide as SPDivide,
    EqualNullSafe as SPEqualNullSafe,
    EqualTo as SPEqualTo,
    Expression as SPExpression,
    GreaterThan as SPGreaterThan,
    GreaterThanOrEqual as SPGreaterThanOrEqual,
    IsNaN as SPIsNaN,
    IsNotNull as SPIsNotNull,
    IsNull as SPIsNull,
    LessThan as SPLessThan,
    LessThanOrEqual as SPLessThanOrEqual,
    Literal as SPLiteral,
    Multiply as SPMultiply,
    NamedExpression as SPNamedExpression,
    Not as SPNot,
    NotEqualTo as SPNotEqualTo,
    NullsFirst as SPNullFirst,
    NullsLast as SPNullLast,
    Or as SPOr,
    Pow as SPPow,
    Remainder as SPRemainder,
    SortOrder as SPSortOrder,
    Subtract as SPSubtract,
    UnaryMinus as SPUnaryMinus,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
    UnresolvedStar as SPUnresolvedStar,
)
from src.snowflake.snowpark.types.sf_types import DataType


class Column:
    def __init__(self, expr):
        if type(expr) == str:
            self.expression = self.__get_expr(expr)
        elif isinstance(expr, SPExpression):
            self.expression = expr
        else:
            raise Exception("Column ctor takes only str or expression.")

    # TODO make subscriptable

    # Overload operators.
    def __eq__(self, other) -> "Column":
        right = self.__to_expr(other)
        return self.with_expr(SPEqualTo(self.expression, right))

    def __ne__(self, other) -> "Column":
        right = self.__to_expr(other)
        return self.with_expr(SPNotEqualTo(self.expression, right))

    def __gt__(self, other) -> "Column":
        return self.with_expr(SPGreaterThan(self.expression, self.__to_expr(other)))

    def __lt__(self, other) -> "Column":
        return self.with_expr(SPLessThan(self.expression, self.__to_expr(other)))

    def __ge__(self, other) -> "Column":
        return self.with_expr(
            SPGreaterThanOrEqual(self.expression, self.__to_expr(other))
        )

    def __le__(self, other) -> "Column":
        return self.with_expr(SPLessThanOrEqual(self.expression, self.__to_expr(other)))

    def __add__(self, other) -> "Column":
        return self.with_expr(SPAdd(self.expression, self.__to_expr(other)))

    def __radd__(self, other) -> "Column":
        return self.with_expr(SPAdd(self.__to_expr(other), self.expression))

    def __sub__(self, other) -> "Column":
        return self.with_expr(SPSubtract(self.expression, self.__to_expr(other)))

    def __rsub__(self, other) -> "Column":
        return self.with_expr(SPSubtract(self.__to_expr(other), self.expression))

    def __mul__(self, other) -> "Column":
        return self.with_expr(SPMultiply(self.expression, self.__to_expr(other)))

    def __rmul__(self, other) -> "Column":
        return self.with_expr(SPMultiply(self.__to_expr(other), self.expression))

    def __truediv__(self, other) -> "Column":
        return self.with_expr(SPDivide(self.expression, self.__to_expr(other)))

    def __rtruediv__(self, other) -> "Column":
        return self.with_expr(SPDivide(self.__to_expr(other), self.expression))

    def __mod__(self, other) -> "Column":
        return self.with_expr(SPRemainder(self.expression, self.__to_expr(other)))

    def __rmod__(self, other) -> "Column":
        return self.with_expr(SPRemainder(self.__to_expr(other), self.expression))

    def __pow__(self, other) -> "Column":
        return self.with_expr(SPPow(self.expression, self.__to_expr(other)))

    def __rpow__(self, other) -> "Column":
        return self.with_expr(SPPow(self.__to_expr(other), self.expression))

    def bitand(self, other) -> "Column":
        return self.with_expr(SPBitwiseAnd(self.__to_expr(other), self.expression))

    def bitor(self, other) -> "Column":
        return self.with_expr(SPBitwiseOr(self.__to_expr(other), self.expression))

    def bitxor(self, other) -> "Column":
        return self.with_expr(SPBitwiseXor(self.__to_expr(other), self.expression))

    def __neg__(self) -> "Column":
        return self.with_expr(SPUnaryMinus(self.expression))

    def equal_null(self, other) -> "Column":
        return self.with_expr(SPEqualNullSafe(self.expression, self.__to_expr(other)))

    def equal_nan(self) -> "Column":
        return self.with_expr(SPIsNaN(self.expression))

    def is_null(self) -> "Column":
        return self.with_expr(SPIsNull(self.expression))

    def is_not_null(self) -> "Column":
        return self.with_expr(SPIsNotNull(self.expression))

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other) -> "Column":
        return self.with_expr(SPAnd(self.expression, self.__to_expr(other)))

    def __rand__(self, other) -> "Column":
        return self.with_expr(SPAnd(self.__to_expr(other), self.expression))

    def __or__(self, other) -> "Column":
        return self.with_expr(SPOr(self.expression, self.__to_expr(other)))

    def __ror__(self, other) -> "Column":
        return self.with_expr(SPAnd(self.__to_expr(other), self.expression))

    def __invert__(self) -> "Column":
        return self.with_expr(SPNot(self.expression))

    def cast(self, to: DataType) -> "Column":
        """Casts the values in the Column to the specified data type."""
        return self.with_expr(SPCast(self.expression, to))

    def desc(self) -> "Column":
        """Returns a Column expression with values sorted in descending order."""
        return self.with_expr(SPSortOrder(self.expression, SPDescending()))

    def desc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in descending order (null values sorted before
        non-null values)."""
        return self.with_expr(
            SPSortOrder(self.expression, SPDescending(), SPNullFirst())
        )

    def desc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in descending order (null values sorted after
        non-null values)."""
        return self.with_expr(
            SPSortOrder(self.expression, SPDescending(), SPNullLast())
        )

    def asc(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order."""
        return self.with_expr(SPSortOrder(self.expression, SPAscending()))

    def asc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order (null values sorted before
        non-null values)."""
        return self.with_expr(
            SPSortOrder(self.expression, SPAscending(), SPNullFirst())
        )

    def asc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order (null values sorted after
        non-null values)."""
        return self.with_expr(SPSortOrder(self.expression, SPAscending(), SPNullLast()))

    def named(self) -> SPExpression:
        if isinstance(self.expression, SPNamedExpression):
            return self.expression
        else:
            return SPUnresolvedAlias(self.expression, None)

    def getName(self) -> Optional[str]:
        """Returns the column name if it has one."""
        return (
            self.expression.name
            if isinstance(self.expression, SPNamedExpression)
            else None
        )

    # TODO revisit toString() functionality
    def __repr__(self):
        return f"Column[{self.expression.to_string()}]"

    def as_(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of [[name]]."""
        return self.name(alias)

    def alias(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of [[name]]."""
        return self.name(alias)

    def name(self, alias: str) -> "Column":
        """Returns a new renamed Column."""
        return self.with_expr(
            SPAlias(self.expression, AnalyzerPackage.quote_name(alias))
        )

    @staticmethod
    def __to_expr(expr) -> SPExpression:
        if type(expr) is Column:
            return expr.expression
        # TODO revisit: instead of doing SPLit(exp).expr we check if SPExpression and return that
        # or crate an SPLiteral expression
        elif isinstance(expr, SPExpression):
            return expr
        else:
            return SPLiteral.create(expr)

    @classmethod
    def expr(cls, e):
        return cls(SPUnresolvedAttribute.quoted(e))

    @staticmethod
    def __get_expr(name):
        if name == "*":
            return SPUnresolvedStar(None)
        elif name.endswith(".*"):
            parts = SPUnresolvedAttribute.parse_attribute_name(name[0:-2])
            return SPUnresolvedStar(parts)
        else:
            return SPUnresolvedAttribute.quoted(AnalyzerPackage.quote_name(name))

    @classmethod
    def with_expr(cls, new_expr):
        return cls(new_expr)


# TODO
# class CaseExp(Column):
#
