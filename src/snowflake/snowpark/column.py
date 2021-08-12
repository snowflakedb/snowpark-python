#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from typing import Optional, Union

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.sp_expressions import (
    Add as SPAdd,
    Alias as SPAlias,
    And as SPAnd,
    Ascending as SPAscending,
    BitwiseAnd as SPBitwiseAnd,
    BitwiseOr as SPBitwiseOr,
    BitwiseXor as SPBitwiseXor,
    CaseWhen as SPCaseWhen,
    Cast as SPCast,
    Collate as SPCollate,
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
    Like as SPLike,
    Literal as SPLiteral,
    Multiply as SPMultiply,
    NamedExpression as SPNamedExpression,
    Not as SPNot,
    NotEqualTo as SPNotEqualTo,
    NullsFirst as SPNullFirst,
    NullsLast as SPNullLast,
    Or as SPOr,
    Pow as SPPow,
    RegExp as SPRegExp,
    Remainder as SPRemainder,
    SortOrder as SPSortOrder,
    Star as SPStar,
    SubfieldInt as SPSubfieldInt,
    SubfieldString as SPSubfieldString,
    Subtract as SPSubtract,
    UnaryMinus as SPUnaryMinus,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
)
from snowflake.snowpark.types.sf_types import DataType


class Column:
    def __init__(self, expr: Union[str, SPExpression]):
        if type(expr) == str:
            self.expression = self.__get_expr(expr)
        elif isinstance(expr, SPExpression):
            self.expression = expr
        else:
            raise TypeError("Column constructor only accepts str or expression.")

    def __getitem__(self, field: Union[str, int]):
        if type(field) == str:
            return self.with_expr(SPSubfieldString(self.expression, field))
        elif type(field) == int:
            return self.with_expr(SPSubfieldInt(self.expression, field))
        else:
            raise TypeError(f"unexpected item type: {type(field)}")

    # overload operators
    def __eq__(self, other: "Column") -> "Column":
        right = self.__to_expr(other)
        return self.with_expr(SPEqualTo(self.expression, right))

    def __ne__(self, other: "Column") -> "Column":
        right = self.__to_expr(other)
        return self.with_expr(SPNotEqualTo(self.expression, right))

    def __gt__(self, other: "Column") -> "Column":
        return self.with_expr(SPGreaterThan(self.expression, self.__to_expr(other)))

    def __lt__(self, other: "Column") -> "Column":
        return self.with_expr(SPLessThan(self.expression, self.__to_expr(other)))

    def __ge__(self, other: "Column") -> "Column":
        return self.with_expr(
            SPGreaterThanOrEqual(self.expression, self.__to_expr(other))
        )

    def __le__(self, other: "Column") -> "Column":
        return self.with_expr(SPLessThanOrEqual(self.expression, self.__to_expr(other)))

    def __add__(self, other: "Column") -> "Column":
        return self.with_expr(SPAdd(self.expression, self.__to_expr(other)))

    def __radd__(self, other: "Column") -> "Column":
        return self.with_expr(SPAdd(self.__to_expr(other), self.expression))

    def __sub__(self, other: "Column") -> "Column":
        return self.with_expr(SPSubtract(self.expression, self.__to_expr(other)))

    def __rsub__(self, other: "Column") -> "Column":
        return self.with_expr(SPSubtract(self.__to_expr(other), self.expression))

    def __mul__(self, other: "Column") -> "Column":
        return self.with_expr(SPMultiply(self.expression, self.__to_expr(other)))

    def __rmul__(self, other: "Column") -> "Column":
        return self.with_expr(SPMultiply(self.__to_expr(other), self.expression))

    def __truediv__(self, other: "Column") -> "Column":
        return self.with_expr(SPDivide(self.expression, self.__to_expr(other)))

    def __rtruediv__(self, other: "Column") -> "Column":
        return self.with_expr(SPDivide(self.__to_expr(other), self.expression))

    def __mod__(self, other: "Column") -> "Column":
        return self.with_expr(SPRemainder(self.expression, self.__to_expr(other)))

    def __rmod__(self, other: "Column") -> "Column":
        return self.with_expr(SPRemainder(self.__to_expr(other), self.expression))

    def __pow__(self, other: "Column") -> "Column":
        return self.with_expr(SPPow(self.expression, self.__to_expr(other)))

    def __rpow__(self, other: "Column") -> "Column":
        return self.with_expr(SPPow(self.__to_expr(other), self.expression))

    def bitand(self, other: "Column") -> "Column":
        return self.with_expr(SPBitwiseAnd(self.__to_expr(other), self.expression))

    def bitor(self, other: "Column") -> "Column":
        return self.with_expr(SPBitwiseOr(self.__to_expr(other), self.expression))

    def bitxor(self, other: "Column") -> "Column":
        return self.with_expr(SPBitwiseXor(self.__to_expr(other), self.expression))

    def __neg__(self) -> "Column":
        return self.with_expr(SPUnaryMinus(self.expression))

    def equal_null(self, other: "Column") -> "Column":
        return self.with_expr(SPEqualNullSafe(self.expression, self.__to_expr(other)))

    def equal_nan(self) -> "Column":
        return self.with_expr(SPIsNaN(self.expression))

    def is_null(self) -> "Column":
        return self.with_expr(SPIsNull(self.expression))

    def is_not_null(self) -> "Column":
        return self.with_expr(SPIsNotNull(self.expression))

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other: "Column") -> "Column":
        return self.with_expr(SPAnd(self.expression, self.__to_expr(other)))

    def __rand__(self, other: "Column") -> "Column":
        return self.with_expr(SPAnd(self.__to_expr(other), self.expression))

    def __or__(self, other: "Column") -> "Column":
        return self.with_expr(SPOr(self.expression, self.__to_expr(other)))

    def __ror__(self, other: "Column") -> "Column":
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

    def like(self, pattern: Union["Column", str]) -> "Column":
        """Allows case-sensitive matching of strings based on comparison with a pattern."""
        return self.with_expr(
            SPLike(
                self.expression,
                pattern.expression
                if type(pattern) == Column
                else Column(SPLiteral.create(pattern)).expression,
            )
        )

    def regexp(self, pattern: Union["Column", str]) -> "Column":
        """Returns true if this Column matches the specified regular expression."""
        return self.with_expr(
            SPRegExp(
                self.expression,
                pattern.expression
                if type(pattern) == Column
                else Column(SPLiteral.create(pattern)).expression,
            )
        )

    def collate(self, collation_spec: str) -> "Column":
        """Returns a copy of the original Column with the specified `collation_spec` property,
        rather than the original collation specification property."""
        return self.with_expr(SPCollate(self.expression, collation_spec))

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

    def __repr__(self):
        return f"Column[{self.expression}]"

    def as_(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`Column.name`."""
        return self.name(alias)

    def alias(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`Column.name`."""
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
    def expr(cls, e: str):
        return cls(SPUnresolvedAttribute.quoted(e))

    @staticmethod
    def __get_expr(name):
        if name == "*":
            return SPStar([])
        else:
            return SPUnresolvedAttribute.quoted(AnalyzerPackage.quote_name(name))

    @classmethod
    def with_expr(cls, new_expr):
        return cls(new_expr)


class CaseExpr(Column):
    def __init__(self, expr: SPCaseWhen):
        super().__init__(expr)
        self.__branches = expr.branches

    def when(self, condition: Column, value: Column) -> "CaseExpr":
        """Appends one more WHEN condition to the CASE expression."""
        return CaseExpr(
            SPCaseWhen([*self.__branches, (condition.expression, value.expression)])
        )

    def otherwise(self, value: Column) -> "CaseExpr":
        """Sets the default result for this CASE expression."""
        return CaseExpr(SPCaseWhen(self.__branches, value.expression))

    def else_(self, value: Column) -> "CaseExpr":
        """Sets the default result for this CASE expression. Alias for otherwise."""
        return self.otherwise(value)

    @classmethod
    def with_expr(cls, new_expr):
        return Column.with_expr(new_expr)
