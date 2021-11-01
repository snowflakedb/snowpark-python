#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional, Union

from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.sp_expressions import (
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
from snowflake.snowpark.types import DataType
from snowflake.snowpark.window import Window, WindowSpec


class Column:
    """
    Represents a column or an expression in a :class:`DataFrame`.
    To create a Column object to refer to a column in a :class:`DataFrame`, you can:

      - Use the :func:`functions.col` function.
      - Use the :func:`DataFrame.col` method.
      - Use the index operator ``[]`` on a dataframe object with a column name.
      - Use the dot operator ``.`` on a dataframe object with a column name.

    Examples::

        from snowflake.snowpark.functions import col
        df.select("name")
        df.select(col("name"))
        df.select(df.col("name"))
        df.select(df["name"])
        df.select(df.name)

    This class also defines utility functions for constructing expressions with Columns.
    Column objects can be built with the operators, summarized by operator precedence,
    in the following table:

    ==============================================  ==============================================
    Operator                                        Description
    ==============================================  ==============================================
    ``x[index]``                                    Index operator to get an item out of a list or dict
    ``**``                                          Power
    ``-x``, ``~x``                                  Unary minus, unary not
    ``*``, ``/``, ``%``                             Multiply, divide, remainder
    ``+``, ``-``                                    Plus, minus
    ``&``                                           And
    ``|``                                           Or
    ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``    Equal to, not equal to, less than, less than or equal to, greater than, greater than or equal to
    ==============================================  ==============================================

    The following examples demonstrate how to use Column objects in expressions::

        df.filter((col("id") == 20) | (col("id") <= 10)) \\
          .filter((df["a"] + df.b) < 10) \\
          .select((col("b") * 10).as_("c"))
    """

    def __init__(self, expr: Union[str, SPExpression]):
        if isinstance(expr, str):
            if expr == "*":
                self.expression = SPStar([])
            else:
                self.expression = SPUnresolvedAttribute.quoted(
                    AnalyzerPackage.quote_name(expr)
                )
        elif isinstance(expr, SPExpression):
            self.expression = expr
        else:
            raise TypeError("Column constructor only accepts str or expression.")

    def __getitem__(self, field: Union[str, int]):
        if type(field) == str:
            return Column(SPSubfieldString(self.expression, field))
        elif type(field) == int:
            return Column(SPSubfieldInt(self.expression, field))
        else:
            raise TypeError(f"Unexpected item type: {type(field)}")

    # overload operators
    def __eq__(self, other: "Column") -> "Column":
        """Equal to."""
        right = self._to_expr(other)
        return Column(SPEqualTo(self.expression, right))

    def __ne__(self, other: "Column") -> "Column":
        """Not equal to."""
        right = self._to_expr(other)
        return Column(SPNotEqualTo(self.expression, right))

    def __gt__(self, other: "Column") -> "Column":
        """Greater than."""
        return Column(SPGreaterThan(self.expression, self._to_expr(other)))

    def __lt__(self, other: "Column") -> "Column":
        """Less than."""
        return Column(SPLessThan(self.expression, self._to_expr(other)))

    def __ge__(self, other: "Column") -> "Column":
        """Greater than or equal to."""
        return Column(SPGreaterThanOrEqual(self.expression, self._to_expr(other)))

    def __le__(self, other: "Column") -> "Column":
        """Less than or equal to."""
        return Column(SPLessThanOrEqual(self.expression, self._to_expr(other)))

    def __add__(self, other: "Column") -> "Column":
        """Plus."""
        return Column(SPAdd(self.expression, self._to_expr(other)))

    def __radd__(self, other: "Column") -> "Column":
        return Column(SPAdd(self._to_expr(other), self.expression))

    def __sub__(self, other: "Column") -> "Column":
        """Minus."""
        return Column(SPSubtract(self.expression, self._to_expr(other)))

    def __rsub__(self, other: "Column") -> "Column":
        return Column(SPSubtract(self._to_expr(other), self.expression))

    def __mul__(self, other: "Column") -> "Column":
        """Multiply."""
        return Column(SPMultiply(self.expression, self._to_expr(other)))

    def __rmul__(self, other: "Column") -> "Column":
        return Column(SPMultiply(self._to_expr(other), self.expression))

    def __truediv__(self, other: "Column") -> "Column":
        """Divide."""
        return Column(SPDivide(self.expression, self._to_expr(other)))

    def __rtruediv__(self, other: "Column") -> "Column":
        return Column(SPDivide(self._to_expr(other), self.expression))

    def __mod__(self, other: "Column") -> "Column":
        """Reminder."""
        return Column(SPRemainder(self.expression, self._to_expr(other)))

    def __rmod__(self, other: "Column") -> "Column":
        return Column(SPRemainder(self._to_expr(other), self.expression))

    def __pow__(self, other: "Column") -> "Column":
        """Power."""
        return Column(SPPow(self.expression, self._to_expr(other)))

    def __rpow__(self, other: "Column") -> "Column":
        return Column(SPPow(self._to_expr(other), self.expression))

    def between(self, lower_bound: "Column", upper_bound: "Column") -> "Column":
        """Between lower bound and upper bound."""
        return (lower_bound <= self) & (self <= upper_bound)

    def bitand(self, other: "Column") -> "Column":
        """Bitwise and."""
        return Column(SPBitwiseAnd(self._to_expr(other), self.expression))

    def bitor(self, other: "Column") -> "Column":
        """Bitwise or."""
        return Column(SPBitwiseOr(self._to_expr(other), self.expression))

    def bitxor(self, other: "Column") -> "Column":
        """Bitwise xor."""
        return Column(SPBitwiseXor(self._to_expr(other), self.expression))

    def __neg__(self) -> "Column":
        """Unary minus."""
        return Column(SPUnaryMinus(self.expression))

    def equal_null(self, other: "Column") -> "Column":
        """Equal to. You can use this for comparisons against a null value."""
        return Column(SPEqualNullSafe(self.expression, self._to_expr(other)))

    def equal_nan(self) -> "Column":
        """Is NaN."""
        return Column(SPIsNaN(self.expression))

    def is_null(self) -> "Column":
        """Is null."""
        return Column(SPIsNull(self.expression))

    def is_not_null(self) -> "Column":
        """Is not null."""
        return Column(SPIsNotNull(self.expression))

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other: "Column") -> "Column":
        """And."""
        return Column(SPAnd(self.expression, self._to_expr(other)))

    def __rand__(self, other: "Column") -> "Column":
        return Column(SPAnd(self._to_expr(other), self.expression))

    def __or__(self, other: "Column") -> "Column":
        """Or."""
        return Column(SPOr(self.expression, self._to_expr(other)))

    def __ror__(self, other: "Column") -> "Column":
        return Column(SPAnd(self._to_expr(other), self.expression))

    def __invert__(self) -> "Column":
        """Unary not."""
        return Column(SPNot(self.expression))

    def cast(self, to: DataType) -> "Column":
        """Casts the value of the Column to the specified data type."""
        return Column(SPCast(self.expression, to))

    def desc(self) -> "Column":
        """Returns a Column expression with values sorted in descending order."""
        return Column(SPSortOrder(self.expression, SPDescending()))

    def desc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted before non-null values)."""
        return Column(SPSortOrder(self.expression, SPDescending(), SPNullFirst()))

    def desc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted after non-null values)."""
        return Column(SPSortOrder(self.expression, SPDescending(), SPNullLast()))

    def asc(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order."""
        return Column(SPSortOrder(self.expression, SPAscending()))

    def asc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted before non-null values)."""
        return Column(SPSortOrder(self.expression, SPAscending(), SPNullFirst()))

    def asc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted after non-null values)."""
        return Column(SPSortOrder(self.expression, SPAscending(), SPNullLast()))

    def like(self, pattern: Union["Column", str]) -> "Column":
        """Allows case-sensitive matching of strings based on comparison with a pattern.

        For details, see the Snowflake documentation on
        `LIKE <https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes>`_.
        """
        return Column(
            SPLike(
                self.expression,
                pattern.expression
                if type(pattern) == Column
                else Column(SPLiteral.create(pattern)).expression,
            )
        )

    def regexp(self, pattern: Union["Column", str]) -> "Column":
        """Returns true if this Column matches the specified regular expression.

        For details, see the Snowflake documentation on
        `regular expressions <https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes>`_.
        """
        return Column(
            SPRegExp(
                self.expression,
                pattern.expression
                if type(pattern) == Column
                else Column(SPLiteral.create(pattern)).expression,
            )
        )

    def collate(self, collation_spec: str) -> "Column":
        """Returns a copy of the original :class:`Column` with the specified `collation_spec`
        property, rather than the original collation specification property.

        For details, see the Snowflake documentation on
        `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.
        """
        return Column(SPCollate(self.expression, collation_spec))

    def getName(self) -> Optional[str]:
        """Returns the column name (if the column has a name)."""
        return (
            self.expression.name
            if isinstance(self.expression, SPNamedExpression)
            else None
        )

    def __repr__(self):
        return f"Column[{self.expression}]"

    def as_(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias)

    def alias(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias)

    def name(self, alias: str) -> "Column":
        """Returns a new renamed Column."""
        return Column(SPAlias(self.expression, AnalyzerPackage.quote_name(alias)))

    def over(self, window: Optional[WindowSpec] = None) -> "Column":
        """
        Returns a windows frame, based on the specified
        :class:`~snowflake.snowpark.window.WindowSpec`.
        """
        if not window:
            window = Window._spec()
        return window._with_aggregate(self.expression)

    def _named(self) -> SPNamedExpression:
        if isinstance(self.expression, SPNamedExpression):
            return self.expression
        else:
            return SPUnresolvedAlias(self.expression, None)

    @staticmethod
    def _to_expr(expr) -> SPExpression:
        if isinstance(expr, Column):
            return expr.expression
        elif isinstance(expr, SPExpression):
            return expr
        else:
            return SPLiteral.create(expr)

    @classmethod
    def _expr(cls, e: str):
        return cls(SPUnresolvedAttribute.quoted(e))


class CaseExpr(Column):
    """
    Represents a `CASE <https://docs.snowflake.com/en/sql-reference/functions/case.html>`_
    expression.

    To construct this object for a CASE expression, call the :func:`functions.when`
    specifying a condition and the corresponding result for that condition.
    Then, call :func:`when` and :func:`otherwise` methods to specify additional conditions
    and results.

    Examples::

        from snowflake.snowpark.functions import when, col, lit
        df.select(
            when(col("col").is_null(), lit(1)) \\
                .when(col("col") == 1, lit(2)) \\
                .otherwise(lit(3))
        )
    """

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
