#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Iterable, Optional, Union

import snowflake.snowpark
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
    InExpression as SPInExpression,
    IsNaN as SPIsNaN,
    IsNotNull as SPIsNotNull,
    IsNull as SPIsNull,
    LessThan as SPLessThan,
    LessThanOrEqual as SPLessThanOrEqual,
    Like as SPLike,
    Literal as SPLiteral,
    MultipleExpression as SPMultipleExpression,
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
    ScalarSubquery as SPScalarSubquery,
    SortOrder as SPSortOrder,
    Star as SPStar,
    SubfieldInt as SPSubfieldInt,
    SubfieldString as SPSubfieldString,
    Subtract as SPSubtract,
    UnaryMinus as SPUnaryMinus,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
    WithinGroup as SPWithinGroup,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    ColumnOrLiteral,
    ColumnOrName,
    LiteralType,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.types import DataType, _type_string_to_type_object
from snowflake.snowpark.window import Window, WindowSpec


def _to_col_if_lit(
    col: Union[ColumnOrLiteral, "snowflake.snowpark.DataFrame"]
) -> "Column":
    if isinstance(col, (Column, snowflake.snowpark.DataFrame, list, tuple, set)):
        return col
    elif isinstance(col, _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
        return Column(SPLiteral(col))
    else:
        raise TypeError(
            f"Expected Column, DataFrame, Iterable or LiteralType, got: {type(col)}"
        )


def _to_col_if_str(col: ColumnOrName, func_name: str) -> "Column":
    if isinstance(col, Column):
        return col
    elif isinstance(col, str):
        return Column(col)
    else:
        raise TypeError(
            f"'{func_name.upper()}' expected Column or str, got: {type(col)}"
        )


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

    def __getitem__(self, field: Union[str, int]) -> "Column":
        if isinstance(field, str):
            return Column(SPSubfieldString(self.expression, field))
        elif isinstance(field, int):
            return Column(SPSubfieldInt(self.expression, field))
        else:
            raise TypeError(f"Unexpected item type: {type(field)}")

    # overload operators
    def __eq__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Equal to."""
        right = Column._to_expr(other)
        return Column(SPEqualTo(self.expression, right))

    def __ne__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Not equal to."""
        right = Column._to_expr(other)
        return Column(SPNotEqualTo(self.expression, right))

    def __gt__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Greater than."""
        return Column(SPGreaterThan(self.expression, Column._to_expr(other)))

    def __lt__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Less than."""
        return Column(SPLessThan(self.expression, Column._to_expr(other)))

    def __ge__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Greater than or equal to."""
        return Column(SPGreaterThanOrEqual(self.expression, Column._to_expr(other)))

    def __le__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Less than or equal to."""
        return Column(SPLessThanOrEqual(self.expression, Column._to_expr(other)))

    def __add__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Plus."""
        return Column(SPAdd(self.expression, Column._to_expr(other)))

    def __radd__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPAdd(Column._to_expr(other), self.expression))

    def __sub__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Minus."""
        return Column(SPSubtract(self.expression, Column._to_expr(other)))

    def __rsub__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPSubtract(Column._to_expr(other), self.expression))

    def __mul__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Multiply."""
        return Column(SPMultiply(self.expression, Column._to_expr(other)))

    def __rmul__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPMultiply(Column._to_expr(other), self.expression))

    def __truediv__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Divide."""
        return Column(SPDivide(self.expression, Column._to_expr(other)))

    def __rtruediv__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPDivide(Column._to_expr(other), self.expression))

    def __mod__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Reminder."""
        return Column(SPRemainder(self.expression, Column._to_expr(other)))

    def __rmod__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPRemainder(Column._to_expr(other), self.expression))

    def __pow__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Power."""
        return Column(SPPow(self.expression, Column._to_expr(other)))

    def __rpow__(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        return Column(SPPow(Column._to_expr(other), self.expression))

    def in_(
        self,
        *vals: Union[
            ColumnOrLiteral,
            Iterable[ColumnOrLiteral],
            "snowflake.snowpark.DataFrame",
        ],
    ) -> "Column":
        """Returns a conditional expression that you can pass to the :meth:`DataFrame.filter`
        or where :meth:`DataFrame.where` to perform the equivalent of a WHERE ... IN query
        with a specified list of values. You can also pass this to a
        :meth:`DataFrame.select` call.

        The expression evaluates to true if the value in the column is one of the values in
        a specified sequence.

        For example, the following code returns a DataFrame that contains the rows where
        the column "a" contains the value 1, 2, or 3. This is equivalent to
        ``SELECT * FROM table WHERE a IN (1, 2, 3)``.

        :meth:`isin` is an alias for :meth:`in_`.

        Examples::

            # Basic example
            df.filter(df("a").in_(lit(1), lit(2), lit(3)))

            # Check in membership for a DataFrame
            df1 = session.table(table1)
            df2 = session.table(table2)
            df2.filter(col("a").in_(df1))

            # Use in with a select method call
            df.select(df("a").in_(lit(1), lit(2), lit(3)))

        Args:
            vals: The values, or a :class:`DataFrame` instance to use to check for membership against this column.
        """
        cols = Utils.parse_positional_args_to_list(*vals)
        cols = [_to_col_if_lit(col) for col in cols]

        column_count = (
            len(self.expression.expressions)
            if isinstance(self.expression, SPMultipleExpression)
            else 1
        )

        def value_mapper(value):
            if isinstance(value, (tuple, set, list)):
                if len(value) == column_count:
                    return SPMultipleExpression([Column._to_expr(v) for v in value])
                else:
                    raise ValueError(
                        f"The number of values {len(value)} does not match the number of columns {column_count}."
                    )
            elif isinstance(value, snowflake.snowpark.DataFrame):
                if len(value.schema.fields) == column_count:
                    return SPScalarSubquery(value._plan)
                else:
                    raise ValueError(
                        f"The number of values {len(value.schema.fields)} does not match the number of columns {column_count}."
                    )
            else:
                return Column._to_expr(value)

        value_expressions = [value_mapper(col) for col in cols]

        if len(cols) != 1 or not isinstance(value_expressions[0], SPScalarSubquery):

            def validate_value(value_expr: SPExpression):
                if isinstance(value_expr, SPLiteral):
                    return
                elif isinstance(value_expr, SPMultipleExpression):
                    for expr in value_expr.expressions:
                        validate_value(expr)
                    return
                else:
                    raise TypeError(
                        f"'{type(value_expr)}' is not supported for the values parameter of the function "
                        f"in(). You must either specify a sequence of literals or a DataFrame that "
                        f"represents a subquery."
                    )

            for ve in value_expressions:
                validate_value(ve)

        return Column(SPInExpression(self.expression, value_expressions))

    # Alias
    isin = in_

    def between(
        self,
        lower_bound: Union[ColumnOrLiteral, SPExpression],
        upper_bound: Union[ColumnOrLiteral, SPExpression],
    ) -> "Column":
        """Between lower bound and upper bound."""
        return (Column._to_expr(lower_bound) <= self) & (
            self <= Column._to_expr(upper_bound)
        )

    def bitand(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Bitwise and."""
        return Column(SPBitwiseAnd(Column._to_expr(other), self.expression))

    def bitor(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Bitwise or."""
        return Column(SPBitwiseOr(Column._to_expr(other), self.expression))

    def bitxor(self, other: Union[ColumnOrLiteral, SPExpression]) -> "Column":
        """Bitwise xor."""
        return Column(SPBitwiseXor(Column._to_expr(other), self.expression))

    def __neg__(self) -> "Column":
        """Unary minus."""
        return Column(SPUnaryMinus(self.expression))

    def equal_null(self, other: "Column") -> "Column":
        """Equal to. You can use this for comparisons against a null value."""
        return Column(SPEqualNullSafe(self.expression, Column._to_expr(other)))

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
        return Column(SPAnd(self.expression, Column._to_expr(other)))

    def __rand__(self, other: "Column") -> "Column":
        return Column(SPAnd(Column._to_expr(other), self.expression))

    def __or__(self, other: "Column") -> "Column":
        """Or."""
        return Column(SPOr(self.expression, Column._to_expr(other)))

    def __ror__(self, other: "Column") -> "Column":
        return Column(SPAnd(Column._to_expr(other), self.expression))

    def __invert__(self) -> "Column":
        """Unary not."""
        return Column(SPNot(self.expression))

    def _cast(self, to: Union[str, DataType], try_: bool = False) -> "Column":
        if isinstance(to, str):
            to = _type_string_to_type_object(to)
        return Column(SPCast(self.expression, to, try_))

    def cast(self, to: Union[str, DataType]) -> "Column":
        """Casts the value of the Column to the specified data type.
        It raises an error when  the conversion can not be performed.
        """
        return self._cast(to, False)

    def try_cast(self, to: DataType) -> "Column":
        """Tries to cast the value of the Column to the specified data type.
        It returns a NULL value instead of raising an error when the conversion can not be performed.
        """
        return self._cast(to, True)

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

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `LIKE <https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes>`_.
        """
        return Column(
            SPLike(
                self.expression,
                Column._to_expr(pattern),
            )
        )

    def regexp(self, pattern: Union["Column", str]) -> "Column":
        """Returns true if this Column matches the specified regular expression.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `regular expressions <https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes>`_.

        :meth:`rlike` is an alias of :meth`regexp`.

        """
        return Column(
            SPRegExp(
                self.expression,
                Column._to_expr(pattern),
            )
        )

    rlike = regexp

    def startswith(self, other: Union["Column", str]) -> "Column":
        """Returns true if this Column starts with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column starts with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        other = snowflake.snowpark.functions.lit(other)
        return snowflake.snowpark.functions.startswith(self, other)

    def substr(
        self,
        start_pos: Union["Column", int],
        length: Union["Column", int],
    ) -> "Column":
        """Returns a substring of this string column.

        Args:
            start_pos: The starting position of the substring. Please note that the first character has position 1 instead of 0 in Snowflake database.
            length: The length of the substring.

        :meth:`substring` is an alias of :meth:`substr`.
        """
        return snowflake.snowpark.functions.substring(self, start_pos, length)

    substring = substr

    def collate(self, collation_spec: str) -> "Column":
        """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
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
        Returns a window frame, based on the specified :class:`~snowflake.snowpark.window.WindowSpec`.
        """
        if not window:
            window = Window._spec()
        return window._with_aggregate(self.expression)

    def within_group(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]]
    ) -> "Column":
        """
        Returns a Column expression that adds a WITHIN GROUP clause
        to sort the rows by the specified columns.

        This method is supported on Column expressions returned by some
        of the aggregate functions, including :func:`functions.array_agg`,
        :func:`functions.listagg`, PERCENTILE_CONT(), and PERCENTILE_DISC().
        For details, see the Snowflake documentation for the aggregate function
        that you are using (e.g. `ARRAY_AGG <https://docs.snowflake.com/en/sql-reference/functions/array_agg.html>`_).

        Examples::

            from snowflake.snowpark.functions import array_agg, col
            from snowflake.snowpark import Window

            df = session.createDataFrame([(3, "v1"), (1, "v3"), (2, "v2")], schema=["a", "b"])
            # create a DataFrame containing the values in "a" sorted by "b"
            df_array_agg = df.select(array_agg("a").within_group("b"))
            # create a DataFrame containing the values in "a" grouped by "b"
            # and sorted by "a" in descending order.
            df_array_agg_window = df.select(array_agg("a").within_group(col("a").desc())).over(Window.partitionBy(col("b")))
        """
        return Column(
            SPWithinGroup(
                self.expression,
                [
                    _to_col_if_str(col, "within_group").expression
                    for col in Utils.parse_positional_args_to_list(*cols)
                ],
            )
        )

    def _named(self) -> SPNamedExpression:
        if isinstance(self.expression, SPNamedExpression):
            return self.expression
        else:
            return SPUnresolvedAlias(self.expression, None)

    @classmethod
    def _to_expr(cls, expr: Union[ColumnOrLiteral, SPExpression]) -> SPExpression:
        """
        Convert a Column object, or an literal value to an expression.
        If it's a Column, get its expression.
        If it's already an expression, return it directly.
        If it's a literal value (here we treat str as literal value instead of column name),
        create a Literal expression.
        """
        if isinstance(expr, cls):
            return expr.expression
        elif isinstance(expr, SPExpression):
            return expr
        else:
            return SPLiteral(expr)

    @classmethod
    def _expr(cls, e: str) -> "Column":
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

    def when(
        self, condition: Column, value: Union["Column", LiteralType]
    ) -> "CaseExpr":
        """Appends one more WHEN condition to the CASE expression."""
        return CaseExpr(
            SPCaseWhen(
                [*self.__branches, (condition.expression, Column._to_expr(value))]
            )
        )

    def otherwise(self, value: Union["Column", LiteralType]) -> "CaseExpr":
        """Sets the default result for this CASE expression."""
        return CaseExpr(SPCaseWhen(self.__branches, Column._to_expr(value)))

    else_ = otherwise
