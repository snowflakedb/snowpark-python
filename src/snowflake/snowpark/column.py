#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Optional, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.binary_expression import (
    Add,
    And,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Divide,
    EqualNullSafe,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Multiply,
    NotEqualTo,
    Or,
    Pow,
    Remainder,
    Subtract,
)
from snowflake.snowpark._internal.analyzer.expression import (
    CaseWhen,
    Collate,
    Expression,
    InExpression,
    Like,
    Literal,
    MultipleExpression,
    NamedExpression,
    RegExp,
    ScalarSubquery,
    Star,
    SubfieldInt,
    SubfieldString,
    UnresolvedAttribute,
    WithinGroup,
)
from snowflake.snowpark._internal.analyzer.sort_expression import (
    Ascending,
    Descending,
    NullsFirst,
    NullsLast,
    SortOrder,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnaryMinus,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
    type_string_to_type_object,
)
from snowflake.snowpark._internal.utils import parse_positional_args_to_list, quote_name
from snowflake.snowpark.types import (
    DataType,
    IntegerType,
    StringType,
    TimestampTimeZone,
    TimestampType,
)
from snowflake.snowpark.window import Window, WindowSpec

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


def _to_col_if_lit(
    col: Union[ColumnOrLiteral, "snowflake.snowpark.DataFrame"], func_name: str
) -> "Column":
    if isinstance(col, (Column, snowflake.snowpark.DataFrame, list, tuple, set)):
        return col
    elif isinstance(col, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
        return Column(Literal(col))
    else:  # pragma: no cover
        raise TypeError(
            f"'{func_name}' expected Column, DataFrame, Iterable or LiteralType, got: {type(col)}"
        )


def _to_col_if_sql_expr(col: ColumnOrSqlExpr, func_name: str) -> "Column":
    if isinstance(col, Column):
        return col
    elif isinstance(col, str):
        return Column._expr(col)
    else:
        raise TypeError(
            f"'{func_name}' expected Column or str as SQL expression, got: {type(col)}"
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


def _to_col_if_str_or_int(col: Union[ColumnOrName, int], func_name: str) -> "Column":
    if isinstance(col, Column):
        return col
    elif isinstance(col, str):
        return Column(col)
    elif isinstance(col, int):
        return Column(Literal(col))
    else:  # pragma: no cover
        raise TypeError(
            f"'{func_name.upper()}' expected Column, int or str, got: {type(col)}"
        )


class Column:
    """Represents a column or an expression in a :class:`DataFrame`.

    To access a Column object that refers a column in a :class:`DataFrame`, you can:

        - Use the column name.
        - Use the :func:`functions.col` function.
        - Use the :func:`DataFrame.col` method.
        - Use the index operator ``[]`` on a dataframe object with a column name.
        - Use the dot operator ``.`` on a dataframe object with a column name.

        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([["John", 1], ["Mike", 11]], schema=["name", "age"])
        >>> df.select("name").collect()
        [Row(NAME='John'), Row(NAME='Mike')]
        >>> df.select(col("name")).collect()
        [Row(NAME='John'), Row(NAME='Mike')]
        >>> df.select(df.col("name")).collect()
        [Row(NAME='John'), Row(NAME='Mike')]
        >>> df.select(df["name"]).collect()
        [Row(NAME='John'), Row(NAME='Mike')]
        >>> df.select(df.name).collect()
        [Row(NAME='John'), Row(NAME='Mike')]

        Snowflake object identifiers, including column names, may or may not be case sensitive depending on a set of rules.
        Refer to `Snowflake Object Identifier Requirements <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_ for details.
        When you use column names with a DataFrame, you should follow these rules.

        The returned column names after a DataFrame is evaluated follow the case-sensitivity rules too.
        The above ``df`` was created with column name "name" while the returned column name after ``collect()`` was called became "NAME".
        It's because the column is regarded as ignore-case so the Snowflake database returns the upper case.

    To create a Column object that represents a constant value, use :func:`snowflake.snowpark.functions.lit`:

        >>> from snowflake.snowpark.functions import lit
        >>> df.select(col("name"), lit("const value").alias("literal_column")).collect()
        [Row(NAME='John', LITERAL_COLUMN='const value'), Row(NAME='Mike', LITERAL_COLUMN='const value')]

    This class also defines utility functions for constructing expressions with Columns.
    Column objects can be built with the operators, summarized by operator precedence,
    in the following table:

    ==============================================  ==============================================
    Operator                                        Description
    ==============================================  ==============================================
    ``x[index]``                                    Index operator to get an item out of a Snowflake ARRAY or OBJECT
    ``**``                                          Power
    ``-x``, ``~x``                                  Unary minus, unary not
    ``*``, ``/``, ``%``                             Multiply, divide, remainder
    ``+``, ``-``                                    Plus, minus
    ``&``                                           And
    ``|``                                           Or
    ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``    Equal to, not equal to, less than, less than or equal to, greater than, greater than or equal to
    ==============================================  ==============================================

        The following examples demonstrate how to use Column objects in expressions:

            >>> df = session.create_dataframe([[20, 5], [1, 2]], schema=["a", "b"])
            >>> df.filter((col("a") == 20) | (col("b") <= 10)).collect()  # use parentheses before and after the | operator.
            [Row(A=20, B=5), Row(A=1, B=2)]
            >>> df.filter((df["a"] + df.b) < 10).collect()
            [Row(A=1, B=2)]
            >>> df.select((col("b") * 10).alias("c")).collect()
            [Row(C=50), Row(C=20)]

        When you use ``|``, ``&``, and ``~`` as logical operators on columns, you must always enclose column expressions
        with parentheses as illustrated in the above example, because their order precedence is higher than ``==``, ``<``, etc.

        Do not use ``and``, ``or``, and ``not`` logical operators on column objects, for instance, ``(df.col1 > 1) and (df.col2 > 2)`` is wrong.
        The reason is Python doesn't have a magic method, or dunder method for them.
        It will raise an error and tell you to use ``|``, ``&`` or ``~``, for which Python has magic methods.
        A side effect is ``if column:`` will raise an error because it has a hidden call to ``bool(a_column)``, like using the ``and`` operator.
        Use ``if a_column is None:`` instead.

    To access elements of a semi-structured Object and Array, use ``[]`` on a Column object:

        >>> from snowflake.snowpark.types import StringType, IntegerType
        >>> df_with_semi_data = session.create_dataframe([[{"k1": "v1", "k2": "v2"}, ["a0", 1, "a2"]]], schema=["object_column", "array_column"])
        >>> df_with_semi_data.select(df_with_semi_data["object_column"]["k1"].alias("k1_value"), df_with_semi_data["array_column"][0].alias("a0_value"), df_with_semi_data["array_column"][1].alias("a1_value")).collect()
        [Row(K1_VALUE='"v1"', A0_VALUE='"a0"', A1_VALUE='1')]
        >>> # The above two returned string columns have JSON literal values because children of semi-structured data are semi-structured.
        >>> # The next line converts JSON literal to a string
        >>> df_with_semi_data.select(df_with_semi_data["object_column"]["k1"].cast(StringType()).alias("k1_value"), df_with_semi_data["array_column"][0].cast(StringType()).alias("a0_value"), df_with_semi_data["array_column"][1].cast(IntegerType()).alias("a1_value")).collect()
        [Row(K1_VALUE='v1', A0_VALUE='a0', A1_VALUE=1)]

    This class has methods for the most frequently used column transformations and operators. Module :mod:`snowflake.snowpark.functions` defines many functions to transform columns.
    """

    def __init__(
        self,
        expr1: Union[str, Expression],
        expr2: Optional[str] = None,
        ast: Optional[proto.SpColumnExpr] = None,
    ) -> None:
        self._ast = ast

        if expr2 is not None:
            # TODO: Find correct entities in AST for df_aliasing
            if isinstance(expr1, str) and isinstance(expr2, str):
                if expr2 == "*":
                    self._expression = Star([], df_alias=expr1)
                else:
                    self._expression = UnresolvedAttribute(
                        quote_name(expr2), df_alias=expr1
                    )
            else:
                raise ValueError(
                    "When Column constructor gets two arguments, both need to be <str>"
                )
        elif isinstance(expr1, str):
            if expr1 == "*":
                self._expression = Star([])
            else:
                self._expression = UnresolvedAttribute(quote_name(expr1))

            # some repetition here, but _expression logic will be eliminated eventually
            if self._ast is None:
                self._ast = proto.SpColumnExpr()
                if expr1 == "*":
                    self._ast.sp_column_sql_expr.sql = "*"
                else:
                    self._ast.sp_column.name = quote_name(expr1)

        elif isinstance(expr1, Expression):
            self._expression = expr1
        else:  # pragma: no cover
            raise TypeError("Column constructor only accepts str or expression.")

    def __getitem__(self, field: Union[str, int]) -> "Column":
        """Accesses an element of ARRAY column by ordinal position, or an element of OBJECT column by key."""
        ast = proto.SpColumnExpr()
        if isinstance(field, str):
            ast.sp_column_apply__string.col.CopyFrom(self._ast)
            ast.sp_column_apply__string.field = field
            return Column(SubfieldString(self._expression, field), ast = ast)
        elif isinstance(field, int):
            ast.sp_column_apply__int.col.CopyFrom(self._ast)
            ast.sp_column_apply__int.idx = field
            return Column(SubfieldInt(self._expression, field), ast = ast)
        else:
            raise TypeError(f"Unexpected item type: {type(field)}")

    # overload operators (for now, assume Expression objects never used for AST)
    def __eq__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Equal to."""
        ast = proto.SpColumnExpr()
        ast.sp_column_equal_to.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_equal_to.rhs, other)
        return Column(EqualTo(self._expression, Column._to_expr(other)), ast = ast)

    def __ne__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Not equal to."""
        ast = proto.SpColumnExpr()
        ast.sp_column_not_equal.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_not_equal.rhs, other)
        return Column(NotEqualTo(self._expression, Column._to_expr(other)), ast = ast)

    def __gt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than."""
        ast = proto.SpColumnExpr()
        ast.sp_column_gt.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_gt.rhs, other)
        return Column(GreaterThan(self._expression, Column._to_expr(other)), ast = ast)

    def __lt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than."""
        ast = proto.SpColumnExpr()
        ast.sp_column_lt.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_lt.rhs, other)
        return Column(LessThan(self._expression, Column._to_expr(other)), ast = ast)

    def __ge__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than or equal to."""
        ast = proto.SpColumnExpr()
        ast.sp_column_geq.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_geq.rhs, other)
        return Column(GreaterThanOrEqual(self._expression, Column._to_expr(other)), ast = ast)

    def __le__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than or equal to."""
        ast = proto.SpColumnExpr()
        ast.sp_column_leq.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_leq.rhs, other)
        return Column(LessThanOrEqual(self._expression, Column._to_expr(other)), ast = ast)

    def __add__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Plus."""
        ast = proto.SpColumnExpr()
        ast.sp_column_plus.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_plus.rhs, other)
        return Column(Add(self._expression, Column._to_expr(other)), ast = ast)

    def __radd__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        ast = proto.SpColumnExpr()
        Column._fill_ast(ast.sp_column_plus.lhs, other)
        ast.sp_column_plus.rhs.CopyFrom(self._ast)
        return Column(Add(Column._to_expr(other), self._expression), ast = ast)

    def __sub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Minus."""
        ast = proto.SpColumnExpr()
        ast.sp_column_minus.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_minus.rhs, other)
        return Column(Subtract(self._expression, Column._to_expr(other)), ast = ast)

    def __rsub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        ast = proto.SpColumnExpr()
        Column._fill_ast(ast.sp_column_minus.lhs, other)
        ast.sp_column_minus.rhs.CopyFrom(self._ast)
        return Column(Subtract(Column._to_expr(other), self._expression), ast = ast)

    def __mul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Multiply."""
        ast = proto.SpColumnExpr()
        ast.sp_column_multiply.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_multiply.rhs, other)
        return Column(Multiply(self._expression, Column._to_expr(other)), ast = ast)

    def __rmul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        ast = proto.SpColumnExpr()
        Column._fill_ast(ast.sp_column_multiply.lhs, other)
        ast.sp_column_multiply.rhs.CopyFrom(self._ast)
        return Column(Multiply(Column._to_expr(other), self._expression), ast = ast)

    def __truediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Divide."""
        ast = proto.SpColumnExpr()
        ast.sp_column_divide.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_divide.rhs, other)
        return Column(Divide(self._expression, Column._to_expr(other)), ast = ast)

    def __rtruediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        ast = proto.SpColumnExpr()
        Column._fill_ast(ast.sp_column_divide.lhs, other)
        ast.sp_column_divide.rhs.CopyFrom(self._ast)
        return Column(Divide(Column._to_expr(other), self._expression), ast = ast)

    def __mod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Reminder."""
        ast = proto.SpColumnExpr()
        ast.sp_column_mod.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_mod.rhs, other)
        return Column(Remainder(self._expression, Column._to_expr(other)), ast = ast)

    def __rmod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        ast = proto.SpColumnExpr()
        Column._fill_ast(ast.sp_column_mod.lhs, other)
        ast.sp_column_mod.rhs.CopyFrom(self._ast)
        return Column(Remainder(Column._to_expr(other), self._expression), ast = ast)

    # TODO: Create a new SpColumnPow IR entity and uncomment below
    def __pow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Power."""
        # ast = proto.SpColumnExpr()
        # ast.sp_column_pow.lhs.CopyFrom(self._ast)
        # Column._fill_ast(ast.sp_column_pow.rhs, other)
        return Column(Pow(self._expression, Column._to_expr(other)))

    def __rpow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        # ast = proto.SpColumnExpr()
        # Column._fill_ast(ast.sp_column_pow.lhs, other)
        # ast.sp_column_pow.rhs.CopyFrom(self._ast)
        return Column(Pow(Column._to_expr(other), self._expression))

    def __bool__(self) -> bool:
        raise TypeError(
            "Cannot convert a Column object into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' if you're building DataFrame filter expressions. For example, use df.filter((col1 > 1) & (col2 > 2)) instead of df.filter(col1 > 1 and col2 > 2)."
        )

    def __iter__(self) -> None:
        raise TypeError(
            "Column is not iterable. This error can occur when you use the Python built-ins for sum, min and max. Please make sure you use the corresponding function from snowflake.snowpark.functions."
        )

    def __round__(self, n=None):
        raise TypeError(
            "Column cannot be rounded. This error can occur when you use the Python built-in round. Please make sure you use the snowflake.snowpark.functions.round function instead."
        )

    def __hash__(self):
        return hash(self._expression)

    # TODO: Implement AST generation for in_ / isin (relies on MultipleExpression and ScalarSubquery)
    def in_(
        self,
        *vals: Union[
            LiteralType,
            Iterable[LiteralType],
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

            >>> from snowflake.snowpark.functions import lit
            >>> df = session.create_dataframe([[1, "x"], [2, "y"] ,[4, "z"]], schema=["a", "b"])
            >>> # Basic example
            >>> df.filter(df["a"].in_(lit(1), lit(2), lit(3))).collect()
            [Row(A=1, B='x'), Row(A=2, B='y')]

            >>> # Check in membership for a DataFrame that has a single column
            >>> df_for_in = session.create_dataframe([[1], [2] ,[3]], schema=["col1"])
            >>> df.filter(df["a"].in_(df_for_in)).sort(df["a"].asc()).collect()
            [Row(A=1, B='x'), Row(A=2, B='y')]

            >>> # Use in with a select method call
            >>> df.select(df["a"].in_(lit(1), lit(2), lit(3)).alias("is_in_list")).collect()
            [Row(IS_IN_LIST=True), Row(IS_IN_LIST=True), Row(IS_IN_LIST=False)]

        Args:
            vals: The values, or a :class:`DataFrame` instance to use to check for membership against this column.
        """
        cols = parse_positional_args_to_list(*vals)
        cols = [_to_col_if_lit(col, "in_") for col in cols]

        column_count = (
            len(self._expression.expressions)
            if isinstance(self._expression, MultipleExpression)
            else 1
        )

        def value_mapper(value):
            if isinstance(value, (tuple, set, list)):
                if len(value) == column_count:
                    return MultipleExpression([Column._to_expr(v) for v in value])
                else:
                    raise ValueError(
                        f"The number of values {len(value)} does not match the number of columns {column_count}."
                    )
            elif isinstance(value, snowflake.snowpark.DataFrame):
                if len(value.schema.fields) == column_count:
                    return ScalarSubquery(value._plan)
                else:
                    raise ValueError(
                        f"The number of values {len(value.schema.fields)} does not match the number of columns {column_count}."
                    )
            else:
                return Column._to_expr(value)

        value_expressions = [value_mapper(col) for col in cols]

        if len(cols) != 1 or not isinstance(value_expressions[0], ScalarSubquery):

            def validate_value(value_expr: Expression):
                if isinstance(value_expr, Literal):
                    return
                elif isinstance(value_expr, MultipleExpression):
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

        return Column(InExpression(self._expression, value_expressions))

    def between(
        self,
        lower_bound: Union[ColumnOrLiteral, Expression],
        upper_bound: Union[ColumnOrLiteral, Expression],
    ) -> "Column":
        """Between lower bound and upper bound."""
        ast = proto.SpColumnExpr()
        ast.sp_column_between.col.CopyFrom(self._ast)
        ast.sp_column_between.lower_bound = Column._to_ast(lower_bound)
        ast.sp_column_between.upper_bound = Column._to_ast(upper_bound)
        return Column((Column._to_expr(lower_bound) <= self) & (
            self <= Column._to_expr(upper_bound)
        )._expression, ast = ast)

    def bitand(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise and."""
        ast = proto.SpColumnExpr()
        ast.sp_column_bit_and.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_bit_and.rhs, other)
        return Column(BitwiseAnd(Column._to_expr(other), self._expression), ast = ast)

    def bitor(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise or."""
        ast = proto.SpColumnExpr()
        ast.sp_column_bit_or.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_bit_or.rhs, other)
        return Column(BitwiseOr(Column._to_expr(other), self._expression), ast = ast)

    def bitxor(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise xor."""
        ast = proto.SpColumnExpr()
        ast.sp_column_bit_xor.lhs.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_bit_xor.rhs, other)
        return Column(BitwiseXor(Column._to_expr(other), self._expression), ast = ast)

    def __neg__(self) -> "Column":
        """Unary minus."""
        # TODO: Need SpColumnNeg IR entity
        # ast = proto.SpColumnExpr()
        # ast.sp_column_neg.col.CopyFrom(self._ast)
        return Column(UnaryMinus(self._expression))

    def equal_null(self, other: "Column") -> "Column":
        """Equal to. You can use this for comparisons against a null value."""
        ast = proto.SpColumnExpr()
        ast.sp_column_equal_null.lhs.CopyFrom(self._ast)
        ast.sp_column_equal_null.rhs.CopyFrom(other._ast)
        return Column(EqualNullSafe(self._expression, Column._to_expr(other)), ast = ast)

    def equal_nan(self) -> "Column":
        """Is NaN."""
        ast = proto.SpColumnExpr()
        ast.sp_column_equal_nan.col.CopyFrom(self._ast)
        return Column(IsNaN(self._expression), ast = ast)

    def is_null(self) -> "Column":
        """Is null."""
        ast = proto.SpColumnExpr()
        ast.sp_column_is_null.col.CopyFrom(self._ast)
        return Column(IsNull(self._expression), ast = ast)

    def is_not_null(self) -> "Column":
        """Is not null."""
        ast = proto.SpColumnExpr()
        ast.sp_column_is_not_null.col.CopyFrom(self._ast)
        return Column(IsNotNull(self._expression), ast = ast)

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other: "Column") -> "Column":
        """And."""
        ast = proto.SpColumnExpr()
        ast.sp_column_and.lhs.CopyFrom(self._ast)
        ast.sp_column_and.rhs.CopyFrom(other._ast)
        return Column(And(self._expression, Column._to_expr(other)), ast = ast)

    def __rand__(self, other: "Column") -> "Column":
        ast = proto.SpColumnExpr()
        ast.sp_column_and.lhs.CopyFrom(other._ast)
        ast.sp_column_and.rhs.CopyFrom(self._ast)
        return Column(And(Column._to_expr(other), self._expression), ast = ast)  # pragma: no cover

    def __or__(self, other: "Column") -> "Column":
        """Or."""
        ast = proto.SpColumnExpr()
        ast.sp_column_or.lhs.CopyFrom(self._ast)
        ast.sp_column_or.rhs.CopyFrom(other._ast)
        return Column(Or(self._expression, Column._to_expr(other)), ast = ast)

    def __ror__(self, other: "Column") -> "Column":
        ast = proto.SpColumnExpr()
        ast.sp_column_or.lhs.CopyFrom(other._ast)
        ast.sp_column_or.rhs.CopyFrom(self._ast)
        return Column(And(Column._to_expr(other), self._expression), ast = ast)  # pragma: no cover

    def __invert__(self) -> "Column":
        """Unary not."""
        # TODO: Need an SpColumnNot IR entity
        # ast = proto.SpColumnExpr()
        # ast.sp_column_not.col.CopyFrom(self._ast)
        return Column(Not(self._expression))

    def _cast(self, to: Union[str, DataType], try_: bool = False) -> "Column":
        ast = proto.SpColumnExpr()
        ast.sp_column_cast.col.CopyFrom(self._ast)
        # TODO: Update SpColumnCast IR entity with new field "try", then uncomment
        # ast.sp_column_cast.try = try_
        if isinstance(to, str):
            to = type_string_to_type_object(to)
            to.fill_ast(ast.sp_column_cast.to)
        return Column(Cast(self._expression, to, try_), ast = ast)

    def cast(self, to: Union[str, DataType]) -> "Column":
        """Casts the value of the Column to the specified data type.
        It raises an error when  the conversion can not be performed.
        """
        return self._cast(to, False)

    def try_cast(self, to: Union[str, DataType]) -> "Column":
        """Tries to cast the value of the Column to the specified data type.
        It returns a NULL value instead of raising an error when the conversion can not be performed.
        """
        return self._cast(to, True)

    def desc(self) -> "Column":
        """Returns a Column expression with values sorted in descending order."""
        ast = proto.SpColumnExpr()
        ast.sp_column_desc.col.CopyFrom(self._ast)
        return Column(SortOrder(self._expression, Descending()), ast = ast)

    def desc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted before non-null values)."""
        ast = proto.SpColumnExpr()
        ast.sp_column_desc.col.CopyFrom(self._ast)
        ast.sp_column_desc.nulls_first = True
        return Column(SortOrder(self._expression, Descending(), NullsFirst()), ast = ast)

    def desc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted after non-null values)."""
        ast = proto.SpColumnExpr()
        ast.sp_column_desc.col.CopyFrom(self._ast)
        ast.sp_column_desc.nulls_first = False
        return Column(SortOrder(self._expression, Descending(), NullsLast()), ast = ast)

    def asc(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order."""
        ast = proto.SpColumnExpr()
        ast.sp_column_asc.col.CopyFrom(self._ast)
        return Column(SortOrder(self._expression, Ascending()), ast = ast)

    def asc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted before non-null values)."""
        ast = proto.SpColumnExpr()
        ast.sp_column_asc.col.CopyFrom(self._ast)
        ast.sp_column_asc.nulls_first = True
        return Column(SortOrder(self._expression, Ascending(), NullsFirst()), ast = ast)

    def asc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted after non-null values)."""
        ast = proto.SpColumnExpr()
        ast.sp_column_asc.col.CopyFrom(self._ast)
        ast.sp_column_asc.nulls_first = False
        return Column(SortOrder(self._expression, Ascending(), NullsLast()), ast = ast)

    def like(self, pattern: ColumnOrLiteralStr) -> "Column":
        """Allows case-sensitive matching of strings based on comparison with a pattern.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `LIKE <https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes>`_.
        """
        ast = proto.SpColumnExpr()
        ast.sp_column_like.col.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_like.pattern, pattern)
        return Column(
            Like(
                self._expression,
                Column._to_expr(pattern),
            ), ast = ast
        )

    def regexp(self, pattern: ColumnOrLiteralStr) -> "Column":
        """Returns true if this Column matches the specified regular expression.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `regular expressions <https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes>`_.

        :meth:`rlike` is an alias of :meth:`regexp`.

        """
        ast = proto.SpColumnExpr()
        ast.sp_column_regexp.col.CopyFrom(self._ast)
        Column._fill_ast(ast.sp_column_regexp.pattern.trait_sp_column_expr, pattern)
        return Column(
            RegExp(
                self._expression,
                Column._to_expr(pattern),
            ), ast = ast
        )

    # TODO: Need to figure out AST entities for string col startswith
    def startswith(self, other: ColumnOrLiteralStr) -> "Column":
        """Returns true if this Column starts with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column starts with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        other = snowflake.snowpark.functions.lit(other)
        return snowflake.snowpark.functions.startswith(self, other)

    # TODO: Need to figure out AST entities for string col endswith
    def endswith(self, other: ColumnOrLiteralStr) -> "Column":
        """Returns true if this Column ends with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column ends with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        other = snowflake.snowpark.functions.lit(other)
        return snowflake.snowpark.functions.endswith(self, other)

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

    def collate(self, collation_spec: str) -> "Column":
        """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
        property, rather than the original collation specification property.

        For details, see the Snowflake documentation on
        `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.
        """
        ast = proto.SpColumnExpr()
        ast.sp_column_collate.col.CopyFrom(self._ast)
        ast.sp_column_collate.collate_spec = collation_spec
        return Column(Collate(self._expression, collation_spec), ast = ast)

    # TODO: Need to figure out AST entities for string col contains
    def contains(self, string: ColumnOrName) -> "Column":
        """Returns true if the column contains `string` for each row.

        Args:
            string: the string to search for in this column.
        """
        return snowflake.snowpark.functions.contains(self, string)

    def get_name(self) -> Optional[str]:
        """Returns the column name (if the column has a name)."""
        return (
            self._expression.name
            if isinstance(self._expression, NamedExpression)
            else None
        )

    def __str__(self):
        return f"Column[{self._expression}]"

    def __repr__(self):
        return f"Column({self._expression})"  # pragma: no cover

    def as_(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant_is_as = True)

    def alias(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant_is_as = False)

    def name(self, alias: str, variant_is_as: bool = True) -> "Column":
        """Returns a new renamed Column."""
        ast = proto.SpColumnExpr()
        ast.sp_column_alias.col.CopyFrom(self._ast)
        ast.sp_column_alias.name = alias
        ast.sp_column_alias.variant_is_as = variant_is_as
        return Column(Alias(self._expression, quote_name(alias)), ast = ast)

    # TODO: Implement AST generation for Column.over (relies on SpWindowSpec AST generation)
    def over(self, window: Optional[WindowSpec] = None) -> "Column":
        """
        Returns a window frame, based on the specified :class:`~snowflake.snowpark.window.WindowSpec`.
        """
        if not window:
            window = Window._spec()
        return window._with_aggregate(self._expression)

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

            >>> from snowflake.snowpark.functions import array_agg, col
            >>> from snowflake.snowpark import Window

            >>> df = session.create_dataframe([(3, "v1"), (1, "v3"), (2, "v2")], schema=["a", "b"])
            >>> # create a DataFrame containing the values in "a" sorted by "b"
            >>> df.select(array_agg("a").within_group("b").alias("new_column")).show()
            ----------------
            |"NEW_COLUMN"  |
            ----------------
            |[             |
            |  3,          |
            |  2,          |
            |  1           |
            |]             |
            ----------------
            <BLANKLINE>
            >>> # create a DataFrame containing the values in "a" grouped by "b"
            >>> # and sorted by "a" in descending order.
            >>> df_array_agg_window = df.select(array_agg("a").within_group(col("a").desc()).over(Window.partitionBy(col("b"))).alias("new_column"))
            >>> df_array_agg_window.show()
            ----------------
            |"NEW_COLUMN"  |
            ----------------
            |[             |
            |  3           |
            |]             |
            |[             |
            |  1           |
            |]             |
            |[             |
            |  2           |
            |]             |
            ----------------
            <BLANKLINE>
        """
        ast = proto.SpColumnExpr()
        ast.sp_column_within_group.col.CopyFrom(self._ast)
        ast.sp_column_within_group.variadic = (len(cols) > 1 or not isinstance(cols[0], (list, tuple, set)))

        order_by_cols = []
        for col in parse_positional_args_to_list(*cols):
            if isinstance(col, Column):
                order_by_cols.append(col)
                ast.sp_column_within_group.cols.append(col._ast)
            elif isinstance(col, str):
                col_ast = ast.sp_column_within_group.cols.add()
                new_col = Column(col, ast = col_ast)
                col_ast.sp_column.name = new_col.get_name()
                order_by_cols.append(new_col)
            else:
                raise TypeError(
                    f"'WITHIN_GROUP' expected Column or str, got: {type(col)}"
                )

        return Column(WithinGroup(self._expression, order_by_cols), ast = ast)

    def _named(self) -> NamedExpression:
        if isinstance(self._expression, NamedExpression):
            return self._expression
        else:
            return UnresolvedAlias(self._expression)
    
    @classmethod
    def _fill_ast(cls, ast: proto.SpColumnExpr, value: ColumnOrLiteral) -> None:
        """
        Copy from a Column object's AST, or copy a literal value into an AST expression.
        """
        if isinstance(value, cls):
            return ast.CopyFrom(value._ast)
        elif isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            # TODO: Using SpColumnSqlExpr as a catchall here, consider adding SpColumnLiteral IR entity with Const type fields
            ast.sp_column_sql_expr.sql = str(value)
            return ast
        else:
            raise TypeError(f"{type(value)} is not a valid type for Column or literal AST.")

    @classmethod
    def _to_expr(cls, expr: Union[ColumnOrLiteral, Expression]) -> Expression:
        """
        Convert a Column object, or an literal value to an expression.
        If it's a Column, get its expression.
        If it's already an expression, return it directly.
        If it's a literal value (here we treat str as literal value instead of column name),
        create a Literal expression.
        """
        if isinstance(expr, cls):
            return expr._expression
        elif isinstance(expr, Expression):
            return expr
        else:
            return Literal(expr)

    @classmethod
    def _expr(cls, e: str) -> "Column":
        return cls(UnresolvedAttribute(e, is_sql_text=True))

    # Add these alias for user code migration
    isin = in_
    astype = cast
    rlike = regexp
    substring = substr
    bitwiseAnd = bitand
    bitwiseOR = bitor
    bitwiseXOR = bitxor
    isNotNull = is_not_null
    isNull = is_null
    eqNullSafe = equal_null
    getName = get_name
    getItem = __getitem__
    getField = __getitem__


class CaseExpr(Column):
    """
    Represents a `CASE <https://docs.snowflake.com/en/sql-reference/functions/case.html>`_
    expression.

    To construct this object for a CASE expression, call the :func:`functions.when`
    specifying a condition and the corresponding result for that condition.
    Then, call :func:`when` and :func:`otherwise` methods to specify additional conditions
    and results.

    Examples::

        >>> from snowflake.snowpark.functions import when, col, lit

        >>> df = session.create_dataframe([[None], [1], [2]], schema=["a"])
        >>> df.select(when(col("a").is_null(), lit(1)) \\
        ...     .when(col("a") == 1, lit(2)) \\
        ...     .otherwise(lit(3)).alias("case_when_column")).collect()
        [Row(CASE_WHEN_COLUMN=1), Row(CASE_WHEN_COLUMN=2), Row(CASE_WHEN_COLUMN=3)]
    """

    def __init__(self, expr: CaseWhen) -> None:
        super().__init__(expr)
        self._branches = expr.branches

    def when(self, condition: ColumnOrSqlExpr, value: ColumnOrLiteral) -> "CaseExpr":
        """
        Appends one more WHEN condition to the CASE expression.

        Args:
            condition: A :class:`Column` expression or SQL text representing the specified condition.
            value: A :class:`Column` expression or a literal value, which will be returned
                if ``condition`` is true.
        """
        return CaseExpr(
            CaseWhen(
                [
                    *self._branches,
                    (
                        _to_col_if_sql_expr(condition, "when")._expression,
                        Column._to_expr(value),
                    ),
                ]
            )
        )

    def otherwise(self, value: ColumnOrLiteral) -> "CaseExpr":
        """Sets the default result for this CASE expression.

        :meth:`else_` is an alias of :meth:`otherwise`.
        """
        return CaseExpr(CaseWhen(self._branches, Column._to_expr(value)))

    # This alias is to sync with snowpark scala
    else_ = otherwise


# We support the metadata columns below based on https://docs.snowflake.com/en/user-guide/querying-metadata
# If the list changes, we will have to add support for new columns
METADATA_FILE_ROW_NUMBER = Column("METADATA$FILE_ROW_NUMBER")
METADATA_FILE_CONTENT_KEY = Column("METADATA$FILE_CONTENT_KEY")
METADATA_FILE_LAST_MODIFIED = Column("METADATA$FILE_LAST_MODIFIED")
METADATA_START_SCAN_TIME = Column("METADATA$START_SCAN_TIME")
METADATA_FILENAME = Column("METADATA$FILENAME")

METADATA_COLUMN_TYPES = {
    METADATA_FILE_ROW_NUMBER.get_name(): IntegerType(),
    METADATA_FILE_CONTENT_KEY.getName(): StringType(),
    METADATA_FILE_LAST_MODIFIED.getName(): TimestampType(TimestampTimeZone.NTZ),
    METADATA_START_SCAN_TIME.getName(): TimestampType(TimestampTimeZone.LTZ),
    METADATA_FILENAME.getName(): StringType(),
}
