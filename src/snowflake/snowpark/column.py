#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
import typing
from typing import Any, Optional, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
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
    Attribute,
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
    _InternalAlias,
)
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_expr_from_snowpark_column_or_python_val,
    build_expr_from_snowpark_column_or_sql_str,
    create_ast_for_column,
    snowpark_expression_to_ast,
    with_src_position,
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
from snowflake.snowpark._internal.utils import (
    parse_positional_args_to_list,
    publicapi,
    quote_name,
    split_snowflake_identifier_with_dot,
)
from snowflake.snowpark.types import (
    DataType,
    IntegerType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    ArrayType,
    MapType,
    StructType,
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
        return Column(Literal(col), _emit_ast=False)
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
        return Column(col, _caller_name=None)
    else:
        raise TypeError(
            f"'{func_name.upper()}' expected Column or str, got: {type(col)}"
        )


def _to_col_if_str_or_int(col: Union[ColumnOrName, int], func_name: str) -> "Column":
    if isinstance(col, Column):
        return col
    elif isinstance(col, str):
        return Column(col, _caller_name=None)
    elif isinstance(col, int):
        return Column(Literal(col), _emit_ast=False)
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

    # NOTE: For now assume Expression instances can be safely ignored when building AST
    #       Expression logic can be eliminated entirely once phase 0 is integrated
    #       Currently a breaking example can be created using the Column.isin method as it does not build the AST.
    #       For example, running: df.filter(col("A").isin(1, 2, 3) & col("B")) would fail since the boolean operator
    #       '&' would try to construct an AST using that of the new col("A").isin(1, 2, 3) column (which we currently
    #       don't fill if the only argument provided in the Column constructor is 'expr1' of type Expression)
    @publicapi
    def __init__(
        self,
        expr1: Union[str, Expression],
        expr2: Optional[str] = None,
        _ast: Optional[proto.Expr] = None,
        _emit_ast: bool = True,
        _caller_name: Optional[str] = "Column",
        *,
        _is_qualified_name: bool = False,
    ) -> None:
        self._ast = _ast
        self._expr1 = expr1
        self._expr2 = expr2

        def derive_qualified_name_expr(
            expr: str, df_alias: Optional[str] = None
        ) -> UnresolvedAttribute:
            """Note that this method does not work for full column name like <db>.<schema>.<table>.column."""
            parts = split_snowflake_identifier_with_dot(expr)
            if len(parts) == 1:
                return UnresolvedAttribute(quote_name(parts[0]), df_alias=df_alias)
            else:
                # According to https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation,
                # the json value on the path should be case-sensitive
                return UnresolvedAttribute(
                    f"{quote_name(parts[0])}:{'.'.join(quote_name(part, keep_case=True) for part in parts[1:])}",
                    is_sql_text=True,
                    df_alias=df_alias,
                )

        if expr2 is not None:
            if not (isinstance(expr1, str) and isinstance(expr2, str)):
                raise ValueError(
                    "When Column constructor gets two arguments, both need to be <str>"
                )

            if expr2 == "*":
                self._expression = Star([], df_alias=expr1)
            elif _is_qualified_name:
                self._expression = derive_qualified_name_expr(expr2, expr1)
            else:
                self._expression = UnresolvedAttribute(
                    quote_name(expr2), df_alias=expr1
                )

            # Alias field should be from the parameter provided to DataFrame.alias(self, name: str)
            # A column from the aliased DataFrame instance can be created using this alias like col(<df_alias>, <col_name>)
            # In the IR we will need to store this alias to resolve which DataFrame instance the user is referring to
            if self._ast is None and _emit_ast:
                self._ast = create_ast_for_column(expr1, expr2, _caller_name)

        elif isinstance(expr1, str):
            if expr1 == "*":
                self._expression = Star([])
            elif _is_qualified_name:
                self._expression = derive_qualified_name_expr(expr1)
            else:
                self._expression = UnresolvedAttribute(quote_name(expr1))

            if self._ast is None and _emit_ast:
                self._ast = create_ast_for_column(expr1, None, _caller_name)

        elif isinstance(expr1, Expression):
            self._expression = expr1

            if self._ast is None and _emit_ast:
                if hasattr(expr1, "_ast"):
                    self._ast = expr1._ast
                else:
                    self._ast = snowpark_expression_to_ast(expr1)

        else:  # pragma: no cover
            raise TypeError("Column constructor only accepts str or expression.")

        assert self._expression is not None

    def __should_emit_ast_for_binary(self, other: Any) -> bool:
        """Helper function to determine without a session whether AST should be generated or not based on
        checking whether self and other have an AST."""
        if isinstance(other, (Column, Expression)) and other._ast is None:
            return False
        return self._ast is not None

    def __getitem__(self, field: Union[str, int]) -> "Column":
        """Accesses an element of ARRAY column by ordinal position, or an element of OBJECT column by key."""

        _emit_ast = self._ast is not None
        expr = None
        if isinstance(field, str):
            if _emit_ast:
                expr = proto.Expr()
                ast = with_src_position(expr.column_apply__string)
                ast.col.CopyFrom(self._ast)
                ast.field = field
            return Column(
                SubfieldString(self._expression, field), _ast=expr, _emit_ast=_emit_ast
            )
        elif isinstance(field, int):
            if _emit_ast:
                expr = proto.Expr()
                ast = with_src_position(expr.column_apply__int)
                ast.col.CopyFrom(self._ast)
                ast.idx = field
            return Column(
                SubfieldInt(self._expression, field), _ast=expr, _emit_ast=_emit_ast
            )
        else:
            raise TypeError(f"Unexpected item type: {type(field)}")

    def __eq__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Equal to."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.eq)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)

        right = Column._to_expr(other)
        return Column(EqualTo(self._expression, right), _ast=expr, _emit_ast=_emit_ast)

    def __ne__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Not equal to."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.neq)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)

        right = Column._to_expr(other)
        return Column(
            NotEqualTo(self._expression, right), _ast=expr, _emit_ast=_emit_ast
        )

    def __gt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.gt)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            GreaterThan(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __lt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.lt)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            LessThan(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __ge__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than or equal to."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.geq)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            GreaterThanOrEqual(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __le__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than or equal to."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.leq)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            LessThanOrEqual(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __add__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Plus."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.add)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Add(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __radd__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.add)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Add(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __sub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Minus."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.sub)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Subtract(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rsub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.sub)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Subtract(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __mul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Multiply."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.mul)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Multiply(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rmul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.mul)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Multiply(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __truediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Divide."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.div)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Divide(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rtruediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.div)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Divide(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __mod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Remainder."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.mod)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Remainder(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rmod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.mod)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Remainder(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __pow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Power."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.pow)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Pow(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rpow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.pow)
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            Pow(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

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

    @publicapi
    def in_(
        self,
        *vals: Union[
            LiteralType,
            Iterable[LiteralType],
            "Column",
            Iterable["Column"],
            "snowflake.snowpark.DataFrame",
        ],
        _emit_ast: bool = True,
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

            >>> # Use in with column object
            >>> df2 = session.create_dataframe([[1, 1], [2, 4] ,[3, 0]], schema=["a", "b"])
            >>> df2.select(df2["a"].in_(df2["b"]).alias("is_a_in_b")).collect()
            [Row(IS_A_IN_B=True), Row(IS_A_IN_B=False), Row(IS_A_IN_B=False)]

        Args:
            vals: The literal values, the columns in the same DataFrame, or a :class:`DataFrame` instance to use
                to check for membership against this column.
        """

        cols = parse_positional_args_to_list(*vals)

        # If cols is an empty list then in_ will always be False
        if not cols:
            ast = None
            if _emit_ast:
                ast = proto.Expr()
                proto_ast = ast.column_in
                proto_ast.col.CopyFrom(self._ast)

            return Column(Literal(False), _ast=ast, _emit_ast=_emit_ast)

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
                # literal and column
                if isinstance(value_expr, (Literal, Attribute, UnresolvedAttribute)):
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

        ast = None
        if _emit_ast:
            ast = proto.Expr()
            proto_ast = ast.column_in
            proto_ast.col.CopyFrom(self._ast)
            for val in vals:
                val_ast = proto_ast.values.add()
                if isinstance(val, snowflake.snowpark.dataframe.DataFrame):
                    val._set_ast_ref(val_ast)
                else:
                    build_expr_from_python_val(val_ast, val)

        return Column(
            InExpression(self._expression, value_expressions),
            _ast=ast,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def between(
        self,
        lower_bound: Union[ColumnOrLiteral, Expression],
        upper_bound: Union[ColumnOrLiteral, Expression],
        _emit_ast: bool = True,
    ) -> "Column":
        """Between lower bound and upper bound."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.column_between)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.lower_bound, lower_bound)
            build_expr_from_snowpark_column_or_python_val(ast.upper_bound, upper_bound)

        ret = (Column._to_expr(lower_bound) <= self) & (
            self <= Column._to_expr(upper_bound)
        )
        ret._ast = expr
        return ret

    @publicapi
    def bitand(
        self, other: Union[ColumnOrLiteral, Expression], _emit_ast: bool = True
    ) -> "Column":
        """Bitwise and."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.bit_and)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            BitwiseAnd(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def bitor(
        self, other: Union[ColumnOrLiteral, Expression], _emit_ast: bool = True
    ) -> "Column":
        """Bitwise or."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.bit_or)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            BitwiseOr(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def bitxor(
        self, other: Union[ColumnOrLiteral, Expression], _emit_ast: bool = True
    ) -> "Column":
        """Bitwise xor."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.bit_xor)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            BitwiseXor(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    # Note: For the operator overrides we always emit ast, it simply gets ignored in a call chain.

    def __neg__(self) -> "Column":
        """Unary minus."""

        expr = None
        _emit_ast = self._ast is not None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.neg)
            ast.operand.CopyFrom(self._ast)

        return Column(UnaryMinus(self._expression), _ast=expr, _emit_ast=_emit_ast)

    @publicapi
    def equal_null(self, other: "Column", _emit_ast: bool = True) -> "Column":
        """Equal to. You can use this for comparisons against a null value."""
        expr = None
        if _emit_ast := _emit_ast and self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(expr.column_equal_null)
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            EqualNullSafe(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def equal_nan(self, _emit_ast: bool = True) -> "Column":
        """Is NaN."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.column_equal_nan)
            ast.col.CopyFrom(self._ast)
        return Column(IsNaN(self._expression), _ast=expr, _emit_ast=_emit_ast)

    @publicapi
    def is_null(self, _emit_ast: bool = True) -> "Column":
        """Is null."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.column_is_null)
            ast.col.CopyFrom(self._ast)
        return Column(IsNull(self._expression), _ast=expr, _emit_ast=_emit_ast)

    @publicapi
    def is_not_null(self, _emit_ast: bool = True) -> "Column":
        """Is not null."""
        expr = None
        if _emit_ast and self._ast is not None:
            expr = proto.Expr()
            ast = with_src_position(expr.column_is_not_null)
            ast.col.CopyFrom(self._ast)
        return Column(IsNotNull(self._expression), _ast=expr, _emit_ast=_emit_ast)

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other: Union["Column", Any]) -> "Column":
        """And."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(getattr(expr, "and"))
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)

        return Column(
            And(self._expression, Column._to_expr(other)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def __rand__(self, other: Union["Column", Any]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(getattr(expr, "and"))
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            And(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )  # pragma: no cover

    def __or__(self, other: Union["Column", Any]) -> "Column":
        """Or."""
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(getattr(expr, "or"))
            ast.lhs.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.rhs, other)
        return Column(
            Or(self._expression, Column._to_expr(other)), _ast=expr, _emit_ast=_emit_ast
        )

    def __ror__(self, other: Union["Column", Any]) -> "Column":
        expr = None
        if _emit_ast := self.__should_emit_ast_for_binary(other):
            expr = proto.Expr()
            ast = with_src_position(getattr(expr, "or"))
            build_expr_from_snowpark_column_or_python_val(ast.lhs, other)
            ast.rhs.CopyFrom(self._ast)
        return Column(
            And(Column._to_expr(other), self._expression),
            _ast=expr,
            _emit_ast=_emit_ast,
        )  # pragma: no cover

    def __invert__(self) -> "Column":
        """Unary not."""
        expr = None
        _emit_ast = self._ast is not None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(getattr(expr, "not"))
            ast.operand.CopyFrom(self._ast)

        return Column(Not(self._expression), _ast=expr, _emit_ast=_emit_ast)

    def _cast(
        self,
        to: Union[str, DataType],
        try_: bool = False,
        rename_fields: bool = False,
        add_fields: bool = False,
        _emit_ast: bool = True,
    ) -> "Column":
        if add_fields and rename_fields:
            raise ValueError(
                "is_add and is_rename cannot be set to True at the same time"
            )
        if isinstance(to, str):
            to = type_string_to_type_object(to)

        if isinstance(to, (ArrayType, MapType, StructType)):
            to = to._as_nested()

        if self._ast is None:
            _emit_ast = False

        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_try_cast if try_ else expr.column_cast)
            ast.col.CopyFrom(self._ast)
            to._fill_ast(ast.to)
        return Column(
            Cast(self._expression, to, try_, rename_fields, add_fields),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def cast(
        self,
        to: Union[str, DataType],
        rename_fields: bool = False,
        add_fields: bool = False,
        _emit_ast: bool = True,
    ) -> "Column":
        """Casts the value of the Column to the specified data type.
        It raises an error when  the conversion can not be performed.
        """
        return self._cast(
            to,
            False,
            rename_fields=rename_fields,
            add_fields=add_fields,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def try_cast(
        self,
        to: Union[str, DataType],
        rename_fields: bool = False,
        add_fields: bool = False,
        _emit_ast: bool = True,
    ) -> "Column":
        """Tries to cast the value of the Column to the specified data type.
        It returns a NULL value instead of raising an error when the conversion can not be performed.
        """
        return self._cast(
            to,
            True,
            rename_fields=rename_fields,
            add_fields=add_fields,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def desc(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in descending order."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_desc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_default = True
        return Column(
            SortOrder(self._expression, Descending()), _ast=expr, _emit_ast=_emit_ast
        )

    @publicapi
    def desc_nulls_first(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted before non-null values)."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_desc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_nulls_first = True
        return Column(
            SortOrder(self._expression, Descending(), NullsFirst()),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def desc_nulls_last(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted after non-null values)."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_desc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_nulls_last = True
        return Column(
            SortOrder(self._expression, Descending(), NullsLast()),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def asc(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in ascending order."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_asc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_default = True
        return Column(
            SortOrder(self._expression, Ascending()), _ast=expr, _emit_ast=_emit_ast
        )

    @publicapi
    def asc_nulls_first(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted before non-null values)."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_asc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_nulls_first = True
        return Column(
            SortOrder(self._expression, Ascending(), NullsFirst()),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def asc_nulls_last(self, _emit_ast: bool = True) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted after non-null values)."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_asc)
            ast.col.CopyFrom(self._ast)
            ast.null_order.null_order_nulls_last = True
        return Column(
            SortOrder(self._expression, Ascending(), NullsLast()),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def like(self, pattern: ColumnOrLiteralStr, _emit_ast: bool = True) -> "Column":
        """Allows case-sensitive matching of strings based on comparison with a pattern.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `LIKE <https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes>`_.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_like)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.pattern, pattern)
        return Column(
            Like(self._expression, Column._to_expr(pattern)),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def regexp(
        self,
        pattern: ColumnOrLiteralStr,
        parameters: Optional[ColumnOrLiteralStr] = None,
        _emit_ast: bool = True,
    ) -> "Column":
        """Returns true if this Column matches the specified regular expression.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `regular expressions <https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes>`_.

        :meth:`rlike` is an alias of :meth:`regexp`.

        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_regexp)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.pattern, pattern)
            if parameters is not None:
                build_expr_from_snowpark_column_or_python_val(
                    ast.parameters, parameters
                )

        return Column(
            RegExp(
                self._expression,
                Column._to_expr(pattern),
                None if parameters is None else Column._to_expr(parameters),
            ),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def startswith(self, other: ColumnOrLiteralStr, _emit_ast: bool = True) -> "Column":
        """Returns true if this Column starts with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column starts with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_starts_with)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.prefix, other)
        other = (
            other
            if isinstance(other, Column)
            else snowflake.snowpark.functions.lit(other)
        )
        return Column(
            snowflake.snowpark.functions.startswith(self, other)._expression,
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def endswith(self, other: ColumnOrLiteralStr, _emit_ast: bool = True) -> "Column":
        """Returns true if this Column ends with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column ends with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_ends_with)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.suffix, other)

        other = (
            other
            if isinstance(other, Column)
            else snowflake.snowpark.functions.lit(other)
        )
        return Column(
            snowflake.snowpark.functions.endswith(self, other)._expression,
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def substr(
        self,
        start_pos: Union["Column", int],
        length: Union["Column", int],
        _emit_ast: bool = True,
    ) -> "Column":
        """Returns a substring of this string column.

        Args:
            start_pos: The starting position of the substring. Please note that the first character has position 1 instead of 0 in Snowflake database.
            length: The length of the substring.

        :meth:`substring` is an alias of :meth:`substr`.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_substr)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.pos, start_pos)
            build_expr_from_snowpark_column_or_python_val(ast.len, length)

        return Column(
            snowflake.snowpark.functions.substring(self, start_pos, length)._expression,
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def collate(self, collation_spec: str, _emit_ast: bool = True) -> "Column":
        """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
        property, rather than the original collation specification property.

        For details, see the Snowflake documentation on
        `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_collate)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(
                ast.collation_spec, collation_spec
            )
        return Column(
            Collate(self._expression, collation_spec), _ast=expr, _emit_ast=_emit_ast
        )

    @publicapi
    def contains(self, string: ColumnOrName, _emit_ast: bool = True) -> "Column":
        """Returns true if the column contains `string` for each row.

        Args:
            string: the string to search for in this column.
        """
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_string_contains)
            ast.col.CopyFrom(self._ast)
            build_expr_from_snowpark_column_or_python_val(ast.pattern, string)
        return Column(
            snowflake.snowpark.functions.contains(self, string)._expression,
            _ast=expr,
            _emit_ast=_emit_ast,
        )

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

    @publicapi
    def as_(self, alias: str, _emit_ast: bool = True) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant="as_", _emit_ast=_emit_ast)

    @publicapi
    def alias(self, alias: str, _emit_ast: bool = True) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant="alias", _emit_ast=_emit_ast)

    def _alias(self, alias: str) -> "Column":
        """Returns a new renamed Column called by functions that internally alias the result."""
        return self.name(alias, variant="_alias", _emit_ast=False)

    @publicapi
    def name(
        self,
        alias: str,
        variant: typing.Literal["as_", "alias", "name"] = "name",
        _emit_ast: bool = True,
    ) -> "Column":
        """Returns a new renamed Column."""
        expr = self._expression  # Snowpark expression
        if isinstance(expr, Alias):
            expr = expr.child
        ast_expr = None  # Snowpark IR expression
        if _emit_ast and self._ast is not None:
            ast_expr = proto.Expr()
            ast = with_src_position(ast_expr.column_alias)
            ast.col.CopyFrom(self._ast)
            ast.name = alias
            if variant == "as_":
                ast.fn.column_alias_fn_as = True
            elif variant == "alias":
                ast.fn.column_alias_fn_alias = True
            elif variant == "name":
                ast.fn.column_alias_fn_name = True

        if variant == "_alias":
            return Column(
                _InternalAlias(expr, quote_name(alias)),
                _ast=ast_expr,
                _emit_ast=_emit_ast,
            )
        return Column(
            Alias(expr, quote_name(alias)), _ast=ast_expr, _emit_ast=_emit_ast
        )

    @publicapi
    def over(
        self, window: Optional[WindowSpec] = None, _emit_ast: bool = True
    ) -> "Column":
        """
        Returns a window frame, based on the specified :class:`~snowflake.snowpark.window.WindowSpec`.
        """
        ast = None
        if _emit_ast:
            ast = proto.Expr()
            expr = with_src_position(ast.column_over)
            expr.col.CopyFrom(self._ast)
            if window:
                expr.window_spec.CopyFrom(window._ast)

        if not window:
            window = Window._spec()

        return window._with_aggregate(self._expression, ast=ast, _emit_ast=_emit_ast)

    @publicapi
    def within_group(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]], _emit_ast: bool = True
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
            >>> df.select(array_agg("a").within_group(col("b").asc()).alias("new_column")).show()
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
        # Set up result Column AST representation.
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            ast = with_src_position(expr.column_within_group)
            ast.col.CopyFrom(self._ast)
            ast.cols.variadic = not (
                len(cols) == 1 and isinstance(cols[0], (list, tuple, set))
            )

        # populate columns to order aggregate expression results by
        order_by_cols = []
        for col in parse_positional_args_to_list(*cols):
            if isinstance(col, Column):
                if _emit_ast:
                    assert col._ast is not None
                    ast.cols.args.append(col._ast)
                order_by_cols.append(col)
            elif isinstance(col, str):
                if _emit_ast:
                    col_ast = ast.cols.args.add()
                    col_ast.string_val.v = col
                order_by_cols.append(Column(col))
            else:
                raise TypeError(
                    f"'WITHIN_GROUP' expected Column or str, got: {type(col)}"
                )

        return Column(
            WithinGroup(
                self._expression,
                [order_by_col._expression for order_by_col in order_by_cols],
            ),
            _ast=expr,
            _emit_ast=_emit_ast,
        )

    def _named(self) -> NamedExpression:
        if isinstance(self._expression, NamedExpression):
            return self._expression
        else:
            return UnresolvedAlias(self._expression)

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
    def _expr(cls, e: str, ast: Optional[proto.Expr] = None) -> "Column":
        return cls(UnresolvedAttribute(e, is_sql_text=True), _ast=ast)

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

    @publicapi
    def __init__(
        self,
        expr: CaseWhen,
        _ast: Optional[proto.Expr] = None,
        _emit_ast: bool = True,
        *,
        _is_qualified_name: bool = False,
    ) -> None:
        super().__init__(
            expr, _is_qualified_name=_is_qualified_name, _ast=_ast, _emit_ast=_emit_ast
        )
        self._branches = expr.branches

    @publicapi
    def when(
        self, condition: ColumnOrSqlExpr, value: ColumnOrLiteral, _emit_ast: bool = True
    ) -> "CaseExpr":
        """
        Appends one more WHEN condition to the CASE expression.

        Args:
            condition: A :class:`Column` expression or SQL text representing the specified condition.
            value: A :class:`Column` expression or a literal value, which will be returned
                if ``condition`` is true.
        """

        if _emit_ast:
            case_expr = with_src_position(self._ast.column_case_expr.cases.add())
            build_expr_from_snowpark_column_or_sql_str(case_expr.condition, condition)
            build_expr_from_snowpark_column_or_python_val(case_expr.value, value)

        return CaseExpr(
            CaseWhen(
                [
                    *self._branches,
                    (
                        _to_col_if_sql_expr(condition, "when")._expression,
                        Column._to_expr(value),
                    ),
                ]
            ),
            _ast=self._ast,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def otherwise(self, value: ColumnOrLiteral, _emit_ast: bool = True) -> "CaseExpr":
        """Sets the default result for this CASE expression.

        :meth:`else_` is an alias of :meth:`otherwise`.
        """
        if _emit_ast:
            case_expr = with_src_position(self._ast.column_case_expr.cases.add())
            build_expr_from_snowpark_column_or_python_val(case_expr.value, value)
        return CaseExpr(
            CaseWhen(self._branches, Column._to_expr(value)), _ast=self._ast
        )

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
