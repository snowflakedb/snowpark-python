#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import inspect
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.binary_expression import (
    Add,
    And,
    BinaryExpression,
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
    UnaryExpression,
    UnaryMinus,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.ast_utils import (
    build_const_from_python_val,
    setattr_if_not_none,
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

    # NOTE: For now assume Expression instances can be safely ignored when building AST
    #       Expression logic can be eliminated entirely once phase 0 is integrated
    #       Currently a breaking example can be created using the Column.isin method as it does not build the AST.
    #       For example, running: df.filter(col("A").isin(1, 2, 3) & col("B")) would fail since the boolean operator
    #       '&' would try to construct an AST using that of the new col("A").isin(1, 2, 3) column (which we currently
    #       don't fill if the only argument provided in the Column constructor is 'expr1' of type Expression)
    def __init__(
        self,
        expr1: Union[str, Expression],
        expr2: Optional[str] = None,
        ast: Optional[proto.Expr] = None,
    ) -> None:
        self._ast = ast

        if expr2 is not None:
            if not (isinstance(expr1, str) and isinstance(expr2, str)):
                raise ValueError(
                    "When Column constructor gets two arguments, both need to be <str>"
                )

            if expr2 == "*":
                self._expression = Star([], df_alias=expr1)
            else:
                self._expression = UnresolvedAttribute(
                    quote_name(expr2), df_alias=expr1
                )

            # Alias field should be from the parameter provided to DataFrame.alias(self, name: str)
            # A column from the aliased DataFrame instance can be created using this alias like col(<df_alias>, <col_name>)
            # In the IR we will need to store this alias to resolve which DataFrame instance the user is referring to
            if self._ast is None:
                if expr2 == "*":
                    self._ast = Column._create_ast(
                        property="sp_column_sql_expr",
                        assign_fields={"sql": "*"},
                        assign_opt_fields={"df_alias": expr1},
                    )
                else:
                    self._ast = Column._create_ast(
                        property="sp_column",
                        assign_fields={"name": quote_name(expr2)},
                        assign_opt_fields={"df_alias": expr1},
                    )
        elif isinstance(expr1, str):
            if expr1 == "*":
                self._expression = Star([])
            else:
                self._expression = UnresolvedAttribute(quote_name(expr1))

            if self._ast is None:
                if expr1 == "*":
                    self._ast = Column._create_ast(
                        property="sp_column_sql_expr", assign_fields={"sql": "*"}
                    )
                else:
                    self._ast = Column._create_ast(
                        property="sp_column", assign_fields={"name": quote_name(expr1)}
                    )

        elif isinstance(expr1, Expression):
            self._expression = expr1
        else:  # pragma: no cover
            raise TypeError("Column constructor only accepts str or expression.")

    def __getitem__(self, field: Union[str, int]) -> "Column":
        """Accesses an element of ARRAY column by ordinal position, or an element of OBJECT column by key."""
        if isinstance(field, str):
            ast = Column._create_ast(
                property="sp_column_apply__string",
                copy_messages={"col": self._ast},
                assign_fields={"field": quote_name(field, keep_case=True)},
            )
            return Column(SubfieldString(self._expression, field), ast=ast)
        elif isinstance(field, int):
            ast = Column._create_ast(
                property="sp_column_apply__int",
                copy_messages={"col": self._ast},
                assign_fields={"idx": field},
            )
            return Column(SubfieldInt(self._expression, field), ast=ast)
        else:
            raise TypeError(f"Unexpected item type: {type(field)}")

    # overload operators
    def _bin_op_impl(
        self, property: str, operator: BinaryExpression, other: ColumnOrLiteral
    ) -> "Column":
        """Binary __<op>__ operator implementation for columns with automatic AST logging, i.e. op(self, other)

        Args:
            property (str): IR entity representing the binary operation in the Column AST
            operator (BinaryExpression): Snowpark binary operator to build the Expression instance
            other (ColumnOrLiteral): Other input column to be operated over (rhs)

        Returns:
            Column: new column instance with AST representing new operation over inputs in Column._ast
        """
        ast = Column._create_ast(
            property=property,
            copy_messages={"lhs": self._ast},
            fill_expr_asts={"rhs": other},
        )
        return Column(operator(self._expression, Column._to_expr(other)), ast=ast)

    def _bin_op_rimpl(
        self, property: str, operator: BinaryExpression, other: ColumnOrLiteral
    ) -> "Column":
        """Binary __r<op>__ operator implementation for columns with AST logging, i.e. op(other, self)

        Args:
            property (str): IR entity representing the binary operation in the Column AST
            operator (BinaryExpression): Snowpark binary operator to build the Expression instance
            other (ColumnOrLiteral): Other input column to be operated over (lhs)

        Returns:
            Column: new column instance with AST representing new operation over inputs in Column._ast
        """
        ast = Column._create_ast(
            property=property,
            copy_messages={"rhs": self._ast},
            fill_expr_asts={"lhs": other},
        )
        return Column(operator(Column._to_expr(other), self._expression), ast=ast)

    def __eq__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Equal to."""
        return self._bin_op_impl("eq", EqualTo, other)

    def __ne__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Not equal to."""
        return self._bin_op_impl("neq", NotEqualTo, other)

    def __gt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than."""
        return self._bin_op_impl("gt", GreaterThan, other)

    def __lt__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than."""
        return self._bin_op_impl("lt", LessThan, other)

    def __ge__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Greater than or equal to."""
        return self._bin_op_impl("geq", GreaterThanOrEqual, other)

    def __le__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Less than or equal to."""
        return self._bin_op_impl("leq", LessThanOrEqual, other)

    def __add__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Plus."""
        return self._bin_op_impl("add", Add, other)

    def __radd__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("add", Add, other)

    def __sub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Minus."""
        return self._bin_op_impl("sub", Subtract, other)

    def __rsub__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("sub", Subtract, other)

    def __mul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Multiply."""
        return self._bin_op_impl("mul", Multiply, other)

    def __rmul__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("mul", Multiply, other)

    def __truediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Divide."""
        return self._bin_op_impl("div", Divide, other)

    def __rtruediv__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("div", Divide, other)

    def __mod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Remainder."""
        return self._bin_op_impl("mod", Remainder, other)

    def __rmod__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("mod", Remainder, other)

    def __pow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Power."""
        return self._bin_op_impl("pow", Pow, other)

    def __rpow__(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        return self._bin_op_rimpl("pow", Pow, other)

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

    # TODO: SNOW-1474906
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
        ast = Column._create_ast(
            property="sp_column_between",
            copy_messages={"col": self._ast},
            fill_expr_asts={"lower_bound": lower_bound, "upper_bound": upper_bound},
        )
        ret = (Column._to_expr(lower_bound) <= self) & (
            self <= Column._to_expr(upper_bound)
        )
        ret._ast = ast
        return ret

    def bitand(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise and."""
        return self._bin_op_rimpl("bit_and", BitwiseAnd, other)

    def bitor(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise or."""
        return self._bin_op_rimpl("bit_or", BitwiseOr, other)

    def bitxor(self, other: Union[ColumnOrLiteral, Expression]) -> "Column":
        """Bitwise xor."""
        return self._bin_op_rimpl("bit_xor", BitwiseXor, other)

    def _unary_op_impl(
        self, property: str, operator: UnaryExpression, attr: str = "col"
    ) -> "Column":
        """Unary operation implementation for Columns with automatic AST logging

        Args:
            property (str): IR entity representing the unary operation in the Column AST
            operator (UnaryExpression): Snowpark unary operator to build the Expression instance
            attr (str, optional): Field of the IR entity representing this Column instance, or self. Defaults to "col".

        Returns:
            Column: new column instance with AST representing new operation over this Column instance, self.
        """
        ast = Column._create_ast(
            property=property,
            copy_messages={attr: self._ast},
        )
        return Column(operator(self._expression), ast=ast)

    def __neg__(self) -> "Column":
        """Unary minus."""
        return self._unary_op_impl("neg", UnaryMinus, attr="operand")

    def equal_null(self, other: "Column") -> "Column":
        """Equal to. You can use this for comparisons against a null value."""
        return self._bin_op_impl("sp_column_equal_null", EqualNullSafe, other)

    def equal_nan(self) -> "Column":
        """Is NaN."""
        return self._unary_op_impl("sp_column_equal_nan", IsNaN)

    def is_null(self) -> "Column":
        """Is null."""
        return self._unary_op_impl("sp_column_is_null", IsNull)

    def is_not_null(self) -> "Column":
        """Is not null."""
        return self._unary_op_impl("sp_column_is_not_null", IsNotNull)

    # `and, or, not` cannot be overloaded in Python, so use bitwise operators as boolean operators
    def __and__(self, other: "Column") -> "Column":
        """And."""
        return self._bin_op_impl("and", And, other)

    def __rand__(self, other: "Column") -> "Column":
        return self._bin_op_rimpl("and", And, other)  # pragma: no cover

    def __or__(self, other: "Column") -> "Column":
        """Or."""
        return self._bin_op_impl("or", Or, other)

    def __ror__(self, other: "Column") -> "Column":
        return self._bin_op_rimpl("or", Or, other)  # pragma: no cover

    def __invert__(self) -> "Column":
        """Unary not."""
        return self._unary_op_impl("not", Not, attr="operand")

    def _cast(self, to: Union[str, DataType], try_: bool = False) -> "Column":
        if isinstance(to, str):
            to = type_string_to_type_object(to)
        if try_:
            ast = Column._create_ast(
                property="sp_column_try_cast",
                copy_messages={"col": self._ast},
            )
            to._fill_ast(ast.sp_column_try_cast.to)
        else:
            ast = Column._create_ast(
                property="sp_column_cast",
                copy_messages={"col": self._ast},
            )
            to._fill_ast(ast.sp_column_cast.to)

        return Column(Cast(self._expression, to, try_), ast=ast)

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
        ast = Column._create_ast(
            property="sp_column_desc", copy_messages={"col": self._ast}
        )
        return Column(SortOrder(self._expression, Descending()), ast=ast)

    def desc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted before non-null values)."""
        ast = Column._create_ast(
            property="sp_column_desc",
            copy_messages={"col": self._ast},
            assign_opt_fields={"nulls_first": True},
        )
        return Column(SortOrder(self._expression, Descending(), NullsFirst()), ast=ast)

    def desc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in descending order
        (null values sorted after non-null values)."""
        ast = Column._create_ast(
            property="sp_column_desc",
            copy_messages={"col": self._ast},
            assign_opt_fields={"nulls_first": False},
        )
        return Column(SortOrder(self._expression, Descending(), NullsLast()), ast=ast)

    def asc(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order."""
        ast = Column._create_ast(
            property="sp_column_asc",
            copy_messages={"col": self._ast},
        )
        return Column(SortOrder(self._expression, Ascending()), ast=ast)

    def asc_nulls_first(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted before non-null values)."""
        ast = Column._create_ast(
            property="sp_column_asc",
            copy_messages={"col": self._ast},
            assign_opt_fields={"nulls_first": True},
        )
        return Column(SortOrder(self._expression, Ascending(), NullsFirst()), ast=ast)

    def asc_nulls_last(self) -> "Column":
        """Returns a Column expression with values sorted in ascending order
        (null values sorted after non-null values)."""
        ast = Column._create_ast(
            property="sp_column_asc",
            copy_messages={"col": self._ast},
            assign_opt_fields={"nulls_first": False},
        )
        return Column(SortOrder(self._expression, Ascending(), NullsLast()), ast=ast)

    def like(self, pattern: ColumnOrLiteralStr) -> "Column":
        """Allows case-sensitive matching of strings based on comparison with a pattern.

        Args:
            pattern: A :class:`Column` or a ``str`` that indicates the pattern.
                A ``str`` will be interpreted as a literal value instead of a column name.

        For details, see the Snowflake documentation on
        `LIKE <https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes>`_.
        """
        ast = Column._create_ast(
            property="string_like",
            copy_messages={"str": self._ast},
            fill_expr_asts={"pattern": pattern},
        )
        return Column(
            Like(
                self._expression,
                Column._to_expr(pattern),
            ),
            ast=ast,
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
        ast = Column._create_ast(
            property="string_regexp",
            copy_messages={"str": self._ast},
            fill_expr_asts={"pattern": pattern},
        )
        return Column(
            RegExp(
                self._expression,
                Column._to_expr(pattern),
            ),
            ast=ast,
        )

    def startswith(self, other: ColumnOrLiteralStr) -> "Column":
        """Returns true if this Column starts with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column starts with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        ast = Column._create_ast(
            property="string_starts_with",
            copy_messages={"str": self._ast},
            fill_expr_asts={"prefix": other},
        )
        other = snowflake.snowpark.functions.lit(other)
        return Column(
            snowflake.snowpark.functions.startswith(self, other)._expression, ast=ast
        )

    def endswith(self, other: ColumnOrLiteralStr) -> "Column":
        """Returns true if this Column ends with another string.

        Args:
            other: A :class:`Column` or a ``str`` that is used to check if this column ends with it.
                A ``str`` will be interpreted as a literal value instead of a column name.
        """
        ast = Column._create_ast(
            property="string_ends_with",
            copy_messages={"str": self._ast},
            fill_expr_asts={"suffix": other},
        )
        other = snowflake.snowpark.functions.lit(other)
        return Column(
            snowflake.snowpark.functions.startswith(self, other)._expression, ast=ast
        )

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
        ast = Column._create_ast(
            property="string_substr",
            copy_messages={"str": self._ast},
            fill_expr_asts={"pos": start_pos, "len": length},
        )
        return Column(
            snowflake.snowpark.functions.substring(self, start_pos, length)._expression,
            ast=ast,
        )

    def collate(self, collation_spec: str) -> "Column":
        """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
        property, rather than the original collation specification property.

        For details, see the Snowflake documentation on
        `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.
        """
        ast = Column._create_ast(
            property="string_collate",
            copy_messages={"str": self._ast},
            assign_fields={"collation_spec": collation_spec},
        )
        return Column(Collate(self._expression, collation_spec), ast=ast)

    def contains(self, string: ColumnOrName) -> "Column":
        """Returns true if the column contains `string` for each row.

        Args:
            string: the string to search for in this column.
        """
        ast = Column._create_ast(
            property="string_contains",
            copy_messages={"str": self._ast},
            fill_expr_asts={"pattern": string},
        )
        return Column(
            snowflake.snowpark.functions.contains(self, string)._expression, ast=ast
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

    def as_(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant_is_as=True)

    def alias(self, alias: str) -> "Column":
        """Returns a new renamed Column. Alias of :func:`name`."""
        return self.name(alias, variant_is_as=False)

    def name(self, alias: str, variant_is_as: bool = None) -> "Column":
        """Returns a new renamed Column."""
        ast = Column._create_ast(
            property="sp_column_alias",
            copy_messages={"col": self._ast},
            assign_fields={"name": quote_name(alias)},
            assign_opt_fields={"variant_is_as": variant_is_as},
        )
        return Column(Alias(self._expression, quote_name(alias)), ast=ast)

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
        # set up result Column AST representation
        prop_name = "sp_column_within_group"
        ast = Column._create_ast(
            property=prop_name,
            copy_messages={"col": self._ast},
            assign_fields={
                "variadic": (
                    len(cols) > 1 or not isinstance(cols[0], (list, tuple, set))
                )
            },
        )

        # populate columns to order aggregate expression results by
        order_by_cols = []
        for col in parse_positional_args_to_list(*cols):
            if isinstance(col, Column):
                # Column instance, col, must have col._ast member
                getattr(ast, prop_name).cols.append(col._ast)
                order_by_cols.append(col)
            elif isinstance(col, str):
                # create new Column instance, with new AST
                col_ast = getattr(ast, prop_name).cols.add()
                new_col = Column(col, ast=col_ast)
                col_ast.sp_column.name = new_col.get_name()
                order_by_cols.append(new_col)
            else:
                raise TypeError(
                    f"'WITHIN_GROUP' expected Column or str, got: {type(col)}"
                )

        return Column(WithinGroup(self._expression, order_by_cols), ast=ast)

    def _named(self) -> NamedExpression:
        if isinstance(self._expression, NamedExpression):
            return self._expression
        else:
            return UnresolvedAlias(self._expression)

    @staticmethod
    def _create_ast(
        property: Optional[str] = None,
        assign_fields: Dict[str, Any] = {},  # noqa: B006
        assign_opt_fields: Dict[str, Any] = {},  # noqa: B006
        copy_messages: Dict[str, Any] = {},  # noqa: B006
        fill_expr_asts: Dict[str, ColumnOrLiteral] = {},  # noqa: B006
    ) -> proto.SpColumnExpr:
        """General purpose static method to generate the AST representation for a new Snowpark Column instance

        Args:
            property (str, optional): The protobuf property name of a subtype of the SpColumnExpr IR entity. Defaults to None.
            assign_fields (Dict[str, Any], optional): Subtype fields with well known protobuf types that support direct assignment. Defaults to {}.
            copy_messages (Dict[str, Any], optional): Subtype message fields which must be copied into (do not support assignment). Defaults to {}.
            fill_expr_asts (Dict[str, ColumnOrLiteral], optional): Subtype Expr fields that must be filled explicitly from a ColumnOrLiteral type. Defaults to {}.

        Returns:
            proto.SpColumnExpr: Returns fully populated SpColumnExpr AST from given arguments
        """

        ast = proto.Expr()
        if property is not None:
            prop_ast = getattr(ast, property)
            fill_src_position(prop_ast.src)
            for attr, value in assign_fields.items():
                setattr_if_not_none(prop_ast, attr, value)
            for attr, value in assign_opt_fields.items():
                setattr_if_not_none(getattr(prop_ast, attr), "value", value)
            for attr, msg in copy_messages.items():
                if msg is None:
                    call_stack = inspect.stack()
                    curr_frame = call_stack.pop(0)
                    while call_stack and __file__ == curr_frame.filename:
                        column_api = curr_frame.function
                        curr_frame = call_stack.pop(0)
                    raise NotImplementedError(f"Calling Column API \"{column_api}\" which supports AST logging, from File \"{curr_frame.filename}\", line {curr_frame.lineno}, potentially in {curr_frame.function}\n"
                                              f"\t{curr_frame.code_context[0]}"
                                              f"\tAn API from snowflake.snowpark.functions used above has not yet implemented AST logging.")
                getattr(prop_ast, attr).CopyFrom(msg)
            for attr, other in fill_expr_asts.items():
                Column._fill_ast(getattr(prop_ast, attr), other)
        return ast

    @classmethod
    def _fill_ast(cls, ast: proto.Expr, value: ColumnOrLiteral) -> None:
        """Copy from a Column object's AST, or copy a literal value into an AST expression.

        Args:
            ast (proto.SpColumnExpr): A previously created Expr() or SpColumnExpr() IR entity intance to be filled
            value (ColumnOrLiteral): The value from which to populate the provided ast parameter.

        Raises:
            TypeError: An SpColumnExpr can only be populated from another SpColumnExpr or a valid Literal type
        """
        if isinstance(value, cls):
            return ast.CopyFrom(value._ast)
        elif isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            build_const_from_python_val(value, ast)
        elif isinstance(value, Expression):
            pass  # TODO: clean this up
        else:
            raise TypeError(
                f"{type(value)} is not a valid type for Column or literal AST."
            )

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
