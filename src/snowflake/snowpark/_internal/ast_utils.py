#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import inspect
import re
import sys
from functools import reduce
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Expression,
    Literal,
    MultipleExpression,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    ColumnOrLiteral,
)

# This flag causes an explicit error to be raised if any Snowpark object instance is missing an AST or field, when this
# AST or field is required to populate the AST field of a different Snowpark object instance.
FAIL_ON_MISSING_AST = True


def build_const_from_python_val(obj: Any, ast: proto.Expr) -> None:
    """Infer the Const AST expression from obj, and populate the provided ast.Expr() instance

    Args:
        obj (Any): Expected to be any acceptable Python literal or constant value
        ast (proto.Expr): A previously created Expr() IR entity to be filled.

    Raises:
        TypeError: Raised if the Python constant/literal is not supported by the Snowpark client.
    """
    from snowflake.snowpark.column import Column

    if obj is None:
        set_src_position(ast.null_val.src)

    elif isinstance(obj, bool):
        set_src_position(ast.bool_val.src)
        ast.bool_val.v = obj

    elif isinstance(obj, int):
        set_src_position(ast.int64_val.src)
        ast.int64_val.v = obj

    elif isinstance(obj, float):
        set_src_position(ast.float64_val.src)
        ast.float64_val.v = obj

    elif isinstance(obj, str):
        set_src_position(ast.string_val.src)
        ast.string_val.v = obj

    elif isinstance(obj, bytes):
        set_src_position(ast.binary_val.src)
        ast.binary_val.v = obj

    elif isinstance(obj, bytearray):
        set_src_position(ast.binary_val.src)
        ast.binary_val.v = bytes(obj)

    elif isinstance(obj, decimal.Decimal):
        set_src_position(ast.big_decimal_val.src)
        dec_tuple = obj.as_tuple()
        unscaled_val = reduce(lambda val, digit: val * 10 + digit, dec_tuple.digits)
        if dec_tuple.sign != 0:
            unscaled_val *= -1
        req_bytes = (unscaled_val.bit_length() + 7) // 8
        ast.big_decimal_val.unscaled_value = unscaled_val.to_bytes(
            req_bytes, "big", signed=True
        )
        ast.big_decimal_val.scale = dec_tuple.exponent

    elif isinstance(obj, datetime.datetime):
        set_src_position(ast.python_timestamp_val.src)
        if obj.tzinfo is not None:
            ast.python_timestamp_val.tz.offset_seconds = int(
                obj.tzinfo.utcoffset(obj).total_seconds()
            )
            tz = obj.tzinfo.tzname(obj)
            if tz is not None:
                ast.python_timestamp_val.tz.name.value = tz
        else:
            obj = obj.astimezone(datetime.timezone.utc)

        ast.python_timestamp_val.year = obj.year
        ast.python_timestamp_val.month = obj.month
        ast.python_timestamp_val.day = obj.day
        ast.python_timestamp_val.hour = obj.hour
        ast.python_timestamp_val.minute = obj.minute
        ast.python_timestamp_val.second = obj.second
        ast.python_timestamp_val.microsecond = obj.microsecond

    elif isinstance(obj, datetime.date):
        set_src_position(ast.python_date_val.src)
        ast.python_date_val.year = obj.year
        ast.python_date_val.month = obj.month
        ast.python_date_val.day = obj.day

    elif isinstance(obj, datetime.time):
        set_src_position(ast.python_time_val.src)
        datetime_val = datetime.datetime.combine(datetime.date.today(), obj)
        if obj.tzinfo is not None:
            ast.python_time_val.tz.offset_seconds = int(
                obj.tzinfo.utcoffset(datetime_val).total_seconds()
            )
            tz = obj.tzinfo.tzname(datetime_val)
            if tz is not None:
                ast.python_time_val.tz.name.value = tz
        else:
            obj = datetime_val.astimezone(datetime.timezone.utc)

        ast.python_time_val.hour = obj.hour
        ast.python_time_val.minute = obj.minute
        ast.python_time_val.second = obj.second
        ast.python_time_val.microsecond = obj.microsecond

    elif isinstance(obj, dict):
        set_src_position(ast.seq_map_val.src)
        for key, value in obj.items():
            kv_tuple_ast = ast.seq_map_val.kvs.add()
            build_const_from_python_val(key, kv_tuple_ast.vs.add())
            build_const_from_python_val(value, kv_tuple_ast.vs.add())

    elif isinstance(obj, list):
        set_src_position(ast.list_val.src)
        for v in obj:
            build_const_from_python_val(v, ast.list_val.vs.add())

    elif isinstance(obj, tuple):
        set_src_position(ast.tuple_val.src)
        for v in obj:
            build_const_from_python_val(v, ast.tuple_val.vs.add())

    elif isinstance(obj, Column):
        assert hasattr(obj, "_ast"), f"Column object {obj} has no _ast member set."
        assert obj._ast is not None, f"Column object {obj} has _ast set to None."
        ast.CopyFrom(obj._ast)

    else:
        raise NotImplementedError("not supported type: %s" % type(obj))


def build_fn_apply(
    ast: proto.Expr,
    builtin_name: str,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:
    """
    Creates AST encoding for ApplyExpr(BuiltinFn(<builtin_name>, List(<args...>), Map(<kwargs...>))) for builtin
    functions.
    Args:
        ast: Expr node to fill
        builtin_name: Name of the builtin function to call.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    """

    expr = with_src_position(ast.apply_expr)

    fn = proto.BuiltinFn()
    fn.name = builtin_name
    set_src_position(fn.src)
    expr.fn.builtin_fn.CopyFrom(fn)

    for arg in args:
        if isinstance(arg, proto.Expr):
            expr.pos_args.append(arg)
        elif hasattr(arg, "_ast"):
            assert arg._ast, f"Column object {arg} has no _ast member set."
            expr.pos_args.append(arg._ast)
        else:
            pos_arg = proto.Expr()
            build_const_from_python_val(arg, pos_arg)
            expr.pos_args.append(pos_arg)

    for name, arg in kwargs.items():
        kwarg = proto.Tuple_String_Expr()
        kwarg._1 = name
        if isinstance(arg, proto.Expr):
            kwarg._2.CopyFrom(arg)
        elif isinstance(arg, snowflake.snowpark.Column):
            assert arg._ast, f"Column object {name}={arg} has no _ast member set."
            kwarg._2.CopyFrom(arg._ast)
        else:
            build_const_from_python_val(arg, kwarg._2)
        expr.named_args.append(kwarg)


def get_first_non_snowpark_stack_frame() -> inspect.FrameInfo:
    """Searches up through the call stack using inspect library to find the first stack frame
    of a caller within a file which does not lie within the Snowpark library itself.

    Returns:
        inspect.FrameInfo: The FrameInfo object of the lowest caller outside of the Snowpark repo.
    """
    # TODO: Once `with_src_position()` is used exclusively, this can be abandoned.
    idx = 0
    call_stack = inspect.stack()
    curr_frame = call_stack[idx]
    snowpark_path = Path(__file__).parents[1]
    while snowpark_path in Path(curr_frame.filename).parents:
        idx += 1
        curr_frame = call_stack[idx]
    return curr_frame


# TODO: remove this function and convert all callers to with_src_position.
def set_src_position(src: proto.SrcPosition) -> None:
    """Uses the method to retrieve the first non snowpark stack frame, and sets the SrcPosition IR entity
    with the filename, and lineno which can be retrieved. In Python 3.11 and up the end line and column
    offsets can also be retrieved from the FrameInfo.positions field.

    Args:
        src (proto.SrcPosition): SrcPosition builder.
    """
    frame = get_first_non_snowpark_stack_frame()

    src.file = frame.filename
    src.start_line = frame.lineno

    if sys.version_info >= (3, 11):
        pos = frame.positions
        if pos.lineno is not None:
            src.start_line = pos.lineno
        if pos.end_lineno is not None:
            src.end_line = pos.end_lineno
        if pos.col_offset is not None:
            src.start_column = pos.col_offset
        if pos.end_col_offset is not None:
            src.end_column = pos.end_col_offset


assignment_re = re.compile(r"^\s*([a-zA-Z_]\w*)\s*=.*$", re.DOTALL)


def with_src_position(
    expr_ast: proto.Expr, assign: Optional[proto.Assign] = None
) -> proto.Expr:
    """
    Sets the src_position on the supplied Expr AST node and returns it.
    N.B. This function assumes it's always invoked from a public API, meaning that the caller's caller
    is always the code of interest.
    """
    frame = (
        get_first_non_snowpark_stack_frame()
    )  # TODO: implement the assumption above to minimize overhead.
    source_line = frame.code_context[0].strip() if frame.code_context else ""

    src = expr_ast.src
    src.file = frame.filename
    src.start_line = frame.lineno
    if sys.version_info >= (3, 11):
        pos = frame.positions
        if pos.lineno is not None:
            src.start_line = pos.lineno
        if pos.end_lineno is not None:
            src.end_line = pos.end_lineno
        if pos.col_offset is not None:
            src.start_column = pos.col_offset
        if pos.end_col_offset is not None:
            src.end_column = pos.end_col_offset

    if assign is not None:
        match = assignment_re.fullmatch(source_line)
        if match is not None:
            assign.symbol.value = match.group(1)

    return expr_ast


def _fill_ast_with_snowpark_column_or_literal(
    ast: proto.Expr, value: ColumnOrLiteral
) -> None:
    """Copy from a Column object's AST, or copy a literal value into an AST expression.

    Args:
        ast (proto.SpColumnExpr): A previously created Expr() or SpColumnExpr() IR entity intance to be filled
        value (ColumnOrLiteral): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: An SpColumnExpr can only be populated from another SpColumnExpr or a valid Literal type
    """
    if isinstance(value, snowflake.snowpark.Column):
        if value._ast is None and FAIL_ON_MISSING_AST:
            raise NotImplementedError(
                f"Column({value._expression})._ast is None due to the use of a Snowpark API which does not support AST logging yet."
            )
        elif value._ast is not None:
            ast.CopyFrom(value._ast)
    elif isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
        build_const_from_python_val(value, ast)
    elif isinstance(value, Expression):
        # Expressions must be handled by caller.
        pass
    else:
        raise TypeError(f"{type(value)} is not a valid type for Column or literal AST.")


def fill_ast_for_column(
    expr: proto.Expr, name1: str, name2: Optional[str], fn_name="col"
) -> None:
    """
    Fill in expr node to encode Snowpark Column created through col(...) / column(...).
    Args:
        expr: Ast node to fill in.
        name1: When name2 is None, this corresponds col_name. Else, this is df_alias.
        name2: When not None, this is col_name.
        fn_name: alias to use when encoding Snowpark column (should be "col" or "column").

    """

    # Handle the special case * (as a SQL column expr).
    if name2 == "*":
        ast = with_src_position(expr.sp_column_sql_expr)
        ast.sql = "*"
        if name1 is not None:
            ast.df_alias.value = name1
        return expr

    if name1 == "*" and name2 is None:
        ast = with_src_position(expr.sp_column_sql_expr)
        ast.sql = "*"
        return expr

    # Regular form (without *): build as function ApplyExpr.
    kwargs = (
        {"df_alias": name1, "col_name": name2}
        if name2 is not None
        else {"col_name": name1}
    )

    # To replicate Snowpark behavior (overloads do NOT seem to work at the moment)
    # - use args.
    args = tuple(kwargs.values())
    kwargs = {}

    build_fn_apply(expr, fn_name, *args, **kwargs)


def create_ast_for_column(
    name1: str, name2: Optional[str], fn_name="col"
) -> proto.Expr:
    """
    Helper function to create Ast for Snowpark Column. Cf. fill_ast_for_column on parameter details.
    """
    ast = proto.Expr()
    fill_ast_for_column(ast, name1, name2, fn_name)
    return ast


def snowpark_expression_to_ast(expr: Expression) -> proto.Expr:
    """
    Converts Snowpark expression expr to protobuf ast.
    Args:
        expr: A Snowpark expression (or instance of a derived class from Expression).

    Returns:
        protobuf expression.
    """
    if hasattr(expr, "_ast"):
        return expr._ast

    if isinstance(expr, Alias):
        # TODO: Not sure if this can come up in a real use case. We see this use case for internal calls, where
        # we don't need an AST.
        return None
    elif isinstance(expr, Attribute):
        return create_ast_for_column(expr.name, None)
    elif isinstance(expr, Literal):
        ast = proto.Expr()
        build_const_from_python_val(expr.value, ast)
        return ast
    elif isinstance(expr, UnresolvedAttribute):
        # Unresolved means treatment as sql expression.
        ast = proto.Expr()
        sql_expr_ast = with_src_position(ast.sp_column_sql_expr)
        sql_expr_ast.sql = expr.sql
        return ast
    elif isinstance(expr, MultipleExpression):
        # Convert to list of expressions.
        ast = proto.Expr()
        for child_expr in expr.expressions:
            ast_list = ast.list_val.vs.add()
            ast_list.CopyFrom(snowpark_expression_to_ast(child_expr))
        return ast
    elif isinstance(expr, CaseWhen):
        # TODO: Not sure if this can come up in a real use case. We see this use case for internal calls, where
        # we don't need an AST.
        return None
    else:
        raise NotImplementedError(
            f"Snowpark expr {expr} of type {type(expr)} is an expression with missing AST or for which an AST can not be auto-generated."
        )
