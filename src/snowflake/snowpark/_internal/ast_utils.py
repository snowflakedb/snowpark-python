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
    Expression,
    Literal,
    MultipleExpression,
    UnresolvedAttribute,
)
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
            setattr_if_not_none(
                ast.python_timestamp_val.tz.name, "value", obj.tzinfo.tzname(obj)
            )
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
            setattr_if_not_none(
                ast.python_time_val.tz.name, "value", obj.tzinfo.tzname(datetime_val)
            )
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

    Returns:

    """

    expr = ast.apply_expr

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
    idx = 0
    call_stack = inspect.stack()
    curr_frame = call_stack[idx]
    snowpark_path = Path(__file__).parents[1]
    while snowpark_path in Path(curr_frame.filename).parents:
        idx += 1
        curr_frame = call_stack[idx]
    return curr_frame


# TODO: SNOW-1476291
# TODO: better name.
def get_symbol() -> Optional[str]:
    """Using the code context from a FrameInfo object, and applies a regexp to match the
    symbol left of the "=" sign in the assignment expression

    Returns:
        Optional[str]: None if symbol name could not be matched using the regexp, symbol otherwise.
    """
    re_symbol_name = re.compile(r"^\s*([a-zA-Z_]\w*)\s*=.*$", re.DOTALL)
    code = get_first_non_snowpark_stack_frame().code_context
    if code is not None:
        for line in code:
            match = re_symbol_name.fullmatch(line)
            if match is not None:
                return match.group(1)


def set_src_position(ast: proto.SrcPosition) -> None:
    """Uses the method to retrieve the first non snowpark stack frame, and sets the SrcPosition IR entity
    with the filename, and lineno which can be retrieved. In Python 3.11 and up the end line and column
    offsets can also be retrieved from the FrameInfo.positions field.

    Args:
        ast (proto.SrcPosition): A previously created SrcPosition IR entity to be set.
    """
    curr_frame = get_first_non_snowpark_stack_frame()

    ast.file = curr_frame.filename
    ast.start_line = curr_frame.lineno

    if sys.version_info >= (3, 11):
        code_context = curr_frame.positions
        setattr_if_not_none(ast, "start_line", code_context.lineno)
        setattr_if_not_none(ast, "end_line", code_context.end_lineno)
        setattr_if_not_none(ast, "start_column", code_context.col_offset)
        setattr_if_not_none(ast, "end_column", code_context.end_col_offset)


def with_src_position(expr_ast: proto.Expr) -> proto.Expr:
    """Sets the src_position on the supplied Expr AST node and returns it."""
    set_src_position(expr_ast.src)
    return expr_ast


def setattr_if_not_none(obj: Any, attr: str, val: Any) -> None:
    if val is not None:
        setattr(obj, attr, val)


def _fill_column_ast(ast: proto.Expr, value: ColumnOrLiteral) -> None:
    """Copy from a Column object's AST, or copy a literal value into an AST expression.

    Args:
        ast (proto.SpColumnExpr): A previously created Expr() or SpColumnExpr() IR entity intance to be filled
        value (ColumnOrLiteral): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: An SpColumnExpr can only be populated from another SpColumnExpr or a valid Literal type
    """
    if isinstance(value, snowflake.snowpark.Column):
        return ast.CopyFrom(value._ast)
    elif isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
        build_const_from_python_val(value, ast)
    elif isinstance(value, Expression):
        pass  # TODO: clean this up
    else:
        raise TypeError(f"{type(value)} is not a valid type for Column or literal AST.")


def create_ast_for_column_method(
    property: Optional[str] = None,
    assign_fields: Dict[str, Any] = {},  # noqa: B006
    assign_opt_fields: Dict[str, Any] = {},  # noqa: B006
    copy_messages: Dict[str, Any] = {},  # noqa: B006
    fill_expr_asts: Dict[str, ColumnOrLiteral] = {},  # noqa: B006
) -> proto.SpColumnExpr:
    """General purpose function to generate the AST representation for a new Snowpark Column instance

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
        for attr, value in assign_fields.items():
            setattr_if_not_none(prop_ast, attr, value)
        for attr, value in assign_opt_fields.items():
            setattr_if_not_none(getattr(prop_ast, attr), "value", value)
        for attr, msg in copy_messages.items():
            if msg is None and FAIL_ON_MISSING_AST:
                call_stack = inspect.stack()
                curr_frame = call_stack.pop(0)
                while call_stack and __file__ == curr_frame.filename:
                    column_api = curr_frame.function
                    curr_frame = call_stack.pop(0)
                if not Path(__file__).parents[0] in Path(curr_frame.filename).parents:
                    raise NotImplementedError(
                        f'Calling Column API "{column_api}" which supports AST logging, from File "{curr_frame.filename}", line {curr_frame.lineno}\n'
                        f"\t{curr_frame.code_context[0].strip()}\n"
                        f"A Snowpark API which returns a Column instance used above has not yet implemented AST logging."
                    )
            getattr(prop_ast, attr).CopyFrom(msg)
        for attr, other in fill_expr_asts.items():
            _fill_column_ast(getattr(prop_ast, attr), other)
    return ast


def fill_ast_for_column(
    expr: proto.Expr, name1: str, name2: Optional[str], fn_name="col"
):
    # When name2 is None, corresponds to col(col_name: str).
    # Else, corresponds to col(df_alias: str, col_name: str)

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


def create_ast_for_column(name1: str, name2: Optional[str], fn_name="col"):
    # When name2 is None, corresponds to col(col_name: str).
    # Else, corresponds to col(df_alias: str, col_name: str)

    # Handle the special case * (as a SQL column expr).
    if name2 == "*":
        expr = proto.Expr()
        ast = with_src_position(expr.sp_column_sql_expr)
        ast.sql = "*"
        if name1 is not None:
            ast.df_alias.value = name1
        return expr

    if name1 == "*" and name2 is None:
        expr = proto.Expr()
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

    expr = proto.Expr()
    build_fn_apply(expr, fn_name, *args, **kwargs)
    return expr


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

    if isinstance(expr, Attribute):
        return create_ast_for_column(expr.name, None)
    elif isinstance(expr, Literal):
        ast = proto.Expr()
        build_const_from_python_val(expr.value, ast)
        return ast
    elif isinstance(expr, UnresolvedAttribute):
        # Unresolved means treatment as sql expression.
        return create_ast_for_column_method(
            property="sp_column_sql_expr",
            assign_fields={"sql": expr.sql},
        )
    elif isinstance(expr, MultipleExpression):
        # Convert to list of expressions.
        ast = proto.Expr()
        for child_expr in expr.expressions:
            ast_list = ast.list_val.vs.add()
            ast_list.CopyFrom(snowpark_expression_to_ast(child_expr))
        return ast
    else:
        raise NotImplementedError(
            f"Snowpark expr {expr} of type {type(expr)} is an expression with missing AST or for which an AST can not be auto-generated."
        )
