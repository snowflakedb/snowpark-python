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
from typing import Any, Optional

import snowflake.snowpark._internal.proto.ast_pb2 as proto

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


def setattr_if_not_none(obj: Any, attr: str, val: Any) -> None:
    if val is not None:
        setattr(obj, attr, val)
