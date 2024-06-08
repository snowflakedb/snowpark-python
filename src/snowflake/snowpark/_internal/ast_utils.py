import datetime
import decimal
import inspect
import re
import sys

from functools import reduce
from pathlib import Path
from typing import Any, List, Optional

import snowflake.snowpark._internal.proto.ast_pb2 as proto

NoneType = type(None)
PYTHON_TO_AST_CONST_MAPPINGS = {
    NoneType: "null_val",
    bool: "bool_val",
    int: "int64_val",
    float: "float64_val",
    str: "string_val",
    bytearray: "binary_val",
    bytes: "binary_val",
    decimal.Decimal: "big_decimal_val",
    datetime.date: "date_val",
    datetime.time: "python_time_val",
    datetime.datetime: "python_timestamp_val",
}
def infer_const_ast(obj: Any, ast: proto.Expr) -> None:
    """Infer the Const AST expression from obj, and populate the provided ast.Expr() instance"""
    if obj is None:
        fill_src_position(ast.null_val.src)
        return
        
    const_variant = PYTHON_TO_AST_CONST_MAPPINGS.get(type(obj))
    if isinstance(obj, decimal.Decimal):
        dec_tuple = obj.as_tuple()
        unscaled_val = reduce(lambda val, digit: val*10 + digit, dec_tuple.digits)
        if dec_tuple.sign != 0:
            unscaled_val *= -1
        req_bytes = (unscaled_val.bit_length() + 7) // 8
        getattr(ast, const_variant).unscaled_value = unscaled_val.to_bytes(req_bytes, 'big', signed=True)
        getattr(ast, const_variant).scale = dec_tuple.exponent
    
    elif isinstance(obj, datetime.datetime):
        if obj.tzinfo is not None:
            getattr(ast, const_variant).tz.value = obj.tzname()
        getattr(ast, const_variant).v = obj.astimezone(datetime.timezone.utc).timestamp()

    elif isinstance(obj, datetime.date):
        datetime_val = datetime.datetime(obj.year, obj.month, obj.day)
        getattr(ast, const_variant).v = int(datetime_val.timestamp())

    elif isinstance(obj, datetime.time):
        if obj.tzinfo is not None:
            getattr(ast, const_variant).tz.value = obj.tzname()
        datetime_val = datetime.datetime.combine(datetime.date.today(), obj)
        datetime_val = datetime_val.astimezone(datetime.timezone.utc).replace(1970, 1, 1)
        getattr(ast, const_variant).v = datetime_val.timestamp()

    elif isinstance(obj, bytearray):
        getattr(ast, const_variant).v = bytes(obj)

    elif const_variant is not None:
        getattr(ast, const_variant).v = obj
    
    elif isinstance(obj, dict):
        for key, value in obj.items():
            kv_tuple_ast = ast.seq_map_val.kvs.add()
            infer_const_ast(key, kv_tuple_ast.vs.add())
            infer_const_ast(value, kv_tuple_ast.vs.add())
    
    elif isinstance(obj, list):
        for v in obj:
            infer_const_ast(v, ast.list_val.vs.add())
    
    elif isinstance(obj, tuple):
        for v in obj:
            infer_const_ast(v, ast.tuple_val.vs.add())
    
    else:
        raise TypeError("not supported type: %s" % type(obj))


def get_non_snowpark_stack_frame() -> inspect.FrameInfo:
    idx = 0
    call_stack = inspect.stack()
    curr_frame = call_stack[idx]
    snowpark_path = Path(__file__).parents[1]
    while snowpark_path in Path(curr_frame.filename).parents:
        idx += 1
        curr_frame = call_stack[idx]
    return curr_frame


# TODO: Instead of regexp, grab assign statments using Python ast library
RE_SYMBOL_NAME = re.compile(r'^\s*([a-zA-Z_]\w*)\s*=.*$', re.DOTALL)
def get_symbol() -> Optional[str]:
    code = get_non_snowpark_stack_frame().code_context
    if code is not None:
        for line in code:
            match = RE_SYMBOL_NAME.fullmatch(line)
            if match is not None:
                return match.group(1)


def fill_src_position(ast: proto.SrcPosition) -> None:
    curr_frame = get_non_snowpark_stack_frame()

    ast.file = curr_frame.filename
    ast.start_line = curr_frame.lineno
    
    if sys.version_info[1] >= 11:
        code_context = curr_frame.positions
        setattr_if_not_none(ast, "start_line", code_context.lineno)
        setattr_if_not_none(ast, "end_line", code_context.end_lineno)
        setattr_if_not_none(ast, "start_column", code_context.col_offset)
        setattr_if_not_none(ast, "end_column", code_context.end_col_offset)


def setattr_if_not_none(obj: Any, attr: str, val: Any) -> None:
    if val is not None:
        setattr(obj, attr, val)
