#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import ast
import datetime
import decimal
import inspect
import logging
import os
import platform
import sys
import typing
from functools import reduce
from logging import getLogger
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import dateutil
from dateutil.tz import tzlocal

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Expression,
    FunctionExpression,
    Literal,
    MultipleExpression,
    Star,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SaveMode
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    ColumnOrLiteral,
    ColumnOrName,
    ColumnOrSqlExpr,
)
from snowflake.snowpark._internal.utils import str_to_enum
from snowflake.snowpark.types import DataType, StructType

# TODO(SNOW-1791994): Enable pyright type checks for this file.


# This flag causes an explicit error to be raised if any Snowpark object instance is missing an AST or field, when this
# AST or field is required to populate the AST field of a different Snowpark object instance.
FAIL_ON_MISSING_AST = True

# The path to the snowpark package.
SNOWPARK_LIB_PATH = Path(__file__).parent.parent.resolve()

# Test mode. In test mode, the source filename is ignored.
SRC_POSITION_TEST_MODE = False

_logger = getLogger(__name__)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def debug_check_missing_ast(ast, container) -> None:  # type: ignore[no-untyped-def] # pragma: no cover
    """
    Debug check for missing AST. This is invoked with various arguments that are expected to be non-NULL if the AST
    is emitted correctly.
    """
    if ast is None and FAIL_ON_MISSING_AST:
        _logger.debug(container._explain_string())
        raise NotImplementedError(
            f"DataFrame with API usage {container._plan.api_calls} is missing complete AST logging."
        )


# Use python's builtin ast and NodeVisitor class.
class ExtractAssignmentVisitor(ast.NodeVisitor):
    # TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
    def __init__(self) -> None:  # pragma: no cover
        super().__init__()
        self.symbols: Optional[Union[str, List[str]]] = None

    # TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
    def visit_Assign(self, node: ast.Assign) -> None:  # pragma: no cover
        assert len(node.targets) == 1
        target = node.targets[0]

        if isinstance(target, ast.Name):
            self.symbols = target.id
        elif isinstance(target, ast.Tuple):
            self.symbols = [name.id for name in target.elts]  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "expr" has no attribute "id"
        else:
            raise ValueError(f"Unsupported target {ast.dump(target)}")


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def extract_assign_targets(
    source_line: str,
) -> Optional[Union[str, List[str]]]:  # pragma: no cover
    """
    Extracts the targets as strings for a python assignment.
    Args:
        source_line: A string, e.g. "a, b, c = df.random_split([0.2, 0.3, 0.5])"

    Returns:
        None if extraction fails, or list of strings for the symbol names, or a single string if it is a single target.
    """
    # It may happen that an incomplete source line is submitted that can't be
    # successfully parsed into a python ast tree.
    # Ultimately, for an assign statement of the form <left> = <right>
    # in this function we only care about extracting <left>.
    # For this reason, when '=' is found, replace <right> with w.l.o.g. None.
    if "=" in source_line:
        source_line = source_line[: source_line.find("=")] + " = None"

    try:
        tree = ast.parse(source_line.strip())
        v = ExtractAssignmentVisitor()
        v.visit(tree)
        return v.symbols
    except Exception:
        # Indicate parse/extraction failure with None.
        return None


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def fill_timezone(
    ast: proto.Expr, obj: Union[datetime.datetime, datetime.time]
) -> None:  # pragma: no cover

    datetime_val = (
        obj
        if isinstance(obj, datetime.datetime)
        else datetime.datetime.combine(datetime.date.today(), obj)
    )

    if obj.tzinfo is not None:
        utc_offset = obj.tzinfo.utcoffset(datetime_val)
        if utc_offset is not None:
            ast.tz.offset_seconds = int(utc_offset.total_seconds())  # type: ignore[attr-defined]
        tz = obj.tzinfo.tzname(datetime_val)
        if tz is not None:
            ast.tz.name.value = tz  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "tz"
    else:
        # tzinfo=None means that the local timezone will be used.
        # Retrieve name of the local timezone and encode as part of the AST.
        if platform.system() == "Windows":
            # Windows is a special case, msvcrt is broken so timezones are not properly propagated. Relying on
            # environment variable for test. Cf. override_time_zone in test_ast_driver.py for details.
            tz_env = os.environ.get("TZ")
            if tz_env:
                tz = dateutil.tz.gettz(tz_env)  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Optional[tzinfo]", variable has type "Optional[str]")
                tz_name = tz.tzname(datetime.datetime.now())  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "str" of "Optional[str]" has no attribute "tzname", Item "None" of "Optional[str]" has no attribute "tzname"
                ast.tz.offset_seconds = int(tz.utcoffset(datetime_val).total_seconds())  # type: ignore[attr-defined, union-attr] # TODO(SNOW-1491199) # "Expr" has no attribute "tz", Item "str" of "Optional[str]" has no attribute "utcoffset", Item "None" of "Optional[str]" has no attribute "utcoffset"
            else:
                logging.warn(
                    "Assuming UTC timezone for Windows, but actual timezone may be different."
                )
                ast.tz.offset_seconds = int(tzlocal().utcoffset(obj).total_seconds())  # type: ignore[arg-type, attr-defined, union-attr] # TODO(SNOW-1491199) # "Expr" has no attribute "tz", Item "None" of "Optional[timedelta]" has no attribute "total_seconds", Argument 1 to "utcoffset" of "tzlocal" has incompatible type "Union[datetime, time]"; expected "Optional[datetime]"
                tz_name = datetime.datetime.now(tzlocal()).tzname()
        else:
            ast.tz.offset_seconds = int(  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "tz"
                tzlocal().utcoffset(datetime_val).total_seconds()  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "None" of "Optional[timedelta]" has no attribute "total_seconds"
            )
            tz_name = datetime.datetime.now(tzlocal()).tzname()
        ast.tz.name.value = tz_name  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "tz"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_python_val(
    expr_builder: proto.Expr, obj: Any
) -> None:  # pragma: no cover
    """Infer the Const AST expression from obj, and populate the provided ast.Expr() instance

    Args:
        obj (Any): Expected to be any acceptable Python literal or constant value
        ast (proto.Expr): A previously created Expr() IR entity to be filled.

    Raises:
        TypeError: Raised if the Python constant/literal is not supported by the Snowpark client.
    """
    from snowflake.snowpark.column import Column
    from snowflake.snowpark.row import Row

    if obj is None:
        with_src_position(expr_builder.null_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "NullVal"; expected "Expr"

    # Keep objects most high up in the class hierarchy first, i.e. a Row is a tuple.
    elif isinstance(obj, Column):

        # Special case: Column holds Literal, for Literals no ast is per default generated.
        if isinstance(obj._expression, Literal):
            expr_builder.CopyFrom(snowpark_expression_to_ast(obj._expression))
        else:
            expr_builder.CopyFrom(obj._ast)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Column" has no attribute "_ast"; maybe "_cast"?

    elif isinstance(obj, Row):
        ast = with_src_position(expr_builder.sp_row)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpRow"; expected "Expr"
        if hasattr(obj, "_named_values") and obj._named_values is not None:
            for field in obj._fields:
                ast.names.list.append(field)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "names"
                build_expr_from_python_val(ast.vs.add(), obj._named_values[field])  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "vs"
        else:
            for field in obj:
                build_expr_from_python_val(ast.vs.add(), field)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "vs"

    elif isinstance(obj, bool):
        ast = with_src_position(expr_builder.bool_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "BoolVal"; expected "Expr"
        ast.v = obj  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, int):
        ast = with_src_position(expr_builder.int64_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "Int64Val"; expected "Expr"
        ast.v = obj  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, float):
        ast = with_src_position(expr_builder.float64_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "Float64Val"; expected "Expr"
        ast.v = obj  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, str):
        ast = with_src_position(expr_builder.string_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "StringVal"; expected "Expr"
        ast.v = obj  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, bytes):
        ast = with_src_position(expr_builder.binary_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "BinaryVal"; expected "Expr"
        ast.v = obj  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, bytearray):
        ast = with_src_position(expr_builder.binary_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "BinaryVal"; expected "Expr"
        ast.v = bytes(obj)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"

    elif isinstance(obj, decimal.Decimal):
        ast = with_src_position(expr_builder.big_decimal_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "BigDecimalVal"; expected "Expr"
        dec_tuple = obj.as_tuple()
        unscaled_val = reduce(lambda val, digit: val * 10 + digit, dec_tuple.digits)
        if dec_tuple.sign != 0:
            unscaled_val *= -1

        # In two-complement -1 with one byte is 0xFF. We encode arbitrary length integers
        # in full bytes. Therefore, round up to fullest byte. To restore the sign, add another byte.
        req_bytes = unscaled_val.bit_length() // 8 + 1

        ast.unscaled_value = unscaled_val.to_bytes(req_bytes, "big", signed=True)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "unscaled_value"
        ast.scale = dec_tuple.exponent  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "scale"

    elif isinstance(obj, datetime.datetime):
        ast = with_src_position(expr_builder.python_timestamp_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "PythonTimestampVal"; expected "Expr"

        fill_timezone(ast, obj)

        ast.year = obj.year  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "year"
        ast.month = obj.month  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "month"
        ast.day = obj.day  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "day"
        ast.hour = obj.hour  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "hour"
        ast.minute = obj.minute  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "minute"
        ast.second = obj.second  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "second"
        ast.microsecond = obj.microsecond  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "microsecond"

    elif isinstance(obj, datetime.date):
        ast = with_src_position(expr_builder.python_date_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "PythonDateVal"; expected "Expr"
        ast.year = obj.year  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "year"
        ast.month = obj.month  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "month"
        ast.day = obj.day  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "day"

    elif isinstance(obj, datetime.time):
        ast = with_src_position(expr_builder.python_time_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "PythonTimeVal"; expected "Expr"

        fill_timezone(ast, obj)

        ast.hour = obj.hour  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "hour"
        ast.minute = obj.minute  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "minute"
        ast.second = obj.second  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "second"
        ast.microsecond = obj.microsecond  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "microsecond"

    elif isinstance(obj, dict):
        ast = with_src_position(expr_builder.seq_map_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SeqMapVal"; expected "Expr"
        for key, value in obj.items():
            kv_ast = ast.kvs.add()  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "kvs"
            build_expr_from_python_val(kv_ast.vs.add(), key)
            build_expr_from_python_val(kv_ast.vs.add(), value)

    elif isinstance(obj, list):
        ast = with_src_position(expr_builder.list_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ListVal"; expected "Expr"
        for v in obj:
            build_expr_from_python_val(ast.vs.add(), v)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "vs"

    elif isinstance(obj, tuple):
        ast = with_src_position(expr_builder.tuple_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "TupleVal"; expected "Expr"
        for v in obj:
            build_expr_from_python_val(ast.vs.add(), v)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "vs"
    elif isinstance(obj, snowflake.snowpark.dataframe.DataFrame):
        ast = with_src_position(expr_builder.sp_dataframe_ref)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpDataframeRef"; expected "Expr"
        assert (
            obj._ast_id is not None
        ), "Dataframe object to encode as part of AST does not have an id assigned. Missing AST for object or previous operation?"
        ast.id.bitfield1 = obj._ast_id  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "id"
    elif isinstance(obj, snowflake.snowpark.table_function.TableFunctionCall):
        raise NotImplementedError(
            "TODO SNOW-1629946: Implement TableFunctionCall with args."
        )
    elif isinstance(obj, snowflake.snowpark._internal.type_utils.DataType):
        ast = with_src_position(expr_builder.sp_datatype_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpDatatypeVal"; expected "Expr"
        obj._fill_ast(ast.datatype)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast", "Expr" has no attribute "datatype"
    elif isinstance(obj, snowflake.snowpark._internal.analyzer.expression.Literal):
        build_expr_from_python_val(expr_builder, obj.value)
    else:
        raise NotImplementedError("not supported type: %s" % type(obj))


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_proto_from_struct_type(
    schema: "snowflake.snowpark.types.StructType", expr: proto.SpStructType
) -> None:  # pragma: no cover
    from snowflake.snowpark.types import StructType

    assert isinstance(schema, StructType)

    expr.structured = schema.structured
    for field in schema.fields:
        ast_field = expr.fields.add()
        field.column_identifier._fill_ast(ast_field.column_identifier)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "ColumnIdentifier" has no attribute "_fill_ast"
        field.datatype._fill_ast(ast_field.data_type)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
        ast_field.nullable = field.nullable


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def _set_fn_name(
    name: Union[str, Iterable[str]], fn: proto.FnNameRefExpr
) -> None:  # pragma: no cover
    """
    Set the function name in the AST. The function name can be a string or an iterable of strings.
    Args:
        name: The function name to set in the AST.
        fn: The function reference expression to set the name in. The caller must provide the correct type of function.

    Raises:
        ValueError: Raised if the function name is not a string or an iterable of strings.
    """
    if isinstance(name, str):
        fn.name.fn_name_flat.name = name  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "FnNameRefExpr" has no attribute "name"
    elif isinstance(name, Iterable):
        fn.name.fn_name_structured.name.extend(name)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "FnNameRefExpr" has no attribute "name"
    else:
        raise ValueError(
            f"Invalid function name: {name}. The function name must be a string or an iterable of strings."
        )


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_sp_table_name(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation
    expr_builder: proto.SpTableName, name: Union[str, Iterable[str]]
):  # pragma: no cover
    if isinstance(name, str):
        expr_builder.sp_table_name_flat.name = name
    elif isinstance(name, Iterable):
        expr_builder.sp_table_name_structured.name.extend(name)
    else:
        raise ValueError(f"Invalid name type {type(name)} for SpTableName entity.")


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_builtin_fn_apply(
    ast: proto.Expr,
    builtin_name: str,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:  # pragma: no cover
    """
    Creates AST encoding for ApplyExpr(BuiltinFn(<builtin_name>), List(<args...>), Map(<kwargs...>)) for builtin
    functions.
    Args:
        ast: Expr node to fill.
        builtin_name: Name of the builtin function to call.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    """
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    _set_fn_name(builtin_name, expr.fn.builtin_fn)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args, **kwargs)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udf_apply(
    ast: proto.Expr,
    udf_id: int,
    *args: Tuple[Union[proto.Expr, Any]],
) -> None:  # pragma: no cover
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    expr.fn.sp_fn_ref.id.bitfield1 = udf_id  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udaf_apply(
    ast: proto.Expr,
    udaf_id: int,
    *args: Tuple[Union[proto.Expr, Any]],
) -> None:  # pragma: no cover
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    expr.fn.sp_fn_ref.id.bitfield1 = udaf_id  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udtf_apply(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a type annotation for one or more arguments
    ast: proto.Expr, udtf_id: int, *args: Tuple[Union[proto.Expr, Any]], **kwargs
) -> None:  # pragma: no cover
    """Encodes a call to UDTF into ast as a Snowpark IR expression."""
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    expr.fn.sp_fn_ref.id.bitfield1 = udtf_id  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args, **kwargs)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_sproc_apply(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a type annotation for one or more arguments
    ast: proto.Expr,
    sproc_id: int,
    statement_params: Optional[Dict[str, str]] = None,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs,
) -> None:  # pragma: no cover
    """Encodes a call to stored procedure into ast as a Snowpark IR expression."""
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    expr.fn.sp_fn_ref.id.bitfield1 = sproc_id  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args, **kwargs)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_call_table_function_apply(
    ast: proto.Expr,
    name: str,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:  # pragma: no cover
    """
    Creates AST encoding for
        CallTableFunctionExpr(IndirectTableFnNameRef(<table_function_name>), List(<args...>), Map(<kwargs...>))
      for indirect table functions called by name.

    Args:
        ast: Expr node to fill.
        name: Name of the table function to call.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    """
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    _set_fn_name(name, expr.fn.call_table_function_expr)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
    build_fn_apply_args(ast, *args, **kwargs)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_indirect_table_fn_apply(
    ast: proto.Expr,
    func: Union[
        str, List[str], "snowflake.snowpark.table_function.TableFunctionCall", Callable
    ],
    *func_arguments: ColumnOrName,
    **func_named_arguments: ColumnOrName,
) -> None:  # pragma: no cover
    """
    Creates AST encoding for ApplyExpr(<indirect_fn_ref>(<fn_name>), List(<args...>), Map(<kwargs...>)) for indirect
    table function calls.

    Args:
        ast: Expr node to fill.
        func: The table function to call. Can be a string, a list of strings, or a Python object that designates the
         function to call (e.g. TableFunctionCall or a Callable). The Python object must have an Assign statement
          attached to its _ast_stmt field.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    """
    expr = with_src_position(ast.apply_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "ApplyExpr"; expected "Expr"
    if isinstance(
        func, (snowflake.snowpark.table_function.TableFunctionCall, Callable)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "isinstance" has incompatible type "tuple[type[TableFunctionCall], <typing special form>]"; expected "_ClassInfo"
    ):
        stmt = func._ast_stmt  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "str" of "Union[str, list[str], TableFunctionCall, Callable[..., Any]]" has no attribute "_ast_stmt", Item "list[str]" of "Union[str, list[str], TableFunctionCall, Callable[..., Any]]" has no attribute "_ast_stmt", Item "TableFunctionCall" of "Union[str, list[str], TableFunctionCall, Callable[..., Any]]" has no attribute "_ast_stmt", Item "function" of "Union[str, list[str], TableFunctionCall, Callable[..., Any]]" has no attribute "_ast_stmt"
        fn_expr = expr.fn.indirect_table_fn_id_ref  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
        fn_expr.id.bitfield1 = stmt.var_id.bitfield1
    else:
        fn_expr = expr.fn.indirect_table_fn_name_ref  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "fn"
        _set_fn_name(func, fn_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "_set_fn_name" has incompatible type "Union[str, list[str], TableFunctionCall, Callable[..., Any]]"; expected "Union[str, Iterable[str]]"
    build_fn_apply_args(ast, *func_arguments, **func_named_arguments)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "build_fn_apply_args" has incompatible type "*tuple[Union[Column, str], ...]"; expected "tuple[Union[Expr, Any]]", Argument 3 to "build_fn_apply_args" has incompatible type "**dict[str, Union[Column, str]]"; expected "dict[str, Union[Expr, Any]]"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_fn_apply_args(
    ast: proto.Expr,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:  # pragma: no cover
    """
    Creates AST encoding for the argument lists of ApplyExpr.
    Args:
        ast: Expr node to fill
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.
    """
    expr = ast.apply_expr

    for arg in args:
        if isinstance(arg, proto.Expr):
            expr.pos_args.append(arg)
        elif hasattr(arg, "_ast"):

            # Special case: _ast is None but arg is Column(LITERAL).
            if (
                arg._ast is None  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "tuple[Union[Expr, Any]]" has no attribute "_ast"
                and isinstance(arg, snowflake.snowpark.Column)
                and isinstance(
                    arg._expression,
                    snowflake.snowpark._internal.analyzer.expression.Literal,
                )
            ):
                build_expr_from_python_val(expr.pos_args.add(), arg._expression.value)
            elif arg._ast is None and isinstance(arg, snowflake.snowpark.Column):  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "tuple[Union[Expr, Any]]" has no attribute "_ast"
                expr.pos_args.append(snowpark_expression_to_ast(arg._expression))
            else:
                assert (
                    arg._ast  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "tuple[Union[Expr, Any]]" has no attribute "_ast"
                ), f"Object {arg} has member _ast=None set. Expected valid AST."
                expr.pos_args.append(arg._ast)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "tuple[Union[Expr, Any]]" has no attribute "_ast"
        else:
            pos_arg = proto.Expr()
            build_expr_from_python_val(pos_arg, arg)
            expr.pos_args.append(pos_arg)

    for name, arg in kwargs.items():  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "dict[str, Union[Expr, Any]]", variable has type "tuple[Union[Expr, Any]]")
        kwarg = proto.Tuple_String_Expr()
        kwarg._1 = name
        if isinstance(arg, proto.Expr):
            kwarg._2.CopyFrom(arg)
        elif isinstance(arg, snowflake.snowpark.Column):
            assert arg._ast, f"Column object {name}={arg} has no _ast member set."
            kwarg._2.CopyFrom(arg._ast)
        else:
            build_expr_from_python_val(kwarg._2, arg)
        expr.named_args.append(kwarg)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def set_builtin_fn_alias(ast: proto.Expr, alias: str) -> None:  # pragma: no cover
    """
    Set the alias for a builtin function call. Requires that the expression has an ApplyExpr with a BuiltinFn.
    Args:
        ast: Expr node to fill.
        alias: Alias to set for the builtin function.
    """
    _set_fn_name(alias, ast.apply_expr.fn.builtin_fn)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "_set_fn_name" has incompatible type "BuiltinFn"; expected "FnNameRefExpr"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def with_src_position(
    expr_ast: proto.Expr,
    assign: Optional[proto.Assign] = None,
    caller_frame_depth: Optional[int] = None,
    debug: bool = False,
    target_idx: Optional[int] = None,
) -> proto.Expr:  # pragma: no cover
    """
    Sets the src_position on the supplied Expr AST node and returns it.
    N.B. This function assumes it's always invoked from a public API, meaning that the caller's caller
    is always the code of interest.
    Args:
        expr_ast: The AST node to set the src_position on.
        assign: The Assign AST node to set the symbol value on.
        caller_frame_depth: The number of frames to step back from the current frame to find the code of interest.
                            If this is not provided, the filename for each frame is probed to find the code of interest.
        target_idx: If an integer, tries to extract from an assign statement the {target_idx}th symbol. If None, assumes a single target.
    """
    src = expr_ast.src  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "src"
    frame = inspect.currentframe()

    # Best practices for the inspect library are to remove references to frame objects once done with them
    # to avoid reference cycles and memory leaks. The above frame assignment is removed in the finally block.
    try:
        # Need this None guard as depending on the implementation of sys._getframe, frame may be None.
        # Note the assignment to src.file is needed as several upstream uses of this method rely on
        # setting src fields for explicit presence of the encapsulating message in the AST.
        # e.g., Null values have no fields, so the assignment to src fields ensures their presence.
        if frame is None:
            src.file = ""
            return expr_ast

        # NOTE: The inspect module provides many other APIs to get information about the current frame and its callers.
        # All of these (except for currentframe) unnecessarily incur the overhead of resolving the filename for each
        # frame and making sure the file exists (even if the context parameter is set to 0 to avoid capturing lineno,
        # source line, etc.). Since we should know exactly how many frames to step back, we can and should avoid this
        # overhead. Using the inspect.currentframe() (a wrapper around sys._getframe that handles the None case) is the
        # most efficient way to get the current frame. Stepping back from this frame via frame.f_back is also the most
        # efficient method to walk the stack as each frame object contains the minimal amount of needed context.
        if caller_frame_depth is None:
            # If the frame is not None, one guarantee we have is that two frames back is the caller's caller, and this
            # frame contains the code of interest from the user if they are using a simple public API with no further
            # nesting or indirection. This is the most common case.
            frame, prev_frame = frame.f_back.f_back, frame.f_back  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "None" of "Optional[FrameType]" has no attribute "f_back", Item "None" of "Union[FrameType, Any, None]" has no attribute "f_back"
            while (
                frame is not None
                and SNOWPARK_LIB_PATH in Path(frame.f_code.co_filename).parents
            ):
                frame, prev_frame = frame.f_back, frame
        else:
            # If the frame is not None, use the provided stack depth to step back to the relevant frame.
            # This frame should be the first frame outside of the snowpark package, and contain the code of interest.
            curr_frame_depth = 0
            while frame is not None and curr_frame_depth < caller_frame_depth:
                frame, prev_frame = frame.f_back, frame
                curr_frame_depth += 1

        if debug:
            last_snowpark_file = prev_frame.f_code.co_filename  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "None" of "Union[FrameType, Any, None]" has no attribute "f_code"
            assert SNOWPARK_LIB_PATH in Path(last_snowpark_file).parents
            first_non_snowpark_file = frame.f_code.co_filename  # type: ignore[union-attr] # TODO(SNOW-1491199) # Item "None" of "Optional[FrameType]" has no attribute "f_code"
            assert SNOWPARK_LIB_PATH not in Path(first_non_snowpark_file).parents

        # Once we've stepped out of the snowpark package, we should be in the code of interest.
        # However, the code of interest may execute in an environment that is not accessible via the filesystem.
        # e.g. Jupyter notebooks, REPLs, calls to exec, etc.
        filename = frame.f_code.co_filename if frame is not None else ""
        if frame is None or not Path(filename).is_file():
            src.file = ""
            return expr_ast

        # The context argument specifies the number of lines of context to capture around the current line.
        # If IO performance is an issue, this can be set to 0 but this will disable symbol capture. Some
        # potential alternatives to consider here are the linecache and traceback modules.
        frame_info = inspect.getframeinfo(frame, context=1)
        src.file = (
            frame_info.filename
            if not SRC_POSITION_TEST_MODE
            else "SRC_POSITION_TEST_MODE"
        )
        src.start_line = frame_info.lineno
        if sys.version_info >= (3, 11):
            pos = frame_info.positions
            if pos.lineno is not None:
                src.start_line = pos.lineno
            if pos.end_lineno is not None:
                src.end_line = pos.end_lineno
            if pos.col_offset is not None:
                src.start_column = pos.col_offset
            if pos.end_col_offset is not None:
                src.end_column = pos.end_col_offset

        if assign is not None:
            if code := frame_info.code_context:
                source_line = code[frame_info.index]  # type: ignore[index] # TODO(SNOW-1491199) # Invalid index type "Optional[int]" for "list[str]"; expected type "SupportsIndex"
                symbols = extract_assign_targets(source_line)
                if symbols is not None:
                    if target_idx is not None:
                        if isinstance(symbols, list):
                            assign.symbol.value = symbols[target_idx]
                    elif isinstance(symbols, str):
                        assign.symbol.value = symbols
    finally:
        del frame

    return expr_ast


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_snowpark_column(
    expr_builder: proto.Expr, value: "snowflake.snowpark.Column"
) -> None:  # pragma: no cover
    """Copy from a Column object's AST into an AST expression.

    Args:
        ast (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (snowflake.snowpark.Column): The value from which to populate the provided ast parameter.

    Raises:
        NotImplementedError: Raised if the Column object does not have an AST set and FAIL_ON_MISSING_AST is True.
    """
    if value._ast is None and FAIL_ON_MISSING_AST:  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Column" has no attribute "_ast"; maybe "_cast"?
        raise NotImplementedError(
            f"Column({value._expression})._ast is None due to the use of a Snowpark API which does not support AST logging yet."
        )
    elif value._ast is not None:  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Column" has no attribute "_ast"; maybe "_cast"?
        expr_builder.CopyFrom(value._ast)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Column" has no attribute "_ast"; maybe "_cast"?


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_snowpark_column_or_col_name(
    expr_builder: proto.Expr, value: ColumnOrName
) -> None:  # pragma: no cover
    """Copy from a Column object's AST, or copy a column name into an AST expression.

    Args:
        expr_builder (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (ColumnOrName): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: The Expr provided should only be populated from a Snowpark Column with a valid _ast field or a column name
    """
    if isinstance(value, snowflake.snowpark.Column):
        build_expr_from_snowpark_column(expr_builder, value)
    elif isinstance(value, str):
        expr = with_src_position(expr_builder.string_val)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "StringVal"; expected "Expr"
        expr.v = value  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "v"
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or column name AST."
        )


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_snowpark_column_or_sql_str(
    expr_builder: proto.Expr, value: ColumnOrSqlExpr
) -> None:  # pragma: no cover
    """Copy from a Column object's AST, or copy a SQL expression into an AST expression.

    Args:
        ast (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (ColumnOrSqlExpr): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: The Expr provided should only be populated from a Snowpark Column with a valid _ast field or a SQL string
    """
    if isinstance(value, snowflake.snowpark.Column):
        build_expr_from_snowpark_column(expr_builder, value)
    elif isinstance(value, str):
        expr = with_src_position(expr_builder.sp_column_sql_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpColumnSqlExpr"; expected "Expr"
        expr.sql = value  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "sql"
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or SQL expression AST."
        )


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_snowpark_column_or_python_val(
    expr_builder: proto.Expr, value: ColumnOrLiteral
) -> None:  # pragma: no cover
    """Copy from a Column object's AST, or copy a literal value into an AST expression.

    Args:
        ast (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (ColumnOrLiteral): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: The Expr provided should only be populated from a Snowpark Column with a valid _ast field or a literal value
    """
    if isinstance(value, snowflake.snowpark.Column):
        build_expr_from_snowpark_column(expr_builder, value)
    elif isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
        build_expr_from_python_val(expr_builder, value)
    elif isinstance(value, Expression):
        # Expressions must be handled by caller.
        pass
    else:
        raise TypeError(f"{type(value)} is not a valid type for Column or literal AST.")


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_snowpark_column_or_table_fn(
    expr_builder: proto.Expr,
    value: Union[
        "snowflake.snowpark.Column",
        "snowflake.snowpark.table_function.TableFunctionCall",
    ],
) -> None:  # pragma: no cover
    """Copy from a Column object's AST, or TableFunctionCall object's AST, into an AST expression.

    Args:
        expr_builder (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (Union[Column, TableFunctionCall]): The value from which to populate the provided ast parameter.

    Raises:
        TypeError: The Expr provided should only be populated from a Snowpark Column with a valid _ast field or a TableFunctionCall object
    """
    if isinstance(value, snowflake.snowpark.Column):
        build_expr_from_snowpark_column(expr_builder, value)
    elif isinstance(value, snowflake.snowpark.table_function.TableFunctionCall):
        assert value._ast is not None, "TableFunctionCall must have ast assigned."  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "TableFunctionCall" has no attribute "_ast"
        expr_builder.CopyFrom(value._ast)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "TableFunctionCall" has no attribute "_ast"

        # TODO SNOW-1509198: Test this branch more extensively for session.table_function.
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or TableFunctionCall AST generation."
        )


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def fill_ast_for_column(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a type annotation for one or more arguments
    expr: proto.Expr, name1: str, name2: Optional[str], fn_name="col"
) -> None:  # pragma: no cover
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
        ast = with_src_position(expr.sp_column_sql_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpColumnSqlExpr"; expected "Expr"
        ast.sql = "*"  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "sql"
        if name1 is not None:
            ast.df_alias.value = name1  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "df_alias"
        return expr  # type: ignore[return-value] # TODO(SNOW-1491199) # No return value expected

    if name1 == "*" and name2 is None:
        ast = with_src_position(expr.sp_column_sql_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpColumnSqlExpr"; expected "Expr"
        ast.sql = "*"  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "sql"
        return expr  # type: ignore[return-value] # TODO(SNOW-1491199) # No return value expected

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

    build_builtin_fn_apply(expr, fn_name, *args, **kwargs)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 3 to "build_builtin_fn_apply" has incompatible type "*tuple[str, ...]"; expected "tuple[Union[Expr, Any]]", Argument 4 to "build_builtin_fn_apply" has incompatible type "**dict[str, str]"; expected "dict[str, Union[Expr, Any]]"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def create_ast_for_column(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a type annotation for one or more arguments
    name1: str, name2: Optional[str], fn_name="col"
) -> proto.Expr:  # pragma: no cover
    """
    Helper function to create Ast for Snowpark Column. Cf. fill_ast_for_column on parameter details.
    """
    ast = proto.Expr()
    fill_ast_for_column(ast, name1, name2, fn_name)
    return ast


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def snowpark_expression_to_ast(expr: Expression) -> proto.Expr:  # pragma: no cover
    """
    Converts Snowpark expression expr to protobuf ast.
    Args:
        expr: A Snowpark expression (or instance of a derived class from Expression).

    Returns:
        protobuf expression.
    """
    if hasattr(expr, "_ast") and expr._ast is not None:
        return expr._ast

    if isinstance(expr, Alias):
        # TODO: Not sure if this can come up in a real use case. We see this use case for internal calls, where
        # we don't need an AST.
        return None  # type: ignore[return-value] # TODO(SNOW-1491199) # Incompatible return value type (got "None", expected "Expr")
    elif isinstance(expr, Attribute):
        return create_ast_for_column(expr.name, None)
    elif isinstance(expr, Literal):
        ast = proto.Expr()
        build_expr_from_python_val(ast, expr.value)
        return ast
    elif isinstance(expr, UnresolvedAttribute):
        # Unresolved means treatment as sql expression.
        ast = proto.Expr()
        sql_expr_ast = with_src_position(ast.sp_column_sql_expr)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "with_src_position" has incompatible type "SpColumnSqlExpr"; expected "Expr"
        sql_expr_ast.sql = expr.sql  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "sql"
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
        return None  # type: ignore[return-value] # TODO(SNOW-1491199) # Incompatible return value type (got "None", expected "Expr")
    elif isinstance(expr, Star):
        # Comes up in count(), handled there.
        return None  # type: ignore[return-value] # TODO(SNOW-1491199) # Incompatible return value type (got "None", expected "Expr")
    elif isinstance(expr, FunctionExpression):
        # Snowpark pandas API has some usage where injecting the publicapi decorator would lead to issues.
        # Directly translate here.
        ast = proto.Expr()
        build_builtin_fn_apply(
            ast, expr.name, *tuple(map(snowpark_expression_to_ast, expr.children))  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 3 to "build_builtin_fn_apply" has incompatible type "*tuple[Expr, ...]"; expected "tuple[Union[Expr, Any]]", Argument 2 to "map" has incompatible type "Optional[list[Expression]]"; expected "Iterable[Expression]"
        )
        return ast
    else:
        raise NotImplementedError(
            f"Snowpark expr {expr} of type {type(expr)} is an expression with missing AST or for which an AST can not be auto-generated."
        )


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def fill_sp_save_mode(
    expr: proto.SpSaveMode, save_mode: Union[str, SaveMode]
) -> None:  # pragma: no cover
    if isinstance(save_mode, str):
        save_mode = str_to_enum(save_mode.lower(), SaveMode, "`save_mode`")  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Enum", variable has type "Union[str, SaveMode]")

    if save_mode == SaveMode.APPEND:
        expr.sp_save_mode_append = True
    elif save_mode == SaveMode.ERROR_IF_EXISTS:
        expr.sp_save_mode_error_if_exists = True
    elif save_mode == SaveMode.IGNORE:
        expr.sp_save_mode_ignore = True
    elif save_mode == SaveMode.OVERWRITE:
        expr.sp_save_mode_overwrite = True
    elif save_mode == SaveMode.TRUNCATE:
        expr.sp_save_mode_truncate = True


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def fill_sp_write_file(
    expr: proto.Expr,
    location: str,
    *,
    partition_by: Optional[ColumnOrSqlExpr] = None,
    format_type_options: Optional[Dict[str, str]] = None,
    header: bool = False,
    statement_params: Optional[Dict[str, str]] = None,
    block: bool = True,
    **copy_options: dict,
) -> None:  # pragma: no cover
    expr.location = location  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "location"

    if partition_by is not None:
        build_expr_from_snowpark_column_or_sql_str(expr.partition_by, partition_by)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "partition_by"

    if format_type_options is not None:
        for k, v in format_type_options.items():
            t = expr.format_type_options.add()  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "format_type_options"
            t._1 = k
            t._2 = v

    expr.header = header  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "header"

    if statement_params is not None:
        for k, v in statement_params.items():
            t = expr.statement_params.add()  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "statement_params"
            t._1 = k
            t._2 = v

    expr.block = block  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "block"

    if copy_options:
        for k, v in copy_options.items():  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "dict[Any, Any]", variable has type "str")
            t = expr.copy_options.add()  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "Expr" has no attribute "copy_options"
            t._1 = k
            build_expr_from_python_val(t._2, v)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_proto_from_pivot_values(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation
    expr_builder: proto.SpPivotValue,
    values: Optional[Union[Iterable["LiteralType"], "DataFrame"]],  # type: ignore[name-defined] # noqa: F821 # TODO(SNOW-1491199) # Name "LiteralType" is not defined, Name "DataFrame" is not defined
):  # pragma: no cover
    """Helper function to encode Snowpark pivot values that are used in various pivot operations to AST."""
    if not values:
        return

    if isinstance(values, snowflake.snowpark.dataframe.DataFrame):
        expr_builder.sp_pivot_value__dataframe.v.id.bitfield1 = values._ast_id
    else:
        build_expr_from_python_val(expr_builder.sp_pivot_value__expr.v, values)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_proto_from_callable(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation
    expr_builder: proto.SpCallable,
    func: Union[Callable, Tuple[str, str]],
    ast_batch: Optional[AstBatch] = None,
):  # pragma: no cover
    """Registers a python callable (i.e., a function or lambda) to the AstBatch and encodes it as SpCallable protobuf."""

    udf_id = None
    if ast_batch is not None:
        udf_id = ast_batch.register_callable(func)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "register_callable" of "AstBatch" has incompatible type "Union[Callable[..., Any], tuple[str, str]]"; expected "Callable[..., Any]"
        expr_builder.id = udf_id

    if callable(func) and func.__name__ == "<lambda>":
        # Won't be able to extract name, unless there is <sym> = <lambda>
        # use string rep.
        expr_builder.name = "<lambda>"

        # If it is not the first tracked lambda, use a unique ref name.
        if udf_id is not None and udf_id != 0:
            expr_builder.name = f"<lambda [{udf_id}]>"
    elif isinstance(func, tuple) and len(func) == 2:
        # UDxF has been registered from a file (e.g., via session.udf.register_from_file)
        # The second argument is the name, the first the file path.
        expr_builder.name = func[1]
    else:
        # Use the actual function name. Note: We do not support different scopes yet, need to be careful with this then.
        expr_builder.name = func.__name__  # type: ignore[union-attr] # TODO(SNOW-1491199) # error: Item "Tuple[str, ...]" of "Union[Callable[..., Any], Tuple[str, str]]" has no attribute "__name__"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udf(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation, Function is missing a type annotation for one or more arguments
    ast: proto.Udf,
    func: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    name: Optional[str],
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = True,
    is_permanent: bool = False,
    session=None,
    **kwargs,
):  # pragma: no cover
    """Helper function to encode UDF parameters (used in both regular and mock UDFRegistration)."""
    # This is the name the UDF is registered to. Not the name to display when unparsing, that name is captured in callable.

    if name is not None:
        _set_fn_name(name, ast)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "_set_fn_name" has incompatible type "Udf"; expected "FnNameRefExpr"

    build_proto_from_callable(
        ast.func, func, session._ast_batch if session is not None else None
    )

    if return_type is not None:
        return_type._fill_ast(ast.return_type)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    if input_types is not None and len(input_types) != 0:
        for input_type in input_types:
            input_type._fill_ast(ast.input_types.list.add())  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    ast.is_permanent = is_permanent
    if stage_location is not None:
        ast.stage_location = stage_location
    if imports is not None and len(imports) != 0:
        for import_ in imports:
            import_expr = proto.SpTableName()
            build_sp_table_name(import_expr, import_)
            ast.imports.append(import_expr)
    if packages is not None and len(packages) != 0:
        for package in packages:
            if isinstance(package, ModuleType):
                raise NotImplementedError
            ast.packages.append(package)
    ast.replace = replace
    ast.if_not_exists = if_not_exists
    ast.parallel = parallel
    if max_batch_size is not None:
        ast.max_batch_size.value = max_batch_size

    if statement_params is not None and len(statement_params) != 0:
        for k, v in statement_params.items():
            t = ast.statement_params.add()
            t._1 = k
            t._2 = v

    ast.source_code_display = source_code_display
    ast.strict = strict
    ast.secure = secure
    if (
        external_access_integrations is not None
        and len(external_access_integrations) != 0
    ):
        ast.external_access_integrations.extend(external_access_integrations)
    if secrets is not None and len(secrets) != 0:
        for k, v in secrets.items():
            t = ast.secrets.add()
            t._1 = k
            t._2 = v
    ast.immutable = immutable
    if comment is not None:
        ast.comment.value = comment
    for k, v in kwargs.items():
        t = ast.kwargs.add()  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Tuple_String_Expr", variable has type "Tuple_String_String")
        t._1 = k
        build_expr_from_python_val(t._2, v)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "build_expr_from_python_val" has incompatible type "str"; expected "Expr"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udaf(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation, Function is missing a type annotation for one or more arguments
    ast: proto.Udaf,
    handler: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    name: Optional[str],
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    parallel: int = 4,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    statement_params: Optional[Dict[str, str]] = None,
    is_permanent: bool = False,
    session=None,
    **kwargs,
):  # pragma: no cover
    """Helper function to encode UDAF parameters (used in both regular and mock UDFRegistration)."""
    # This is the name the UDAF is registered to. Not the name to display when unparsing, that name is captured in callable.

    if name is not None:
        _set_fn_name(name, ast)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "_set_fn_name" has incompatible type "Udaf"; expected "FnNameRefExpr"

    build_proto_from_callable(
        ast.handler, handler, session._ast_batch if session is not None else None
    )

    if return_type is not None:
        return_type._fill_ast(ast.return_type)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    if input_types is not None and len(input_types) != 0:
        for input_type in input_types:
            input_type._fill_ast(ast.input_types.list.add())  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    ast.is_permanent = is_permanent
    if stage_location is not None:
        ast.stage_location.value = stage_location
    if imports is not None and len(imports) != 0:
        for import_ in imports:
            import_expr = proto.SpTableName()
            build_sp_table_name(import_expr, import_)
            ast.imports.append(import_expr)
    if packages is not None and len(packages) != 0:
        for package in packages:
            if isinstance(package, ModuleType):
                raise NotImplementedError
            ast.packages.append(package)
    ast.replace = replace
    ast.if_not_exists = if_not_exists
    ast.parallel = parallel

    if statement_params is not None and len(statement_params) != 0:
        for k, v in statement_params.items():
            t = ast.statement_params.add()
            t._1 = k
            t._2 = v

    if (
        external_access_integrations is not None
        and len(external_access_integrations) != 0
    ):
        ast.external_access_integrations.extend(external_access_integrations)
    if secrets is not None and len(secrets) != 0:
        for k, v in secrets.items():
            t = ast.secrets.add()
            t._1 = k
            t._2 = v
    ast.immutable = immutable
    if comment is not None:
        ast.comment.value = comment
    for k, v in kwargs.items():
        t = ast.kwargs.add()  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Tuple_String_Expr", variable has type "Tuple_String_String")
        t._1 = k
        build_expr_from_python_val(t._2, v)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "build_expr_from_python_val" has incompatible type "str"; expected "Expr"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_udtf(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a return type annotation, Function is missing a type annotation for one or more arguments
    ast: proto.Udtf,
    handler: Union[Callable, Tuple[str, str]],
    output_schema: Union[  # type: ignore[name-defined] # TODO(SNOW-1491199) # Name "PandasDataFrameType" is not defined
        StructType, Iterable[str], "PandasDataFrameType"  # noqa: F821
    ],  # noqa: F821
    input_types: Optional[List[DataType]],
    name: Optional[str],
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    statement_params: Optional[Dict[str, str]] = None,
    is_permanent: bool = False,
    session=None,
    **kwargs,
):  # pragma: no cover
    """Helper function to encode UDTF parameters (used in both regular and mock UDFRegistration)."""
    # This is the name the UDTF is registered to. Not the name to display when unparsing, that name is captured in callable.

    if name is not None:
        _set_fn_name(name, ast)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "_set_fn_name" has incompatible type "Udtf"; expected "FnNameRefExpr"

    build_proto_from_callable(
        ast.handler, handler, session._ast_batch if session is not None else None
    )

    if output_schema is not None:
        if isinstance(output_schema, DataType):
            output_schema._fill_ast(ast.output_schema.udtf_schema__type.return_type)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
        elif isinstance(output_schema, Sequence) and all(
            isinstance(el, str) for el in output_schema
        ):
            ast.output_schema.udtf_schema__names.schema.extend(output_schema)
        else:
            raise ValueError(f"Can not encode {output_schema} to AST.")

    if input_types is not None and len(input_types) != 0:
        for input_type in input_types:
            input_type._fill_ast(ast.input_types.list.add())  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    ast.is_permanent = is_permanent
    if stage_location is not None:
        ast.stage_location = stage_location
    if imports is not None and len(imports) != 0:
        for import_ in imports:
            import_expr = proto.SpTableName()
            build_sp_table_name(import_expr, import_)
            ast.imports.append(import_expr)
    if packages is not None and len(packages) != 0:
        for package in packages:
            if isinstance(package, ModuleType):
                raise NotImplementedError
            ast.packages.append(package)
    ast.replace = replace
    ast.if_not_exists = if_not_exists
    ast.parallel = parallel

    if statement_params is not None and len(statement_params) != 0:
        for k, v in statement_params.items():
            t = ast.statement_params.add()
            t._1 = k
            t._2 = v

    ast.strict = strict
    ast.secure = secure
    if (
        external_access_integrations is not None
        and len(external_access_integrations) != 0
    ):
        ast.external_access_integrations.extend(external_access_integrations)
    if secrets is not None and len(secrets) != 0:
        for k, v in secrets.items():
            t = ast.secrets.add()
            t._1 = k
            t._2 = v
    ast.immutable = immutable
    if comment is not None:
        ast.comment.value = comment
    for k, v in kwargs.items():
        t = ast.kwargs.add()  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Tuple_String_Expr", variable has type "Tuple_String_String")
        t._1 = k
        build_expr_from_python_val(t._2, v)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "build_expr_from_python_val" has incompatible type "str"; expected "Expr"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def add_intermediate_stmt(ast_batch: AstBatch, o: Any) -> None:  # pragma: no cover
    """
    Helper function that takes an object AST as input and creates an assignment for it.

    This is useful for capturing a potentially complex expression and referring to it from multiple places without
    inlining it everywhere.

    Args:
        ast_batch: The AstBatch instance in which to create the assignment.
        o: The input object. If the object is of type TableFunctionCall, or a callable created by
         functions.table_function, it must have a field named _ast, of type proto.Expr.
    """
    if not isinstance(
        o, (snowflake.snowpark.table_function.TableFunctionCall, Callable)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "isinstance" has incompatible type "tuple[type[TableFunctionCall], <typing special form>]"; expected "_ClassInfo"
    ):
        return
    stmt = ast_batch.assign()
    stmt.expr.CopyFrom(o._ast)
    o._ast_stmt = stmt


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_sproc(  # type: ignore[no-untyped-def] # TODO(SNOW-1491199) # Function is missing a type annotation for one or more arguments
    ast: proto.StoredProcedure,
    func: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    sp_name: str,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    parallel: int = 4,
    strict: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
    statement_params: Optional[Dict[str, str]] = None,
    execute_as: typing.Literal["caller", "owner"] = "owner",
    source_code_display: bool = True,
    is_permanent: bool = False,
    session=None,
    **kwargs,
) -> None:  # pragma: no cover
    """Helper function to encode stored procedure parameters (used in both regular and mock StoredProcedureRegistration)."""

    if sp_name is not None:
        _set_fn_name(sp_name, ast)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 2 to "_set_fn_name" has incompatible type "StoredProcedure"; expected "FnNameRefExpr"

    build_proto_from_callable(
        ast.func, func, session._ast_batch if session is not None else None
    )

    if return_type is not None:
        return_type._fill_ast(ast.return_type)  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    if input_types is not None and len(input_types) != 0:
        for input_type in input_types:
            input_type._fill_ast(ast.input_types.list.add())  # type: ignore[attr-defined] # TODO(SNOW-1491199) # "DataType" has no attribute "_fill_ast"
    ast.is_permanent = is_permanent
    if stage_location is not None:
        ast.stage_location = stage_location
    if imports is not None and len(imports) != 0:
        for import_ in imports:
            import_expr = proto.SpTableName()
            build_sp_table_name(import_expr, import_)
            ast.imports.append(import_expr)
    if packages is not None and len(packages) != 0:
        for package in packages:
            if isinstance(package, ModuleType):
                raise NotImplementedError
            ast.packages.append(package)
    ast.replace = replace
    ast.if_not_exists = if_not_exists
    ast.parallel = parallel

    if statement_params is not None and len(statement_params) != 0:
        for k, v in statement_params.items():
            t = ast.statement_params.add()
            t._1 = k
            t._2 = v

    ast.execute_as = execute_as
    ast.source_code_display = source_code_display
    ast.strict = strict
    if (
        external_access_integrations is not None
        and len(external_access_integrations) != 0
    ):
        ast.external_access_integrations.extend(external_access_integrations)
    if secrets is not None and len(secrets) != 0:
        for k, v in secrets.items():
            t = ast.secrets.add()
            t._1 = k
            t._2 = v
    if comment is not None:
        ast.comment.value = comment
    for k, v in kwargs.items():
        t = ast.kwargs.add()  # type: ignore[assignment] # TODO(SNOW-1491199) # Incompatible types in assignment (expression has type "Tuple_String_Expr", variable has type "Tuple_String_String")
        t._1 = k
        build_expr_from_python_val(t._2, v)  # type: ignore[arg-type] # TODO(SNOW-1491199) # Argument 1 to "build_expr_from_python_val" has incompatible type "str"; expected "Expr"


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def build_expr_from_dict_str_str(
    ast_dict: proto.Tuple_String_String, dict_str_str: Dict[str, str]
) -> None:  # pragma: no cover
    """Populate the AST structure with dictionary for str -> str.

    Args:
        ast_dict (proto.Tuple_String_String: An ast representation for key, value pairs
        dict_str_str (Dict[str, str]): The dictionary mapping str to str.
    """
    for k, v in dict_str_str.items():
        t = ast_dict.add()  # type: ignore[attr-defined, arg-type] # TODO(SNOW-1491199) # "Tuple_String_String" has no attribute "add"
        t._1 = k
        t._2 = v
