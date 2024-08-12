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
from typing import Any, Dict, Iterable, Optional, Tuple, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Expression,
    Literal,
    MultipleExpression,
    Star,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SaveMode
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    ColumnOrLiteral,
    ColumnOrName,
    ColumnOrSqlExpr,
)
from snowflake.snowpark._internal.utils import str_to_enum

# This flag causes an explicit error to be raised if any Snowpark object instance is missing an AST or field, when this
# AST or field is required to populate the AST field of a different Snowpark object instance.
FAIL_ON_MISSING_AST = True


def build_expr_from_python_val(expr_builder: proto.Expr, obj: Any) -> None:
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
        with_src_position(expr_builder.null_val)

    # Keep objects most high up in the class hierarchy first, i.e. a Row is a tuple.
    elif isinstance(obj, Column):
        expr_builder.CopyFrom(obj._ast)

    elif isinstance(obj, Row):
        sp_row_ast = with_src_position(expr_builder.sp_row)
        if hasattr(obj, "_named_values") and obj._named_values is not None:
            for field in obj._fields:
                sp_row_ast.names.list.append(field)
                build_expr_from_python_val(
                    sp_row_ast.vs.add(), obj._named_values[field]
                )
        else:
            for field in obj:
                build_expr_from_python_val(sp_row_ast.vs.add(), field)

    elif isinstance(obj, bool):
        bool_val_ast = with_src_position(expr_builder.bool_val)
        bool_val_ast.v = obj

    elif isinstance(obj, int):
        int64_val_ast = with_src_position(expr_builder.int64_val)
        int64_val_ast.v = obj

    elif isinstance(obj, float):
        float64_val_ast = with_src_position(expr_builder.float64_val)
        float64_val_ast.v = obj

    elif isinstance(obj, str):
        string_val_ast = with_src_position(expr_builder.string_val)
        string_val_ast.v = obj

    elif isinstance(obj, bytes):
        binary_val_ast = with_src_position(expr_builder.binary_val)
        binary_val_ast.v = obj

    elif isinstance(obj, bytearray):
        binary_val_ast = with_src_position(expr_builder.binary_val)
        binary_val_ast.v = bytes(obj)

    elif isinstance(obj, decimal.Decimal):
        big_decimal_val_ast = with_src_position(expr_builder.big_decimal_val)
        dec_tuple = obj.as_tuple()
        unscaled_val = reduce(lambda val, digit: val * 10 + digit, dec_tuple.digits)
        if dec_tuple.sign != 0:
            unscaled_val *= -1
        req_bytes = (unscaled_val.bit_length() + 7) // 8
        big_decimal_val_ast.unscaled_value = unscaled_val.to_bytes(
            req_bytes, "big", signed=True
        )
        big_decimal_val_ast.scale = dec_tuple.exponent

    elif isinstance(obj, datetime.datetime):
        python_timestamp_val_ast = with_src_position(expr_builder.python_timestamp_val)
        if obj.tzinfo is not None:
            python_timestamp_val_ast.tz.offset_seconds = int(
                obj.tzinfo.utcoffset(obj).total_seconds()
            )
            tz = obj.tzinfo.tzname(obj)
            if tz is not None:
                python_timestamp_val_ast.tz.name.value = tz
        else:
            obj = obj.astimezone(datetime.timezone.utc)

        python_timestamp_val_ast.year = obj.year
        python_timestamp_val_ast.month = obj.month
        python_timestamp_val_ast.day = obj.day
        python_timestamp_val_ast.hour = obj.hour
        python_timestamp_val_ast.minute = obj.minute
        python_timestamp_val_ast.second = obj.second
        python_timestamp_val_ast.microsecond = obj.microsecond

    elif isinstance(obj, datetime.date):
        python_date_val_ast = with_src_position(expr_builder.python_date_val)
        python_date_val_ast.year = obj.year
        python_date_val_ast.month = obj.month
        python_date_val_ast.day = obj.day

    elif isinstance(obj, datetime.time):
        python_time_val_ast = with_src_position(expr_builder.python_time_val)
        datetime_val = datetime.datetime.combine(datetime.date.today(), obj)
        if obj.tzinfo is not None:
            python_time_val_ast.tz.offset_seconds = int(
                obj.tzinfo.utcoffset(datetime_val).total_seconds()
            )
            tz = obj.tzinfo.tzname(datetime_val)
            if tz is not None:
                python_time_val_ast.tz.name.value = tz
        else:
            obj = datetime_val.astimezone(datetime.timezone.utc)

        python_time_val_ast.hour = obj.hour
        python_time_val_ast.minute = obj.minute
        python_time_val_ast.second = obj.second
        python_time_val_ast.microsecond = obj.microsecond

    elif isinstance(obj, dict):
        seq_map_val_ast = with_src_position(expr_builder.seq_map_val)
        for key, value in obj.items():
            kv_tuple_ast = seq_map_val_ast.kvs.add()
            build_expr_from_python_val(kv_tuple_ast.vs.add(), key)
            build_expr_from_python_val(kv_tuple_ast.vs.add(), value)

    elif isinstance(obj, list):
        list_val_ast = with_src_position(expr_builder.list_val)
        for v in obj:
            build_expr_from_python_val(list_val_ast.vs.add(), v)

    elif isinstance(obj, tuple):
        tuple_val_ast = with_src_position(expr_builder.tuple_val)
        for v in obj:
            build_expr_from_python_val(tuple_val_ast.vs.add(), v)

    else:
        raise NotImplementedError("not supported type: %s" % type(obj))


def build_proto_from_struct_type(
    schema: "snowflake.snowpark.types.StructType", expr: proto.SpStructType
) -> None:
    from snowflake.snowpark.types import StructType

    assert isinstance(schema, StructType)

    expr.structured = schema.structured
    for field in schema.fields:
        ast_field = expr.fields.add()
        field.column_identifier._fill_ast(ast_field.column_identifier)
        field.datatype._fill_ast(ast_field.data_type)
        ast_field.nullable = field.nullable


def _set_fn_name(name: Union[str, Iterable[str]], fn: proto.FnRefExpr) -> None:
    """
    Set the function name in the AST. The function name can be a string or an iterable of strings.
    Args:
        name: The function name to set in the AST.
        fn: The function reference expression to set the name in. The caller must provide the correct type of function.

    Raises:
        ValueError: Raised if the function name is not a string or an iterable of strings.
    """
    if isinstance(name, str):
        fn.name.fn_name_flat.name = name
    elif isinstance(name, Iterable):
        fn.name.fn_name_structured.name.extend(name)
    else:
        raise ValueError(
            f"Invalid function name: {name}. The function name must be a string or an iterable of strings."
        )


def build_sp_table_name(
    expr_builder: proto.SpTableName, name: Union[str, Iterable[str]]
):
    if isinstance(name, str):
        expr_builder.sp_table_name_flat.name = name
    elif isinstance(name, Iterable):
        expr_builder.sp_table_name_structured.name.extend(name)
    else:
        raise ValueError(f"Invalid name type {type(name)} for SpTableName entity.")


def build_builtin_fn_apply(
    ast: proto.Expr,
    builtin_name: str,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:
    """
    Creates AST encoding for ApplyExpr(BuiltinFn(<builtin_name>), List(<args...>), Map(<kwargs...>)) for builtin
    functions.
    Args:
        ast: Expr node to fill.
        builtin_name: Name of the builtin function to call.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    """
    expr = with_src_position(ast.apply_expr)
    _set_fn_name(builtin_name, expr.fn.builtin_fn)
    with_src_position(expr.fn.builtin_fn)
    build_fn_apply_args(ast, *args, **kwargs)


def build_udf_apply(
    ast: proto.Expr,
    udf_name: str,
    *args: Tuple[Union[proto.Expr, Any]],
) -> None:
    expr = with_src_position(ast.apply_expr)
    _set_fn_name(udf_name, expr.fn.udf)
    with_src_position(expr.fn.udf)
    build_fn_apply_args(ast, *args)


def build_session_table_fn_apply(
    ast: proto.Expr,
    name: Union[str, Iterable[str]],
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:
    """
    Creates AST encoding for ApplyExpr(SessionTableFn(<name>), List(<args...>), Map(<kwargs...>)) for session table functions.
    Args:
        ast: Expr node to fill.
        name: Name of the session table function to call.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.
    """
    expr = with_src_position(ast.apply_expr)
    _set_fn_name(name, expr.fn.session_table_fn)
    with_src_position(expr.fn.session_table_fn)
    build_fn_apply_args(ast, *args, **kwargs)


def build_table_fn_apply(
    ast: proto.Expr,
    name: Union[str, Iterable[str], None],
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:
    """
    Creates AST encoding for ApplyExpr(TableFn(<name>), List(<args...>), Map(<kwargs...>)) for table functions.
    Args:
        ast: Expr node to fill.
        name: Name of the table function to call. The name can be None and is ignored for table function calls of type SessionTableFn.
        *args: Positional arguments to pass to function.
        **kwargs: Keyword arguments to pass to function.

    Requires that ast.apply_expr.fn.table_fn.call_type is set to a valid TableFnCallType.
    """
    expr = with_src_position(ast.apply_expr)
    assert (
        ast.apply_expr.fn.table_fn.call_type.WhichOneof("variant") is not None
    ), f"Explicitly set the call type before calling this function {str(ast.apply_expr.fn.table_fn)}"
    if not expr.fn.table_fn.call_type.table_fn_call_type__session_table_fn:
        assert (
            name is not None
        ), f"Table function name must be provided {str(ast.apply_expr.fn.table_fn)}"
        _set_fn_name(name, expr.fn.table_fn)
    with_src_position(expr.fn.table_fn)
    build_fn_apply_args(ast, *args, **kwargs)


def build_fn_apply_args(
    ast: proto.Expr,
    *args: Tuple[Union[proto.Expr, Any]],
    **kwargs: Dict[str, Union[proto.Expr, Any]],
) -> None:
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
            assert arg._ast, f"Column object {arg} has no _ast member set."
            expr.pos_args.append(arg._ast)
        else:
            pos_arg = proto.Expr()
            build_expr_from_python_val(pos_arg, arg)
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
            build_expr_from_python_val(kwarg._2, arg)
        expr.named_args.append(kwarg)


def set_builtin_fn_alias(ast: proto.Expr, alias: str) -> None:
    """
    Set the alias for a builtin function call. Requires that the expression has an ApplyExpr with a BuiltinFn.
    Args:
        ast: Expr node to fill.
        alias: Alias to set for the builtin function.
    """
    _set_fn_name(alias, ast.apply_expr.fn.builtin_fn)


assignment_re = re.compile(r"^\s*([a-zA-Z_]\w*)\s*=.*$", re.DOTALL)

def with_src_position(
    expr_ast: proto.Expr, 
    assign: Optional[proto.Assign] = None,
    max_stack_lookback: int = 3,
) -> proto.Expr:
    """
    Sets the src_position on the supplied Expr AST node and returns it.
    N.B. This function assumes it's always invoked from a public API, meaning that the caller's caller
    is always the code of interest.
    Args:
        expr_ast: The AST node to set the src_position on.
        assign: The Assign AST node to set the symbol value on.
        max_stack_lookback: The maximum number of stack frames to look back from the caller's caller if 
                            the caller's caller is still in the snowpark package.
    """
    src = expr_ast.src
    frame = inspect.currentframe()

    # Best practices for the inspect library are to remove references to frame objects once done with them
    # to avoid reference cycles and memory leaks. The above frame assignment is removed in the finally block.
    try:
        # Need this None guard as depending on the implementation of sys._getframe, frame may be None.
        # Note the assignment to src.file is needed as several upstream uses of this method rely on 
        # setting src fields for explicit presence of the encapsulating message in the AST.
        # e.g., Null values have no fields, so the assignment to src fields ensures their presence.
        if frame is None:
            src.file = "<unknown>"
            return expr_ast
        
        # If frame is not None, we can step back to the caller's caller via f_back twice.
        # Since all uses of this function should be from public APIs, this should immediately reach the
        # code of interest most of the time. Could still be None depending on the execution context.
        frame = frame.f_back.f_back
        filename = frame.f_code.co_filename if frame is not None else "<unknown>"

        stack_depth = 0
        # If the caller's caller is in the snowpark package, keep stepping back until we're out of it.
        snowpark_path = Path(__file__).parents[1]
        while stack_depth < max_stack_lookback and \
        frame is not None and snowpark_path in Path(filename).parents:
            frame = frame.f_back
            filename = frame.f_code.co_filename
            stack_depth += 1

        # Again, once we've stepped out of the snowpark package, we should be in the code of interest.
        # However, the code of interest may execute in an environment that is not accessible via the filesystem.
        # e.g. Jupyter notebooks, REPLs, calls to exec, etc.
        if frame is None or not Path(filename).is_file():
            src.file = "<unknown>"
            return expr_ast
        
        # The context argument specifies the number of lines of context to capture around the current line.
        # If IO performance is an issue, this can be set to 0 but this will disable symbol capture. Some
        # potential alternatives to consider here are the linecache and traceback modules.
        frame_info = inspect.getframeinfo(frame, context=1)
        src.file = frame_info.filename
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
                source_line = code[frame_info.index]
                if match := assignment_re.fullmatch(source_line):
                    assign.symbol.value = match.group(1)
    finally:
        del frame

    return expr_ast


def build_expr_from_snowpark_column(
    expr_builder: proto.Expr, value: "snowflake.snowpark.Column"
) -> None:
    """Copy from a Column object's AST into an AST expression.

    Args:
        ast (proto.Expr): A previously created Expr() IR entity intance to be filled
        value (snowflake.snowpark.Column): The value from which to populate the provided ast parameter.

    Raises:
        NotImplementedError: Raised if the Column object does not have an AST set and FAIL_ON_MISSING_AST is True.
    """
    if value._ast is None and FAIL_ON_MISSING_AST:
        raise NotImplementedError(
            f"Column({value._expression})._ast is None due to the use of a Snowpark API which does not support AST logging yet."
        )
    elif value._ast is not None:
        expr_builder.CopyFrom(value._ast)


def build_expr_from_snowpark_column_or_col_name(
    expr_builder: proto.Expr, value: ColumnOrName
) -> None:
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
        expr = with_src_position(expr_builder.string_val)
        expr.v = value
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or column name AST."
        )


def build_expr_from_snowpark_column_or_sql_str(
    expr_builder: proto.Expr, value: ColumnOrSqlExpr
) -> None:
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
        expr = with_src_position(expr_builder.sp_column_sql_expr)
        expr.sql = value
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or SQL expression AST."
        )


def build_expr_from_snowpark_column_or_python_val(
    expr_builder: proto.Expr, value: ColumnOrLiteral
) -> None:
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


def build_expr_from_snowpark_column_or_table_fn(
    expr_builder: proto.Expr,
    value: Union[
        "snowflake.snowpark.Column",
        "snowflake.snowpark.table_function.TableFunctionCall",
    ],
) -> None:
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
        raise NotImplementedError(
            "SNOW-1509198: No support for TableFunctionCall AST generation"
        )
    else:
        raise TypeError(
            f"{type(value)} is not a valid type for Column or TableFunctionCall AST generation."
        )


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

    build_builtin_fn_apply(expr, fn_name, *args, **kwargs)


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
        build_expr_from_python_val(ast, expr.value)
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
    elif isinstance(expr, Star):
        # Comes up in count(), handled there.
        return None
    else:
        raise NotImplementedError(
            f"Snowpark expr {expr} of type {type(expr)} is an expression with missing AST or for which an AST can not be auto-generated."
        )


def fill_sp_save_mode(expr: proto.SpSaveMode, save_mode: Union[str, SaveMode]) -> None:
    if isinstance(save_mode, str):
        save_mode = str_to_enum(save_mode.lower(), SaveMode, "`save_mode`")

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
) -> None:
    expr.location = location

    if partition_by is not None:
        build_expr_from_snowpark_column_or_sql_str(expr.partition_by, partition_by)

    if format_type_options is not None:
        for k, v in format_type_options.items():
            t = expr.format_type_options.add()
            t._1 = k
            t._2 = v

    expr.header = header

    if statement_params is not None:
        for k, v in statement_params.items():
            t = expr.statement_params.add()
            t._1 = k
            t._2 = v

    expr.block = block

    if copy_options:
        for k, v in copy_options.items():
            t = expr.copy_options.add()
            t._1 = k
            build_expr_from_python_val(t._2, v)
