#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Optional

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.expression import (
    FunctionExpression,
    Literal,
)
from snowflake.snowpark._internal.ast.utils import (
    build_builtin_fn_apply,
    build_function_expr,
)
from snowflake.snowpark._internal.type_utils import (
    ColumnOrLiteral,
)
from snowflake.snowpark._internal.utils import (
    parse_positional_args_to_list,
)
from snowflake.snowpark.column import (
    Column,
)
from snowflake.snowpark.types import (
    DataType,
)


# check function to allow test_dataframe_alias_negative to pass in AST mode.
def _check_column_parameters(name1: str, name2: Optional[str]) -> None:
    if not isinstance(name1, str):
        raise ValueError(
            f"Expects first argument to be of type str, got {type(name1)}."
        )

    if name2 is not None and not isinstance(name2, str):
        raise ValueError(
            f"Expects second argument to be of type str or None, got {type(name2)}."
        )


def lit(
    literal: ColumnOrLiteral,
    datatype: Optional[DataType] = None,
    _emit_ast: bool = True,
) -> Column:

    if _emit_ast:
        ast = proto.Expr()
        if datatype is None:
            build_builtin_fn_apply(ast, "lit", literal)
        else:
            build_builtin_fn_apply(ast, "lit", literal, datatype)

        if isinstance(literal, Column):
            # Create new Column, and assign expression of current Column object.
            # This will encode AST correctly.
            c = Column("", _emit_ast=False)
            c._expression = literal._expression
            c._ast = ast
            return c
        return Column(Literal(literal, datatype=datatype), _ast=ast, _emit_ast=True)

    if isinstance(literal, Column):
        return literal

    return Column(Literal(literal, datatype=datatype), _ast=None, _emit_ast=False)


def _call_function(
    name: str,
    *args: ColumnOrLiteral,
    is_distinct: bool = False,
    api_call_source: Optional[str] = None,
    is_data_generator: bool = False,
    _ast: proto.Expr = None,
    _emit_ast: bool = True,
) -> Column:

    if _emit_ast and _ast is None:
        _ast = build_function_expr(name, args)

    args_list = parse_positional_args_to_list(*args)
    expressions = [Column._to_expr(arg) for arg in args_list]
    return Column(
        FunctionExpression(
            name,
            expressions,
            is_distinct=is_distinct,
            api_call_source=api_call_source,
            is_data_generator=is_data_generator,
        ),
        _ast=_ast,
        _emit_ast=_emit_ast,
    )


def call_function(
    function_name: str,
    *args: ColumnOrLiteral,
    _emit_ast: bool = True,
) -> Column:
    ast = (
        build_function_expr("call_function", [function_name, *args])
        if _emit_ast
        else None
    )
    return _call_function(function_name, *args, _ast=ast, _emit_ast=_emit_ast)


def function(function_name: str, _emit_ast: bool = True) -> Callable:
    return lambda *args: call_function(function_name, *args, _emit_ast=_emit_ast)


def col(
    name1: str,
    name2: Optional[str] = None,
    _emit_ast: bool = True,
    *,
    _is_qualified_name: bool = False,
) -> Column:

    _check_column_parameters(name1, name2)

    if name2 is None:
        return Column(
            name1,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="col",
        )
    else:
        return Column(
            name1,
            name2,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="col",
        )


builtin = function
