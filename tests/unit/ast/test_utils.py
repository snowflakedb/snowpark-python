#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import pytest

from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_table_name,
    build_view_name,
    build_name,
    extract_src_from_expr,
)
from snowflake.snowpark._internal.proto.generated import ast_pb2 as proto


@pytest.mark.parametrize(
    "value", [0, 1, -1, 3.141, "1234567", "12345678.9876543212345676543456"]
)
def test_decimal_encoding(value):
    v = Decimal(value)
    expr = proto.Expr()
    build_expr_from_python_val(expr, v)


def test_build_expr_from_python_val_dict():
    value = {"key1": "value1", "key2": "value2"}
    expr = proto.Expr()
    build_expr_from_python_val(expr, value)
    assert expr.seq_map_val.kvs[0].vs[0].string_val.v == "key1"
    assert expr.seq_map_val.kvs[0].vs[1].string_val.v == "value1"
    assert expr.seq_map_val.kvs[1].vs[0].string_val.v == "key2"
    assert expr.seq_map_val.kvs[1].vs[1].string_val.v == "value2"


def test_build_expr_from_python_val_list():
    value = [1, 2, 3]
    expr = proto.Expr()
    build_expr_from_python_val(expr, value)
    assert expr.list_val.vs[0].int64_val.v == 1
    assert expr.list_val.vs[1].int64_val.v == 2
    assert expr.list_val.vs[2].int64_val.v == 3


def test_build_expr_from_python_val_tuple():
    value = (1, 2, 3)
    expr = proto.Expr()
    build_expr_from_python_val(expr, value)
    assert expr.tuple_val.vs[0].int64_val.v == 1
    assert expr.tuple_val.vs[1].int64_val.v == 2
    assert expr.tuple_val.vs[2].int64_val.v == 3


def test_build_name():
    expr = proto.Name()
    build_name("foo", expr)
    assert expr.HasField("name_flat")
    assert expr.name_flat.name == "foo"
    expr = proto.Name()
    build_name(["foo", "bar", "baz"], expr)
    assert expr.HasField("name_structured")
    assert expr.name_structured.name == ["foo", "bar", "baz"]
    try:
        expr = proto.Name()
        build_name(123, expr)
        raise AssertionError("Expected the previous call to raise an exception")
    except ValueError:
        pass


def test_build_table_name_error():
    try:
        expr = proto.NameRef()
        build_table_name(expr, 42)
        raise AssertionError("Expected the previous call to raise an exception")
    except ValueError as e:
        assert "Invalid table name" in str(e)


def test_build_view_name_error():
    try:
        expr = proto.NameRef()
        build_view_name(expr, 42)
        raise AssertionError("Expected the previous call to raise an exception")
    except ValueError as e:
        assert "Invalid view name" in str(e)


def test_extract_src_from_expr():
    def verify_src(
        expr: proto.Expr,
        file: int,
        start_line: int,
        end_line: int,
        start_column: int,
        end_column: int,
    ):
        return (
            expr.file == file
            and expr.start_line == start_line
            and expr.end_line == end_line
            and expr.start_column == start_column
            and expr.end_column == end_column
        )

    expr = proto.Expr()
    expr.string_val.v = "test"
    expr.dataframe_select.src.file = 2
    expr.dataframe_select.src.start_line = 20
    expr.dataframe_select.src.end_line = 20
    expr.dataframe_select.src.start_column = 5
    expr.dataframe_select.src.end_column = 15

    result = extract_src_from_expr(expr)
    assert verify_src(result, 2, 20, 20, 5, 15)

    expr2 = proto.Expr()
    expr2.string_val.v = "test2"
    expr2.dataframe_sort.src.file = 1
    expr2.dataframe_sort.src.start_line = 3
    expr2.dataframe_sort.src.end_line = 6
    expr2.dataframe_sort.src.start_column = 7
    expr2.dataframe_sort.src.end_column = 8

    result = extract_src_from_expr(expr2)
    assert verify_src(result, 1, 3, 6, 7, 8)

    expr = proto.Expr()
    result = extract_src_from_expr(expr)
    assert result is None
