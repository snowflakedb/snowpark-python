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
