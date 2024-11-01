#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import pytest

import snowflake.snowpark._internal.generated.proto.ast_pb2 as proto
from snowflake.snowpark._internal.ast_utils import build_expr_from_python_val


@pytest.mark.parametrize(
    "value", [0, 1, -1, 3.141, "1234567", "12345678.9876543212345676543456"]
)
def test_decimal_encoding(value):
    v = Decimal(value)
    expr = proto.Expr()
    build_expr_from_python_val(expr, v)
