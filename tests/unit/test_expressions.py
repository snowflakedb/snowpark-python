#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal

import pytest

from snowflake.snowpark._internal.sp_expressions import (
    AggregateExpression,
    Attribute,
    AttributeReference,
    Complete,
    Count,
    FunctionExpression,
    Literal,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.sp_types.sp_data_types import DecimalType, IntegerType
from snowflake.snowpark._internal.sp_types.types_package import _type_mappings
from snowflake.snowpark.exceptions import SnowparkPlanException


def test_expression_sql():
    attribute = Attribute("A")
    assert "A" == attribute.sql()

    ar = AttributeReference("A", DecimalType(), True)
    assert "A" == ar.sql()

    unresolved_attribute = UnresolvedAttribute(["namepart1", "namepart2"])
    assert "namepart1.namepart2" == unresolved_attribute.sql()

    function_expression = FunctionExpression(
        name="func",
        arguments=[AttributeReference("A", IntegerType(), True)],
        is_distinct=False,
    )
    assert "func(A)" == function_expression.sql()

    agg_expr = AggregateExpression(
        Count(AttributeReference("A", IntegerType(), False)), Complete(), False, None
    )
    assert "COUNT(A)" == agg_expr.sql()


def test_literal():
    basic_data = [
        1,
        "one",
        1.0,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        bytes("a", "utf-8"),
        decimal.Decimal(0.5),
    ]

    structured_data = [(1, 1), [2, 2], {"1": 2}]

    for d in basic_data:
        assert isinstance(Literal(d).datatype, _type_mappings[type(d)])

    for d in structured_data:
        with pytest.raises(SnowparkPlanException) as ex_info:
            Literal(d)
        assert "Cannot create a Literal" in str(ex_info)
