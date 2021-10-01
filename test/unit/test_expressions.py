#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.sp_expressions import *
from snowflake.snowpark._internal.sp_types.sp_data_types import DecimalType, IntegerType


def test_expression_sql():
    attribute = Attribute("A")
    assert "A" == attribute.sql()

    ar = AttributeReference("A", DecimalType, True)
    assert "A" == ar.sql()

    unresolved_attribute = UnresolvedAttribute(["namepart1", "namepart2"])
    assert "namepart1.namepart2" == unresolved_attribute.sql()

    unresolved_function = UnresolvedFunction(
        name="func",
        arguments=[AttributeReference("A", IntegerType(), True)],
        is_distinct=False,
    )
    assert "func(A)" == unresolved_function.sql()

    agg_expr = AggregateExpression(
        Count(AttributeReference("A", IntegerType(), False)), Complete(), False, None
    )
    assert "COUNT(A)" == agg_expr.sql()
