#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal

import pytest

from snowflake.snowpark._internal.analyzer.expression import Attribute, Literal
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnaryMinus,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    LocalTempView,
    PersistedView,
)
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark.exceptions import SnowparkPlanException
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DataType, PandasSeriesType


def test_literal():
    valid_data = [
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
        (1, 1),
        [2, 2],
        {"1": 2},
    ]

    invalid_data = [pytest]

    for d in valid_data:
        assert isinstance(Literal(d).datatype, infer_type(d).__class__)

    for d in invalid_data:
        with pytest.raises(SnowparkPlanException) as ex_info:
            Literal(d)
        assert "Cannot create a Literal" in str(ex_info)

    with pytest.raises(SnowparkPlanException) as ex_info:
        Literal(1, PandasSeriesType(DataType()))
    assert "Cannot create a Literal" in str(ex_info)


def test_view_type_str():
    assert str(PersistedView()) == "Persisted"
    assert str(LocalTempView()) == "LocalTemp"


def test_unary_expression_str():
    expr = col("a")._expression

    assert str(UnaryMinus(expr)) == '- "A"'
    assert str(IsNull(expr)) == '"A" IS NULL'
    assert str(IsNotNull(expr)) == '"A" IS NOT NULL'
    assert str(IsNaN(expr)) == "\"A\" = 'NAN'"
    assert str(Not(expr)) == 'NOT "A"'
    assert str(Alias(expr, '"B"')) == '"A" AS "B"'


def test_attribute():
    attr = Attribute("a", DataType())
    assert attr.with_name("b").name == '"B"'


def test_query():
    q = Query("select 1", query_id_place_holder="uuid")
    assert eval(repr(q)) == q

    q = Query("'select 1'", query_id_place_holder="'uuid'", is_ddl_on_temp_object=True)
    assert eval(repr(q)) == q
