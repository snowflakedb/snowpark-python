#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import datetime
import decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.expression import Attribute, Literal
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    BatchInsertQuery,
    CteSegment,
    Query,
    QueryLineInterval,
    SnowflakePlan,
)
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
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkPlanException
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DataType, DecFloatType, PandasSeriesType


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

    # check explicitly setting the datatype
    explicit_literal = Literal(decimal.Decimal(0.5), datatype=DecFloatType())
    assert explicit_literal.datatype == DecFloatType()
    assert explicit_literal.value == decimal.Decimal(0.5)

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


def test_batch_insert_query_copy_preserves_metadata(mock_session):
    query = BatchInsertQuery("INSERT INTO temp_name VALUES (?)", [Row(1)])
    query.query_id_place_holder = "query_id"
    query.is_ddl_on_temp_object = True
    query.temp_obj_name_placeholder = ("temp_name", TempObjectType.TABLE)
    query.params = [1]
    query.query_line_intervals = [QueryLineInterval(0, 0, "owner")]
    owner = SnowflakePlan([Query("SELECT 1")], None, session=mock_session)
    query.cte_segments = [CteSegment(0, len(query.sql), owner)]

    copied = copy.copy(query)
    assert type(copied) is BatchInsertQuery
    assert copied is not query
    assert copied == query
    assert copied.rows is query.rows
    assert copied.params is query.params
    assert copied.query_line_intervals is query.query_line_intervals
    assert copied.cte_segments is query.cte_segments
    assert copied.cte_segments[0].plan is owner


def test_query():
    q = Query("select 1", query_id_place_holder="uuid")
    assert eval(repr(q)) == q

    q = Query("'select 1'", query_id_place_holder="'uuid'", is_ddl_on_temp_object=True)
    assert eval(repr(q)) == q
