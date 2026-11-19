#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal

import pytest

from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    ColumnSum,
    Collate,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    NamedFunctionExpression,
    RegExp,
    ScalarSubquery,
    SubfieldInt,
    SubfieldString,
    WithinGroup,
    expression_children,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    has_window_function_exp,
)
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
    Filter,
    LocalTempView,
    PersistedView,
)
from snowflake.snowpark._internal.analyzer.window_expression import (
    RankRelatedFunctionExpression,
    RowFrame,
    SpecifiedWindowFrame,
    WindowSpecDefinition,
)
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark.exceptions import SnowparkPlanException
from snowflake.snowpark.functions import col, row_number, sql_expr
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import DataType, DecFloatType, PandasSeriesType


@pytest.mark.parametrize(
    "wrap",
    [
        lambda window: window,
        lambda window: Not(window),
        lambda window: InExpression(window, [Literal(1)]),
        lambda window: InExpression(Literal(1), [window]),
        lambda window: CaseWhen([(window, Literal(1))]),
        lambda window: CaseWhen([(Literal(True), window)]),
        lambda window: CaseWhen([(Literal(True), Literal(1))], window),
        lambda window: MultipleExpression([window]),
        lambda window: Like(window, Literal("a")),
        lambda window: Like(Literal("a"), window),
        lambda window: RegExp(window, Literal("a")),
        lambda window: RegExp(Literal("a"), window),
        lambda window: RegExp(Literal("a"), Literal("b"), window),
        lambda window: Collate(window, "en"),
        lambda window: SubfieldInt(window, 0),
        lambda window: SubfieldString(window, "field"),
        lambda window: WithinGroup(window, [Literal(1)]),
        lambda window: WithinGroup(Literal(1), [window]),
        lambda window: ListAgg(window, ",", False),
        lambda window: ColumnSum([window]),
        lambda window: FunctionExpression("abs", [window], False),
        lambda window: NamedFunctionExpression("test", {"argument": window}),
        lambda window: RankRelatedFunctionExpression(window, 1, Literal(0), False),
        lambda window: RankRelatedFunctionExpression(Literal(0), 1, window, False),
        lambda window: SpecifiedWindowFrame(RowFrame(), window, Literal(0)),
        lambda window: SpecifiedWindowFrame(RowFrame(), Literal(0), window),
        lambda window: WindowSpecDefinition(
            [window], [], SpecifiedWindowFrame(RowFrame(), Literal(0), Literal(0))
        ),
    ],
)
def test_window_predicate_detector_operands(wrap):
    window = row_number().over(Window.order_by("a"))._expression
    assert has_window_function_exp([wrap(window)])


def test_window_predicate_detector_scope():
    from unittest.mock import Mock

    window = row_number().over(Window.order_by("a"))._expression
    subquery = ScalarSubquery(Mock(projection=[window]))
    assert not has_window_function_exp(
        [None, subquery, sql_expr("row_number() over () = 1")._expression]
    )
    assert not has_window_function_exp(
        [FunctionExpression("seq4", [], False, is_data_generator=True)]
    )
    assert not has_window_function_exp([row_number()._expression])
    assert has_window_function_exp([InExpression(subquery, [window])])
    assert not has_window_function_exp([InExpression(Literal(1), [subquery])])
    pattern = Literal("a")
    assert expression_children(Like(window, pattern)) == [window, pattern]
    assert expression_children(subquery) is None


def test_filter_qualify_and_having_are_exclusive():
    from unittest.mock import Mock

    predicate = (row_number().over(Window.order_by("a")) == 1)._expression
    child = Mock()
    assert not Filter(predicate, child).is_qualify
    assert Filter(predicate, child, is_qualify=True).is_qualify
    with pytest.raises(ValueError, match="both HAVING and QUALIFY"):
        Filter(predicate, child, is_having=True, is_qualify=True)


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


def test_query():
    q = Query("select 1", query_id_place_holder="uuid")
    assert eval(repr(q)) == q

    q = Query("'select 1'", query_id_place_holder="'uuid'", is_ddl_on_temp_object=True)
    assert eval(repr(q)) == q
