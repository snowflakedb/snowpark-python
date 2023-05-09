#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.binary_expression import (
    Add,
    And,
    BinaryArithmeticExpression,
    BinaryExpression,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Divide,
    EqualNullSafe,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Multiply,
    NotEqualTo,
    Or,
    Pow,
    Remainder,
    Subtract,
)
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
    COLUMN_DEPENDENCY_EMPTY,
    Attribute,
    Collate,
    Expression,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    RegExp,
    ScalarSubquery,
    Seq,
    SnowflakeUDF,
    Star,
    SubfieldString,
    UnresolvedAttribute,
    WithinGroup,
)
from snowflake.snowpark._internal.analyzer.grouping_set import (
    Cube,
    GroupingSet,
    GroupingSetsExpression,
    Rollup,
)
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, SortOrder
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnaryExpression,
    UnaryMinus,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.window_expression import (
    CurrentRow,
    Lag,
    Lead,
    RankRelatedFunctionExpression,
    RowFrame,
    SpecialFrameBoundary,
    SpecifiedWindowFrame,
    UnboundedFollowing,
    UnboundedPreceding,
    WindowExpression,
    WindowFrame,
    WindowSpecDefinition,
)
from snowflake.snowpark.functions import col, when
from snowflake.snowpark.types import IntegerType


def test_expression():
    a = Expression()
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY
    b = Expression(child=UnresolvedAttribute("a"))
    assert b.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY
    # root class Expression always returns empty dependency


def test_literal():
    a = Literal(5)
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY


def test_attribute():
    a = Attribute("A", IntegerType())
    assert a.dependent_column_names() == {"A"}


def test_unresolved_attribute():
    a = UnresolvedAttribute("A")
    assert a.dependent_column_names() == {"A"}

    b = UnresolvedAttribute("a > 1", is_sql_text=True)
    assert b.dependent_column_names() == COLUMN_DEPENDENCY_ALL

    c = UnresolvedAttribute("$1 > 1", is_sql_text=True)
    assert c.dependent_column_names() == COLUMN_DEPENDENCY_DOLLAR


def test_case_when():
    a = Column("a")
    b = Column("b")
    z = when(a > b, col("c")).when(a < b, col("d")).else_(col("e"))
    assert z._expression.dependent_column_names() == {'"A"', '"B"', '"C"', '"D"', '"E"'}


def test_collate():
    a = Collate(UnresolvedAttribute("a"), "spec")
    assert a.dependent_column_names() == {"a"}


def test_function_expression():
    a = FunctionExpression("test_func", [UnresolvedAttribute(x) for x in "abcd"], False)
    assert a.dependent_column_names() == set("abcd")


def test_in_expression():
    a = InExpression(UnresolvedAttribute("e"), [UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcde")


def test_like():
    a = Like(UnresolvedAttribute("a"), UnresolvedAttribute("b"))
    assert a.dependent_column_names() == {"a", "b"}


def test_list_agg():
    a = ListAgg(UnresolvedAttribute("a"), ",", True)
    assert a.dependent_column_names() == {"a"}


def test_multiple_expression():
    a = MultipleExpression([UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcd")


def test_reg_exp():
    a = RegExp(UnresolvedAttribute("a"), UnresolvedAttribute("b"))
    assert a.dependent_column_names() == {"a", "b"}


def test_scalar_subquery():
    a = ScalarSubquery(None)
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_DOLLAR


def test_snowflake_udf():
    a = SnowflakeUDF(
        "udf_name", [UnresolvedAttribute(x) for x in "abcd"], IntegerType()
    )
    assert a.dependent_column_names() == set("abcd")


def test_star():
    a = Star([Attribute(x, IntegerType()) for x in "abcd"])
    assert a.dependent_column_names() == set("abcd")


def test_subfield_string():
    a = SubfieldString(UnresolvedAttribute("a"), "field")
    assert a.dependent_column_names() == {"a"}


def test_within_group():
    a = WithinGroup(UnresolvedAttribute("e"), [UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcde")


@pytest.mark.parametrize(
    "expression_class",
    [UnaryExpression, IsNaN, IsNotNull, IsNull, Not, UnaryMinus, UnresolvedAlias],
)
def test_unary_expression(expression_class):
    a = expression_class(child=UnresolvedAttribute("a"))
    assert a.dependent_column_names() == {"a"}


def test_alias():
    a = Alias(child=Add(UnresolvedAttribute("a"), UnresolvedAttribute("b")), name="c")
    assert a.dependent_column_names() == {"a", "b"}


def test_cast():
    a = Cast(UnresolvedAttribute("a"), IntegerType())
    assert a.dependent_column_names() == {"a"}


@pytest.mark.parametrize(
    "expression_class",
    [
        BinaryExpression,
        Add,
        And,
        BinaryArithmeticExpression,
        BinaryExpression,
        BitwiseAnd,
        BitwiseOr,
        BitwiseXor,
        Divide,
        EqualNullSafe,
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Multiply,
        NotEqualTo,
        Or,
        Pow,
        Remainder,
        Subtract,
    ],
)
def test_binary_expression(expression_class):
    a = UnresolvedAttribute("A", False)
    b = UnresolvedAttribute("B", False)
    binary_expression = expression_class(a, b)
    assert a.dependent_column_names() == {"A"}
    assert b.dependent_column_names() == {"B"}
    assert binary_expression.dependent_column_names() == {"A", "B"}


@pytest.mark.parametrize(
    "expression_class",
    [
        Cube,
        GroupingSet,
        Rollup,
    ],
)
def test_grouping_set(expression_class):
    a = expression_class(
        [
            UnresolvedAttribute("a"),
            UnresolvedAttribute("b"),
            UnresolvedAttribute("c"),
            UnresolvedAttribute("d"),
        ]
    )
    assert a.dependent_column_names() == {"a", "b", "c", "d"}


def test_grouping_sets_expression():
    a = GroupingSetsExpression(
        [
            [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
            [UnresolvedAttribute("c"), UnresolvedAttribute("d")],
        ]
    )
    assert a.dependent_column_names() == {"a", "b", "c", "d"}


def test_sort_order():
    a = SortOrder(UnresolvedAttribute("a"), Ascending())
    assert a.dependent_column_names() == {"a"}


def test_specified_window_frame():
    a = SpecifiedWindowFrame(
        RowFrame(), UnresolvedAttribute("a"), UnresolvedAttribute("b")
    )
    assert a.dependent_column_names() == {"a", "b"}


@pytest.mark.parametrize("expression_class", [RankRelatedFunctionExpression, Lag, Lead])
def test_rank_related_function_expression(expression_class):
    a = expression_class(UnresolvedAttribute("a"), 1, UnresolvedAttribute("b"), False)
    assert a.dependent_column_names() == {"a", "b"}


def test_window_spec_definition():
    a = WindowSpecDefinition(
        [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
        [
            SortOrder(UnresolvedAttribute("c"), Ascending()),
            SortOrder(UnresolvedAttribute("d"), Ascending()),
        ],
        SpecifiedWindowFrame(
            RowFrame(), UnresolvedAttribute("e"), UnresolvedAttribute("f")
        ),
    )
    assert a.dependent_column_names() == set("abcdef")


def test_window_expression():
    window_spec_definition = WindowSpecDefinition(
        [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
        [
            SortOrder(UnresolvedAttribute("c"), Ascending()),
            SortOrder(UnresolvedAttribute("d"), Ascending()),
        ],
        SpecifiedWindowFrame(
            RowFrame(), UnresolvedAttribute("e"), UnresolvedAttribute("f")
        ),
    )
    a = WindowExpression(UnresolvedAttribute("x"), window_spec_definition)
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_ALL


@pytest.mark.parametrize(
    "expression_class",
    [
        SpecialFrameBoundary,
        UnboundedPreceding,
        UnboundedFollowing,
        CurrentRow,
        WindowFrame,
    ],
)
def test_other_window_expressions(expression_class):
    a = expression_class()
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY


def test_seq_expression():
    assert Seq(1, 0).dependent_column_names() == COLUMN_DEPENDENCY_ALL
