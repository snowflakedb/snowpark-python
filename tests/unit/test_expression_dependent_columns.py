#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
    assert a.dependent_column_names_with_duplication() == []
    b = Expression(child=UnresolvedAttribute("a"))
    assert b.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY
    assert b.dependent_column_names_with_duplication() == []
    # root class Expression always returns empty dependency


def test_literal():
    a = Literal(5)
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY
    assert a.dependent_column_names_with_duplication() == []


def test_attribute():
    a = Attribute("A", IntegerType())
    assert a.dependent_column_names() == {"A"}
    assert a.dependent_column_names_with_duplication() == ["A"]


def test_unresolved_attribute():
    a = UnresolvedAttribute("A")
    assert a.dependent_column_names() == {"A"}
    assert a.dependent_column_names_with_duplication() == ["A"]

    b = UnresolvedAttribute("a > 1", is_sql_text=True)
    assert b.dependent_column_names() == COLUMN_DEPENDENCY_ALL
    assert b.dependent_column_names_with_duplication() == []

    c = UnresolvedAttribute("$1 > 1", is_sql_text=True)
    assert c.dependent_column_names() == COLUMN_DEPENDENCY_DOLLAR
    assert c.dependent_column_names_with_duplication() == ["$"]


def test_case_when():
    a = Column("a")
    b = Column("b")
    z = when(a > b, col("c")).when(a < b, col("d")).else_(col("e"))
    assert z._expression.dependent_column_names() == {'"A"', '"B"', '"C"', '"D"', '"E"'}
    # verify column '"A"', '"B"' occurred twice in the dependency columns
    assert z._expression.dependent_column_names_with_duplication() == [
        '"A"',
        '"B"',
        '"C"',
        '"A"',
        '"B"',
        '"D"',
        '"E"',
    ]


def test_collate():
    a = Collate(UnresolvedAttribute("a"), "spec")
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


def test_function_expression():
    a = FunctionExpression("test_func", [UnresolvedAttribute(x) for x in "abcd"], False)
    assert a.dependent_column_names() == set("abcd")
    assert a.dependent_column_names_with_duplication() == list("abcd")

    # expressions with duplicated dependent column
    b = FunctionExpression(
        "test_func", [UnresolvedAttribute(x) for x in "abcdad"], False
    )
    assert b.dependent_column_names() == set("abcd")
    assert b.dependent_column_names_with_duplication() == list("abcdad")


def test_in_expression():
    a = InExpression(UnresolvedAttribute("e"), [UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcde")
    assert a.dependent_column_names_with_duplication() == list("eabcd")


def test_like():
    a = Like(UnresolvedAttribute("a"), UnresolvedAttribute("b"))
    assert a.dependent_column_names() == {"a", "b"}
    assert a.dependent_column_names_with_duplication() == ["a", "b"]

    # with duplication
    b = Like(UnresolvedAttribute("a"), UnresolvedAttribute("a"))
    assert b.dependent_column_names() == {"a"}
    assert b.dependent_column_names_with_duplication() == ["a", "a"]


def test_list_agg():
    a = ListAgg(UnresolvedAttribute("a"), ",", True)
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


def test_multiple_expression():
    a = MultipleExpression([UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcd")
    assert a.dependent_column_names_with_duplication() == list("abcd")

    # with duplication
    a = MultipleExpression([UnresolvedAttribute(x) for x in "abcdbea"])
    assert a.dependent_column_names() == set("abcde")
    assert a.dependent_column_names_with_duplication() == list("abcdbea")


def test_reg_exp():
    a = RegExp(UnresolvedAttribute("a"), UnresolvedAttribute("b"))
    assert a.dependent_column_names() == {"a", "b"}
    assert a.dependent_column_names_with_duplication() == ["a", "b"]

    b = RegExp(UnresolvedAttribute("a"), UnresolvedAttribute("a"))
    assert b.dependent_column_names() == {"a"}
    assert b.dependent_column_names_with_duplication() == ["a", "a"]


def test_scalar_subquery():
    a = ScalarSubquery(None)
    assert a.dependent_column_names() == COLUMN_DEPENDENCY_DOLLAR
    assert a.dependent_column_names_with_duplication() == list(COLUMN_DEPENDENCY_DOLLAR)


def test_snowflake_udf():
    a = SnowflakeUDF(
        "udf_name", [UnresolvedAttribute(x) for x in "abcd"], IntegerType()
    )
    assert a.dependent_column_names() == set("abcd")
    assert a.dependent_column_names_with_duplication() == list("abcd")

    # with duplication
    b = SnowflakeUDF(
        "udf_name", [UnresolvedAttribute(x) for x in "abcdfc"], IntegerType()
    )
    assert b.dependent_column_names() == set("abcdf")
    assert b.dependent_column_names_with_duplication() == list("abcdfc")


def test_star():
    a = Star([Attribute(x, IntegerType()) for x in "abcd"])
    assert a.dependent_column_names() == set("abcd")
    assert a.dependent_column_names_with_duplication() == list("abcd")

    b = Star([])
    assert b.dependent_column_names() == COLUMN_DEPENDENCY_ALL
    assert b.dependent_column_names_with_duplication() == []


def test_subfield_string():
    a = SubfieldString(UnresolvedAttribute("a"), "field")
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


def test_within_group():
    a = WithinGroup(UnresolvedAttribute("e"), [UnresolvedAttribute(x) for x in "abcd"])
    assert a.dependent_column_names() == set("abcde")
    assert a.dependent_column_names_with_duplication() == list("eabcd")

    b = WithinGroup(
        UnresolvedAttribute("e"), [UnresolvedAttribute(x) for x in "abcdea"]
    )
    assert b.dependent_column_names() == set("abcde")
    assert b.dependent_column_names_with_duplication() == list("eabcdea")


@pytest.mark.parametrize(
    "expression_class",
    [UnaryExpression, IsNaN, IsNotNull, IsNull, Not, UnaryMinus, UnresolvedAlias],
)
def test_unary_expression(expression_class):
    a = expression_class(child=UnresolvedAttribute("a"))
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


def test_alias():
    a = Alias(child=Add(UnresolvedAttribute("a"), UnresolvedAttribute("b")), name="c")
    assert a.dependent_column_names() == {"a", "b"}
    assert a.dependent_column_names_with_duplication() == ["a", "b"]


def test_cast():
    a = Cast(UnresolvedAttribute("a"), IntegerType())
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


@pytest.mark.parametrize(
    "expression_class",
    [
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

    assert a.dependent_column_names_with_duplication() == ["A"]
    assert b.dependent_column_names_with_duplication() == ["B"]
    assert binary_expression.dependent_column_names_with_duplication() == ["A", "B"]

    # hierarchical expressions with duplication
    hierarchical_binary_expression = expression_class(expression_class(a, b), b)
    assert hierarchical_binary_expression.dependent_column_names() == {"A", "B"}
    assert hierarchical_binary_expression.dependent_column_names_with_duplication() == [
        "A",
        "B",
        "B",
    ]


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
    assert a.dependent_column_names_with_duplication() == ["a", "b", "c", "d"]

    # with duplication
    b = expression_class(
        [
            UnresolvedAttribute("a"),
            UnresolvedAttribute("a"),
            UnresolvedAttribute("c"),
        ]
    )
    assert b.dependent_column_names() == {"a", "c"}
    assert b.dependent_column_names_with_duplication() == ["a", "a", "c"]


def test_grouping_sets_expression():
    a = GroupingSetsExpression(
        [
            [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
            [UnresolvedAttribute("c"), UnresolvedAttribute("d")],
        ]
    )
    assert a.dependent_column_names() == {"a", "b", "c", "d"}
    assert a.dependent_column_names_with_duplication() == ["a", "b", "c", "d"]


def test_sort_order():
    a = SortOrder(UnresolvedAttribute("a"), Ascending())
    assert a.dependent_column_names() == {"a"}
    assert a.dependent_column_names_with_duplication() == ["a"]


def test_specified_window_frame():
    a = SpecifiedWindowFrame(
        RowFrame(), UnresolvedAttribute("a"), UnresolvedAttribute("b")
    )
    assert a.dependent_column_names() == {"a", "b"}
    assert a.dependent_column_names_with_duplication() == ["a", "b"]

    # with duplication
    b = SpecifiedWindowFrame(
        RowFrame(), UnresolvedAttribute("a"), UnresolvedAttribute("a")
    )
    assert b.dependent_column_names() == {"a"}
    assert b.dependent_column_names_with_duplication() == ["a", "a"]


@pytest.mark.parametrize("expression_class", [RankRelatedFunctionExpression, Lag, Lead])
def test_rank_related_function_expression(expression_class):
    a = expression_class(UnresolvedAttribute("a"), 1, UnresolvedAttribute("b"), False)
    assert a.dependent_column_names() == {"a", "b"}
    assert a.dependent_column_names_with_duplication() == ["a", "b"]


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
    assert a.dependent_column_names_with_duplication() == list("abcdef")


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
    assert a.dependent_column_names() == set("abcdefx")
    assert a.dependent_column_names_with_duplication() == list("xabcdef")


def test_window_expression_with_duplication_columns():
    window_spec_definition = WindowSpecDefinition(
        [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
        [
            SortOrder(UnresolvedAttribute("c"), Ascending()),
            SortOrder(UnresolvedAttribute("a"), Ascending()),
        ],
        SpecifiedWindowFrame(
            RowFrame(), UnresolvedAttribute("e"), UnresolvedAttribute("f")
        ),
    )
    a = WindowExpression(UnresolvedAttribute("e"), window_spec_definition)
    assert a.dependent_column_names() == set("abcef")
    assert a.dependent_column_names_with_duplication() == list("eabcaef")


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
    assert a.dependent_column_names_with_duplication() == []
