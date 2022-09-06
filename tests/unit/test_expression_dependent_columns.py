#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.analyzer.binary_expression import Add
from snowflake.snowpark._internal.analyzer.expression import UnresolvedAttribute
from snowflake.snowpark._internal.analyzer.grouping_set import GroupingSetsExpression
from snowflake.snowpark._internal.analyzer.unary_expression import Alias


def test_add_column_dependency():
    a = UnresolvedAttribute("A", False)
    b = UnresolvedAttribute("B", False)
    add = Add(a, b)
    alias = Alias(add, "C")
    assert a.dependent_column_names() == {"A"}
    assert b.dependent_column_names() == {"B"}
    assert add.dependent_column_names() == {"A", "B"}
    assert alias.dependent_column_names() == {"A", "B"}


def test_grouping_set_dependency():
    exp = GroupingSetsExpression(
        [
            [UnresolvedAttribute("a"), UnresolvedAttribute("b")],
            [UnresolvedAttribute("c"), UnresolvedAttribute("d")],
        ]
    )
    assert sorted(exp.dependent_column_names()) == ["a", "b", "c", "d"]
