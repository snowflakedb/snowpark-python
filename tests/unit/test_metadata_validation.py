#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.binary_expression import Add
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
    Literal,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.metadata_utils import (
    find_missing_column,
    validate_clause_columns,
    validate_projection_columns,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark.exceptions import SnowparkPlanException

KNOWN = {'"A"', '"B"'}


@pytest.mark.parametrize(
    "dependent, known, produced, expected",
    [
        ({'"A"'}, KNOWN, frozenset(), None),
        ({'"A"', '"C"'}, KNOWN, frozenset(), '"C"'),
        ({'"Z"', '"C"'}, KNOWN, frozenset(), '"C"'),  # first missing, sorted
        (set(), KNOWN, frozenset(), None),
        ({'"X"'}, KNOWN, {'"X"'}, None),  # produced by another column
        # no-op: names not stored or references not derivable
        ({'"C"'}, None, frozenset(), None),
        (COLUMN_DEPENDENCY_ALL, KNOWN, frozenset(), None),
        (COLUMN_DEPENDENCY_DOLLAR, KNOWN, frozenset(), None),
        # no-op: qualified, path-like and positional names
        ({"t.c"}, KNOWN, frozenset(), None),
        ({'"A"', "$1"}, KNOWN, frozenset(), None),
        ({"*"}, KNOWN, frozenset(), None),
    ],
)
def test_find_missing_column(dependent, known, produced, expected):
    assert find_missing_column(dependent, known, produced) == expected


def attr(name):
    return UnresolvedAttribute(name)


def test_projection_missing_column_raises_with_hint():
    with pytest.raises(SnowparkPlanException) as ex_info:
        validate_projection_columns(
            [attr('"A"'), attr('"C"')],
            KNOWN,
            mock.Mock(),
            defaultdict(dict),
            ['"A"', '"B"'],
        )
    assert ex_info.value.error_code == "1200"
    assert 'Invalid identifier "C"' in str(ex_info.value)
    assert "existing quoted column identifiers: ['\"A\"', '\"B\"']" in str(
        ex_info.value
    )


def test_projection_valid_columns_do_not_raise():
    validate_projection_columns(
        [attr('"A"'), Alias(Add(attr('"B"'), Literal(1)), '"N"')],
        KNOWN,
        mock.Mock(),
        defaultdict(dict),
    )


def test_projection_lateral_alias_is_allowed():
    cols = [
        Alias(Add(attr('"A"'), Literal(1)), '"X"'),
        Alias(Add(attr('"X"'), Literal(1)), '"Y"'),
    ]
    validate_projection_columns(cols, KNOWN, mock.Mock(), defaultdict(dict))


def test_projection_column_does_not_satisfy_itself():
    # a plain missing column must not count as produced by itself
    with pytest.raises(SnowparkPlanException):
        validate_projection_columns(
            [attr('"C"')], KNOWN, mock.Mock(), defaultdict(dict)
        )
    with pytest.raises(SnowparkPlanException):
        validate_projection_columns(
            [Alias(Add(attr('"C"'), Literal(1)), '"C"')],
            KNOWN,
            mock.Mock(),
            defaultdict(dict),
        )


def test_projection_plain_missing_column_is_not_a_producer():
    # the second column is a plain reference to a missing name and cannot make the first valid
    with pytest.raises(SnowparkPlanException):
        validate_projection_columns(
            [Alias(attr('"C"'), '"D"'), attr('"C"')],
            KNOWN,
            mock.Mock(),
            defaultdict(dict),
        )


def test_projection_noop_when_names_unavailable_or_aliased():
    validate_projection_columns([attr('"C"')], None, mock.Mock(), defaultdict(dict))
    aliased = defaultdict(dict, {"df1": {"a": '"A"'}})
    validate_projection_columns([attr('"C"')], KNOWN, mock.Mock(), aliased)


def test_clause_missing_column_raises():
    with pytest.raises(SnowparkPlanException) as ex_info:
        validate_clause_columns(
            [Add(attr('"A"'), attr('"C"'))], KNOWN, defaultdict(dict)
        )
    assert ex_info.value.error_code == "1200"
    assert 'Invalid identifier "C"' in str(ex_info.value)


def test_clause_valid_or_uncertain_does_not_raise():
    validate_clause_columns([attr('"A"')], KNOWN, defaultdict(dict))
    validate_clause_columns([attr('"C"')], None, defaultdict(dict))
    validate_clause_columns(
        [attr('"C"')], KNOWN, defaultdict(dict, {"df1": {"a": '"A"'}})
    )
    validate_clause_columns(
        [UnresolvedAttribute("a + 1", is_sql_text=True)], KNOWN, defaultdict(dict)
    )
