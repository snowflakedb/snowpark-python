#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark._internal.utils import (
    QUERY_TAG_STRING,
    create_or_update_statement_params_with_query_tag,
    create_statement_query_tag,
)


def test_generate_query_tag():
    query_tag = create_statement_query_tag()
    assert "test_query_tag" in query_tag  # file name in query tag
    assert "test_generate_query_tag" in query_tag  # calling function name in query tag


def test_generate_query_tag_skip_1_level():
    query_tag = create_statement_query_tag(skip_levels=1)
    assert "test_query_tag" in query_tag  # file name in query tag
    assert (
        "test_generate_query_tag_skip_1_level" in query_tag
    )  # calling function name in query tag


def test_generate_query_tag_skip_2_levels():
    query_tag = create_statement_query_tag(skip_levels=2)
    assert "test_query_tag" not in query_tag  # skipped last call stack
    assert (
        "test_generate_query_tag_skip_2_level" not in query_tag
    )  # skipped last call stack


@pytest.mark.parametrize("collect_stacktrace", [True, False])
def test_create_or_update_statement_with_query_tag(collect_stacktrace):
    statement_params = create_or_update_statement_params_with_query_tag(
        None, None, collect_stacktrace=collect_stacktrace
    )
    if collect_stacktrace:
        assert len(statement_params) == 1
        assert (
            "test_query_tag" in statement_params[QUERY_TAG_STRING]
        )  # file name in query tag
        assert (
            "test_create_or_update_statement_with_query_tag"
            in statement_params[QUERY_TAG_STRING]
        )  # calling function name in query tag
    else:
        assert len(statement_params or {}) == 0

    statement_params = create_or_update_statement_params_with_query_tag(
        {}, None, collect_stacktrace=collect_stacktrace
    )
    if collect_stacktrace:
        assert len(statement_params) == 1
        assert (
            "test_query_tag" in statement_params[QUERY_TAG_STRING]
        )  # file name in query tag
        assert (
            "test_create_or_update_statement_with_query_tag"
            in statement_params[QUERY_TAG_STRING]
        )  # calling function name in query tag
    else:
        assert len(statement_params or {}) == 0

    statement_params = create_or_update_statement_params_with_query_tag(
        {"KEY": "VALUE"}, None, collect_stacktrace=collect_stacktrace
    )
    if collect_stacktrace:
        assert len(statement_params) == 2
        assert statement_params["KEY"] == "VALUE"
        assert (
            "test_query_tag" in statement_params[QUERY_TAG_STRING]
        )  # file name in query tag
        assert (
            "test_create_or_update_statement_with_query_tag"
            in statement_params[QUERY_TAG_STRING]
        )  # calling function name in query tag
    else:
        assert len(statement_params or {}) == 1

    # session has an existing query tag
    fake_session_query_tag = "FAKE_SESSION_QUERY_TAG"
    statement_params = create_or_update_statement_params_with_query_tag(
        None, fake_session_query_tag, collect_stacktrace=collect_stacktrace
    )
    assert statement_params is None

    statement_params = create_or_update_statement_params_with_query_tag(
        {}, fake_session_query_tag, collect_stacktrace=collect_stacktrace
    )
    assert statement_params == {}

    statement_params = create_or_update_statement_params_with_query_tag(
        {"KEY": "VALUE"}, fake_session_query_tag, collect_stacktrace=collect_stacktrace
    )
    assert statement_params == {"KEY": "VALUE"}

    # query tag is passed
    input_statement_params = {QUERY_TAG_STRING: "FAKE_QUERY_TAG", "KEY": "VALUE"}
    statement_params = create_or_update_statement_params_with_query_tag(
        input_statement_params, None, collect_stacktrace=collect_stacktrace
    )
    assert statement_params == input_statement_params

    statement_params = create_or_update_statement_params_with_query_tag(
        input_statement_params,
        fake_session_query_tag,
        collect_stacktrace=collect_stacktrace,
    )
    assert statement_params == input_statement_params

    # test passing skip levels
    statement_params = create_or_update_statement_params_with_query_tag(
        None, None, skip_levels=1, collect_stacktrace=collect_stacktrace
    )
    if collect_stacktrace:
        assert statement_params[QUERY_TAG_STRING]
        assert (
            "test_query_tag" in statement_params[QUERY_TAG_STRING]
        )  # file name in query tag
        assert (
            "test_create_or_update_statement_with_query_tag"
            in statement_params[QUERY_TAG_STRING]
        )  # calling function name in query tag
    else:
        assert len(statement_params or {}) == 0

    statement_params = create_or_update_statement_params_with_query_tag(
        None, None, skip_levels=2, collect_stacktrace=collect_stacktrace
    )
    if collect_stacktrace:
        assert statement_params[QUERY_TAG_STRING]
        assert (
            "test_query_tag" not in statement_params[QUERY_TAG_STRING]
        )  # skipped last call stack
        assert (
            "test_create_or_update_statement_with_query_tag"
            not in statement_params[QUERY_TAG_STRING]
        )  # skipped last call stack
    else:
        assert len(statement_params or {}) == 0
