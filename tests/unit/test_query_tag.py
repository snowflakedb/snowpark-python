#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.utils import (
    QUERY_TAG_STRING,
    create_or_update_statement_params_with_query_tag,
)


def test_create_or_update_statement_with_query_tag():
    statement_params = create_or_update_statement_params_with_query_tag(None, None)
    assert len(statement_params) == 0

    statement_params = create_or_update_statement_params_with_query_tag({}, None)
    assert len(statement_params) == 0

    statement_params = create_or_update_statement_params_with_query_tag(
        {"KEY": "VALUE"}, None
    )
    assert len(statement_params) == 1

    # session has an existing query tag
    fake_session_query_tag = "FAKE_SESSION_QUERY_TAG"
    statement_params = create_or_update_statement_params_with_query_tag(
        None, fake_session_query_tag
    )
    assert statement_params is None

    statement_params = create_or_update_statement_params_with_query_tag(
        {}, fake_session_query_tag
    )
    assert statement_params == {}

    statement_params = create_or_update_statement_params_with_query_tag(
        {"KEY": "VALUE"}, fake_session_query_tag
    )
    assert statement_params == {"KEY": "VALUE"}

    # query tag is passed
    input_statement_params = {QUERY_TAG_STRING: "FAKE_QUERY_TAG", "KEY": "VALUE"}
    statement_params = create_or_update_statement_params_with_query_tag(
        input_statement_params, None
    )
    assert statement_params == input_statement_params

    statement_params = create_or_update_statement_params_with_query_tag(
        input_statement_params, fake_session_query_tag
    )
    assert statement_params == input_statement_params
