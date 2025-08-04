#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.context as context
from unittest import mock


def test_get_active_session(session):
    assert session == get_active_session()


def test_context_configure_development_features(session):
    original_ast_enabled = session.ast_enabled
    try:
        # Test when get_active_session() returns None
        with mock.patch.object(context, "get_active_session", return_value=None):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is False
            assert context._enable_dataframe_trace_on_error is False

        # Test when get_active_session() throws an exception
        with mock.patch.object(
            context, "get_active_session", side_effect=RuntimeError("test")
        ):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is False
            assert context._enable_dataframe_trace_on_error is False

        # Test when get_active_session() returns a valid session
        with mock.patch.object(
            context, "get_active_session", return_value=mock.MagicMock()
        ):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is True
            assert context._enable_dataframe_trace_on_error is False
            assert session.ast_enabled is True
    finally:
        session.ast_enabled = original_ast_enabled
