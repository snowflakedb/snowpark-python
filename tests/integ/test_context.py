#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.context as context
import snowflake.snowpark.session as session
from unittest import mock


def test_get_active_session(session):
    assert session == get_active_session()


def test_context_configure_development_features():
    try:
        # Test when _get_active_sessions() returns None
        with mock.patch.object(session, "_get_active_sessions", return_value=None):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is True
            assert context._enable_dataframe_trace_on_error is False
            assert context._debug_eager_schema_validation is False

        # Test when _get_active_sessions() returns a valid session
        mock_session1 = mock.MagicMock()
        mock_session1._set_ast_enabled_internal = mock.MagicMock()
        mock_session2 = mock.MagicMock()
        mock_session2._set_ast_enabled_internal = mock.MagicMock()

        with mock.patch.object(
            session, "_get_active_sessions", return_value=[mock_session1, mock_session2]
        ):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is True
            assert context._enable_dataframe_trace_on_error is False
            mock_session1._set_ast_enabled_internal.assert_called_once_with(True)
            mock_session2._set_ast_enabled_internal.assert_called_once_with(True)

        new_session = session.Session.builder.create()
        assert new_session.ast_enabled is True
        new_session.close()
    finally:
        context.configure_development_features(
            enable_trace_sql_errors_to_dataframe=False
        )
