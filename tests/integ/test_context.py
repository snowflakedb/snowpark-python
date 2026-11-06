#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.context as context
import snowflake.snowpark.session as session
from snowflake.snowpark._internal import debug_config
from unittest import mock


def test_get_active_session(session):
    assert session == get_active_session()


def test_context_configure_development_features():
    try:
        # Test when _get_active_sessions() returns None
        with mock.patch.object(session, "_get_active_sessions", return_value=set()):
            context.configure_development_features(
                enable_trace_sql_errors_to_dataframe=True
            )
            assert context._enable_trace_sql_errors_to_dataframe is True
            assert context._enable_dataframe_trace_on_error is False
            assert not debug_config._defaults.get("eager_schema_validation", False)

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
            mock_session1._set_ast_enabled_internal.assert_not_called()
            mock_session2._set_ast_enabled_internal.assert_not_called()

    finally:
        # a call without arguments turns every development feature off
        context.configure_development_features()
        assert context._enable_trace_sql_errors_to_dataframe is False


def test_development_features_leave_session_settings_alone(session):
    original_ast = session.ast_enabled
    original_cleaner = session._auto_clean_up_temp_table_enabled
    try:
        context.configure_development_features(enable_dataframe_trace_on_error=True)
        context.configure_development_features(enable_eager_schema_validation=True)
        # a call that only passes other features keeps the tracing feature on
        assert context._enable_dataframe_trace_on_error is True
        # the tracing features neither enable the AST nor change the temp table clean-up
        assert session.ast_enabled == original_ast
        assert session._auto_clean_up_temp_table_enabled == original_cleaner
    finally:
        context.configure_development_features()
    assert session.ast_enabled == original_ast
    assert session._auto_clean_up_temp_table_enabled == original_cleaner
