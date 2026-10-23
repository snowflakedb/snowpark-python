#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
from unittest import mock

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark.session import Session

FLAGS = {
    "enable_eager_schema_validation": "_debug_eager_schema_validation",
    "enable_dataframe_trace_on_error": "_enable_dataframe_trace_on_error",
    "enable_trace_sql_errors_to_dataframe": "_enable_trace_sql_errors_to_dataframe",
    "enable_metadata_validation": "_enable_metadata_validation",
}


def make_session(cleaner_enabled=True):
    # Session.__new__ skips the connection; only the attributes used here are set.
    session = Session.__new__(Session)
    session._auto_clean_up_temp_table_enabled = cleaner_enabled
    session._auto_clean_up_temp_table_disabled_by_ast = False
    return session


@pytest.fixture
def sessions():
    """Active sessions seen by configure_development_features."""
    active = []
    with mock.patch(
        "snowflake.snowpark.session._get_active_sessions",
        side_effect=lambda **_: active,
    ):
        yield active


@pytest.fixture
def ast_state():
    """Fake AST state shared by the session and the context module."""
    state = {"enabled": False, "calls": []}

    def set_state(source, value):
        state["calls"].append((source, value))
        state["enabled"] = value

    with mock.patch(
        "snowflake.snowpark.session.set_ast_state", side_effect=set_state
    ), mock.patch(
        "snowflake.snowpark._internal.utils.set_ast_state", side_effect=set_state
    ), mock.patch(
        "snowflake.snowpark._internal.utils.is_ast_enabled",
        side_effect=lambda: state["enabled"],
    ):
        yield state


@pytest.fixture(autouse=True)
def reset_features(sessions, ast_state):
    context.configure_development_features()
    ast_state["calls"].clear()
    yield
    context.configure_development_features()


def current_flags():
    return {name: getattr(context, attr) for name, attr in FLAGS.items()}


def test_all_features_are_off_by_default():
    assert not any(current_flags().values())


@pytest.mark.parametrize("flag", FLAGS)
def test_only_passed_features_change(flag):
    context.configure_development_features(**{flag: True})
    expected = {name: name == flag for name in FLAGS}
    assert current_flags() == expected


def test_later_calls_keep_features_that_are_not_passed():
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    context.configure_development_features(enable_eager_schema_validation=True)
    context.configure_development_features(enable_metadata_validation=True)
    assert current_flags() == {
        "enable_eager_schema_validation": True,
        "enable_dataframe_trace_on_error": True,
        "enable_trace_sql_errors_to_dataframe": False,
        "enable_metadata_validation": True,
    }


def test_passing_false_turns_off_only_that_feature():
    context.configure_development_features(
        enable_eager_schema_validation=True,
        enable_dataframe_trace_on_error=True,
        enable_metadata_validation=True,
    )
    context.configure_development_features(enable_eager_schema_validation=False)
    assert current_flags() == {
        "enable_eager_schema_validation": False,
        "enable_dataframe_trace_on_error": True,
        "enable_trace_sql_errors_to_dataframe": False,
        "enable_metadata_validation": True,
    }


def test_call_without_arguments_turns_everything_off():
    context.configure_development_features(
        enable_eager_schema_validation=True,
        enable_dataframe_trace_on_error=True,
        enable_trace_sql_errors_to_dataframe=True,
        enable_metadata_validation=True,
    )
    context.configure_development_features()
    assert not any(current_flags().values())


def test_tracing_features_enable_ast_in_all_sessions_and_turning_off_leaves_it(
    sessions, ast_state
):
    first, second = make_session(), make_session(cleaner_enabled=False)
    sessions.extend([first, second])

    context.configure_development_features(enable_dataframe_trace_on_error=True)
    assert ast_state["enabled"] is True
    assert first._auto_clean_up_temp_table_enabled is False
    assert second._auto_clean_up_temp_table_enabled is False

    ast_state["calls"].clear()
    context.configure_development_features()
    assert not any(current_flags().values())
    assert ast_state["calls"] == []
    assert ast_state["enabled"] is True
    assert first._auto_clean_up_temp_table_enabled is False


def test_features_without_tracing_do_not_touch_ast_or_cleaner(sessions, ast_state):
    session = make_session()
    sessions.append(session)
    context.configure_development_features(
        enable_eager_schema_validation=True, enable_metadata_validation=True
    )
    context.configure_development_features()
    assert ast_state["calls"] == []
    assert session._auto_clean_up_temp_table_enabled is True


def test_disabling_ast_restores_the_cleaner_it_disabled(ast_state):
    session = make_session()

    session._set_ast_enabled_internal(True)
    assert ast_state["enabled"] is True
    assert session._auto_clean_up_temp_table_enabled is False

    session._set_ast_enabled_internal(False)
    assert ast_state["enabled"] is False
    assert session._auto_clean_up_temp_table_enabled is True
    assert session._auto_clean_up_temp_table_disabled_by_ast is False


def test_disabling_ast_keeps_a_cleaner_that_was_already_off(ast_state):
    session = make_session(cleaner_enabled=False)

    session._set_ast_enabled_internal(True)
    session._set_ast_enabled_internal(False)

    assert session._auto_clean_up_temp_table_enabled is False


def test_disabling_ast_without_enabling_it_leaves_the_cleaner_alone(ast_state):
    session = make_session()

    session._set_ast_enabled_internal(False)

    assert session._auto_clean_up_temp_table_enabled is True


def test_explicit_cleaner_choice_is_kept_when_ast_is_disabled(ast_state):
    session = make_session()
    session._conn = mock.MagicMock()
    session._session_id = 1
    session._lock = threading.RLock()

    session._set_ast_enabled_internal(True)
    session.auto_clean_up_temp_table_enabled = False  # explicit user choice
    session._set_ast_enabled_internal(False)

    assert session._auto_clean_up_temp_table_enabled is False
