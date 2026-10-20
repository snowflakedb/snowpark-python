#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark._internal.utils import AstFlagSource
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
    session._auto_clean_up_temp_table_disabled_by_dev_features = False
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


def test_turning_features_off_restores_cleaner_and_ast(sessions, ast_state):
    session = make_session(cleaner_enabled=True)
    sessions.append(session)

    context.configure_development_features(enable_dataframe_trace_on_error=True)
    assert ast_state["enabled"] is True
    assert session._auto_clean_up_temp_table_enabled is False

    context.configure_development_features()
    assert ast_state["enabled"] is False
    assert ast_state["calls"][-1] == (AstFlagSource.USER, False)
    assert session._auto_clean_up_temp_table_enabled is True


def test_ast_and_cleaner_stay_on_while_one_tracing_feature_is_left(sessions, ast_state):
    session = make_session()
    sessions.append(session)
    context.configure_development_features(
        enable_dataframe_trace_on_error=True, enable_trace_sql_errors_to_dataframe=True
    )

    context.configure_development_features(enable_dataframe_trace_on_error=False)
    assert ast_state["enabled"] is True
    assert session._auto_clean_up_temp_table_enabled is False

    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    assert ast_state["enabled"] is False
    assert session._auto_clean_up_temp_table_enabled is True


def test_features_without_tracing_do_not_touch_ast_or_cleaner(sessions, ast_state):
    session = make_session()
    sessions.append(session)
    context.configure_development_features(
        enable_eager_schema_validation=True, enable_metadata_validation=True
    )
    context.configure_development_features()
    assert ast_state["calls"] == []
    assert session._auto_clean_up_temp_table_enabled is True


def test_ast_enabled_before_the_features_is_not_turned_off(sessions, ast_state):
    ast_state["enabled"] = True
    session = make_session()
    sessions.append(session)

    context.configure_development_features(enable_dataframe_trace_on_error=True)
    context.configure_development_features()

    assert ast_state["enabled"] is True
    assert (AstFlagSource.USER, False) not in ast_state["calls"]
    # the cleaner was still switched off by the features, so it is restored
    assert session._auto_clean_up_temp_table_enabled is True


def test_cleaner_disabled_by_the_user_is_not_enabled_again(sessions):
    session = make_session(cleaner_enabled=False)
    sessions.append(session)

    context.configure_development_features(enable_dataframe_trace_on_error=True)
    context.configure_development_features()

    assert session._auto_clean_up_temp_table_enabled is False


def test_enabling_twice_still_restores_the_cleaner(sessions):
    session = make_session()
    sessions.append(session)

    context.configure_development_features(enable_dataframe_trace_on_error=True)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    context.configure_development_features()

    assert session._auto_clean_up_temp_table_enabled is True


def test_session_created_while_features_are_on_is_restored(sessions, ast_state):
    # no session exists yet, so nothing is enabled when the features are configured
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    assert ast_state["enabled"] is False

    # a session created afterwards enables AST for the features itself
    late_session = make_session()
    late_session._enable_ast_for_development_features()
    sessions.append(late_session)
    assert ast_state["enabled"] is True
    assert late_session._auto_clean_up_temp_table_enabled is False

    context.configure_development_features()

    assert late_session._auto_clean_up_temp_table_enabled is True
    assert ast_state["enabled"] is False


def test_all_active_sessions_are_restored(sessions):
    first, second = make_session(), make_session(cleaner_enabled=False)
    sessions.extend([first, second])

    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    context.configure_development_features()

    assert first._auto_clean_up_temp_table_enabled is True
    assert second._auto_clean_up_temp_table_enabled is False
