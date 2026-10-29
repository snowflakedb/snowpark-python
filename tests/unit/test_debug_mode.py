#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark.debug_mode import (
    EAGER_SCHEMA_VALIDATION_ENV_VAR,
    DebugMode,
    EagerSchemaValidation,
)


def make_session():
    session = SimpleNamespace(_lock=threading.RLock())
    session.eager_schema_validation = EagerSchemaValidation(session)
    session.debug = DebugMode(session)
    return session


@pytest.fixture(autouse=True)
def reset_global_settings(monkeypatch):
    monkeypatch.delenv(EAGER_SCHEMA_VALIDATION_ENV_VAR, raising=False)
    with mock.patch("snowflake.snowpark.session._get_active_sessions", return_value=[]):
        context.configure_development_features()
        yield
        context.configure_development_features()


def test_disabled_by_default():
    assert make_session().eager_schema_validation.enabled is False


def test_enable_and_disable():
    eager = make_session().eager_schema_validation
    eager.enable()
    assert eager.enabled is True
    eager.disable()
    assert eager.enabled is False


def test_context_manager_restores_the_previous_setting_including_nested_and_errors():
    eager = make_session().eager_schema_validation
    eager.enable()
    with eager:
        eager.disable()
        with eager:
            assert eager.enabled is True
        assert eager.enabled is False
    assert eager.enabled is True

    eager.disable()
    with pytest.raises(RuntimeError):
        with eager:
            assert eager.enabled is True
            raise RuntimeError
    assert eager.enabled is False


def test_global_setting_is_the_fallback_and_explicit_setting_wins():
    eager = make_session().eager_schema_validation
    context.configure_development_features(enable_eager_schema_validation=True)
    assert eager.enabled is True
    eager.disable()
    assert eager.enabled is False
    context.configure_development_features()
    eager.enable()
    assert eager.enabled is True


def test_sessions_are_independent():
    first, second = make_session(), make_session()
    first.eager_schema_validation.enable()
    assert second.eager_schema_validation.enabled is False


@pytest.mark.parametrize("value", ["true", "True", "TRUE", " true "])
def test_env_var_enables_it_with_a_warning(monkeypatch, caplog, value):
    monkeypatch.setenv(EAGER_SCHEMA_VALIDATION_ENV_VAR, value)
    with caplog.at_level(logging.WARNING, logger="snowflake.snowpark.debug_mode"):
        session = make_session()
    assert session.eager_schema_validation.enabled is True
    assert EAGER_SCHEMA_VALIDATION_ENV_VAR in caplog.text


@pytest.mark.parametrize("value", ["", "no", "1"])
def test_env_var_with_other_values_is_ignored(monkeypatch, value):
    monkeypatch.setenv(EAGER_SCHEMA_VALIDATION_ENV_VAR, value)
    assert make_session().eager_schema_validation.enabled is False


def test_explicit_calls_override_the_env_var(monkeypatch):
    monkeypatch.setenv(EAGER_SCHEMA_VALIDATION_ENV_VAR, "true")
    eager = make_session().eager_schema_validation
    eager.disable()
    assert eager.enabled is False


def test_explicit_false_in_env_var_beats_the_global_setting(monkeypatch):
    monkeypatch.setenv(EAGER_SCHEMA_VALIDATION_ENV_VAR, "false")
    context.configure_development_features(enable_eager_schema_validation=True)
    assert make_session().eager_schema_validation.enabled is False


def test_log_levels(caplog):
    def own_levels():
        # the multithreaded-mode warning of the session is logged by another logger
        levels = [
            r.levelno
            for r in caplog.records
            if r.name == "snowflake.snowpark.debug_mode"
        ]
        caplog.clear()
        return levels

    eager = make_session().eager_schema_validation
    with caplog.at_level(logging.INFO, logger="snowflake.snowpark.debug_mode"):
        own_levels()
        eager.enable()
        assert own_levels() == [logging.INFO]
        eager.disable()
        assert own_levels() == [logging.INFO]
        with eager:
            pass
        assert own_levels() == []


def test_debug_enables_and_disables_everything():
    session = make_session()
    session.debug.enable_all()
    assert session.eager_schema_validation.enabled is True
    assert context._enable_dataframe_trace_on_error is True
    assert context._enable_trace_sql_errors_to_dataframe is True

    session.debug.disable_all()
    assert session.eager_schema_validation.enabled is False
    assert context._enable_dataframe_trace_on_error is False
    assert context._enable_trace_sql_errors_to_dataframe is False


def test_debug_keeps_the_global_eager_setting_of_other_sessions():
    context.configure_development_features(enable_eager_schema_validation=True)
    session = make_session()
    session.debug.enable_all()
    session.debug.disable_all()
    assert context._debug_eager_schema_validation is True
