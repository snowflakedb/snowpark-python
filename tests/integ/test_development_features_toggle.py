#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Turning the debug features on and off is clean (goal C of the debug feature test matrix).

After any sequence of on and off the session is in its baseline state; DataFrames built with the
features off and run with them on (and the reverse) behave as when the features are off; and two
sessions with different settings do not affect each other.
"""

import logging

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, sum as sum_
from tests.utils import IS_IN_STORED_PROC

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="needs a live connection",
        run=False,
    ),
]

FEATURES = (
    "eager_schema_validation",
    "trace_sql_errors_to_dataframe",
    "dataframe_trace_on_error",
)
_DEBUG_HEADER = "--- Additional Debug Information ---"
_LINEAGE = "Trace of the most recent dataframe operations associated with the error"
_SWALLOWED = (
    "Error when getting the df_transform_debug_trace",
    "Failed to record the dataframe trace",
)


@pytest.fixture(autouse=True)
def features_off(session):
    session.debug.set()
    context.configure_development_features()
    yield
    session.debug.set()
    context.configure_development_features()


@pytest.fixture(params=[True, False], ids=["multiline", "singleline"])
def multiline(request, session):
    original = session._generate_multiline_queries
    if request.param:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()
    yield request.param
    if original:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()


def _reference(session):
    return (
        session.create_dataframe([(1, "a"), (2, "b")], schema=["ID", "NAME"])
        .filter(col("ID") > 0)
        .group_by("NAME")
        .agg(sum_("ID").alias("S"))
    )


def _state(session):
    return {
        "ast_enabled": session.ast_enabled,
        "auto_clean_up_temp_table_enabled": session.auto_clean_up_temp_table_enabled,
        "multiline": session._generate_multiline_queries,
        **{name: getattr(session.debug, name) for name in FEATURES},
        "reference_sql": _reference(session).queries["queries"],
    }


def _toggle_with_methods(session, on):
    for name in FEATURES:
        getattr(session.debug, f"{'enable' if on else 'disable'}_{name}")()


def _toggle_with_set(session, on):
    if on:
        session.debug.set(**dict.fromkeys(FEATURES, True))
    else:
        session.debug.set()


def _toggle_with_configure(session, on):
    if on:
        context.configure_development_features(
            enable_eager_schema_validation=True,
            enable_dataframe_trace_on_error=True,
            enable_trace_sql_errors_to_dataframe=True,
        )
    else:
        context.configure_development_features()


@pytest.mark.parametrize(
    "toggle",
    [_toggle_with_methods, _toggle_with_set, _toggle_with_configure],
    ids=["methods", "set", "configure"],
)
def test_on_off_restores_baseline(session, multiline, toggle):
    baseline = _state(session)
    for on in (True, False, True, False):
        toggle(session, on)
        state = _state(session)
        assert all(state[name] is on for name in FEATURES)
    assert _state(session) == baseline

    toggle(session, True)
    session.debug.set()
    assert _state(session) == baseline


def test_set_changes_only_the_features_passed(session):
    session.debug.set(eager_schema_validation=True)
    session.debug.set(dataframe_trace_on_error=True, trace_sql_errors_to_dataframe=None)
    assert [getattr(session.debug, name) for name in FEATURES] == [True, False, True]


def _clean(session):
    return session.create_dataframe(
        [(1, "a"), (2, "b"), (3, "a")], schema=["ID", "NAME"]
    )


def _result(df):
    return sorted(df.collect(), key=str), df.schema, df.queries["queries"]


def _failing(df):
    return df.select(col("ID") + col("DOES_NOT_EXIST"))


@pytest.mark.parametrize(
    "built_on", [False, True], ids=["built_off_run_on", "built_on_run_off"]
)
def test_mixed_age_plans(session, multiline, caplog, built_on):
    def build():
        return _clean(session).filter(col("ID") > 1)

    expected = _result(build().group_by("NAME").agg(sum_("ID").alias("S")))
    with pytest.raises(SnowparkSQLException) as expected_error:
        _failing(build()).collect()

    session.debug.set(**dict.fromkeys(FEATURES, built_on))
    old = build()
    session.debug.set(**dict.fromkeys(FEATURES, not built_on))
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark"):
        mixed = old.group_by("NAME").agg(sum_("ID").alias("S"))
        actual = _result(mixed)
        with pytest.raises(SnowparkSQLException) as error:
            _failing(old).collect()
    assert [m for m in _SWALLOWED if m in caplog.text] == []
    assert actual[:2] == expected[:2]
    # The parts of the plan built with the sql error tracing on have line breaks; no literal
    # here contains whitespace.
    assert ["".join(q.split()) for q in actual[2]] == [
        "".join(q.split()) for q in expected[2]
    ]

    assert error.type is expected_error.type
    assert error.value.error_code == expected_error.value.error_code
    if built_on:
        assert _DEBUG_HEADER not in str(error.value)
    else:
        assert _LINEAGE in str(error.value)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Cannot create a session in a stored proc"
)
def test_two_sessions_do_not_affect_each_other(session, db_parameters):
    with Session.builder.configs(db_parameters).create() as other:
        other.sql_simplifier_enabled = session.sql_simplifier_enabled
        session.debug.set(**dict.fromkeys(FEATURES, True))
        other_before = _state(other)
        assert all(getattr(other.debug, name) is False for name in FEATURES)

        with pytest.raises(SnowparkSQLException) as on:
            _failing(_clean(session)).collect()
        with pytest.raises(SnowparkSQLException) as off:
            _failing(_clean(other)).collect()
        assert _LINEAGE in str(on.value)
        assert _DEBUG_HEADER not in str(off.value)

        # The other way around.
        session.debug.set()
        other.debug.set(dataframe_trace_on_error=True)
        assert all(getattr(session.debug, name) is False for name in FEATURES)
        with pytest.raises(SnowparkSQLException) as off:
            _failing(_clean(session)).collect()
        with pytest.raises(SnowparkSQLException) as on:
            _failing(_clean(other)).collect()
        assert _DEBUG_HEADER not in str(off.value)
        assert _LINEAGE in str(on.value)

        other.debug.set()
        assert _state(other) == other_before
