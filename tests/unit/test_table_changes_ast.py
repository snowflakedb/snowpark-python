#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark.session import Session


@pytest.fixture
def fake_session():
    session = mock.create_autospec(Session, _session_id=123456)
    session._conn = mock.MagicMock()
    session._ast_batch = AstBatch(session)
    return session


def table_ast(session, **kwargs):
    """Call Session.table without a connection and return its Table AST node."""
    with mock.patch("snowflake.snowpark.session.Table") as table_cls:
        Session.table(session, "T", _emit_ast=True, **kwargs)
    return table_cls.call_args.kwargs["_ast_stmt"].expr.table


def test_session_table_records_changes_and_end_fields(fake_session):
    ast = table_ast(
        fake_session,
        changes="default",
        time_travel_mode="at",
        statement="q1",
        end_timestamp="2023-01-01 12:00:00",
        end_timestamp_type="LTZ",
    )
    assert ast.changes.value == "default"
    assert ast.time_travel_mode.value == "at"
    assert ast.statement.value == "q1"
    assert ast.end_timestamp.string_val.v == "2023-01-01 12:00:00"
    assert ast.end_timestamp_type.value == "LTZ"
    assert not ast.HasField("end_statement")
    assert not ast.HasField("end_offset")


@pytest.mark.parametrize("end_offset", [-60, "-60", " -60 "])
def test_session_table_records_end_offset_as_int(fake_session, end_offset):
    ast = table_ast(
        fake_session,
        changes="append_only",
        time_travel_mode="at",
        statement="q1",
        end_offset=end_offset,
    )
    assert ast.changes.value == "append_only"
    assert ast.end_offset.value == -60


@pytest.mark.parametrize("end_offset", ["abc", 1.5, True, 5])
def test_session_table_invalid_end_offset_raises_value_error(fake_session, end_offset):
    with pytest.raises(ValueError, match="'end_offset' must be"):
        table_ast(
            fake_session,
            changes="default",
            time_travel_mode="at",
            statement="q1",
            end_offset=end_offset,
        )


def test_session_table_without_changes_leaves_new_fields_unset(fake_session):
    ast = table_ast(fake_session, time_travel_mode="at", offset=-60)
    for field in (
        "changes",
        "end_statement",
        "end_offset",
        "end_timestamp",
        "end_timestamp_type",
    ):
        assert not ast.HasField(field)
