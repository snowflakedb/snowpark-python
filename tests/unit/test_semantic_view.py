#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.session import Session


@pytest.fixture
def fake_session():
    """A Session stand-in whose ``sql`` is a mock, so the emitted text can be asserted."""
    session = mock.create_autospec(Session, _session_id=123456)
    session._conn = mock.MagicMock()
    return session


def call(session, *args, **kwargs):
    """Invoke the unbound method so no live connection is needed."""
    return Session.semantic_view(session, *args, **kwargs)


def emitted(session):
    session.sql.assert_called_once()
    return session.sql.call_args[0][0]


# --- clause rendering ------------------------------------------------------


def test_metrics_only(fake_session):
    call(fake_session, "V", metrics=["orders.revenue"])
    assert (
        emitted(fake_session) == "SELECT * FROM SEMANTIC_VIEW(V METRICS orders.revenue)"
    )


def test_members_joined_with_comma_space(fake_session):
    call(fake_session, "V", metrics=["a", "b", "c"])
    assert emitted(fake_session) == "SELECT * FROM SEMANTIC_VIEW(V METRICS a, b, c)"


def test_str_clause_goes_in_verbatim(fake_session):
    """A string is never comma-split: ``METRICS <text>`` already accepts a list."""
    call(fake_session, "V", dimensions="NATION.NAME, REGION.NAME AS REGION_NAME")
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V DIMENSIONS NATION.NAME, REGION.NAME AS REGION_NAME)"
    )


def test_str_clause_with_comma_inside_a_function_is_not_split(fake_session):
    call(fake_session, "V", dimensions="TO_CHAR(orders.order_date,'YY')")
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V DIMENSIONS TO_CHAR(orders.order_date,'YY'))"
    )


def test_tuple_element_is_table_dot_attribute(fake_session):
    """A 2-tuple element is (table, attribute), as in ``session.table(("db","sc","t"))``."""
    call(
        fake_session, "V", dimensions=[("DATE", "CALENDAR_YEAR"), ("PRODUCT", "MARKET")]
    )
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V DIMENSIONS DATE.CALENDAR_YEAR, PRODUCT.MARKET)"
    )


def test_inline_alias_is_preserved(fake_session):
    call(fake_session, "V", metrics=["orders.revenue AS REV"])
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V METRICS orders.revenue AS REV)"
    )


# --- emission order --------------------------------------------------------


def test_emission_order_is_dimensions_metrics_facts_where(fake_session):
    """Keyword order cannot express clause order, so one fixed order is emitted.

    ``facts`` with ``metrics`` is rejected by the server (010268) and deliberately
    not checked here; the client validates form, not semantics.
    """
    call(fake_session, "V", where="a = 1", facts="f", metrics="m", dimensions="d")
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V DIMENSIONS d METRICS m FACTS f WHERE a = 1)"
    )


def test_where_comes_last(fake_session):
    call(fake_session, "V", where="customers.region = 'EMEA'", metrics="orders.revenue")
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V METRICS orders.revenue "
        "WHERE customers.region = 'EMEA')"
    )


# --- the at-least-one-clause rule -----------------------------------------


def test_no_clause_raises_value_error_and_sends_no_query(fake_session):
    with pytest.raises(ValueError, match="at least one"):
        call(fake_session, "V")
    fake_session.sql.assert_not_called()


def test_where_alone_does_not_satisfy_the_rule(fake_session):
    """``SEMANTIC_VIEW(v WHERE x = 'y')`` is 001003 on the server."""
    with pytest.raises(ValueError, match="at least one"):
        call(fake_session, "V", where="x = 'y'")
    fake_session.sql.assert_not_called()


# --- rejected clause value types ------------------------------------------


def test_top_level_tuple_raises_type_error(fake_session):
    """Two members and member-with-alias are both plausible, so refuse to guess."""
    with pytest.raises(TypeError) as exc:
        call(fake_session, "V", metrics=("a", "b"))
    message = str(exc.value)
    assert "metrics" in message
    assert 'metrics=["a", "b"]' in message
    assert 'metrics=[("a", "b")]' in message
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("bad", [("a",), ("a", "b", "c")])
def test_tuple_element_of_wrong_arity_raises_type_error(fake_session, bad):
    with pytest.raises(TypeError) as exc:
        call(fake_session, "V", metrics=["ok", bad])
    message = str(exc.value)
    assert "metrics" in message
    assert "1" in message  # the offending element's index
    fake_session.sql.assert_not_called()


def test_set_is_rejected(fake_session):
    """Set iteration order is arbitrary and clause order is positional."""
    with pytest.raises(TypeError):
        call(fake_session, "V", metrics={"a", "b"})
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("bad", [b"revenue", bytearray(b"revenue")])
def test_bytes_is_rejected(fake_session, bad):
    """bytes is Iterable, so an ``isinstance(x, str)`` guard alone would let it through."""
    with pytest.raises(TypeError):
        call(fake_session, "V", metrics=bad)
    fake_session.sql.assert_not_called()


def test_int_is_rejected(fake_session):
    with pytest.raises(TypeError):
        call(fake_session, "V", metrics=1)
    fake_session.sql.assert_not_called()


# --- name handling ---------------------------------------------------------


def test_plain_name(fake_session):
    call(fake_session, "SALES", metrics="m")
    assert "SEMANTIC_VIEW(SALES METRICS m)" in emitted(fake_session)


def test_fully_qualified_name(fake_session):
    call(fake_session, "DB.SC.SALES", metrics="m")
    assert "SEMANTIC_VIEW(DB.SC.SALES METRICS m)" in emitted(fake_session)


def test_iterable_name_is_dot_joined(fake_session):
    call(fake_session, ["DB", "SC", "SALES"], metrics="m")
    assert "SEMANTIC_VIEW(DB.SC.SALES METRICS m)" in emitted(fake_session)


def test_quoted_name_is_not_altered(fake_session):
    call(fake_session, 'DB.SC."my view"', metrics="m")
    assert 'SEMANTIC_VIEW(DB.SC."my view" METRICS m)' in emitted(fake_session)


@pytest.mark.parametrize(
    "bad_name", ["", "a-b", "A.B.C.D", "DB.SC.", ".SALES", "1abc", "sales "]
)
def test_invalid_name_raises_and_sends_no_query(fake_session, bad_name):
    with pytest.raises(SnowparkInvalidObjectNameException):
        call(fake_session, bad_name, metrics="m")
    fake_session.sql.assert_not_called()


def test_database_and_schema_keywords_are_not_accepted(fake_session):
    """Qualification lives in the dotted name, as with ``Session.table``."""
    with pytest.raises(TypeError):
        call(fake_session, "SALES", metrics="m", database="DB", schema="SC")


# --- plumbing --------------------------------------------------------------


def test_returns_what_sql_returns(fake_session):
    df = call(fake_session, "V", metrics="m")
    assert df is fake_session.sql.return_value


def test_api_call_source_is_attributed(fake_session):
    df = call(fake_session, "V", metrics="m")
    plan = df._select_statement or df._plan
    assert plan.api_calls == [{"name": "Session.semantic_view"}]


def test_local_testing_is_rejected(fake_session):
    fake_session._conn = mock.create_autospec(MockServerConnection)
    fake_session._conn.log_not_supported_error.side_effect = NotImplementedError
    with pytest.raises(NotImplementedError):
        call(fake_session, "V", metrics="m")
    fake_session._conn.log_not_supported_error.assert_called_once()
    assert (
        fake_session._conn.log_not_supported_error.call_args.kwargs[
            "external_feature_name"
        ]
        == "Session.semantic_view"
    )
