#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from enum import Enum
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


def test_str_subclass_renders_as_its_value(fake_session):
    """``class X(str, Enum)`` formats as its repr, so clause text is concatenated."""

    class Member(str, Enum):
        REGION = "customers.region"
        TABLE = "customers"
        COLUMN = "region"

    class Condition(str, Enum):
        EMEA = "customers.region = 'EMEA'"

    call(
        fake_session,
        "V",
        dimensions=Member.REGION,
        metrics=[(Member.TABLE, Member.COLUMN)],
        where=Condition.EMEA,
    )
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V DIMENSIONS customers.region "
        "METRICS customers.region WHERE customers.region = 'EMEA')"
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


# --- empty values ----------------------------------------------------------
# An empty value is not None, so it passes the at-least-one rule and renders to a
# bare keyword: ``SEMANTIC_VIEW(V METRICS )`` is 001003.


@pytest.mark.parametrize("empty,kind", [([], "list"), ((), "tuple")])
def test_empty_container_raises_and_sends_no_query(fake_session, empty, kind):
    """The message names the container type. "Omit it" is wrong advice for a list that
    came back empty from a comprehension, so the two cases stay distinguishable."""
    with pytest.raises(ValueError, match=f"metrics is an empty {kind}"):
        call(fake_session, "V", metrics=empty)
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_blank_clause_string_raises_and_sends_no_query(fake_session, blank):
    with pytest.raises(ValueError, match="metrics is empty"):
        call(fake_session, "V", metrics=blank)
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("param", ["dimensions", "metrics", "facts"])
def test_blank_member_in_a_list_raises_with_its_index(fake_session, param):
    """The message names the caller's parameter, not just the SQL keyword."""
    with pytest.raises(ValueError, match=rf"{param}\[1\] is an empty member name"):
        call(fake_session, "V", **{param: ["orders.revenue", ""]})
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("bad", [("", "region"), ("customers", ""), ("  ", " ")])
def test_blank_tuple_part_raises_instead_of_emitting_a_dangling_dot(fake_session, bad):
    """``DIMENSIONS .region`` is 001003; the plain-string path already rejects this."""
    with pytest.raises(
        ValueError, match=r"metrics\[0\] has an empty table or attribute"
    ):
        call(fake_session, "V", metrics=[bad])
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_where_raises_and_sends_no_query(fake_session, blank):
    with pytest.raises(ValueError, match="where is empty"):
        call(fake_session, "V", metrics="m", where=blank)
    fake_session.sql.assert_not_called()


def test_empty_clause_is_rejected_not_dropped(fake_session):
    with pytest.raises(ValueError, match="metrics is an empty list"):
        call(fake_session, "V", dimensions="customers.region", metrics=[])
    fake_session.sql.assert_not_called()


# --- rejected clause value types ------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (("a", "b"), "a, b"),  # a bare pair is simply two members
        (("a", "b", "c"), "a, b, c"),
        (("a",), "a"),
        ((("a", "b"),), "a.b"),  # tuple holding one (table, attribute) pair
        ((("a", "b"), ("c", "d")), "a.b, c.d"),
        (("a", ("b", "c")), "a, b.c"),  # mixed
    ],
)
def test_tuple_container_is_accepted(fake_session, value, expected):
    """A tuple is a container of members, exactly like a list."""
    call(fake_session, "V", metrics=value)
    assert emitted(fake_session) == f"SELECT * FROM SEMANTIC_VIEW(V METRICS {expected})"


@pytest.mark.parametrize("bad", [("a",), ("a", "b", "c")])
def test_tuple_element_of_wrong_arity_raises_type_error(fake_session, bad):
    with pytest.raises(TypeError) as exc:
        call(fake_session, "V", metrics=["ok", bad])
    assert re.search(r"metrics\[1\]", str(exc.value))
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("bad", [1, None, 1.5, ["nested"], {"k": "v"}])
def test_member_that_is_neither_string_nor_tuple_is_rejected(fake_session, bad):
    with pytest.raises(
        TypeError,
        match=r"metrics\[1\] should be a string or a \(table, attribute\) tuple",
    ):
        call(fake_session, "V", metrics=["ok", bad])
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize("bad", [("a", 1), (1, "b"), ("a", None)])
def test_tuple_element_contents_must_be_strings(fake_session, bad):
    with pytest.raises(TypeError) as exc:
        call(fake_session, "V", metrics=[bad])
    message = str(exc.value)
    assert "metrics[0]" in message
    assert "(table, attribute)" in message
    fake_session.sql.assert_not_called()


def test_set_is_rejected(fake_session):
    """Set iteration order is arbitrary and clause order is positional."""
    with pytest.raises(TypeError):
        call(fake_session, "V", metrics={"a", "b"})
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize(
    "bad", [b"revenue", bytearray(b"revenue"), memoryview(b"revenue")]
)
def test_bytes_is_rejected(fake_session, bad):
    """bytes is Iterable, so an ``isinstance(x, str)`` guard alone would let it through."""
    with pytest.raises(TypeError, match="should be a string, or a sequence of members"):
        call(fake_session, "V", metrics=bad)
    fake_session.sql.assert_not_called()


def test_int_is_rejected(fake_session):
    with pytest.raises(TypeError, match="should be a string, or a sequence of members"):
        call(fake_session, "V", metrics=1)
    fake_session.sql.assert_not_called()


@pytest.mark.parametrize(
    "bad", [123, {"x = 1"}, (c for c in "ab"), b"x = 1", memoryview(b"x = 1")]
)
def test_non_sequence_where_is_rejected(fake_session, bad):
    with pytest.raises(
        TypeError, match="where should be a string, or a sequence of conditions"
    ):
        call(fake_session, "V", metrics="m", where=bad)
    fake_session.sql.assert_not_called()


def test_where_sequence_is_and_joined_with_each_element_parenthesized(fake_session):
    """``AND``-joined, each element parenthesized so an inner ``OR`` still binds first."""
    call(
        fake_session,
        "V",
        metrics="m",
        where=["region = 'EMEA' OR region = 'APAC'", "customer = 'Chen'"],
    )
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V METRICS m "
        "WHERE (region = 'EMEA' OR region = 'APAC') AND (customer = 'Chen'))"
    )


def test_single_condition_sequence_is_still_parenthesized(fake_session):
    call(fake_session, "V", metrics="m", where=["a = 1"])
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V METRICS m WHERE (a = 1))"
    )


def test_where_string_is_emitted_verbatim_without_parentheses(fake_session):
    call(fake_session, "V", metrics="m", where="a = 1 AND b = 2")
    assert emitted(fake_session) == (
        "SELECT * FROM SEMANTIC_VIEW(V METRICS m WHERE a = 1 AND b = 2)"
    )


@pytest.mark.parametrize("empty,kind", [([], "list"), ((), "tuple")])
def test_empty_where_container_is_rejected(fake_session, empty, kind):
    with pytest.raises(ValueError, match=f"where is an empty {kind}"):
        call(fake_session, "V", metrics="m", where=empty)
    fake_session.sql.assert_not_called()


def test_blank_condition_in_a_where_sequence_is_rejected(fake_session):
    with pytest.raises(ValueError, match=r"where\[1\] is an empty condition"):
        call(fake_session, "V", metrics="m", where=["a = 1", "  "])
    fake_session.sql.assert_not_called()


def test_non_string_condition_is_rejected_with_its_index(fake_session):
    with pytest.raises(TypeError, match=r"where\[1\] should be a string"):
        call(fake_session, "V", metrics="m", where=["a = 1", 2])
    fake_session.sql.assert_not_called()


# --- name handling ---------------------------------------------------------


def test_plain_name(fake_session):
    call(fake_session, "SALES", metrics="m")
    assert "SEMANTIC_VIEW(SALES METRICS m)" in emitted(fake_session)


def test_fully_qualified_name(fake_session):
    call(fake_session, "DB.SC.SALES", metrics="m")
    assert "SEMANTIC_VIEW(DB.SC.SALES METRICS m)" in emitted(fake_session)


@pytest.mark.parametrize("parts", [["DB", "SC", "SALES"], ("DB", "SC", "SALES")])
def test_sequence_name_is_dot_joined(fake_session, parts):
    call(fake_session, parts, metrics="m")
    assert "SEMANTIC_VIEW(DB.SC.SALES METRICS m)" in emitted(fake_session)


@pytest.mark.parametrize("bad", [{"DB", "SC", "SALES"}, frozenset({"DB"})])
def test_set_name_is_rejected(fake_session, bad):
    """A set would dot-join in an arbitrary order."""
    with pytest.raises(TypeError, match="name should be a string"):
        call(fake_session, bad, metrics="m")
    fake_session.sql.assert_not_called()


def test_deque_is_accepted_because_the_annotation_says_sequence(fake_session):
    """A deque is a Sequence, so the runtime must not reject what the signature allows."""
    from collections import deque

    call(fake_session, "V", metrics=deque(["a", "b"]))
    assert emitted(fake_session) == "SELECT * FROM SEMANTIC_VIEW(V METRICS a, b)"


def test_sequence_name_is_validated_after_joining(fake_session):
    """The join happens first, so validation must see the joined name."""
    with pytest.raises(SnowparkInvalidObjectNameException):
        call(fake_session, ["DB", "a-b", "V"], metrics="m")
    fake_session.sql.assert_not_called()


def test_blank_name_part_is_passed_through(fake_session):
    """``DB..V`` is valid Snowflake for ``DB.PUBLIC.V``."""
    call(fake_session, ["DB", "", "V"], metrics="m")
    assert emitted(fake_session) == "SELECT * FROM SEMANTIC_VIEW(DB..V METRICS m)"


@pytest.mark.parametrize("bad", [b"DB", bytearray(b"DB"), memoryview(b"DB"), 1, None])
def test_non_sequence_name_is_rejected(fake_session, bad):
    with pytest.raises(TypeError, match="name should be a string"):
        call(fake_session, bad, metrics="m")
    fake_session.sql.assert_not_called()


def test_non_string_name_part_is_rejected_with_its_index(fake_session):
    with pytest.raises(TypeError, match=r"name\[1\] should be a string"):
        call(fake_session, ["DB", 1, "SALES"], metrics="m")
    fake_session.sql.assert_not_called()


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


@pytest.mark.parametrize("emit", [True, False])
def test_emit_ast_is_forwarded_to_sql(fake_session, emit):
    call(fake_session, "V", metrics="m", _emit_ast=emit)
    assert fake_session.sql.call_args.kwargs["_emit_ast"] is emit


def test_publicapi_injects_emit_ast_when_the_caller_omits_it(fake_session):
    """Fails if ``@publicapi`` is dropped: the parameter default would win instead."""
    with mock.patch(
        "snowflake.snowpark._internal.utils.is_ast_enabled", return_value=False
    ):
        call(fake_session, "V", metrics="m")
    assert fake_session.sql.call_args.kwargs["_emit_ast"] is False


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
