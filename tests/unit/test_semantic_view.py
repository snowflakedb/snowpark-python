#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.utils import quote_name
from snowflake.snowpark.semantic_view import SemanticView, SemanticViewQuery
from snowflake.snowpark.session import Session


@pytest.fixture
def fake_session():
    return mock.create_autospec(Session, _session_id=123456)


def test_semantic_view_fqn(fake_session):
    sv = SemanticView(fake_session, "DB.SC.SV")
    assert sv.fqn == "DB.SC.SV"


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("facts", ("f1",)),
        ("metrics", ("m1",)),
        ("dimensions", ("d1",)),
        ("where", ("d1 = 1",)),
    ],
)
def test_semantic_view_stubs_not_implemented(fake_session, method_name, args):
    sv = SemanticView(fake_session, "DB.SC.SV")
    with pytest.raises(NotImplementedError):
        getattr(sv, method_name)(*args)


def test_query_clause_defaults_are_empty_tuples(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    assert q._metrics == ()
    assert q._facts == ()
    assert q._dimensions == ()
    assert q._where == ()


def test_query_init_keeps_other_clauses_empty(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV", metrics=("m1",))
    assert q._metrics == ("m1",)
    assert q._facts == ()
    assert q._dimensions == ()
    assert q._where == ()
    assert q._fqn == "DB.SC.SV"


def test_query_metrics_returns_new_instance(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    q2 = q.metrics("a")
    assert q2 is not q
    assert q._metrics == ()
    assert q2._metrics == ("a",)


def test_query_metrics_replaces(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV").metrics("a").metrics("b")
    assert q._metrics == ("b",)


def test_query_metrics_keeps_other_clauses(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV", dimensions=("d1",))
    q2 = q.metrics("a")
    assert q2._metrics == ("a",)
    assert q2._dimensions == ("d1",)


def test_query_facts_returns_new_instance(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    q2 = q.facts("f1")
    assert q2 is not q
    assert q._facts == ()
    assert q2._facts == ("f1",)


def test_query_facts_replaces(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV").facts("f1").facts("f2")
    assert q._facts == ("f2",)


def test_query_facts_keeps_other_clauses(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV", metrics=("m1",))
    q2 = q.facts("f1")
    assert q2._facts == ("f1",)
    assert q2._metrics == ("m1",)


def test_query_dimensions_returns_new_instance(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    q2 = q.dimensions("d1")
    assert q2 is not q
    assert q._dimensions == ()
    assert q2._dimensions == ("d1",)


def test_query_dimensions_replaces(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV").dimensions("d1").dimensions("d2")
    assert q._dimensions == ("d2",)


def test_query_dimensions_keeps_other_clauses(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV", metrics=("m1",))
    q2 = q.dimensions("d1")
    assert q2._dimensions == ("d1",)
    assert q2._metrics == ("m1",)


def test_query_where_returns_new_instance(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    q2 = q.where("d1 = 1")
    assert q2 is not q
    assert q._where == ()
    assert q2._where == ("d1 = 1",)


def test_query_where_ands(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV").where("x = 1").where("y = 2")
    assert q._where == ("x = 1", "y = 2")


def test_query_where_keeps_other_clauses(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV", metrics=("m1",))
    q2 = q.where("d1 = 1")
    assert q2._where == ("d1 = 1",)
    assert q2._metrics == ("m1",)


def test_to_df_empty_raises_and_sends_no_query(fake_session):
    q = SemanticViewQuery(fake_session, "DB.SC.SV")
    with pytest.raises(ValueError, match="at least one"):
        q.to_df()
    fake_session.sql.assert_not_called()


def test_to_df_sql_order_and_where_and(fake_session):
    q = (
        SemanticViewQuery(fake_session, "DB.SC.SV")
        .metrics("m1", "m2")
        .dimensions("d1")
        .where("x = 1")
        .where("y = 2")
    )
    df = q.to_df()
    fake_session.sql.assert_called_once_with(
        "SELECT * FROM SEMANTIC_VIEW("
        "DB.SC.SV METRICS m1, m2 DIMENSIONS d1 WHERE x = 1 AND y = 2)"
    )
    assert df is fake_session.sql.return_value


def test_to_df_renders_alias_tuple(fake_session):
    from snowflake.snowpark._internal.utils import quote_name

    q = SemanticViewQuery(fake_session, "DB.SC.SV").metrics(("nation.name", "NN"))
    q.to_df()
    fake_session.sql.assert_called_once_with(
        "SELECT * FROM SEMANTIC_VIEW("
        f"DB.SC.SV METRICS nation.name AS {quote_name('NN')})"
    )
