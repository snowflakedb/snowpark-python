#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD
from tests.utils import IS_IN_STORED_PROC


def test_query_history(session):
    with session.query_history() as query_listener:
        session.sql("select 0").collect()
    assert len(query_listener.queries) == 1
    assert query_listener.queries[0].query_id is not None
    assert query_listener.queries[0].sql_text == "select 0"


def test_query_history_stop_listening(session):
    with session.query_history() as query_listener:
        session.sql("select 0").collect()
    session.sql(
        "select 1"
    ).collect()  # the query from this action shouldn't be recorded by query_listener
    assert len(query_listener.queries) == 1
    assert query_listener.queries[0].query_id is not None
    assert query_listener.queries[0].sql_text == "select 0"


def test_query_history_two_listeners(session):
    with session.query_history() as query_listener:
        session.sql("select 0").collect()
        with session.query_history() as query_listener1:
            session.sql("select 1").collect()

    assert len(query_listener1.queries) == 1
    assert query_listener1.queries[0].query_id is not None
    assert query_listener1.queries[0].sql_text == "select 1"

    assert len(query_listener.queries) == 2
    assert (
        query_listener.queries[0].query_id is not None
        and query_listener.queries[1].query_id is not None
    )
    assert (
        query_listener.queries[0].sql_text == "select 0"
        and query_listener.queries[1].sql_text == "select 1"
    )


def test_query_history_multiple_actions(session):
    with session.query_history() as query_listener:
        session.sql("select 0").collect()
        session.sql("select 1").collect()
        session.sql("select 2").collect()
    assert len(query_listener.queries) == 3
    assert all(query.query_id is not None for query in query_listener.queries)
    assert query_listener.queries[0].sql_text == "select 0"
    assert query_listener.queries[1].sql_text == "select 1"
    assert query_listener.queries[2].sql_text == "select 2"


def test_query_history_no_actions(session):
    with session.query_history() as query_listener:
        pass  # no action
    assert len(query_listener.queries) == 0


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="alter session is not supported in owner's right stored proc",
)
@pytest.mark.parametrize("use_scoped_temp_objects", [True, False])
def test_query_history_executemany(session, use_scoped_temp_objects):
    """Large local data frame uses ServerConnection.run_batch_insert instead of ServerConnection.run_query.
    run_batch_insert create a temp table and use parameter binding to insert values, then select from the temp table.
    Finally it drops the temp table.
    """
    origin_use_scoped_temp_objects_setting = session._use_scoped_temp_objects
    try:
        session._use_scoped_temp_objects = use_scoped_temp_objects
        with session.query_history() as query_listener:
            session.create_dataframe(
                [[1]] * (ARRAY_BIND_THRESHOLD + 1), schema=["a"]
            ).collect()

        queries = query_listener.queries
        assert all(query.query_id is not None for query in queries)
        assert (
            f"CREATE  OR  REPLACE  {'SCOPED TEMPORARY' if use_scoped_temp_objects else 'TEMPORARY'}"
            in queries[0].sql_text
        )
        assert "alter session set query_tag" in queries[1].sql_text
        assert (
            "INSERT  INTO" in queries[2].sql_text
            and "VALUES (?)" in queries[2].sql_text
        )
        assert "alter session unset query_tag" in queries[3].sql_text
        assert 'SELECT "A" FROM' in queries[4].sql_text
        assert "DROP  TABLE  If  EXISTS" in queries[5].sql_text  # post action
    finally:
        session._use_scoped_temp_objects = origin_use_scoped_temp_objects_setting
