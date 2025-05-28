#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import threading
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from unittest import mock

import pytest

from snowflake.connector.errors import Error
from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD
from tests.utils import IS_IN_STORED_PROC, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Query history is a SQL feature",
        run=False,
    ),
]


def multi_thread_helper_function(session):
    session.sql(f"select {threading.get_ident()}").collect()


def multi_thread_describe_helper_function(session):
    df = session.sql(f"select {threading.get_ident()}")
    df.columns
    df.collect()


def test_query_history(session):
    with session.query_history() as query_listener:
        session.sql("select 0").collect()
    assert len(query_listener.queries) == 1
    assert query_listener.queries[0].query_id is not None
    assert query_listener.queries[0].sql_text == "select 0"
    assert not query_listener.queries[0].is_describe
    print(query_listener.queries)


def test_query_history_with_describe(session):
    with session.query_history(True) as query_listener:
        df = session.sql("select 0")
        df.columns
        df.collect()
    assert len(query_listener.queries) == 2
    for query in query_listener.queries:
        assert query.query_id is not None
        assert query.sql_text == "select 0"
    print(query_listener.queries)
    assert query_listener.queries[0].is_describe
    assert not query_listener.queries[1].is_describe


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
    with session.query_history(True) as query_history:
        df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        df = df.filter(df.a == 1)
        df.collect()

    if session.sql_simplifier_enabled and not session.reduce_describe_query_enabled:
        assert len(query_history.queries) == 3
        assert query_history.queries[0].is_describe
        assert query_history.queries[1].is_describe
        assert not query_history.queries[2].is_describe
    else:
        assert len(query_history.queries) == 2
        assert query_history.queries[0].is_describe
        assert not query_history.queries[1].is_describe

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
        assert (
            "INSERT  INTO" in queries[1].sql_text
            and "VALUES (?)" in queries[1].sql_text
        )
        assert 'SELECT "A" FROM' in Utils.normalize_sql(queries[2].sql_text)
        assert "DROP  TABLE  If  EXISTS" in queries[3].sql_text  # post action
    finally:
        session._use_scoped_temp_objects = origin_use_scoped_temp_objects_setting


def test_query_history_with_multi_thread(session):
    works = []
    with session.query_history(include_thread_id=True) as query_history:
        with ThreadPoolExecutor(max_workers=2) as tpe:
            for _ in range(6):
                future = tpe.submit(multi_thread_helper_function, session)
                works.append(future)
            _, _ = wait(works, return_when=ALL_COMPLETED)
    thread_numbers = set()
    for query in query_history.queries:
        assert query.sql_text.split(" ")[-1] == str(query.thread_id)
        thread_numbers.add(query.thread_id)
    print(query_history.queries)
    assert len(thread_numbers) == 2


def test_query_history_without_multi_thread(session):
    with session.query_history(include_thread_id=True) as query_history:
        for _ in range(5):
            multi_thread_helper_function(session)
    thread_numbers = set()
    for query in query_history.queries:
        assert query.sql_text.split(" ")[-1] == str(query.thread_id)
        # assert it equals to main thread id
        assert query.thread_id == threading.get_ident()
        thread_numbers.add(query.thread_id)
    assert len(thread_numbers) == 1


def test_query_history_with_multi_thread_and_describe(session):
    works = []
    with session.query_history(
        include_thread_id=True, include_describe=True
    ) as query_history:
        with ThreadPoolExecutor(max_workers=2) as tpe:
            for _ in range(6):
                future = tpe.submit(multi_thread_describe_helper_function, session)
                works.append(future)
            _, _ = wait(works, return_when=ALL_COMPLETED)
    thread_numbers = set()
    for query in query_history.queries:
        if query.is_describe:
            assert query.sql_text.split(" ")[-1] == str(query.thread_id)
        thread_numbers.add(query.thread_id)
    assert len(thread_numbers) == 2


def test_query_history_when_execution_raise_error(session):
    exception = Error(sfqid="fake_id", query="fake_query")
    with session.query_history(include_error=True) as query_listener:
        with mock.patch(
            "snowflake.connector.cursor.SnowflakeCursor.execute", side_effect=exception
        ):
            with pytest.raises(Error):
                session.sql("select 0").collect()
        record = query_listener.queries[0]
        assert record.query_id == "fake_id"
        assert record.sql_text == "fake_query"


def test_query_history_when_async_execution_raise_error(session):
    exception = Error(sfqid="fake_id", query="fake_query")
    with session.query_history(include_error=True) as query_listener:
        with mock.patch(
            "snowflake.connector.cursor.SnowflakeCursor.execute_async",
            side_effect=exception,
        ):
            with pytest.raises(Error):
                res = session.sql("select 0").collect_nowait()
                res.result()
        record = query_listener.queries[0]
        assert record.query_id == "fake_id"
        assert record.sql_text == "fake_query"
