#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

import pytest

from snowflake.snowpark.functions import lit
from snowflake.snowpark.row import Row
from tests.utils import IS_IN_STORED_PROC, Utils


def test_concurrent_select_queries(session):
    def run_select(session_, thread_id):
        df = session_.sql(f"SELECT {thread_id} as A")
        assert df.collect()[0][0] == thread_id

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(run_select, session, i)


def test_concurrent_dataframe_operations(session):
    try:
        table_name = Utils.random_table_name()
        data = [(i, 11 * i) for i in range(10)]
        df = session.create_dataframe(data, ["A", "B"])
        df.write.save_as_table(table_name, table_type="temporary")

        def run_dataframe_operation(session_, thread_id):
            df = session_.table(table_name)
            df = df.filter(df.a == lit(thread_id))
            df = df.with_column("C", df.b + 100 * df.a)
            df = df.rename(df.a, "D").limit(1)
            return df

        dfs = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            df_futures = [
                executor.submit(run_dataframe_operation, session, i) for i in range(10)
            ]

            for future in as_completed(df_futures):
                dfs.append(future.result())

        main_df = dfs[0]
        for df in dfs[1:]:
            main_df = main_df.union(df)

        Utils.check_answer(
            main_df, [Row(D=i, B=11 * i, C=11 * i + 100 * i) for i in range(10)]
        )

    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query and query listeners are not supported",
    run=False,
)
def test_query_listener(session):
    def run_select(session_, thread_id):
        session_.sql(f"SELECT {thread_id} as A").collect()

    with session.query_history() as history:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10):
                executor.submit(run_select, session, i)

    queries_sent = [query.sql_text for query in history.queries]
    assert len(queries_sent) == 10
    for i in range(10):
        assert f"SELECT {i} as A" in queries_sent


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Query tag is a SQL feature",
    run=False,
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="show parameters is not supported in stored procedure"
)
def test_query_tagging(session):
    def set_query_tag(session_, thread_id):
        session_.query_tag = f"tag_{thread_id}"

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(set_query_tag, session, i)

    actual_query_tag = session.sql("SHOW PARAMETERS LIKE 'QUERY_TAG'").collect()[0][1]
    assert actual_query_tag == session.query_tag


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query is not supported",
    run=False,
)
def test_session_stage_created_once(session):
    with patch.object(
        session._conn, "run_query", wraps=session._conn.run_query
    ) as patched_run_query:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in range(10):
                executor.submit(session.get_session_stage)

        assert patched_run_query.call_count == 1


def test_action_ids_are_unique(session):
    with ThreadPoolExecutor(max_workers=10) as executor:
        action_ids = set()
        futures = [executor.submit(session._generate_new_action_id) for _ in range(10)]

        for future in as_completed(futures):
            action_ids.add(future.result())

    assert len(action_ids) == 10
