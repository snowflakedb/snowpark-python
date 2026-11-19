#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import heapq

from snowflake.snowpark.functions import (
    avg,
    col,
    concat,
    current_date,
    datediff,
    lit,
    to_timestamp,
)
from snowflake.snowpark.session import Session
from snowflake.snowpark._internal.ast.batch import get_dependent_bind_ids


def get_dependencies_from_id(session: Session, ast_id: int) -> set[int]:
    bind_stmt = session._ast_batch._bind_stmt_cache[ast_id]
    return get_dependent_bind_ids(bind_stmt)


def test_transitive_closure(session: Session, tables):
    visits_df = session.table(tables.visits)
    assert get_dependencies_from_id(session, visits_df._ast_id) == set()

    users_df = session.table(tables.user_profiles)
    assert get_dependencies_from_id(session, users_df._ast_id) == set()

    filtered_visits_df = visits_df.filter(col("country") == "USA")
    assert get_dependencies_from_id(session, filtered_visits_df._ast_id) == {
        visits_df._ast_id
    }

    users_df2 = users_df.with_column(
        "age", datediff("year", current_date(), col("dob"))
    )
    assert get_dependencies_from_id(session, users_df2._ast_id) == {users_df._ast_id}

    filtered_users_df = users_df2.filter(
        (col("membership_type") == "Palladium") & (col("age") > 25)
    )
    assert get_dependencies_from_id(session, filtered_users_df._ast_id) == {
        users_df2._ast_id
    }

    expanded_visits_df = filtered_visits_df.with_column(
        "visit_duration_minutes",
        datediff(
            "minute", to_timestamp(col("start_time")), to_timestamp(col("end_time"))
        ),
    )
    assert get_dependencies_from_id(session, expanded_visits_df._ast_id) == {
        filtered_visits_df._ast_id
    }

    expanded_users_df = filtered_users_df.with_column(
        "full_name", concat(col("first_name"), lit(" "), col("last_name"))
    )
    assert get_dependencies_from_id(session, expanded_users_df._ast_id) == {
        filtered_users_df._ast_id
    }

    joined_df = expanded_visits_df.join(expanded_users_df, on="user_id", how="left")
    assert get_dependencies_from_id(session, joined_df._ast_id) == {
        expanded_visits_df._ast_id,
        expanded_users_df._ast_id,
    }

    grouped_df = joined_df.group_by("membership_type")
    assert get_dependencies_from_id(session, grouped_df._ast_id) == {joined_df._ast_id}

    aggregated_df = grouped_df.agg(
        avg("visit_duration_minutes").alias("avg_visit_duration")
    )
    assert get_dependencies_from_id(session, aggregated_df._ast_id) == {
        grouped_df._ast_id
    }

    sorted_df = aggregated_df.sort(col("avg_visit_duration").desc())
    assert get_dependencies_from_id(session, sorted_df._ast_id) == {
        aggregated_df._ast_id
    }

    limited_df = sorted_df.limit(100, 0)
    assert get_dependencies_from_id(session, limited_df._ast_id) == {sorted_df._ast_id}

    assert session._ast_batch._eval_ids == set()

    all_bind_ids = [
        visits_df._ast_id,
        users_df._ast_id,
        filtered_visits_df._ast_id,
        users_df2._ast_id,
        filtered_users_df._ast_id,
        expanded_visits_df._ast_id,
        expanded_users_df._ast_id,
        joined_df._ast_id,
        grouped_df._ast_id,
        aggregated_df._ast_id,
        sorted_df._ast_id,
        limited_df._ast_id,
    ]
    assert session._ast_batch._cur_request_bind_ids == set(all_bind_ids)
    assert session._ast_batch.cur_stmts_closure() == list(
        session._ast_batch._bind_stmt_cache.keys()
    )

    for id in all_bind_ids:
        assert id in session._ast_batch._bind_stmt_cache
        assert id == heapq.heappop(session._ast_batch._cur_request_bind_id_q)
