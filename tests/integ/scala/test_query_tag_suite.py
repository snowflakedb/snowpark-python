#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

# This file contains the query tag related tests in Scala's APIInternalSuite.scala
# Because the implementation of query tag is different between the two languages,
# the tests below differ from Scala tests.
# For scala test code, refer to APIInternalSuite.scala and QueryTagSuite.scala

import pytest

from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD
from snowflake.snowpark._internal.utils import TempObjectType, is_in_stored_procedure
from tests.utils import Utils


@pytest.mark.skipif(is_in_stored_procedure(), reason="VCRPY isn't available in SP yet.")
@pytest.mark.snowflake_vcr
@pytest.mark.parametrize(
    "query_tag",
    [
        "'test_tag'",
        "te\\st_tag",
        "te\nst_tag",
        r"\utest_tag",
        "test tag",
        'test"tag',
    ],
)
def test_set_query_tag(session, query_tag):
    """Test set query_tag properties on session"""
    try:
        session.query_tag = query_tag
        assert session.query_tag == query_tag
    finally:
        Utils.unset_query_tag(session)


@pytest.mark.skipif(is_in_stored_procedure(), reason="VCRPY isn't available in SP yet.")
@pytest.mark.snowflake_vcr
def test_query_tags_in_session(session):
    query_tag = "adsf94984"
    view_name = "ghlkglk892389"
    temp_view_name = "gjkfjk7832783"
    try:
        session.query_tag = query_tag
        session.create_dataframe(["a", "b", "c"]).collect()
        session.create_dataframe(["a", "b", "c"]).count()
        session.create_dataframe(["a", "b", "c"]).show()
        session.create_dataframe(["a", "b", "c"]).first()
        session.create_dataframe(["a", "b", "c"]).to_pandas()
        session.create_dataframe(["a", "b", "c"]).create_or_replace_temp_view(view_name)
        session.create_dataframe(["a", "b", "c"]).create_or_replace_view(temp_view_name)
        query_history = get_query_history_for_tags(session, query_tag)
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, temp_view_name)

        assert (
            len(query_history) == 8
        )  # 7 DataFrame queries + 1 query that get sql history
    finally:
        Utils.unset_query_tag(session)


@pytest.mark.skipif(is_in_stored_procedure(), reason="VCRPY isn't available in SP yet.")
@pytest.mark.snowflake_vcr
@pytest.mark.parametrize(
    "code, func_name",
    [
        ('session.create_dataframe(["a", "b", "c"]).collect()', "collect"),
        ('session.create_dataframe(["a", "b", "c"]).count()', "count"),
        ('session.create_dataframe(["a", "b", "c"]).show()', "show"),
        ('session.create_dataframe(["a", "b", "c"]).first()', "first"),
        ('session.create_dataframe(["a", "b", "c"]).to_pandas()', "to_pandas"),
        (
            """
    view_name = "trackback_view"
    session.create_dataframe(["a", "b", "c"]).create_or_replace_view(view_name)
    Utils.drop_view(session, view_name)
    """,
            "create_or_replace_view",
        ),
        (
            """
    temp_view_name = "temp_trackback_view"
    session.create_dataframe(["a", "b", "c"]).create_or_replace_temp_view(temp_view_name)
    Utils.drop_view(session, temp_view_name)
    """,
            "create_or_replace_temp_view",
        ),
    ],
)
def test_query_tags_from_trackback(session, code, func_name):
    """Create a function with random name and check if the random name is in query tag of sql history"""
    exec(
        f"""def {func_name}(session):
            {code}
        """,
        globals(),
    )
    random_name_func = globals().get(f"{func_name}")
    random_name_func(session)
    query_history = get_query_history_for_tags(session, func_name)
    assert len(query_history) == 1


@pytest.mark.skipif(is_in_stored_procedure(), reason="VCRPY isn't available in SP yet.")
@pytest.mark.snowflake_vcr
@pytest.mark.parametrize("data", ["a", "'a'", "\\a", "a\n", r"\ua", " a", '"a'])
def test_large_local_relation_query_tag_from_traceback(session, data):
    session.create_dataframe(
        [[data] * (ARRAY_BIND_THRESHOLD + 1)]
    ).count()  # trigger large local relation query
    query_history = get_query_history_for_tags(
        session, "test_large_local_relation_query_tag_from_traceback"
    )
    assert len(query_history) > 0  # some hidden SQLs are run so it's not exactly 1.


@pytest.mark.skipif(is_in_stored_procedure(), reason="VCRPY isn't available in SP yet.")
@pytest.mark.snowflake_vcr
def test_query_tag_for_cache_result(session):
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    session.query_tag = query_tag
    session.create_dataframe([1, 2, 3]).cache_result()
    Utils.unset_query_tag(session)
    query_history = get_query_history_for_tags(session, query_tag)
    assert len(query_history) == 3


def get_query_history_for_tags(session, query_tag):
    query_result = session._conn.run_query(
        f"select query_text from table(information_schema.query_history()) "
        f"where contains(query_tag, '{query_tag}') and session_id = '{session._conn.get_session_id()}'",
        log_on_exception=True,
    )
    return query_result["data"]
