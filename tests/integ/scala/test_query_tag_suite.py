#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

# This file contains the query tag related tests in Scala's APIInternalSuite.scala
# Because the implementation of query tag is different between the two languages,
# the tests below differ from Scala tests.
# For scala test code, refer to APIInternalSuite.scala and QueryTagSuite.scala

import pytest

from snowflake.snowpark._internal.analyzer_obj import ARRAY_BIND_THRESHOLD
from snowflake.snowpark._internal.utils import TempObjectType
from tests.utils import Utils


def test_set_query_tag(session):
    """Test set query_tag properties on session"""
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    try:
        session.query_tag = query_tag
        assert session.query_tag == query_tag
    finally:
        Utils.unset_query_tag(session)


def test_query_tags_in_session(session):
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
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


@pytest.mark.parametrize(
    "code",
    [
        'session.create_dataframe(["a", "b", "c"]).collect()',
        'session.create_dataframe(["a", "b", "c"]).count()',
        'session.create_dataframe(["a", "b", "c"]).show()',
        'session.create_dataframe(["a", "b", "c"]).first()',
        'session.create_dataframe(["a", "b", "c"]).to_pandas()',
        """
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    session.create_dataframe(["a", "b", "c"]).create_or_replace_view(view_name)
    Utils.drop_view(session, view_name)
    """,
        """
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    session.create_dataframe(["a", "b", "c"]).create_or_replace_temp_view(temp_view_name)
    Utils.drop_view(session, temp_view_name)
    """,
    ],
)
def test_query_tags_from_trackback(session, code):
    """Create a function with random name and check if the random name is in query tag of sql history"""

    random_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    exec(
        f"""def {random_name}_func(session):
            {code}
        """,
        globals(),
    )
    random_name_func = globals().get(f"{random_name}_func")
    random_name_func(session)
    query_history = get_query_history_for_tags(session, random_name)
    assert len(query_history) == 1


def test_large_local_relation_query_tag_from_traceback(session):
    session.create_dataframe(
        [["a"] * (ARRAY_BIND_THRESHOLD + 1)]
    ).count()  # trigger large local relation query
    query_history = get_query_history_for_tags(
        session, "test_large_local_relation_query_tag_from_traceback"
    )
    assert len(query_history) > 0  # some hidden SQLs are run so it's not exactly 1.


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
        f"where contains(query_tag, '{query_tag}') and session_id = '{session._conn.get_session_id()}'"
    )
    return query_result["data"]
