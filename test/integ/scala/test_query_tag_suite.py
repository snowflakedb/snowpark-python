#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# This file contains the query tag related tests in Scala's APIInternalSuite.scala
# Because the implementation of query tag is different between the two languages,
# the tests below differ from Scala tests.
# For scala test code, refer to APIInternalSuite.scala and QueryTagSuite.scala

from test.utils import Utils

import pytest


def test_set_query_tag(session):
    """Test set query_tag properties on session"""
    query_tag = Utils.random_name()
    try:
        session.query_tag = query_tag
        assert session.query_tag == query_tag
    finally:
        Utils.unset_query_tag(session)


def test_query_tags_in_session(session):
    query_tag = Utils.random_name()
    view_name = Utils.random_name()
    temp_view_name = Utils.random_name()
    try:
        session.query_tag = query_tag
        session.createDataFrame(["a", "b", "c"]).collect()
        session.createDataFrame(["a", "b", "c"]).count()
        session.createDataFrame(["a", "b", "c"]).show()
        session.createDataFrame(["a", "b", "c"]).createOrReplaceTempView(view_name)
        session.createDataFrame(["a", "b", "c"]).createOrReplaceView(temp_view_name)
        query_history = get_query_history_for_tags(session, query_tag)
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, temp_view_name)

        assert (
            len(query_history) == 6
        )  # 5 DataFrame queries + 1 query that get sql history
    finally:
        Utils.unset_query_tag(session)


@pytest.mark.parametrize(
    "code",
    [
        'session.createDataFrame(["a", "b", "c"]).collect()',
        'session.createDataFrame(["a", "b", "c"]).count()',
        'session.createDataFrame(["a", "b", "c"]).show()',
        """
    view_name = Utils.random_name()
    session.createDataFrame(["a", "b", "c"]).createOrReplaceView(view_name)
    Utils.drop_view(session, view_name)
    """,
        """
    temp_view_name = Utils.random_name()
    session.createDataFrame(["a", "b", "c"]).createOrReplaceTempView(temp_view_name)
    Utils.drop_view(session, temp_view_name)
    """,
    ],
)
def test_query_tags_from_trackback(session, code):
    """Create a function with random name and check if the random name is in query tag of sql history"""

    random_name = Utils.random_name()
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


def get_query_history_for_tags(session, query_tag):
    query_result = session.conn.run_query(
        f"select query_text from table(information_schema.query_history()) where contains(query_tag, '{query_tag}')"
    )
    return query_result["data"]
