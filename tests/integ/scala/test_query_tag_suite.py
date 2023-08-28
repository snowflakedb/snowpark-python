#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

# This file contains the query tag related tests in Scala's APIInternalSuite.scala
# Because the implementation of query tag is different between the two languages,
# the tests below differ from Scala tests.
# For scala test code, refer to APIInternalSuite.scala and QueryTagSuite.scala

import pytest

from snowflake.snowpark import QueryHistory
from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD
from snowflake.snowpark._internal.utils import QUERY_TAG_STRING, TempObjectType
from tests.utils import Utils


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
        remote_query_tag = get_remote_query_tag(session)
        assert remote_query_tag == query_tag
        session.query_tag = None
        remote_query_tag = get_remote_query_tag(session)
        assert remote_query_tag == ""
        assert session.query_tag is None
    finally:
        Utils.unset_query_tag(session)


def test_query_tags_in_session(session):
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    try:
        session.query_tag = query_tag
        with session.query_history() as query_history:
            session.create_dataframe(["a", "b", "c"]).collect()
            session.create_dataframe(["a", "b", "c"]).count()
            session.create_dataframe(["a", "b", "c"]).show()
            session.create_dataframe(["a", "b", "c"]).first()
            session.create_dataframe(["a", "b", "c"]).to_pandas()
            session.create_dataframe(["a", "b", "c"]).create_or_replace_temp_view(
                view_name
            )
            session.create_dataframe(["a", "b", "c"]).create_or_replace_view(
                temp_view_name
            )
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, temp_view_name)

        assert (
            len(get_local_query_tags(query_history)) == 0
        )  # The session has query tag. Each SQL statement shouldn't have its own query tag in the statement params.
    finally:
        Utils.unset_query_tag(session)
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, temp_view_name)


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
    with session.query_history() as query_history:
        random_name_func(session)
    assert random_name in query_history._debug_info[0].get("QUERY_TAG")


def test_large_local_relation_query_tag(session):
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    session.query_tag = query_tag
    try:
        with session.query_history() as query_history:
            session.create_dataframe(
                [["a"] * (ARRAY_BIND_THRESHOLD + 1)]
            ).count()  # trigger large local relation query
        set_session_query_tag = [
            x for x in query_history._debug_info if "set_session_query_tag" in x
        ]
        assert len(set_session_query_tag) == 0
        unset_session_query_tag = [
            x for x in query_history._debug_info if "unset_session_query_tag" in x
        ]
        assert len(unset_session_query_tag) == 0
    finally:
        Utils.unset_query_tag(session)


@pytest.mark.parametrize("data", ["a", "'a'", "\\a", "a\n", r"\ua", " a", '"a'])
def test_large_local_relation_query_tag_from_traceback(session, data):
    with session.query_history() as query_history:
        session.create_dataframe(
            [[data] * (ARRAY_BIND_THRESHOLD + 1)]
        ).count()  # trigger large local relation query
    set_session_query_tag = [
        x for x in query_history._debug_info if "set_session_query_tag" in x
    ]
    assert len(set_session_query_tag) == 1
    assert "count" in set_session_query_tag[0]["set_session_query_tag"]
    unset_session_query_tag = [
        x for x in query_history._debug_info if "unset_session_query_tag" in x
    ]
    assert len(unset_session_query_tag) == 1
    assert "count" in unset_session_query_tag[0]["unset_session_query_tag"]


def test_session_query_tag_for_cache_result(session):
    query_tag = Utils.random_name_for_temp_object(TempObjectType.QUERY_TAG)
    session.query_tag = query_tag
    try:
        with session.query_history() as query_history:
            session.create_dataframe([1, 2, 3]).cache_result()
        assert (
            len(get_local_query_tags(query_history)) == 0
        )  # there is session query tag so no statement params are set.
    finally:
        Utils.unset_query_tag(session)


def test_statement_params_query_tag_for_cache_result(session):
    with session.query_history() as query_history:
        session.create_dataframe([1, 2, 3]).cache_result()
    assert "cache_result" in query_history._debug_info[0].get("QUERY_TAG")


def get_local_query_tags(query_history: QueryHistory):
    return [
        x[QUERY_TAG_STRING] for x in query_history._debug_info if QUERY_TAG_STRING in x
    ]


def get_remote_query_tag(session) -> str:
    rows = session.sql("show parameters like 'query_tag' in session").collect()
    return rows[0][1]
