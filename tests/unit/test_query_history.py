#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import QueryRecord


def test_query_record_repr():
    record = QueryRecord("fake_id", "fake_text")
    record_with_thread_id = QueryRecord("fake_id", "fake_text", thread_id=123)
    record_with_is_describe = QueryRecord("fake_id", "fake_text", is_describe=True)
    record_with_both = QueryRecord(
        "fake_id", "fake_text", is_describe=True, thread_id=123
    )

    assert record.__repr__() == "QueryRecord(query_id=fake_id, sql_text=fake_text)"
    assert (
        record_with_thread_id.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, thread_id=123)"
    )
    assert (
        record_with_is_describe.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, is_describe=True)"
    )
    assert (
        record_with_both.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, is_describe=True, thread_id=123)"
    )
