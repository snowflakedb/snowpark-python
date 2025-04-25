#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark import QueryRecord
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


class CustomException(BaseException):
    pass


@sql_count_checker(query_count=3)
def test_sql_counter_with_decorator():
    for _ in range(3):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert len(df) == 3


@pytest.mark.parametrize("test_arg", [1, 2])
@sql_count_checker(query_count=3)
def test_sql_counter_with_decorator_with_parametrize(test_arg):
    for _ in range(3):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert len(df) == 3


@pytest.mark.parametrize(
    "num_queries, check_sql_counter",
    [
        (1, True),
        (2, False),
        (3, True),
    ],
)
def test_sql_counter_with_fixture(num_queries, check_sql_counter, sql_counter):
    # This is added here so that enforcement of sql counts detects and does not flag this test.
    with SqlCounter(query_count=0):
        pass

    for i in range(num_queries):
        df = pd.DataFrame({"a": [1, 2, 3]})
        if i % 2 == 0:
            df = df.merge(df)
        assert len(df) == 3
    if check_sql_counter:
        sql_counter.expects(query_count=num_queries, join_count=(num_queries + 1) / 2)


@pytest.mark.parametrize("num_queries", [1, 2, 3])
def test_sql_counter_with_fixture_with_repeat_checks_inside_loop(
    sql_counter, num_queries
):
    for i in range(num_queries):
        for _ in range(i):
            df = pd.DataFrame({"a": [1, 2, 3]})
            df = df.merge(df)
            assert len(df) == 3
        sql_counter.expects(query_count=i, join_count=i)


@sql_count_checker(no_check=True)
def test_sql_counter_with_context_manager_inside_loop():
    for _ in range(3):
        with SqlCounter(query_count=1) as sc:
            df = pd.DataFrame({"a": [1, 2, 3]})
            assert len(df) == 3

        with pytest.raises(
            AssertionError, match="SqlCounter is dead and can no longer be used."
        ):
            sc.expects(query_count=0)


@sql_count_checker(no_check=True)
def test_sql_counter_with_multiple_checks(session):
    expected_describe_count = 0
    if not session.reduce_describe_query_enabled and session.sql_simplifier_enabled:
        expected_describe_count = 3
    with SqlCounter(query_count=1, describe_count=expected_describe_count):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert len(df) == 3

    with SqlCounter(query_count=1, describe_count=expected_describe_count):
        df = pd.DataFrame({"b": [4, 5, 6]})
        assert len(df) == 3

    with SqlCounter(query_count=1, describe_count=expected_describe_count):
        df = pd.DataFrame({"c": [7, 8, 9]})
        assert len(df) == 3


@sql_count_checker(no_check=True)
def test_sql_counter_with_context_manager_outside_loop(session):
    expected_describe_count = 0
    if not session.reduce_describe_query_enabled and session.sql_simplifier_enabled:
        expected_describe_count = 9
    sc = SqlCounter(query_count=3, describe_count=expected_describe_count)
    sc.__enter__()
    for _ in range(3):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert len(df) == 3
    sc.__exit__(None, None, None)


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_sql_counter_with_df_udtf_count():
    df = pd.DataFrame([[1, 2], [3, 4]]).apply(lambda x: str(type(x)), axis=1, raw=True)
    assert len(df) == 2


@sql_count_checker(query_count=4, udf_count=1)
def test_sql_counter_with_series_udf_count():
    df = pd.Series([1, 2, 3, None]).apply(lambda x: x + 1)
    assert len(df.to_pandas()) == 4


@sql_count_checker(
    query_count=11,
    high_count_expected=True,
    high_count_reason="This test validates high_count_reason",
)
def test_high_sql_count_pass():
    for i in range(11):
        df = pd.DataFrame({"a": list(range(i))})
        assert len(df) == i


def test_sql_count_with_joins():
    thread_id = threading.get_ident()
    with SqlCounter(query_count=1, join_count=1) as sql_counter:
        sql_counter._notify(
            QueryRecord(
                query_id="1", sql_text="SELECT A FROM X JOIN Y", thread_id=thread_id
            )
        )

    with SqlCounter(query_count=1, join_count=2) as sql_counter:
        sql_counter._notify(
            QueryRecord(
                query_id="1",
                sql_text="SELECT A FROM X JOIN Y JOIN Z",
                thread_id=thread_id,
            )
        )

    with SqlCounter(query_count=2, join_count=5) as sql_counter:
        sql_counter._notify(
            QueryRecord(
                query_id="1",
                sql_text="SELECT A FROM X JOIN Y JOIN Z",
                thread_id=thread_id,
            )
        )
        sql_counter._notify(
            QueryRecord(
                query_id="2",
                sql_text="SELECT A FROM X JOIN Y JOIN Z JOIN W",
                thread_id=thread_id,
            )
        )


def test_sql_count_by_query_substr():
    with SqlCounter(query_count=1) as sql_counter:
        sql_counter._notify(
            QueryRecord(
                query_id="1",
                sql_text="SELECT A FROM X JOIN Y JOIN W",
                thread_id=threading.get_ident(),
            )
        )

        assert sql_counter._count_by_query_substr(contains=[" JOIN "]) == 1
        assert (
            sql_counter._count_by_query_substr(
                starts_with=["SELECT"], contains=[" JOIN "]
            )
            == 1
        )
        assert (
            sql_counter._count_instances_by_query_substr(
                starts_with=["FOO"], contains=[" JOIN "]
            )
            == 0
        )


def test_sql_count_instances_by_query_substr():
    with SqlCounter(query_count=1) as sql_counter:
        sql_counter._notify(
            QueryRecord(
                query_id="1",
                sql_text="SELECT A FROM X JOIN Y JOIN W",
                thread_id=threading.get_ident(),
            )
        )

        assert sql_counter._count_instances_by_query_substr(contains=[" JOIN "]) == 2
        assert (
            sql_counter._count_instances_by_query_substr(
                starts_with=["SELECT"], contains=[" JOIN "]
            )
            == 2
        )
        assert (
            sql_counter._count_instances_by_query_substr(
                starts_with=["FOO"], contains=[" JOIN "]
            )
            == 0
        )


@pytest.mark.xfail(
    reason="We expect this to fail, but we don't treat as a hard failure since it is validating expect_high_count=False",
    strict=True,
)
@sql_count_checker(query_count=11)
def test_high_sql_count_fail():
    for i in range(11):
        df = pd.DataFrame({"a": list(range(i))})
        assert len(df) == i


@pytest.mark.xfail(
    reason="We expect this to fail since no high_count_reason was provided",
    strict=True,
)
@sql_count_checker(query_count=11, high_count_expected=True)
def test_high_sql_count_expect_high_count_no_reason():
    for i in range(11):
        df = pd.DataFrame({"a": list(range(i))})
        assert len(df) == i


class TestSqlCounterNotRequiredOrCheckedForStrictXfailedTest:
    """
    If we do a strict xfail without specifying the exception type, the
    exception about the missing SQL counter will satisfy the xfail, but
    we don't want to get that exception. Instead, check that we can find
    and expect the custom exception that the test function itself should
    raise.
    """

    @pytest.mark.xfail(raises=CustomException, strict=True)
    def test_counter_not_required(self):
        raise CustomException

    @pytest.mark.xfail(raises=CustomException, strict=True)
    @sql_count_checker(query_count=1)
    def test_inaccurate_counter_not_checked(self):
        raise CustomException


@pytest.mark.parametrize(
    "expected_query_count",
    [0, 1],
    ids=["innacurate_query_count", "accurate_query_count"],
)
@sql_count_checker(query_count=0)
def test_exception_propagates_through_sql_counter_snow_1042244(expected_query_count):
    # look for a custom exception class so we can be sure we're observing the
    # exception we're raising within the body of the SqlCounter, and not
    # an exception that comes from the SqlCounter itself.
    with pytest.raises(CustomException):
        with SqlCounter(query_count=expected_query_count):
            raise CustomException
