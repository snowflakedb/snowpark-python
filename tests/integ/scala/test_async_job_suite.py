#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from tests.utils import Utils


def test_async_collect_common(session):
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_df = df.collect_nowait()
    res = async_df.result()
    Utils.check_answer(
        res,
        [
            Row(A=float("nan"), B=3, C=5),
            Row(A=2.0, B=-4, C=7),
            Row(A=3.0, B=5, C=6),
            Row(A=4.0, B=6, C=8),
        ],
    )


def test_async_collect_empty_result(session):
    pass


def test_async_to_local_iterator_common(session):
    pass


def test_async_to_local_iterator_empty_result(session):
    pass


def test_async_to_pandas_common(session):
    pass


def test_async_to_pandas_empty_result(session):
    pass


def test_async_job_negative(session):
    pass


def test_async_count(session):
    pass


def test_async_first(session):
    pass


def test_async_table_operations(session):
    pass


def test_async_save_as_table(session):
    pass


def test_async_copy_into_location(session):
    pass


def test_async_is_running_and_cancel(session):
    pass
