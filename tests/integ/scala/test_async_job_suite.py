#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import logging
from collections.abc import Iterator
from time import sleep, time

import pytest

try:
    import pandas as pd
    from pandas.testing import assert_frame_equal

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, when_matched, when_not_matched
from snowflake.snowpark.table import DeleteResult, MergeResult, UpdateResult
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils

test_file_csv = "testCSV.csv"
tmp_stage_name1 = Utils.random_stage_name()


def test_async_collect_common(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.collect_nowait()
    res = async_job.result()
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
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    ).filter(col("b") > 100)
    async_job = df.collect_nowait()
    res = async_job.result()
    Utils.check_answer(
        res,
        [],
    )


def test_async_to_local_iterator_common(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.to_local_iterator(block=False)
    res = async_job.result()
    expected_res = df.to_local_iterator()
    for r, e_r in zip(res, expected_res):
        Utils.check_answer(r, e_r)


def test_async_to_local_iterator_empty_result(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    ).filter(col("b") > 100)
    async_job = df.to_local_iterator(block=False)
    res = async_job.result()
    expected_res = df.to_local_iterator()
    for r, e_r in zip(res, expected_res):
        Utils.check_answer(r, e_r)


@pytest.mark.skipif(not is_pandas_available, reason="Pandas is not available")
def test_async_to_pandas_common(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.to_pandas(block=False)
    res = async_job.result()
    expected_res = df.to_pandas()
    Utils.check_answer(
        session.create_dataframe(res), session.create_dataframe(expected_res)
    )


@pytest.mark.skipif(not is_pandas_available, reason="Pandas is not available")
def test_async_to_pandas_batches(session):
    df = session.range(100000).cache_result()
    async_job = df.to_pandas_batches(block=False)
    res = list(async_job.result())
    expected_res = list(df.to_pandas_batches())
    assert len(res) > 0
    assert len(expected_res) > 0
    for r, er in zip(res, expected_res):
        assert_frame_equal(r, er)
        break


@pytest.mark.skipif(not is_pandas_available, reason="Pandas is not available")
def test_async_to_pandas_empty_result(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    ).filter(col("b") > 100)
    async_job = df.to_pandas(block=False)
    res = async_job.result()
    expected_res = df.to_pandas()
    assert res.values.tolist() == expected_res.values.tolist()


def test_async_job_negative(session):
    # async collect negative case
    df = session.sql("select to_number('not_a_number')")
    async_job = df.collect_nowait()
    with pytest.raises(DatabaseError) as ex_info:
        async_job.result()
    assert "100038: Numeric value 'not_a_number' is not recognized" in str(
        ex_info.value
    ) or f"Status of query '{async_job.query_id}' is FAILED_WITH_ERROR, results are unavailable" in str(
        ex_info.value
    )


def test_async_count(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.count(block=False)
    assert async_job.result() == 4


def test_async_first(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.first(block=False)
    res = async_job.result()
    Utils.check_answer(res, [Row(A=float("nan"), B=3, C=5)])

    async_job = df.first(n=3, block=False)
    res = async_job.result()
    Utils.check_answer(
        res,
        [
            Row(A=float("nan"), B=3, C=5),
            Row(A=2.0, B=-4, C=7),
            Row(A=3.0, B=5, C=6),
        ],
    )


def test_async_table_operations(session):
    table_name = Utils.random_table_name()
    # merge operation
    schema = StructType(
        [StructField("key", IntegerType()), StructField("value", StringType())]
    )
    target_df = session.create_dataframe(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=schema
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.create_dataframe(
        [(10, "new"), (12, "new"), (13, "old")], schema=schema
    )
    res = target.merge(
        source,
        target["key"] == source["key"],
        [
            when_matched().update({"value": source["value"]}),
            when_not_matched().insert({"key": source["key"]}),
        ],
        block=False,
    )
    assert res.result() == MergeResult(rows_inserted=2, rows_updated=2, rows_deleted=0)
    Utils.check_answer(
        target,
        [
            Row(KEY=13, VALUE=None),
            Row(KEY=12, VALUE=None),
            Row(KEY=10, VALUE="new"),
            Row(KEY=10, VALUE="new"),
            Row(KEY=11, VALUE="old"),
        ],
    )
    # delete operation
    target_df = session.create_dataframe(
        [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)], schema=["a", "b"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    res = target.delete(target["a"] == 1, block=False)
    assert res.result() == DeleteResult(rows_deleted=2)
    Utils.check_answer(
        target, [Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]
    )

    # update operation
    target_df = session.create_dataframe(
        [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)], schema=["a", "b"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    res = target.update({"b": 0, "a": target.a + target.b}, block=False)
    assert res.result() == UpdateResult(rows_updated=6, multi_joined_rows_updated=0)
    Utils.check_answer(
        target,
        [
            Row(A=2, B=0),
            Row(A=3, B=0),
            Row(A=3, B=0),
            Row(A=4, B=0),
            Row(A=4, B=0),
            Row(A=5, B=0),
        ],
    )


def test_async_save_as_table(session):
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    async_job = df.write.save_as_table(table_name, create_temp_table=True, block=False)
    while not async_job.is_done():
        sleep(1)
    assert async_job.result() is None
    table_df = session.table(table_name)
    Utils.check_answer(table_df, df)


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Requires stage access")
def test_async_copy_into_location(session):
    remote_location = f"{session.get_session_stage()}/{random_name_for_temp_object(TempObjectType.TABLE)}.csv"
    df = session.create_dataframe(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    test_schema = StructType(
        [
            StructField("a", DoubleType()),
            StructField("b", IntegerType()),
            StructField("c", IntegerType()),
        ]
    )
    # check if copy is successful
    async_job = df.write.copy_into_location(remote_location, block=False)
    res = async_job.result()
    assert res[0].rows_unloaded == 4

    # check the content of copied table
    res = session.read.schema(test_schema).csv(remote_location)
    Utils.check_answer(res, df)


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Requires stage access")
@pytest.mark.skipif(not is_pandas_available, reason="to_pandas requires pandas")
def test_multiple_queries(session, resources_path):
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name1, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_csv, compress=False
    )
    df = session.read.schema(user_schema).csv(f"@{tmp_stage_name1}/{test_file_csv}")
    assert len(df._plan.queries) > 1
    async_job = df.collect_nowait()
    Utils.check_answer(async_job.result(), df)

    Utils.check_answer(session.create_dataframe(df.to_pandas(block=False).result()), df)
    Utils.check_answer(
        session.create_dataframe(next(df.to_pandas_batches(block=False).result())), df
    )

    # make sure temp object is dropped
    temp_object = async_job._post_actions[0].sql.split(" ")[-1]
    with pytest.raises(SnowparkSQLException, match="does not exist or not authorized"):
        session.sql(f"drop file format {temp_object}").collect()


def test_async_batch_insert(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    # create dataframe (large data)
    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df = session.create_dataframe([[1, 2], [1, 3], [4, 4]], schema=["a", "b"])
        async_job = df.collect_nowait()
        Utils.check_answer(
            async_job.result(), [Row(A=1, B=2), Row(A=4, B=4), Row(A=1, B=3)]
        )

        # make sure temp object is dropped
        temp_object = async_job._post_actions[0].sql.split(" ")[-1]
        with pytest.raises(
            SnowparkSQLException, match="does not exist or not authorized"
        ):
            session.sql(f"drop table {temp_object}").collect()

    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="TODO(SNOW-932722): Cancel query is not allowed in stored proc",
)
def test_async_is_running_and_cancel(session):
    async_job = session.sql("select SYSTEM$WAIT(3)").collect_nowait()
    while not async_job.is_done():
        sleep(1.0)
    assert async_job.is_done()

    # set 20s to avoid flakiness
    async_job2 = session.sql("select SYSTEM$WAIT(20)").collect_nowait()
    assert not async_job2.is_done()
    async_job2.cancel()
    start = time()
    while not async_job2.is_done():
        sleep(1.0)
    # If query is canceled, it takes less time than originally needed
    assert (time() - start) < 20
    assert async_job2.is_done()


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Requires large result")
def test_async_place_holder(session):
    exp = session.sql("show functions").where("1=1").collect()
    async_job = session.sql("show functions").where("1=1").collect_nowait()
    Utils.check_answer(async_job.result(), exp)


@pytest.mark.skipif(not is_pandas_available, reason="Pandas is not available")
@pytest.mark.parametrize("create_async_job_from_query_id", [True, False])
def test_create_async_job(session, create_async_job_from_query_id):
    df = session.range(3)
    if create_async_job_from_query_id:
        query_id = df._execute_and_get_query_id()
        async_job = session.create_async_job(query_id)
    else:
        async_job = df.collect_nowait()

    res = async_job.result()
    assert isinstance(res, list)
    assert isinstance(res[0], Row)
    assert res == [Row(0), Row(1), Row(2)]

    res = async_job.result("row")
    assert isinstance(res, list)
    assert isinstance(res[0], Row)

    res = async_job.result("row_iterator")
    assert isinstance(res, Iterator)
    res = list(res)
    assert isinstance(res[0], Row)

    res = async_job.result("pandas")
    assert isinstance(res, pd.DataFrame)

    res = async_job.result("pandas_batches")
    assert isinstance(res, Iterator)
    res = list(res)
    assert isinstance(res[0], pd.DataFrame)

    assert async_job.result("no_result") is None

    with pytest.raises(
        ValueError, match="'invalid_type' is not a valid _AsyncResultType"
    ):
        async_job.result("invalid_type")


def test_create_async_job_negative(session):
    query_id = session.sql("select to_number('not_a_number')").collect_nowait().query_id
    async_job = session.create_async_job(query_id)

    with pytest.raises(DatabaseError) as ex_info:
        async_job.result()
    assert "100038: Numeric value 'not_a_number' is not recognized" in str(
        ex_info.value
    ) or f"Status of query '{query_id}' is FAILED_WITH_ERROR, results are unavailable" in str(
        ex_info.value
    )

    invalid_query_id = "negative_test_invalid_query_id"
    async_job = session.create_async_job(invalid_query_id)
    with pytest.raises(
        ValueError, match=f"Invalid UUID: '{invalid_query_id}'"
    ) as ex_info:
        async_job.result()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="caplog is not supported")
@pytest.mark.parametrize("create_async_job_from_query_id", [True, False])
def test_get_query_from_async_job(session, create_async_job_from_query_id, caplog):
    query_text = "select 1, 2, 3"
    df = session.sql(query_text)
    if create_async_job_from_query_id:
        query_id = df._execute_and_get_query_id()
        async_job = session.create_async_job(query_id)
    else:
        async_job = df.collect_nowait()

    with caplog.at_level(logging.DEBUG):
        if async_job.query is None:
            # query_history might not have the query id right away
            # but there shouldn't be any SQL exception, so check the log
            assert "result is empty" in caplog.text


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="caplog is not supported")
def test_get_query_from_async_job_negative(session, caplog):
    invalid_query_id = "negative_test_invalid_query_id"
    async_job = session.create_async_job(invalid_query_id)

    with caplog.at_level(logging.DEBUG):
        assert async_job.query is None
        assert "result is empty" in caplog.text


@pytest.mark.parametrize("create_async_job_from_query_id", [True, False])
def test_async_job_to_df(session, create_async_job_from_query_id):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    if create_async_job_from_query_id:
        query_id = df._execute_and_get_query_id()
        async_job = session.create_async_job(query_id)
    else:
        async_job = df.collect_nowait()

    new_df = async_job.to_df()
    assert "result_scan" in new_df.queries["queries"][0].lower()
    Utils.check_answer(df, new_df)
