#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import col, when_matched, when_not_matched
from snowflake.snowpark.table import DeleteResult, MergeResult, UpdateResult
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import Utils


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
    # merge operation
    target_df = session.create_dataframe(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"]
    )
    target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
    target = session.table("my_table")
    source = session.create_dataframe(
        [(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"]
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
    target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
    target = session.table("my_table")
    res = target.delete(target["a"] == 1, block=False)
    assert res.result() == DeleteResult(rows_deleted=2)
    Utils.check_answer(
        target, [Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]
    )

    # update operation
    target_df = session.create_dataframe(
        [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)], schema=["a", "b"]
    )
    target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
    target = session.table("my_table")
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
    assert async_job.result() is None
    table_df = session.table(table_name)
    Utils.check_answer(table_df, df)


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
    Utils.check_answer(res, [Row(rows_unloaded=4, input_bytes=27, output_bytes=47)])

    # check the content of copied table
    res = session.read.schema(test_schema).csv(remote_location)
    Utils.check_answer(res, df)


def test_multiple_queries(session):
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )
    session.file.put("../../resources/testCSV.csv", session.get_session_stage())
    df = session.read.schema(user_schema).csv(
        session.get_session_stage() + "/testCSV.csv"
    )
    assert len(df.queries) > 1
    res = df.collect_nowait()
    Utils.check_answer(res.result(), df.collect())


def test_async_is_running_and_cancel(session):
    pass
