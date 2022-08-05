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
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DoubleType, IntegerType, StructField, StructType
from tests.utils import Utils


def test_async_collect_common(session):
    df = session.createDataFrame(
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
    df = session.createDataFrame(
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
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.to_local_iterator(block=False)
    res = async_job.result()
    expected_res = df.to_local_iterator()
    for r, e_r in zip(res, expected_res):
        Utils.check_answer(r, e_r)


def test_async_to_local_iterator_empty_result(session):
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    ).filter(col("b") > 100)
    async_job = df.to_local_iterator(block=False)
    res = async_job.result()
    expected_res = df.to_local_iterator()
    for r, e_r in zip(res, expected_res):
        Utils.check_answer(r, e_r)


def test_async_to_pandas_common(session):
    df = session.createDataFrame(
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
    df = session.createDataFrame(
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
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    async_job = df.count(block=False)
    assert async_job.result() == 4


def test_async_first(session):
    df = session.createDataFrame(
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
    pass


def test_async_save_as_table(session):
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    async_job = df.write.save_as_table(table_name, create_temp_table=True, block=False)
    assert async_job.result() is None
    table_df = session.table(table_name)
    Utils.check_answer(table_df, df)


def test_async_copy_into_location(session):
    remote_location = f"{session.get_session_stage()}/names.csv"
    df = session.createDataFrame(
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


def test_async_is_running_and_cancel(session):
    pass
