#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import DeleteResult, Row, UpdateResult
from snowflake.snowpark.functions import col, mean, min as min_
from tests.utils import TestData, Utils

table_name = Utils.random_name()
table_name2 = Utils.random_name()
table_name3 = Utils.random_name()


def test_update_rows_in_table(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    table = session.table(table_name)
    assert table.update({"b": 0}, col("a") == 1) == UpdateResult(2, 0)
    Utils.check_answer(
        table, [Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)]
    )

    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    assert table.update({"a": 1, "b": 0}) == UpdateResult(6, 0)
    Utils.check_answer(
        table, [Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0)]
    )

    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    assert table.update({"b": col("a") + col("b")}) == UpdateResult(6, 0)
    Utils.check_answer(
        table, [Row(1, 2), Row(1, 3), Row(2, 3), Row(2, 4), Row(3, 4), Row(3, 5)]
    )

    df = session.createDataFrame([1])
    with pytest.raises(AssertionError) as ex_info:
        table.update({"b": 0}, source_data=df)
    assert "condition should also be provided if source_data is provided" in str(
        ex_info
    )


def test_delete_rows_in_table(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    table = session.table(table_name)
    assert table.delete((col("a") == 1) & (col("b") == 2)) == DeleteResult(1)
    Utils.check_answer(table, [Row(1, 1), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)])

    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    assert table.delete() == DeleteResult(6)
    Utils.check_answer(table, [])

    df = session.createDataFrame([1])
    with pytest.raises(AssertionError) as ex_info:
        table.delete(source_data=df)
    assert "condition should also be provided if source_data is provided" in str(
        ex_info
    )


def test_update_with_join(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    TestData.upper_case_data(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t2.update({"n": 0}, t1.a == t2.n, t1) == UpdateResult(3, 3)
    Utils.check_answer(
        t2,
        [Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")],
        sort=False,
    )

    TestData.upper_case_data(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
    assert t2.update({"n": 0}, t2.L == sd.c, sd) == UpdateResult(4, 0)
    Utils.check_answer(
        t2,
        [Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")],
    )


def test_update_with_join_involving_ambiguous_columns(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    TestData.test_data3(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t1.update({"a": 0}, t1["a"] == t2["a"], t2) == UpdateResult(4, 0)
    Utils.check_answer(
        t1,
        [Row(0, 1), Row(0, 2), Row(0, 1), Row(0, 2), Row(3, 1), Row(3, 2)],
        sort=False,
    )

    TestData.upper_case_data(session).write.saveAsTable(
        table_name3, mode="overwrite", create_temp_table=True
    )
    up = session.table(table_name3)
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["L"])
    assert up.update({"n": 0}, up.L == sd.L, sd) == UpdateResult(4, 0)
    Utils.check_answer(
        up,
        [Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")],
    )


def test_update_with_join_with_aggregated_source_data(session):
    tmp = session.createDataFrame([[0, 10]], schema=["k", "v"])
    tmp.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    src = session.createDataFrame([(0, 11), (0, 12), (0, 13)], schema=["k", "v"])
    target = session.table(table_name)
    b = src.groupBy("k").agg(min_(col("v")).as_("v"))
    assert target.update({"v": b["v"]}, target["k"] == b["k"], b) == UpdateResult(1, 0)
    Utils.check_answer(target, [Row(0, 11)])


def test_delete_with_join(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    TestData.upper_case_data(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t2.delete(t1.a == t2.n, t1) == DeleteResult(3)
    Utils.check_answer(t2, [Row(4, "D"), Row(5, "E"), Row(6, "F")], sort=False)

    TestData.upper_case_data(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
    assert t2.delete(t2.L == sd.c, sd) == DeleteResult(4)
    Utils.check_answer(t2, [Row(3, "C"), Row(6, "F")])


def test_delete_with_join_involving_ambiguous_columns(session):
    TestData.test_data2(session).write.saveAsTable(
        table_name, mode="overwrite", create_temp_table=True
    )
    TestData.test_data3(session).write.saveAsTable(
        table_name2, mode="overwrite", create_temp_table=True
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t1.delete(t1["a"] == t2["a"], t2) == DeleteResult(4)
    Utils.check_answer(t1, [Row(3, 1), Row(3, 2)], sort=False)

    TestData.upper_case_data(session).write.saveAsTable(
        table_name3, mode="overwrite", create_temp_table=True
    )
    up = session.table(table_name3)
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["L"])
    assert up.delete(up.L == sd.L, sd) == DeleteResult(4)
    Utils.check_answer(up, [Row(3, "C"), Row(6, "F")])


def test_delete_with_join_with_aggregated_source_data(session):
    tmp = session.createDataFrame([(0, 1), (0, 2), (0, 3)], schema=["k", "v"])
    tmp.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    src = session.createDataFrame([(0, 1), (0, 2), (0, 3)], schema=["k", "v"])
    target = session.table(table_name)
    b = src.groupBy("k").agg(mean(col("v")).as_("v"))
    assert target.delete(target["v"] == b["v"], b) == DeleteResult(1)
    Utils.check_answer(target, [Row(0, 1), Row(0, 3)])
