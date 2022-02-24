#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import copy

import pytest

from snowflake.snowpark import (
    DeleteResult,
    MergeResult,
    Row,
    Table,
    UpdateResult,
    WhenMatchedClause,
    WhenNotMatchedClause,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkTableException
from snowflake.snowpark.functions import (
    col,
    max as max_,
    mean,
    min as min_,
    when_matched,
    when_not_matched,
)
from tests.utils import TempObjectType, TestData, Utils

table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table_name3 = Utils.random_name_for_temp_object(TempObjectType.TABLE)


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
        table.update({"b": 0}, source=df)
    assert "condition should also be provided if source is provided" in str(ex_info)


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
        table.delete(source=df)
    assert "condition should also be provided if source is provided" in str(ex_info)


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


def test_merge_with_update_clause_only(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])

    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [when_matched().update({"desc": source["desc"]})],
        )
        == MergeResult(0, 2, 0)
    )
    Utils.check_answer(target, [Row(10, "new"), Row(10, "new"), Row(11, "old")])

    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [when_matched(target["desc"] == "old").update({"desc": source["desc"]})],
        )
        == MergeResult(0, 1, 0)
    )
    Utils.check_answer(target, [Row(10, "new"), Row(10, "too_old"), Row(11, "old")])


def test_merge_with_delete_clause_only(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])

    assert target.merge(
        source, target["id"] == source["id"], [when_matched().delete()]
    ) == MergeResult(0, 0, 2)
    Utils.check_answer(target, [Row(11, "old")])

    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [when_matched(target["desc"] == "old").delete()],
        )
        == MergeResult(0, 0, 1)
    )
    Utils.check_answer(target, [Row(10, "too_old"), Row(11, "old")])


def test_merge_with_insert_clause_only(session):
    target_df = session.createDataFrame(
        [(10, "old"), (11, "new")], schema=["id", "desc"]
    )
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source = session.createDataFrame([(12, "old"), (12, "new")], schema=["id", "desc"])

    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [when_not_matched().insert({"id": source["id"], "desc": source["desc"]})],
        )
        == MergeResult(2, 0, 0)
    )
    Utils.check_answer(
        target, [Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")]
    )

    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [when_not_matched().insert([source["id"], source["desc"]])],
        )
        == MergeResult(2, 0, 0)
    )
    Utils.check_answer(
        target, [Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")]
    )

    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [
                when_not_matched(source.desc == "new").insert(
                    {"id": source["id"], "desc": source["desc"]}
                )
            ],
        )
        == MergeResult(1, 0, 0)
    )
    Utils.check_answer(target, [Row(10, "old"), Row(11, "new"), Row(12, "new")])


def test_merge_with_matched_and_not_matched_clauses(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source = session.createDataFrame(
        [(10, "new"), (12, "new"), (13, "old")], schema=["id", "desc"]
    )

    assert (
        target.merge(
            source,
            target["id"] == source["id"],
            [
                when_matched(target["desc"] == "too_old").delete(),
                when_matched().update({"desc": source["desc"]}),
                when_not_matched(source["desc"] == "old").insert(
                    {"id": source["id"], "desc": "new"}
                ),
                when_not_matched().insert({"id": source["id"], "desc": source["desc"]}),
            ],
        )
        == MergeResult(2, 1, 1)
    )
    Utils.check_answer(
        target, [Row(10, "new"), Row(11, "old"), Row(12, "new"), Row(13, "new")]
    )


def test_merge_with_aggregated_source(session):
    target_df = session.createDataFrame([(0, 10)], schema=["k", "v"])
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source_df = session.createDataFrame([(0, 10), (0, 11), (0, 12)], schema=["k", "v"])
    source = source_df.groupBy("k").agg(max_(col("v")).as_("v"))

    assert (
        target.merge(
            source,
            target["k"] == source["k"],
            [
                when_matched().update({"v": source["v"]}),
                when_not_matched().insert({"k": source["k"], "v": source["v"]}),
            ],
        )
        == MergeResult(0, 1, 0)
    )
    Utils.check_answer(target, [Row(0, 12)])


def test_merge_with_multiple_clause_conditions(session):
    target_df = session.createDataFrame(
        [(0, 10), (1, 11), (2, 12), (3, 13)], schema=["k", "v"]
    )
    target_df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    target = session.table(table_name)
    source = session.createDataFrame(
        [(0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26), (7, 27)],
        schema=["k", "v"],
    )

    assert (
        target.merge(
            source,
            target["k"] == source["k"],
            [
                when_matched(source["v"] < 21).delete(),
                when_matched(source["v"] > 22).update({"v": (source["v"] - 20)}),
                when_matched(source["v"] != 21).delete(),
                when_matched().update({"v": source["v"]}),
                when_not_matched(source["v"] < 25).insert(
                    [source["k"], source["v"] - 20]
                ),
                when_not_matched(source["v"] > 26).insert({"k": source["k"]}),
                when_not_matched(source["v"] != 25).insert({"v": source["v"]}),
                when_not_matched().insert({"k": source["k"], "v": source["v"]}),
            ],
        )
        == MergeResult(4, 2, 2)
    )
    Utils.check_answer(
        target,
        [Row(1, 21), Row(3, 3), Row(4, 4), Row(5, 25), Row(7, None), Row(None, 26)],
    )


def test_copy(session):
    df = session.createDataFrame([1, 2], schema=["a"])
    df.write.saveAsTable(table_name, mode="overwrite", create_temp_table=True)
    table = session.table(table_name)
    assert isinstance(table, Table)
    cloned = copy.copy(table)
    assert isinstance(table, Table)
    cloned.delete(col("a") == 1)
    Utils.check_answer(session.table(table_name), [Row(2)])


def test_match_clause_negative(session):
    with pytest.raises(SnowparkTableException) as ex_info:
        WhenMatchedClause().update({}).delete()
    assert "update has been specified for WhenMatchedClause to merge table" in str(
        ex_info
    )
    with pytest.raises(SnowparkTableException) as ex_info:
        WhenNotMatchedClause().insert({}).insert({})
    assert "insert has been specified for WhenNotMatchedClause to merge table" in str(
        ex_info
    )
