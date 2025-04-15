#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import datetime

import pytest

from snowflake.connector.options import installed_pandas
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
    lit,
    max as max_,
    mean,
    min as min_,
    when_matched,
    when_not_matched,
)
from snowflake.snowpark.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)
from tests.utils import IS_IN_STORED_PROC, TestData, Utils

table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table_name3 = Utils.random_name_for_temp_object(TempObjectType.TABLE)


def test_update_rows_in_table(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    table = session.table(table_name)
    assert table.update({"b": 0}, col("a") == 1) == UpdateResult(2, 0)
    Utils.check_answer(
        table, [Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)]
    )

    assert table.update(
        {"b": 0}, col("a") == 1, statement_params={"SF_PARTNER": "FAKE_PARTNER"}
    ) == UpdateResult(2, 0)
    Utils.check_answer(
        table, [Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)]
    )

    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    assert table.update({"a": 1, "b": 0}) == UpdateResult(6, 0)
    Utils.check_answer(
        table, [Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0)]
    )

    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    assert table.update({"b": col("a") + col("b")}) == UpdateResult(6, 0)
    Utils.check_answer(
        table, [Row(1, 2), Row(1, 3), Row(2, 3), Row(2, 4), Row(3, 4), Row(3, 5)]
    )

    df = session.createDataFrame([1])
    with pytest.raises(AssertionError) as ex_info:
        table.update({"b": 0}, source=df)
    assert "condition should also be provided if source is provided" in str(ex_info)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1019159 ERROR_ON_NONDETERMINISTIC_UPDATE is not supported in Local Testing",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Cannot alter session in SP",
)
def test_update_rows_nondeterministic_update(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    table = session.table(table_name)
    session.sql("alter session set ERROR_ON_NONDETERMINISTIC_UPDATE = true").collect()
    try:
        assert table.update({"b": 0}, col("a") == 1) == UpdateResult(2)
    finally:
        session.sql("alter session unset ERROR_ON_NONDETERMINISTIC_UPDATE").collect()


def test_delete_rows_in_table(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    table = session.table(table_name)
    assert table.delete((col("a") == 1) & (col("b") == 2)) == DeleteResult(1)
    Utils.check_answer(table, [Row(1, 1), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)])

    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    assert table.delete() == DeleteResult(6)
    Utils.check_answer(table, [])

    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    assert table.delete(
        statement_params={"SF_PARTNER": "FAKE_PARTNER"}
    ) == DeleteResult(6)
    Utils.check_answer(table, [])

    df = session.createDataFrame([1])
    with pytest.raises(AssertionError) as ex_info:
        table.delete(source=df)
    assert "condition should also be provided if source is provided" in str(ex_info)


def test_update_with_join(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t2.update({"n": 0}, t1.a == t2.n, t1) == UpdateResult(3, 3)
    Utils.check_answer(
        t2,
        [Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")],
        sort=False,
    )

    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
    assert t2.update({"n": 0}, t2.L == sd.c, sd) == UpdateResult(4, 0)
    Utils.check_answer(
        t2,
        [Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")],
    )


def test_update_with_join_involving_ambiguous_columns(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    TestData.test_data3(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t1.update({"a": 0}, t1["a"] == t2["a"], t2) == UpdateResult(4, 0)
    Utils.check_answer(
        t1,
        [Row(0, 1), Row(0, 2), Row(0, 1), Row(0, 2), Row(3, 1), Row(3, 2)],
        sort=False,
    )

    TestData.upper_case_data(session).write.save_as_table(
        table_name3, mode="overwrite", table_type="temporary"
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
    tmp.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    src = session.createDataFrame([(0, 11), (0, 12), (0, 13)], schema=["k", "v"])
    target = session.table(table_name)
    b = src.groupBy("k").agg(min_(col("v")).as_("v"))
    assert target.update({"v": b["v"]}, target["k"] == b["k"], b) == UpdateResult(1, 0)
    Utils.check_answer(target, [Row(0, 11)])


def test_delete_with_join(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t2.delete(t1.a == t2.n, t1) == DeleteResult(3)
    Utils.check_answer(t2, [Row(4, "D"), Row(5, "E"), Row(6, "F")], sort=False)

    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
    assert t2.delete(t2.L == sd.c, sd) == DeleteResult(4)
    Utils.check_answer(t2, [Row(3, "C"), Row(6, "F")])


def test_delete_with_join_involving_ambiguous_columns(session):
    TestData.test_data2(session).write.save_as_table(
        table_name, mode="overwrite", table_type="temporary"
    )
    TestData.test_data3(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t1 = session.table(table_name)
    t2 = session.table(table_name2)
    assert t1.delete(t1["a"] == t2["a"], t2) == DeleteResult(4)
    Utils.check_answer(t1, [Row(3, 1), Row(3, 2)], sort=False)

    TestData.upper_case_data(session).write.save_as_table(
        table_name3, mode="overwrite", table_type="temporary"
    )
    up = session.table(table_name3)
    sd = session.createDataFrame(["A", "B", "D", "E"], schema=["L"])
    assert up.delete(up.L == sd.L, sd) == DeleteResult(4)
    Utils.check_answer(up, [Row(3, "C"), Row(6, "F")])


def test_delete_with_join_with_aggregated_source_data(session):
    tmp = session.createDataFrame([(0, 1), (0, 2), (0, 3)], schema=["k", "v"])
    tmp.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    src = session.createDataFrame([(0, 1), (0, 2), (0, 3)], schema=["k", "v"])
    target = session.table(table_name)
    b = src.groupBy("k").agg(mean(col("v")).as_("v"))
    assert target.delete(target["v"] == b["v"], b) == DeleteResult(1)
    Utils.check_answer(target, [Row(0, 1), Row(0, 3)])


def test_merge_with_update_clause_only(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])

    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_matched().update({"desc": source["desc"]})],
    ) == MergeResult(0, 2, 0)
    Utils.check_answer(target, [Row(10, "new"), Row(10, "new"), Row(11, "old")])

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_matched(target["desc"] == "old").update({"desc": source["desc"]})],
    ) == MergeResult(0, 1, 0)
    Utils.check_answer(target, [Row(10, "new"), Row(10, "too_old"), Row(11, "old")])

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_matched(target["desc"] == "old").update({"desc": source["desc"]})],
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    ) == MergeResult(0, 1, 0)
    Utils.check_answer(target, [Row(10, "new"), Row(10, "too_old"), Row(11, "old")])


def test_merge_with_delete_clause_only(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])

    assert target.merge(
        source, target["id"] == source["id"], [when_matched().delete()]
    ) == MergeResult(0, 0, 2)
    Utils.check_answer(target, [Row(11, "old")])

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_matched(target["desc"] == "old").delete()],
    ) == MergeResult(0, 0, 1)
    Utils.check_answer(target, [Row(10, "too_old"), Row(11, "old")])


def test_merge_with_insert_clause_only(session, local_testing_mode):
    target_df = session.createDataFrame(
        [(10, "old"), (11, "new")], schema=["id", "desc"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame([(12, "old"), (12, "new")], schema=["id", "desc"])

    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_not_matched().insert({"id": source["id"], "desc": source["desc"]})],
    ) == MergeResult(2, 0, 0)
    Utils.check_answer(
        target, [Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")]
    )

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    assert target.merge(
        source,
        target["id"] == source["id"],
        [when_not_matched().insert([source["id"], source["desc"]])],
    ) == MergeResult(2, 0, 0)
    Utils.check_answer(
        target, [Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")]
    )

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    assert target.merge(
        source,
        target["id"] == source["id"],
        [
            when_not_matched(source.desc == "new").insert(
                {"id": source["id"], "desc": source["desc"]}
            )
        ],
    ) == MergeResult(1, 0, 0)
    Utils.check_answer(target, [Row(10, "old"), Row(11, "new"), Row(12, "new")])

    fixed_datetime = datetime.datetime(2024, 7, 18, 12, 12, 12)
    insert_datetime = datetime.datetime(2024, 8, 13, 10, 1, 50)
    target_df = session.create_dataframe(
        [("id1", fixed_datetime, fixed_datetime.date(), fixed_datetime.time())],
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("col_datetime", TimestampType(TimestampTimeZone.NTZ)),
                StructField("col_date", DateType()),
                StructField("col_time", TimeType()),
            ]
        ),
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source_df = session.create_dataframe(
        [
            ("id1"),
            ("id2"),
            ("id3"),
        ],
        schema=StructType([StructField("id", StringType())]),
    )
    assert target.merge(
        source_df,
        target["id"] == source_df["id"],
        [
            when_not_matched().insert(
                {
                    "id": source_df["id"],
                    "col_datetime": insert_datetime,
                    "col_date": insert_datetime.date(),
                    "col_time": insert_datetime.time(),
                }
            )
        ],
    ) == MergeResult(2, 0, 0)

    Utils.check_answer(
        target,
        [
            Row("id1", fixed_datetime, fixed_datetime.date(), fixed_datetime.time()),
            Row("id2", insert_datetime, insert_datetime.date(), insert_datetime.time()),
            Row("id3", insert_datetime, insert_datetime.date(), insert_datetime.time()),
        ],
    )


def test_merge_with_matched_and_not_matched_clauses(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame(
        [(10, "new"), (12, "new"), (13, "old")], schema=["id", "desc"]
    )

    assert target.merge(
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
    ) == MergeResult(2, 1, 1)
    Utils.check_answer(
        target, [Row(10, "new"), Row(11, "old"), Row(12, "new"), Row(13, "new")]
    )


def test_merge_with_aggregated_source(session):
    target_df = session.createDataFrame([(0, 10)], schema=["k", "v"])
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source_df = session.createDataFrame([(0, 10), (0, 11), (0, 12)], schema=["k", "v"])
    source = source_df.groupBy("k").agg(max_(col("v")).as_("v"))

    assert target.merge(
        source,
        target["k"] == source["k"],
        [
            when_matched().update({"v": source["v"]}),
            when_not_matched().insert({"k": source["k"], "v": source["v"]}),
        ],
    ) == MergeResult(0, 1, 0)
    Utils.check_answer(target, [Row(0, 12)])


def test_merge_with_multiple_clause_conditions(session):
    schema = StructType(
        [StructField("k", IntegerType()), StructField("v", IntegerType())]
    )
    target_df = session.createDataFrame(
        [(0, 10), (1, 11), (2, 12), (3, 13)], schema=schema
    )

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame(
        [(0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26), (7, 27)],
        schema=schema,
    )

    assert target.merge(
        source,
        target["k"] == source["k"],
        [
            when_matched(source["v"] < 21).delete(),
            when_matched(source["v"] > 22).update({"v": (source["v"] - 20)}),
            when_matched(source["v"] != 21).delete(),
            when_matched().update({"v": source["v"]}),
            when_not_matched(source["v"] < 25).insert([source["k"], source["v"] - 20]),
            when_not_matched(source["v"] > 26).insert({"k": source["k"]}),
            when_not_matched(source["v"] != 25).insert({"v": source["v"]}),
            when_not_matched().insert({"k": source["k"], "v": source["v"]}),
        ],
    ) == MergeResult(4, 2, 2)

    Utils.check_answer(
        target,
        [Row(1, 21), Row(3, 3), Row(4, 4), Row(5, 25), Row(7, None), Row(None, 26)],
    )


def test_copy(session):
    df = session.createDataFrame([1, 2], schema=["a"])
    df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
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


def test_update_clause_negative(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])
    match_clause = when_matched()
    match_clause._clause = True

    with pytest.raises(SnowparkTableException) as ex_info:
        target.merge(
            source,
            target["id"] == source["id"],
            [match_clause.update({"desc": source["desc"]})],
        )
    assert ex_info.value.error_code == "1115"


def test_merge_clause_negative(session):
    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )
    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)
    source = session.createDataFrame([(10, "new")], schema=["id", "desc"])

    with pytest.raises(TypeError) as ex_info:
        target.merge(
            source,
            target["id"] == source["id"],
            [None],
        )
    assert "clauses only accepts WhenMatchedClause or WhenNotMatchedClause" in str(
        ex_info
    )


def test_update_with_large_dataframe(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t2 = session.table(table_name2)

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
        assert t2.update({"n": 0}, t2.L == sd.c, sd) == UpdateResult(4, 0)
        Utils.check_answer(
            t2,
            [
                Row(0, "A"),
                Row(0, "B"),
                Row(0, "D"),
                Row(0, "E"),
                Row(3, "C"),
                Row(6, "F"),
            ],
        )
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


def test_delete_with_large_dataframe(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    TestData.upper_case_data(session).write.save_as_table(
        table_name2, mode="overwrite", table_type="temporary"
    )
    t2 = session.table(table_name2)

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        sd = session.createDataFrame(["A", "B", "D", "E"], schema=["c"])
        assert t2.delete(t2.L == sd.c, sd) == DeleteResult(4)
        Utils.check_answer(t2, [Row(3, "C"), Row(6, "F")])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


def test_merge_with_large_dataframe(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    target_df = session.createDataFrame(
        [(10, "old"), (10, "too_old"), (11, "old")], schema=["id", "desc"]
    )

    target_df.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)

    original_value = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        source = session.createDataFrame(
            [(10, "new"), (12, "new"), (13, "old")], schema=["id", "desc"]
        )
        assert target.merge(
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
        ) == MergeResult(2, 1, 1)
        Utils.check_answer(
            target, [Row(10, "new"), Row(11, "old"), Row(12, "new"), Row(13, "new")]
        )
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_value


def test_update_with_join_involving_null_values(session):
    t1_name = Utils.random_table_name()
    t2_name = Utils.random_table_name()
    session.create_dataframe(
        [
            [
                1,
                "one",
                None,
            ],
            [2, "two", None],
        ],
        schema=["num", "en", "fr"],
    ).write.save_as_table(t1_name, table_type="temp")
    session.create_dataframe(
        [[1, "un"], [2, "deux"]], schema=["num", "fr"]
    ).write.save_as_table(t2_name, table_type="temp")

    t1 = session.table(t1_name)
    t2 = session.table(t2_name)
    assert t1.update({"fr": t2["fr"]}, t1["num"] == t2["num"], t2) == UpdateResult(2, 0)
    Utils.check_answer(
        t1,
        [Row(1, "one", "un"), Row(2, "two", "deux")],
    )


def test_merge_multi_operation(session):
    target = session.create_dataframe([[1, "a"]], schema=["id", "name"])
    target.write.save_as_table("target", mode="overwrite")
    target = session.table("target")

    source = session.create_dataframe([[2, "b"]], schema=["id", "name"])
    # update + insert
    target.merge(
        source,
        target["id"] == source["id"],
        [
            when_matched().update({"name": source["name"]}),
            when_not_matched().insert({"id": source["id"], "name": source["name"]}),
        ],
    )
    assert target.sort(col("id")).collect() == [Row(1, "a"), Row(2, "b")]

    # delete + insert
    target.merge(
        source,
        target["id"] == source["id"],
        [
            when_matched().delete(),
            when_not_matched().insert({"id": source["id"], "name": source["name"]}),
        ],
    )
    assert target.sort(col("id")).collect() == [Row(1, "a")]


@pytest.mark.skipif(
    not installed_pandas,
    reason="Test requires pandas.",
)
def test_snow_1694649_repro_merge_with_equal_null(session):
    # Force temp table
    import pandas as pd

    df1 = session.create_dataframe(pd.DataFrame({"A": [0, 1], "B": ["a", "b"]}))
    df2 = session.create_dataframe(pd.DataFrame({"A": [0, 1], "B": ["a", "c"]}))

    df1.merge(
        source=df2,
        join_expr=df1["A"].equal_null(df2["A"]),
        clauses=[
            when_matched(
                ~(df1["A"].equal_null(df2["A"])) & (df1["B"].equal_null(df2["B"]))
            ).update({"A": lit(3)})
        ],
    )
    assert session.table(df1.table_name).order_by("A").collect() == [
        Row(0, "a"),
        Row(1, "b"),
    ]


def test_update_with_variant_type(session):
    # map type
    df1 = session.create_dataframe(
        [(1, 1, {}), (2, 1, {}), (2, 2, {})], schema=["a", "b", "c"]
    )
    df1.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)

    target.update({"c": {1: 2}}, target["a"] == 1)
    assert target.collect() == [
        Row(1, 1, '{\n  "1": 2\n}'),
        Row(2, 1, "{}"),
        Row(2, 2, "{}"),
    ]

    target.update({"c": {"a": "b"}}, target["c"] == {})
    assert target.collect() == [
        Row(1, 1, '{\n  "1": 2\n}'),
        Row(2, 1, '{\n  "a": "b"\n}'),
        Row(2, 2, '{\n  "a": "b"\n}'),
    ]

    # array type
    df2 = session.create_dataframe(
        [(1, 1, []), (2, 1, []), (2, 2, [])], schema=["a", "b", "c"]
    )
    df2.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)

    target.update({"c": [1, "a"]}, target["a"] == 1)
    assert target.collect() == [
        Row(1, 1, '[\n  1,\n  "a"\n]'),
        Row(2, 1, "[]"),
        Row(2, 2, "[]"),
    ]

    target.update({"c": [1, "a"]}, target["c"] == [])
    assert target.collect() == [
        Row(1, 1, '[\n  1,\n  "a"\n]'),
        Row(2, 1, '[\n  1,\n  "a"\n]'),
        Row(2, 2, '[\n  1,\n  "a"\n]'),
    ]

    # variant type
    df3 = session.create_dataframe(
        [(1, 1, []), (2, 1, {}), (2, 2, [])],
        schema=StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", IntegerType()),
                StructField("c", VariantType()),
            ]
        ),
    )
    df3.write.save_as_table(table_name, mode="overwrite", table_type="temporary")
    target = session.table(table_name)

    target.update({"c": [1, "a"]}, target["a"] == 1)
    assert target.collect() == [
        Row(1, 1, '[\n  1,\n  "a"\n]'),
        Row(2, 1, "{}"),
        Row(2, 2, "[]"),
    ]

    target.update({"c": {"a": "b"}}, target["c"] == [])
    assert target.collect() == [
        Row(1, 1, '[\n  1,\n  "a"\n]'),
        Row(2, 1, "{}"),
        Row(2, 2, '{\n  "a": "b"\n}'),
    ]


@pytest.mark.skipif(
    not installed_pandas,
    reason="Test requires pandas.",
)
def test_snow_1707286_repro_merge_with_(session):
    import pandas as pd

    df1 = session.create_dataframe(pd.DataFrame({"A": [1, 2, 3, 4, 5]}))
    df2 = session.create_dataframe(pd.DataFrame({"A": [3, 4]}))

    table = df1.where(col("A") > 2).cache_result()

    table.update(
        assignments={"A": lit(9)},
        condition=table["A"] == df2["A"],
        source=df2,
    )
    assert table.to_pandas().eq(pd.DataFrame({"A": [9, 9, 5]})).all().item()


def test_merge_into_empty_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    schema = StructType([StructField("id", IntegerType())])

    try:
        df = session.create_dataframe([], schema=schema)
        df.write.save_as_table(table_name, mode="overwrite")
        table = session.table(table_name)
        df_to_append = session.create_dataframe([1], schema=schema)

        table.merge(
            df_to_append,
            lit(literal=False),
            [
                when_not_matched().insert(df_to_append),
            ],
        )
    finally:
        Utils.drop_table(session, table_name)
