#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import copy
import re

import pytest

from snowflake.snowpark import DataFrame, Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkJoinException,
    SnowparkSQLAmbiguousJoinException,
    SnowparkSQLInvalidIdException,
)
from snowflake.snowpark.functions import coalesce, col, count, is_null, lit
from tests.utils import Utils


def test_join_using(session):
    df = session.create_dataframe([[i, str(i)] for i in range(1, 4)]).to_df(
        ["int", "str"]
    )
    df2 = session.create_dataframe([[i, str(i + 1)] for i in range(1, 4)]).to_df(
        ["int", "str"]
    )
    assert df.join(df2, "int").collect() == [
        Row(1, "1", "2"),
        Row(2, "2", "3"),
        Row(3, "3", "4"),
    ]


def test_join_using_multiple_columns(session):
    df = session.create_dataframe([[i, i + 1, str(i)] for i in range(1, 4)]).to_df(
        ["int", "int2", "str"]
    )
    df2 = session.create_dataframe([[i, i + 1, str(i + 1)] for i in range(1, 4)]).to_df(
        ["int", "int2", "str"]
    )

    res = df.join(df2, ["int", "int2"]).collect()
    assert sorted(res, key=lambda x: x[0]) == [
        Row(1, 2, "1", "2"),
        Row(2, 3, "2", "3"),
        Row(3, 4, "3", "4"),
    ]


def test_full_outer_join_followed_by_inner_join(session):

    a = session.create_dataframe([[1, 2], [2, 3]]).to_df(["a", "b"])
    b = session.create_dataframe([[2, 5], [3, 4]]).to_df(["a", "c"])
    c = session.create_dataframe([[3, 1]]).to_df(["a", "d"])

    ab = a.join(b, ["a"], "fullouter")
    abc = ab.join(c, "a")
    assert abc.collect() == [Row(3, None, 4, 1)]


def test_limit_with_join(session):
    df = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(
        ["int", "int2", "str"]
    )
    df2 = session.create_dataframe([[1, 1, "1"], [2, 3, "5"]]).to_df(
        ["int", "int2", "str"]
    )
    limit = 1310721
    inner = (
        df.limit(limit)
        .join(df2.limit(limit), ["int", "int2"], "inner")
        .agg(count(col("int")))
    )
    assert inner.collect() == [Row(1)]


def test_default_inner_join(session):
    df = session.create_dataframe([1, 2]).to_df(["a"])
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )

    res = df.join(df2).collect()
    res.sort(key=lambda x: (x[0], x[1]))
    assert res == [
        Row(1, 1, "test1"),
        Row(1, 2, "test2"),
        Row(2, 1, "test1"),
        Row(2, 2, "test2"),
    ]


def test_default_inner_join_using_column(session):

    df = session.create_dataframe([1, 2]).to_df(["a"])
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )

    assert df.join(df2, "a").collect() == [Row(1, "test1"), Row(2, "test2")]
    assert df.join(df2, "a").filter(col("a") > 1).collect() == [Row(2, "test2")]


def test_3_way_joins(session):

    df1 = session.create_dataframe([1, 2]).to_df(["a"])
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )
    df3 = session.create_dataframe(
        [[f"test{i}", f"hello{i}"] for i in range(1, 3)]
    ).to_df(["key", "val"])

    # 3 way join with column renaming
    res = df1.join(df2, "a").to_df(["num", "key"]).join(df3, ["key"]).collect()
    assert res == [Row("test1", 1, "hello1"), Row("test2", 2, "hello2")]


def test_default_inner_join_with_join_conditions(session):

    df1 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )
    df2 = session.create_dataframe([[i, f"num{i}"] for i in range(1, 3)]).to_df(
        ["num", "val"]
    )

    res = df1.join(df2, df1["a"] == df2["num"]).collect()
    assert sorted(res, key=lambda x: x[0]) == [
        Row(1, "test1", 1, "num1"),
        Row(2, "test2", 2, "num2"),
    ]


def test_join_with_multiple_conditions(session):

    df1 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )
    df2 = session.create_dataframe([[i, f"num{i}"] for i in range(1, 3)]).to_df(
        ["num", "val"]
    )

    res = df1.join(df2, (df1["a"] == df2["num"]) & (df1["b"] == df2["val"])).collect()
    assert res == []


def test_join_with_ambiguous_column_in_condidtion(session_cnx):
    with session_cnx() as session:
        df = session.create_dataframe([1, 2]).to_df(["a"])
        df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
            ["a", "b"]
        )

        with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
            df.join(df2, col("a") == col("a")).collect()
        assert "The reference to the column 'A' is ambiguous." in ex_info.value.message


def test_join_using_multiple_columns_and_specifying_join_type(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "int int, int2 int, str string")
        session.sql(
            f"insert into {table_name1} values(1, 2, '1'),(3, 4, '3')"
        ).collect()
        Utils.create_table(session, table_name2, "int int, int2 int, str string")
        session.sql(
            f"insert into {table_name2} values(1, 3, '1'),(5, 6, '5')"
        ).collect()

        df = session.table(table_name1)
        df2 = session.table(table_name2)

        assert df.join(df2, ["int", "str"], "inner").collect() == [Row(1, "1", 2, 3)]

        res = df.join(df2, ["int", "str"], "left").collect()
        assert sorted(res, key=lambda x: x[0]) == [
            Row(1, "1", 2, 3),
            Row(3, "3", 4, None),
        ]

        res = df.join(df2, ["int", "str"], "right").collect()
        assert sorted(res, key=lambda x: x[0]) == [
            Row(1, "1", 2, 3),
            Row(5, "5", None, 6),
        ]

        res = df.join(df2, ["int", "str"], "outer").collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(1, "1", 2, 3),
            Row(3, "3", 4, None),
            Row(5, "5", None, 6),
        ]

        assert df.join(df2, ["int", "str"], "left_semi").collect() == [Row(1, 2, "1")]
        assert df.join(df2, ["int", "str"], "semi").collect() == [Row(1, 2, "1")]

        assert df.join(df2, ["int", "str"], "left_anti").collect() == [Row(3, 4, "3")]
        assert df.join(df2, ["int", "str"], "anti").collect() == [Row(3, 4, "3")]
    finally:
        Utils.drop_table(session, table_name1)
        Utils.drop_table(session, table_name2)


def test_join_using_conditions_and_specifying_join_type(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    try:
        Utils.create_table(session, table_name1, "a1 int, b1 int, str1 string")
        session.sql(
            f"insert into {table_name1} values(1, 2, '1'),(3, 4, '3')"
        ).collect()
        Utils.create_table(session, table_name2, "a2 int, b2 int, str2 string")
        session.sql(
            f"insert into {table_name2} values(1, 3, '1'),(5, 6, '5')"
        ).collect()

        df = session.table(table_name1)
        df2 = session.table(table_name2)

        join_cond = (df["a1"] == df2["a2"]) & (df["str1"] == df2["str2"])

        Utils.check_answer(df.join(df2, join_cond, "left_semi"), [Row(1, 2, "1")])
        Utils.check_answer(df.join(df2, join_cond, "semi"), [Row(1, 2, "1")])
        Utils.check_answer(df.join(df2, join_cond, "left_anti"), [Row(3, 4, "3")])
        Utils.check_answer(df.join(df2, join_cond, "anti"), [Row(3, 4, "3")])
    finally:
        Utils.drop_table(session, table_name1)
        Utils.drop_table(session, table_name2)


def test_natural_join(session):
    df = session.create_dataframe([1, 2]).to_df("a")
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        "a", "b"
    )
    Utils.check_answer(df.natural_join(df2), [Row(1, "test1"), Row(2, "test2")])


def test_natural_outer_join(session):
    df1 = session.create_dataframe([[1, "1"], [3, "3"]]).to_df("a", "b")
    df2 = session.create_dataframe([[1, "1"], [4, "4"]]).to_df("a", "c")
    Utils.check_answer(
        df1.natural_join(df2, "left"), [Row(1, "1", "1"), Row(3, "3", None)]
    )
    Utils.check_answer(
        df1.natural_join(df2, "right"), [Row(1, "1", "1"), Row(4, None, "4")]
    )
    Utils.check_answer(
        df1.natural_join(df2, "outer"),
        [Row(1, "1", "1"), Row(3, "3", None), Row(4, None, "4")],
    )


def test_cross_join(session):
    df1 = session.create_dataframe([[1, "1"], [3, "3"]]).to_df(["int", "str"])
    df2 = session.create_dataframe([[2, "2"], [4, "4"]]).to_df(["int", "str"])

    res = df1.cross_join(df2).collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "1", 2, "2"),
        Row(1, "1", 4, "4"),
        Row(3, "3", 2, "2"),
        Row(3, "3", 4, "4"),
    ]

    res = df2.cross_join(df1).collect()
    res.sort(key=lambda x: (x[0], x[2]))
    assert res == [
        Row(2, "2", 1, "1"),
        Row(2, "2", 3, "3"),
        Row(4, "4", 1, "1"),
        Row(4, "4", 3, "3"),
    ]


def test_join_ambiguous_columns_with_specified_sources(session):

    df = session.create_dataframe([1, 2]).to_df(["a"])
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 3)]).to_df(
        ["a", "b"]
    )

    res = df.join(df2, df["a"] == df2["a"]).collect()
    assert sorted(res, key=lambda x: x[0]) == [
        Row(1, 1, "test1"),
        Row(2, 2, "test2"),
    ]

    res = df.join(df2, df["a"] == df2["a"]).select(df["a"] * df2["a"], "b").collect()
    assert sorted(res, key=lambda x: x[0]) == [Row(1, "test1"), Row(4, "test2")]


def test_join_ambiguous_columns_without_specified_sources(session_cnx):
    with session_cnx() as session:
        df = session.create_dataframe([[1, "one"], [2, "two"]]).to_df(
            ["intcol", " stringcol"]
        )
        df2 = session.create_dataframe([[1, "one"], [3, "three"]]).to_df(
            ["intcol", " bcol"]
        )

        for join_type in ["inner", "leftouter", "rightouter", "full_outer"]:
            with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
                df.join(df2, col("intcol") == col("intcol")).collect()
            assert (
                "The reference to the column 'INTCOL' is ambiguous."
                in ex_info.value.message
            )

            with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
                df.join(df2, df["intcol"] == df2["intcol"]).select("intcol").collect()
            assert (
                "The reference to the column 'INTCOL' is ambiguous."
                in ex_info.value.message
            )


def test_join_expression_ambiguous_columns(session):
    lhs = session.create_dataframe([[1, -1, "one"], [2, -2, "two"]]).to_df(
        ["intcol", "negcol", "lhscol"]
    )
    rhs = session.create_dataframe([[1, -10, "one"], [2, -20, "two"]]).to_df(
        ["intcol", "negcol", "rhscol"]
    )

    df = lhs.join(rhs, lhs["intcol"] == rhs["intcol"]).select(
        lhs["intcol"] + rhs["intcol"],
        lhs["negcol"],
        rhs["negcol"],
        "lhscol",
        "rhscol",
    )

    res = sorted(df.collect(), key=lambda x: x[0])
    assert res == [Row(2, -1, -10, "one", "one"), Row(4, -2, -20, "two", "two")]


@pytest.mark.skip("Ignored in Scala tests since this only produces a warning")
def test_semi_join_expression_ambiguous_columns(session_cnx):
    with session_cnx() as session:
        lhs = session.create_dataframe([[1, -1, "one"], [2, -2, "two"]]).to_df(
            ["intcol", "negcol", "lhscol"]
        )
        rhs = session.create_dataframe([[1, -10, "one"], [2, -20, "two"]]).to_df(
            ["intcol", "negcol", "rhscol"]
        )

        with pytest.raises(SnowparkSQLInvalidIdException) as ex_info:
            lhs.join(rhs, lhs["intcol"] == rhs["intcol"], "leftsemi").select(
                rhs["intcol"]
            ).collect()
        assert 'Column referenced with df["INTCOL"]' in str(ex_info)
        assert "not present" in str(ex_info)

        with pytest.raises(SnowparkSQLInvalidIdException) as ex_info:
            lhs.join(rhs, lhs["intcol"] == rhs["intcol"], "leftanti").select(
                rhs["intcol"]
            ).collect()
        assert 'Column referenced with df["INTCOL"]' in str(ex_info)
        assert "not present" in str(ex_info)


def test_semi_join_with_columns_from_LHS(session):
    lhs = session.create_dataframe([[1, -1, "one"], [2, -2, "two"]]).to_df(
        ["intcol", "negcol", "lhscol"]
    )
    rhs = session.create_dataframe([[1, -10, "one"], [2, -20, "two"]]).to_df(
        ["intcol", "negcol", "rhscol"]
    )

    res = (
        lhs.join(rhs, lhs["intcol"] == rhs["intcol"], "leftsemi")
        .select("intcol")
        .collect()
    )
    assert res == [Row(1), Row(2)]

    res = (
        rhs.join(lhs, lhs["intcol"] == rhs["intcol"], "leftsemi")
        .select("intcol")
        .collect()
    )
    assert res == [Row(1), Row(2)]

    res = (
        lhs.join(
            rhs,
            (lhs["intcol"] == rhs["intcol"]) & (lhs["negcol"] == rhs["negcol"]),
            "leftsemi",
        )
        .select(lhs["intcol"])
        .collect()
    )
    assert res == []

    res = (
        lhs.join(rhs, lhs["intcol"] == rhs["intcol"], "leftanti")
        .select("intcol")
        .collect()
    )
    assert res == []

    res = (
        lhs.join(rhs, lhs["intcol"] == rhs["intcol"], "leftanti")
        .select(lhs["intcol"])
        .collect()
    )
    assert res == []

    res = (
        lhs.join(
            rhs,
            (lhs["intcol"] == rhs["intcol"]) & (lhs["negcol"] == rhs["negcol"]),
            "leftanti",
        )
        .select(lhs["intcol"])
        .collect()
    )
    assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2)]


def test_using_joins(session):
    lhs = session.create_dataframe([[1, -1, "one"], [2, -2, "two"]]).to_df(
        ["intcol", "negcol", "lhscol"]
    )
    rhs = session.create_dataframe([[1, -10, "one"], [2, -20, "two"]]).to_df(
        ["intcol", "negcol", "rhscol"]
    )

    for join_type in ["inner", "leftouter", "rightouter", "full_outer"]:
        res = lhs.join(rhs, ["intcol"], join_type).select("*").collect()
        assert res == [
            Row(1, -1, "one", -10, "one"),
            Row(2, -2, "two", -20, "two"),
        ]

        res = lhs.join(rhs, ["intcol"], join_type).collect()
        assert res == [
            Row(1, -1, "one", -10, "one"),
            Row(2, -2, "two", -20, "two"),
        ]

        with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
            lhs.join(rhs, ["intcol"], join_type).select("negcol").collect()
        assert "reference to the column 'NEGCOL' is ambiguous" in ex_info.value.message

        res = lhs.join(rhs, ["intcol"], join_type).select("intcol").collect()
        assert res == [Row(1), Row(2)]
        res = (
            lhs.join(rhs, ["intcol"], join_type)
            .select(lhs["negcol"], rhs["negcol"])
            .collect()
        )
        assert sorted(res, key=lambda x: -x[0]) == [Row(-1, -10), Row(-2, -20)]


def test_columns_with_and_without_quotes(session):

    lhs = session.create_dataframe([[1, 1.0]]).to_df(["intcol", "doublecol"])
    rhs = session.create_dataframe([[1, 2.0]]).to_df(['"INTCOL"', '"DoubleCol"'])

    res = (
        lhs.join(rhs, lhs["intcol"] == rhs["intcol"])
        .select(lhs['"INTCOL"'], rhs["intcol"], "doublecol", col('"DoubleCol"'))
        .collect()
    )
    assert res == [Row(1, 1, 1.0, 2.0)]

    res = (
        lhs.join(rhs, lhs["doublecol"] == rhs['"DoubleCol"'])
        .select(lhs['"INTCOL"'], rhs["intcol"], "doublecol", col('"DoubleCol"'))
        .collect()
    )
    assert res == []

    # Below LHS and RHS are swapped but we still default to using the column name as is.
    res = (
        lhs.join(rhs, col("doublecol") == col('"DoubleCol"'))
        .select(lhs['"INTCOL"'], rhs["intcol"], "doublecol", col('"DoubleCol"'))
        .collect()
    )
    assert res == []

    with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
        lhs.join(rhs, col("intcol") == col('"INTCOL"')).collect()
    assert "reference to the column 'INTCOL' is ambiguous." in ex_info.value.message


def test_aliases_multiple_levels_deep(session):

    lhs = session.create_dataframe([[1, -1, "one"], [2, -2, "two"]]).to_df(
        ["intcol", "negcol", "lhscol"]
    )
    rhs = session.create_dataframe([[1, -10, "one"], [2, -20, "two"]]).to_df(
        ["intcol", "negcol", "rhscol"]
    )

    res = (
        lhs.join(rhs, lhs["intcol"] == rhs["intcol"])
        .select(
            (lhs["negcol"] + rhs["negcol"]).alias("newCol"),
            lhs["intcol"],
            rhs["intcol"],
        )
        .select((lhs["intcol"] + rhs["intcol"]), "newCol")
        .collect()
    )
    assert sorted(res, key=lambda x: x[0]) == [Row(2, -11), Row(4, -22)]


def test_join_sql_as_the_backing_dataframe(session):

    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "int int, int2 int, str string")
        session.sql(
            f"insert into {table_name1} values(1, 2, '1'),(3, 4, '3')"
        ).collect()

        df = session.sql(f"select * from {table_name1} where int2< 10")
        df2 = session.sql(
            "select 1 as INT, 3 as INT2, '1' as STR UNION select 5 as INT, 6 as INT2, '5' as STR"
        )

        assert df.join(df2, ["int", "str"], "inner").collect() == [Row(1, "1", 2, 3)]

        assert df.join(df2, ["int", "str"], "left").collect() == [
            Row(1, "1", 2, 3),
            Row(3, "3", 4, None),
        ]

        assert df.join(df2, ["int", "str"], "right").collect() == [
            Row(1, "1", 2, 3),
            Row(5, "5", None, 6),
        ]

        res = df.join(df2, ["int", "str"], "outer").collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(1, "1", 2, 3),
            Row(3, "3", 4, None),
            Row(5, "5", None, 6),
        ]

        assert df.join(df2, ["int", "str"], "left_semi").collect() == [Row(1, 2, "1")]
        assert df.join(df2, ["int", "str"], "semi").collect() == [Row(1, 2, "1")]

        assert df.join(df2, ["int", "str"], "left_anti").collect() == [Row(3, 4, "3")]
        assert df.join(df2, ["int", "str"], "anti").collect() == [Row(3, 4, "3")]

    finally:
        Utils.drop_table(session, table_name1)


def test_negative_test_for_self_join_with_conditions(session):

    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "c1 int, c2 int")
        session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
        df = session.table(table_name1)
        self_dfs = [df, DataFrame(df.session, df._plan)]

        msg = (
            "You cannot join a DataFrame with itself because the column references cannot be resolved "
            "correctly. Instead, create a copy of the DataFrame with copy.copy(), and join the DataFrame with "
            "this copy."
        )

        for df2 in self_dfs:
            for join_type in ["", "inner", "left", "right", "outer"]:
                with pytest.raises(SnowparkJoinException) as ex_info:
                    if not join_type:
                        df.join(df2, df["c1"] == df["c2"]).collect()
                    else:
                        df.join(df2, df["c1"] == df["c2"], join_type).collect()
                assert msg in ex_info.value.message

    finally:
        Utils.drop_table(session, table_name1)


def test_clone_can_help_these_self_joins(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "c1 int, c2 int")
        session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
        df = session.table(table_name1)
        cloned_df = copy.copy(df)

        # inner self join
        assert df.join(cloned_df, df["c1"] == cloned_df["c2"]).collect() == [
            Row(2, 3, 1, 2)
        ]

        # left self join
        res = df.join(cloned_df, df["c1"] == cloned_df["c2"], "left").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, 2, None, None), Row(2, 3, 1, 2)]

        # right self join
        res = df.join(cloned_df, df["c1"] == cloned_df["c2"], "right").collect()
        res.sort(key=lambda x: x[0] or 0)
        assert res == [Row(None, None, 2, 3), Row(2, 3, 1, 2)]

        # outer self join
        res = df.join(cloned_df, df["c1"] == cloned_df["c2"], "outer").collect()
        res.sort(key=lambda x: x[0] or 0)
        assert res == [
            Row(None, None, 2, 3),
            Row(1, 2, None, None),
            Row(2, 3, 1, 2),
        ]

    finally:
        Utils.drop_table(session, table_name1)


def test_natural_cross_joins(session):

    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "c1 int, c2 int")
        session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
        df = session.table(table_name1)
        df2 = df  # Another reference of "df"
        cloned_df = copy.copy(df)

        # "natural join" supports self join
        assert df.natural_join(df2).collect() == [Row(1, 2), Row(2, 3)]
        assert df.natural_join(cloned_df).collect() == [Row(1, 2), Row(2, 3)]

        # "cross join" supports self join
        res = df.cross_join(df2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(1, 2, 1, 2),
            Row(1, 2, 2, 3),
            Row(2, 3, 1, 2),
            Row(2, 3, 2, 3),
        ]

        res = df.cross_join(df2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(1, 2, 1, 2),
            Row(1, 2, 2, 3),
            Row(2, 3, 1, 2),
            Row(2, 3, 2, 3),
        ]

    finally:
        Utils.drop_table(session, table_name1)


def test_clone_with_join_dataframe(session):

    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "c1 int, c2 int")
        session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
        df = session.table(table_name1)

        assert df.collect() == [Row(1, 2), Row(2, 3)]

        cloned_df = copy.copy(df)
        #  Cloned DF has the same conent with original DF
        assert cloned_df.collect() == [Row(1, 2), Row(2, 3)]

        join_df = df.join(cloned_df, df["c1"] == cloned_df["c2"])
        assert join_df.collect() == [Row(2, 3, 1, 2)]
        # Cloned join DF
        cloned_join_df = copy.copy(join_df)
        assert cloned_join_df.collect() == [Row(2, 3, 1, 2)]

    finally:
        Utils.drop_table(session, table_name1)


def test_join_of_join(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "c1 int, c2 int")
        session.sql(f"insert into {table_name1} values(1, 1), (2, 2)").collect()
        df_l = session.table(table_name1)
        df_r = copy.copy(df_l)
        df_j = df_l.join(df_r, df_l["c1"] == df_r["c1"])

        assert df_j.collect() == [Row(1, 1, 1, 1), Row(2, 2, 2, 2)]

        df_j_clone = copy.copy(df_j)
        # Because of duplicate column name rename, we have to get a name.
        col_name = df_j.schema.fields[0].name
        df_j_j = df_j.join(df_j_clone, df_j[col_name] == df_j_clone[col_name])

        assert df_j_j.collect() == [
            Row(1, 1, 1, 1, 1, 1, 1, 1),
            Row(2, 2, 2, 2, 2, 2, 2, 2),
        ]

    finally:
        Utils.drop_table(session, table_name1)


def test_negative_test_join_of_join(session_cnx):
    with session_cnx() as session:
        table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 1), (2, 2)").collect()
            df_l = session.table(table_name1)
            df_r = copy.copy(df_l)
            df_j = df_l.join(df_r, df_l["c1"] == df_r["c1"])
            df_j_clone = copy.copy(df_j)

            with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
                df_j.join(df_j_clone, df_l["c1"] == df_r["c1"]).collect()
            assert "reference to the column 'C1' is ambiguous" in ex_info.value.message

        finally:
            Utils.drop_table(session, table_name1)


def test_drop_on_join(session):
    table_name_1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name_2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.create_dataframe([[1, "a", True], [2, "b", False]]).to_df(
            "a", "b", "c"
        ).write.save_as_table(table_name_1)
        session.create_dataframe([[3, "a", True], [4, "b", False]]).to_df(
            "a", "b", "c"
        ).write.save_as_table(table_name_2)
        df1 = session.table(table_name_1)
        df2 = session.table(table_name_2)
        df3 = df1.join(df2, df1["c"] == df2["c"]).drop(df1["a"], df2["b"], df1["c"])
        Utils.check_answer(df3, [Row("a", 3, True), Row("b", 4, False)])
        df4 = df3.drop(df2["c"], df1["b"], col("other"))
        Utils.check_answer(df4, [Row(3), Row(4)])
    finally:
        Utils.drop_table(session, table_name_1)
        Utils.drop_table(session, table_name_2)


def test_drop_on_self_join(session):
    table_name_1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.create_dataframe([[1, "a", True], [2, "b", False]]).to_df(
            "a", "b", "c"
        ).write.save_as_table(table_name_1)
        df1 = session.table(table_name_1)
        df2 = copy.copy(df1)
        df3 = df1.join(df2, df1["c"] == df2["c"]).drop(df1["a"], df2["b"], df1["c"])
        Utils.check_answer(df3, [Row("a", 1, True), Row("b", 2, False)])
        df4 = df3.drop(df2["c"], df1["b"], col("other"))
        Utils.check_answer(df4, [Row(1), Row(2)])
    finally:
        Utils.drop_table(session, table_name_1)


def test_with_column_on_join(session):
    table_name_1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name_2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.create_dataframe([[1, "a", True], [2, "b", False]]).to_df(
            "a", "b", "c"
        ).write.save_as_table(table_name_1)
        session.create_dataframe([[3, "a", True], [4, "b", False]]).to_df(
            "a", "b", "c"
        ).write.save_as_table(table_name_2)
        df1 = session.table(table_name_1)
        df2 = session.table(table_name_2)
        Utils.check_answer(
            df1.join(df2, df1["c"] == df2["c"])
            .drop(df1["b"], df2["b"], df1["c"])
            .with_column("newColumn", df1["a"] + df2["a"]),
            [Row(1, 3, True, 4), Row(2, 4, False, 6)],
        )
    finally:
        Utils.drop_table(session, table_name_1)
        Utils.drop_table(session, table_name_2)


def test_process_outer_join_results_using_the_non_nullable_columns_in_the_join_outpu(
    session,
):
    df1 = session.create_dataframe([(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]).to_df(
        "id", "count"
    )
    df2 = session.create_dataframe([[0], [1]]).to_df("id").group_by("id").count()

    Utils.check_answer(
        df1.join(df2, df1["id"] == df2["id"], "left_outer").filter(
            is_null(df2["count"])
        ),
        [Row(2, 0, None, None), Row(3, 0, None, None), Row(4, 0, None, None)],
    )

    # Coallesce data using non-nullable columns in input tables
    df3 = session.create_dataframe([[1, 1]]).to_df("a", "b")
    df4 = session.create_dataframe([[2, 2]]).to_df("a", "b")
    Utils.check_answer(
        df3.join(df4, df3["a"] == df4["a"], "outer").select(
            coalesce(df3["a"], df3["b"]), coalesce(df4["a"], df4["b"])
        ),
        [Row(1, None), Row(None, 2)],
    )


def test_outer_join_conversion(session):

    df = session.create_dataframe([(1, 2, "1"), (3, 4, "3")]).to_df(
        ["int", "int2", "str"]
    )
    df2 = session.create_dataframe([(1, 3, "1"), (5, 6, "5")]).to_df(
        ["int", "int2", "str"]
    )

    # outer -> left
    outer_join_2_left = (
        df.join(df2, df["int"] == df2["int"], "outer").where(df["int"] >= 3).collect()
    )
    assert outer_join_2_left == [Row(3, 4, "3", None, None, None)]

    # outer -> right
    outer_join_2_right = (
        df.join(df2, df["int"] == df2["int"], "outer").where(df2["int"] >= 3).collect()
    )
    assert outer_join_2_right == [Row(None, None, None, 5, 6, "5")]

    # outer -> inner
    outer_join_2_inner = (
        df.join(df2, df["int"] == df2["int"], "outer")
        .where((df["int"] == 1) & (df2["int2"] == 3))
        .collect()
    )
    assert outer_join_2_inner == [Row(1, 2, "1", 1, 3, "1")]

    # right -> inner
    right_join_2_inner = (
        df.join(df2, df["int"] == df2["int"], "right").where(df["int"] > 0).collect()
    )
    assert right_join_2_inner == [Row(1, 2, "1", 1, 3, "1")]

    # left -> inner
    left_join_2_inner = (
        df.join(df2, df["int"] == df2["int"], "left").where(df2["int"] > 0).collect()
    )
    assert left_join_2_inner == [Row(1, 2, "1", 1, 3, "1")]


def test_dont_throw_analysis_exception_in_check_cartesian(session):
    """Don't throw Analysis Exception in CheckCartesianProduct when join condition is false or null"""
    df = session.range(10).to_df(["id"])
    dfNull = session.range(10).select(lit(None).as_("b"))
    df.join(dfNull, col("id") == col("b"), "left").collect()

    dfOne = df.select(lit(1).as_("a"))
    dfTwo = session.range(10).select(lit(2).as_("b"))
    dfOne.join(dfTwo, col("a") == col("b"), "left").collect()


def test_name_alias_on_multiple_join(session):
    table_trips = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_stations = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temp table {table_trips} (starttime timestamp, "
            f"start_station_id int, end_station_id int)"
        ).collect()
        session.sql(
            f"create or replace temp table {table_stations} "
            f"(station_id int, station_name string)"
        ).collect()

        df_trips = session.table(table_trips)
        df_start_stations = session.table(table_stations)
        df_end_stations = session.table(table_stations)

        # assert no error
        df = (
            df_trips.join(
                df_end_stations,
                df_trips["end_station_id"] == df_end_stations["station_id"],
            )
            .join(
                df_start_stations,
                df_trips["start_station_id"] == df_start_stations["station_id"],
            )
            .filter(df_trips["starttime"] >= "2021-01-01")
            .select(
                df_start_stations["station_name"],
                df_end_stations["station_name"],
                df_trips["starttime"],
            )
        )

        res = df.collect()
    finally:
        Utils.drop_table(session, table_trips)
        Utils.drop_table(session, table_stations)


def test_name_alias_on_multiple_join_unnormalized_name(session):

    table_trips = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_stations = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temp table {table_trips} (starttime timestamp, "
            f'"start<station>id" int, "end+station+id" int)'
        ).collect()
        session.sql(
            f"create or replace temp table {table_stations} "
            f'("station^id" int, "station%name" string)'
        ).collect()

        df_trips = session.table(table_trips)
        df_start_stations = session.table(table_stations)
        df_end_stations = session.table(table_stations)

        # assert no error
        df = (
            df_trips.join(
                df_end_stations,
                df_trips["end+station+id"] == df_end_stations["station^id"],
            )
            .join(
                df_start_stations,
                df_trips["start<station>id"] == df_start_stations["station^id"],
            )
            .filter(df_trips["starttime"] >= "2021-01-01")
            .select(
                df_start_stations["station%name"],
                df_end_stations["station%name"],
                df_trips["starttime"],
            )
        )

        res = df.collect()
    finally:
        Utils.drop_table(session, table_trips)
        Utils.drop_table(session, table_stations)


def test_report_error_when_refer_common_col(session_cnx):
    with session_cnx() as session:
        df1 = session.create_dataframe([[1, 2]]).to_df(["a", "b"])
        df2 = session.create_dataframe([[1, 2]]).to_df(["c", "d"])
        df3 = session.create_dataframe([[1, 2]]).to_df(["e", "f"])

        df4 = df1.join(df2, df1["a"] == df2["c"])
        df5 = df3.join(df2, df2["c"] == df3["e"])
        df6 = df4.join(df5, df4["a"] == df5["e"])

        with pytest.raises(SnowparkSQLAmbiguousJoinException) as ex_info:
            df6.select("*").select(df2["c"]).collect()
        assert "The reference to the column 'C' is ambiguous." in ex_info.value.message


def test_select_all_on_join_result(session):

    df_left = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df_right = session.create_dataframe([[3, 4]]).to_df("c", "d")

    df = df_left.join(df_right)

    assert (
        df.select("*")._show_string(10)
        == """-------------------------
|"A"  |"B"  |"C"  |"D"  |
-------------------------
|1    |2    |3    |4    |
-------------------------
"""
    )
    assert (
        df.select(df["*"])._show_string(10)
        == """-------------------------
|"A"  |"B"  |"C"  |"D"  |
-------------------------
|1    |2    |3    |4    |
-------------------------
"""
    )
    assert (
        df.select(df_left["*"], df_right["*"])._show_string(10)
        == """-------------------------
|"A"  |"B"  |"C"  |"D"  |
-------------------------
|1    |2    |3    |4    |
-------------------------
"""
    )

    assert (
        df.select(df_right["*"], df_left["*"])._show_string(10)
        == """-------------------------
|"C"  |"D"  |"A"  |"B"  |
-------------------------
|3    |4    |1    |2    |
-------------------------
"""
    )


def test_select_left_right_on_join_result(session):

    df_left = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df_right = session.create_dataframe([[3, 4]]).to_df("c", "d")

    df = df_left.join(df_right)
    # Select left or right
    assert (
        df.select(df_left["*"])._show_string(10)
        == """-------------
|"A"  |"B"  |
-------------
|1    |2    |
-------------
"""
    )
    assert (
        df.select(df_right["*"])._show_string(10)
        == """-------------
|"C"  |"D"  |
-------------
|3    |4    |
-------------
"""
    )


def test_select_left_right_combination_on_join_result(session):

    df_left = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df_right = session.create_dataframe([[3, 4]]).to_df("c", "d")

    df = df_left.join(df_right)
    # Select left["*"] and right['c']
    assert (
        df.select(df_left["*"], df_right["c"])._show_string(10)
        == """-------------------
|"A"  |"B"  |"C"  |
-------------------
|1    |2    |3    |
-------------------
"""
    )
    assert (
        df.select(df_left["*"], df_right.c)._show_string(10)
        == """-------------------
|"A"  |"B"  |"C"  |
-------------------
|1    |2    |3    |
-------------------
"""
    )
    # select left["*"] and left["a"]
    assert (
        df.select(df_left["*"], df_left["a"].as_("l_a"))._show_string(10)
        == """---------------------
|"A"  |"B"  |"L_A"  |
---------------------
|1    |2    |1      |
---------------------
"""
    )
    # select right["*"] and right["c"]
    assert (
        df.select(df_right["*"], df_right["c"].as_("R_C"))._show_string(10)
        == """---------------------
|"C"  |"D"  |"R_C"  |
---------------------
|3    |4    |3      |
---------------------
"""
    )

    # select right["*"] and left["a"]
    assert (
        df.select(df_right["*"], df_left["a"])._show_string(10)
        == """-------------------
|"C"  |"D"  |"A"  |
-------------------
|3    |4    |1    |
-------------------
"""
    )


def test_select_columns_on_join_result_with_conflict_name(session):
    df_left = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df_right = session.create_dataframe([[3, 4]]).to_df("a", "d")
    df = df_left.join(df_right)

    df1 = df.select(df_left["*"], df_right["*"])
    # Get all columns
    # The result column name will be like:
    # |"l_Z36B_A" |"B" |"r_ztcn_A" |"D" |
    assert len(re.search('"l_.*_A"', df1.schema.fields[0].name).group(0)) > 0
    assert df1.schema.fields[1].name == "B"
    assert len(re.search('"r_.*_A"', df1.schema.fields[2].name).group(0)) > 0
    assert df1.schema.fields[3].name == "D"
    assert df1.collect() == [Row(1, 2, 3, 4)]

    df2 = df.select(df_right.a, df_left.a)
    # Get right-left conflict columns
    # The result column column name will be like:
    # |"r_v3Ms_A"  |"l_Xb7d_A"  |
    assert len(re.search('"r_.*_A"', df2.schema.fields[0].name).group(0)) > 0
    assert len(re.search('"l_.*_A"', df2.schema.fields[1].name).group(0)) > 0
    assert df2.collect() == [Row(3, 1)]

    df3 = df.select(df_left.a, df_right.a)
    # Get left-right conflict columns
    # The result column column name will be like:
    # |"l_v3Ms_A"  |"r_Xb7d_A"  |
    assert len(re.search('"l_.*_A"', df3.schema.fields[0].name).group(0)) > 0
    assert len(re.search('"r_.*_A"', df3.schema.fields[1].name).group(0)) > 0
    assert df3.collect() == [Row(1, 3)]

    df4 = df.select(df_right["*"], df_left.a)
    # Get rightAll-left conflict columns
    # The result column column name will be like:
    # |"r_ClxT_A"  |"D"  |"l_q8l5_A"  |
    assert len(re.search('"r_.*_A"', df4.schema.fields[0].name).group(0)) > 0
    assert df4.schema.fields[1].name == "D"
    assert len(re.search('"l_.*_A"', df4.schema.fields[2].name).group(0)) > 0
    assert df4.collect() == [Row(3, 4, 1)]
