#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import json
import math
from array import array
from collections import namedtuple
from decimal import Decimal
from itertools import product

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Column, Row
from snowflake.snowpark._internal.sp_expressions import (
    AttributeReference as SPAttributeReference,
    Star as SPStar,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkColumnException
from snowflake.snowpark.functions import col, concat, lit, when
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)
from tests.utils import TestData, TestFiles, Utils


def test_read_stage_file_show(session, resources_path):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)
    test_file_on_stage = f"@{tmp_stage_name}/testCSV.csv"

    try:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name, test_files.test_file_csv, compress=False
        )
        user_schema = StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", DoubleType()),
            ]
        )
        result_str = (
            session.read.option("purge", False)
            .schema(user_schema)
            .csv(test_file_on_stage)
            ._show_string()
        )
        assert (
            result_str
            == """
-------------------
|"A"  |"B"  |"C"  |
-------------------
|1    |one  |1.2  |
|2    |two  |2.2  |
-------------------
""".lstrip()
        )
    finally:
        Utils.drop_stage(session, tmp_stage_name)


def test_distinct(session_cnx):
    """Tests df.distinct()."""
    with session_cnx() as session:
        df = session.create_dataframe(
            [
                [1, 1],
                [1, 1],
                [2, 2],
                [3, 3],
                [4, 4],
                [5, 5],
                [None, 1],
                [1, None],
                [None, None],
            ]
        ).to_df("id", "v")

        res = df.distinct().sort(["id", "v"]).collect()
        assert res == [
            Row(None, None),
            Row(None, 1),
            Row(1, None),
            Row(1, 1),
            Row(2, 2),
            Row(3, 3),
            Row(4, 4),
            Row(5, 5),
        ]

        res = df.select(col("id")).distinct().sort(["id"]).collect()
        assert res == [Row(None), Row(1), Row(2), Row(3), Row(4), Row(5)]

        res = df.select(col("v")).distinct().sort(["v"]).collect()
        assert res == [Row(None), Row(1), Row(2), Row(3), Row(4), Row(5)]


def test_first(session_cnx):
    """Tests df.first()."""
    with session_cnx() as session:
        df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"], [4, "d"]]).to_df(
            "id", "v"
        )

        # empty first, should default to 1
        res = df.first()
        assert res == Row(1, "a")

        res = df.first(0)
        assert res == []

        res = df.first(1)
        assert res == [Row(1, "a")]

        res = df.first(2)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, "a"), Row(2, "b")]

        res = df.first(3)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c")]

        res = df.first(4)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

        # Negative value is equivalent to collect()
        res = df.first(-1)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

        # first-value larger than cardinality
        res = df.first(123)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")]

        # test invalid type argument passed to first
        with pytest.raises(ValueError) as ex_info:
            df.first("abc")
        assert "Invalid type of argument passed to first()" in str(ex_info)


def test_new_df_from_range(session_cnx):
    """Tests df.range()."""
    with session_cnx() as session:
        # range(start, end, step)
        df = session.range(1, 10, 2)
        res = df.collect()
        expected = [Row(1), Row(3), Row(5), Row(7), Row(9)]
        assert res == expected

        # range(start, end)
        df = session.range(1, 10)
        res = df.collect()
        expected = [
            Row(1),
            Row(2),
            Row(3),
            Row(4),
            Row(5),
            Row(6),
            Row(7),
            Row(8),
            Row(9),
        ]
        assert res == expected

        # range(end)
        df = session.range(10)
        res = df.collect()
        expected = [
            Row(0),
            Row(1),
            Row(2),
            Row(3),
            Row(4),
            Row(5),
            Row(6),
            Row(7),
            Row(8),
            Row(9),
        ]
        assert res == expected


def test_select_single_column(session_cnx):
    """Tests df.select() on dataframes with a single column."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).select("id").collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") < 4).select("id").collect()
        expected = [Row(1), Row(3)]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 4).collect()
        expected = [Row(1), Row(3)]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 3).collect()
        expected = [Row(1), Row(3)]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 0).collect()
        expected = []
        assert res == expected


def test_select_star(session_cnx):
    """Tests df.select('*')."""
    with session_cnx() as session:
        # Single column
        res = session.range(3, 8).select("*").collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        result = res == expected
        assert result

        # Two columns
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df.select("*").collect()
        expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
        assert res == expected


def test_df_subscriptable(session_cnx):
    """Tests select & filter as df[...]"""
    with session_cnx() as session:
        # Star, single column
        res = session.range(3, 8)[["*"]].collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected

        # Star, two columns
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[["*"]].collect()
        expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
        assert res == expected
        # without double brackets should refer to a Column object
        assert type(df["*"]) == Column

        # single column, str type
        df = session.range(3, 8)
        res = df[["ID"]].collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected
        assert type(df["ID"]) == Column

        # single column, int type
        df = session.range(3, 8)
        res = df[df[0] > 5].collect()
        expected = [Row(6), Row(7)]
        assert res == expected
        assert type(df[0]) == Column

        # two columns, list type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[["ID", "ID_PRIME"]].collect()
        expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
        assert res == expected

        # two columns, tuple type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[("ID", "ID_PRIME")].collect()
        expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
        assert res == expected

        # two columns, int type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[[df[1].getName()]].collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected


def test_filter(session):
    """Tests for df.filter()."""
    df = session.range(1, 10, 2)
    res = df.filter(col("id") > 4).collect()
    expected = [Row(5), Row(7), Row(9)]
    assert res == expected

    res = df.filter(col("id") < 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 4).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 3).collect()
    expected = [Row(1), Row(3)]
    assert res == expected

    res = df.filter(col("id") <= 0).collect()
    expected = []
    assert res == expected

    # sql text
    assert (
        df.filter(col("id") > 4).collect()
        == df.filter("id > 4").collect()
        == [Row(5), Row(7), Row(9)]
    )
    assert df.filter(col("id") <= 0).collect() == df.filter("id <= 0").collect() == []

    df = session.create_dataframe(["aa", "bb"], schema=["a"])
    # In SQL expression, we need to use the upper case here when put double quotes
    # around an identifier, as the case in double quotes will be preserved.
    assert (
        df.filter("\"A\" = 'aa'").collect()
        == df.filter("a = 'aa'").collect()
        == [Row("aa")]
    )


def test_filter_incorrect_type(session):
    """Tests for incorrect type passed to DataFrame.filter()."""
    df = session.range(1, 10, 2)

    with pytest.raises(TypeError) as ex_info:
        df.filter(1234)
    assert (
        "'filter/where' expected Column or str as SQL expression, got: <class 'int'>"
        in str(ex_info)
    )


def test_filter_chained(session_cnx):
    """Tests for chained DataFrame.filter() operations"""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") < 4).filter(col("id") < 4).collect()
        expected = [Row(1), Row(3)]
        assert res == expected

        res = (
            session.range(1, 10, 2)
            .filter(col("id") <= 4)
            .filter(col("id") >= 0)
            .collect()
        )
        expected = [Row(1), Row(3)]
        assert res == expected

        res = (
            session.range(1, 10, 2)
            .filter(col("id") <= 3)
            .filter(col("id") != 5)
            .collect()
        )
        expected = [Row(1), Row(3)]
        assert res == expected


def test_filter_chained_col_objects_int(session_cnx):
    """Tests for chained DataFrame.filter() operations."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") >= 5).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") >= 1).filter(col("id") >= 5).collect()
        expected = [Row(5), Row(7), Row(9)]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") == 5).collect()
        expected = [Row(5)]
        assert res == expected


def test_drop(session_cnx):
    """Test for dropping columns from a dataframe."""
    with session_cnx() as session:
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df.drop("id").select("id_prime").collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected

        # dropping all columns should raise exception
        with pytest.raises(Exception):
            df.drop("id").drop("id_prime")

        # Drop second column renamed several times
        df2 = (
            session.range(3, 8)
            .select(["id", col("id").alias("id_prime")])
            .select(["id", col("id_prime").alias("id_prime_2")])
            .select(["id", col("id_prime_2").alias("id_prime_3")])
            .select(["id", col("id_prime_3").alias("id_prime_4")])
            .drop("id_prime_4")
        )
        res = df2.select("id").collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected


def test_alias(session_cnx):
    """Test for dropping columns from a dataframe."""
    with session_cnx() as session:
        # Selecting non-existing column (already renamed) should fail
        with pytest.raises(Exception):
            session.range(3, 8).select(col("id").alias("id_prime")).select(
                col("id").alias("id_prime")
            ).collect()

        # Rename column several times
        df = (
            session.range(3, 8)
            .select(col("id").alias("id_prime"))
            .select(col("id_prime").alias("id_prime_2"))
            .select(col("id_prime_2").alias("id_prime_3"))
            .select(col("id_prime_3").alias("id_prime_4"))
        )
        res = df.select("id_prime_4").collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert res == expected


def test_join_inner(session_cnx):
    """Test for inner join of dataframes."""
    with session_cnx() as session:
        # Implicit inner join on single column
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id").collect()
        expected = [Row(5), Row(6), Row(7)]
        assert res == expected

        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "inner").collect()
        expected = [Row(5), Row(6), Row(7)]
        assert res == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id").collect()
        expected = [Row(5, 5, 5), Row(6, 6, 6), Row(7, 7, 7)]
        assert res == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row(5, 5, 5), Row(6, 6, 6), Row(7, 7, 7)]
        res = df1.join(df2, "id").collect()
        assert res == expected


def test_join_left_anti(session_cnx):
    """Test for left-anti join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row(3), Row(4)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row(3, 3), Row(4, 4)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row(3, 3), Row(4, 4)]
        assert sorted(res, key=lambda r: r[0]) == expected


def test_join_left_outer(session_cnx):
    """Test for left-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [
            Row(3, 3, None),
            Row(4, 4, None),
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [
            Row(3, 3, None),
            Row(4, 4, None),
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected


def test_join_right_outer(session_cnx):
    """Test for right-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [Row(5), Row(6), Row(7), Row(8), Row(9)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(8, None, 8),
            Row(9, None, 9),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(8, None, 8),
            Row(9, None, 9),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected


def test_join_left_semi(session_cnx):
    """Test for left semi join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_semi").collect()
        expected = [Row(5), Row(6), Row(7)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_semi").collect()
        expected = [Row(5, 5), Row(6, 6), Row(7, 7)]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row(5, 5), Row(6, 6), Row(7, 7)]
        res = df1.join(df2, "id", "left_semi").collect()
        assert sorted(res, key=lambda r: r[0]) == expected


def test_join_cross(session_cnx):
    """Test for cross join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.cross_join(df2).collect()
        expected = [Row(x, y) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r[0], r[1])) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.cross_join(df2).collect()
        expected = [Row(x, x, y, y) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r[0], r[1])) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row(x, x, y, y) for x, y in product(range(3, 8), range(5, 10))]
        res = df1.cross_join(df2).collect()
        assert sorted(res, key=lambda r: (r[0], r[2])) == expected

        with pytest.raises(Exception) as ex:
            df1.join(df2, col("id"), "cross")
        assert "Cross joins cannot take columns as input." in str(ex.value)

        # Case, join on same-name column, other columns have different name, select common column.
        this = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        other = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        df_cross = this.cross_join(other).select([this.col("id"), other.col("id")])
        res = df_cross.collect()
        expected = [Row(x, y) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r[0], r[1])) == expected


def test_join_outer(session_cnx):
    """Test for outer join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "outer").collect()
        expected = [
            Row(3),
            Row(4),
            Row(5),
            Row(6),
            Row(7),
            Row(8),
            Row(9),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "outer").collect()
        expected = [
            Row(3, 3, None),
            Row(4, 4, None),
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(8, None, 8),
            Row(9, None, 9),
        ]
        assert sorted(res, key=lambda r: r[0]) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [
            Row(3, 3, None),
            Row(4, 4, None),
            Row(5, 5, 5),
            Row(6, 6, 6),
            Row(7, 7, 7),
            Row(8, None, 8),
            Row(9, None, 9),
        ]
        res = df1.join(df2, "id", "outer").collect()
        assert sorted(res, key=lambda r: r[0]) == expected


def test_toDF(session_cnx):
    """Test df.to_df()."""
    with session_cnx() as session:
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])

        # calling to_df() with fewer new names than columns should fail
        with pytest.raises(Exception) as ex:
            df.to_df(["new_name"])
        assert "The number of columns doesn't match. Old column names (2):" in str(
            ex.value
        )

        res = (
            df.to_df(["rename1", "rename2"])
            .select([col("rename1"), col("rename2")])
            .collect()
        )
        expected = [Row(3, 3), Row(4, 4), Row(5, 5), Row(6, 6), Row(7, 7)]
        assert sorted(res, key=lambda r: r[0]) == expected

        res = df.to_df(["rename1", "rename2"]).columns
        assert res == ["RENAME1", "RENAME2"]

        df_prime = df.to_df(["rename1", "rename2"])
        res = df_prime.select(df_prime.RENAME1).collect()
        expected = [Row(3), Row(4), Row(5), Row(6), Row(7)]
        assert sorted(res, key=lambda r: r[0]) == expected


def test_df_col(session_cnx):
    """Test df.col()"""
    with session_cnx() as session:
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        c = df.col("id")
        assert type(c) == Column
        assert type(c.expression) == SPAttributeReference

        c = df.col("*")
        assert type(c) == Column
        assert type(c.expression) == SPStar


def test_create_dataframe_with_basic_data_types(session_cnx):
    with session_cnx() as session:
        data1 = [
            1,
            "one",
            1.0,
            datetime.datetime.strptime(
                "2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"
            ),
            datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
            datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
            True,
            bytearray("a", "utf-8"),
            Decimal(0.5),
        ]
        data2 = [
            0,
            "",
            0.0,
            datetime.datetime.min,
            datetime.time.min,
            datetime.date.min,
            False,
            bytes(),
            Decimal(0),
        ]
        expected_names = ["_{}".format(idx + 1) for idx in range(len(data1))]
        expected_rows = [Row(*data1), Row(*data2)]
        df = session.create_dataframe([data1, data2])
        assert [field.name for field in df.schema.fields] == expected_names
        assert [type(field.datatype) for field in df.schema.fields] == [
            LongType,
            StringType,
            DoubleType,
            TimestampType,
            TimeType,
            DateType,
            BooleanType,
            BinaryType,
            DecimalType,
        ]
        result = df.collect()
        assert result == expected_rows
        assert result[0].asDict(True) == {k: v for k, v in zip(expected_names, data1)}
        assert result[1].asDict(True) == {k: v for k, v in zip(expected_names, data2)}
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_with_semi_structured_data_types(session_cnx):
    with session_cnx() as session:
        data = [
            ["'", 2],
            ("'", 2),
            [[1, 2], [2, 1]],
            array("I", [1, 2, 3]),
            {"'": 1},
        ]
        df = session.create_dataframe([data])
        assert [type(field.datatype) for field in df.schema.fields] == [
            ArrayType,
            ArrayType,
            ArrayType,
            ArrayType,
            MapType,
        ]
        assert df.collect() == [
            Row(
                '[\n  "\'",\n  2\n]',
                '[\n  "\'",\n  2\n]',
                "[\n  [\n    1,\n    2\n  ],\n  [\n    2,\n    1\n  ]\n]",
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "\'": 1\n}',
            )
        ]


def test_create_dataframe_with_dict(session):
    data = {"snow_{}".format(idx + 1): idx ** 3 for idx in range(5)}
    expected_names = [name.upper() for name in data.keys()]
    expected_rows = [Row(*data.values())]
    df = session.create_dataframe([data])
    for field, expected_name in zip(df.schema.fields, expected_names):
        assert Utils.equals_ignore_case(field.name, expected_name)
    result = df.collect()
    assert result == expected_rows
    assert result[0].asDict(True) == {
        k: v for k, v in zip(expected_names, data.values())
    }
    assert df.select(expected_names).collect() == expected_rows

    # dicts with different keys
    df = session.createDataFrame([{"a": 1}, {"b": 2}])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, None), Row(None, 2)])

    df = session.createDataFrame([{"a": 1}, {"d": 2, "e": 3}, {"c": 4, "b": 5}])
    assert [field.name for field in df.schema.fields] == ["A", "D", "E", "C", "B"]
    Utils.check_answer(
        df,
        [
            Row(1, None, None, None, None),
            Row(None, 2, 3, None, None),
            Row(None, None, None, 4, 5),
        ],
    )


def test_create_dataframe_with_namedtuple(session):
    Data = namedtuple("Data", ["snow_{}".format(idx + 1) for idx in range(5)])
    data = Data(*[idx ** 3 for idx in range(5)])
    expected_names = [name.upper() for name in data._fields]
    expected_rows = [Row(*data)]
    df = session.createDataFrame([data])
    for field, expected_name in zip(df.schema.fields, expected_names):
        assert Utils.equals_ignore_case(field.name, expected_name)
    result = df.collect()
    assert result == expected_rows
    assert result[0].asDict(True) == {k: v for k, v in zip(expected_names, data)}
    assert df.select(expected_names).collect() == expected_rows

    # dicts with different namedtuples
    Data1 = namedtuple("Data", ["a", "b"])
    Data2 = namedtuple("Data", ["d", "c"])
    df = session.createDataFrame([Data1(1, 2), Data2(3, 4)])
    assert [field.name for field in df.schema.fields] == ["A", "B", "D", "C"]
    Utils.check_answer(df, [Row(1, 2, None, None), Row(None, None, 3, 4)])


def test_create_dataframe_with_row(session):
    row1 = Row(a=1, b=2)
    row2 = Row(a=3, b=4)
    row3 = Row(d=5, c=6, e=7)
    row4 = Row(7, 8)
    row5 = Row(9, 10)

    df = session.createDataFrame([row1, row2])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [row1, row2])

    df = session.createDataFrame([row4, row5])
    assert [field.name for field in df.schema.fields] == ["_1", "_2"]
    Utils.check_answer(df, [row4, row5])

    df = session.createDataFrame([row3])
    assert [field.name for field in df.schema.fields] == ["D", "C", "E"]
    Utils.check_answer(df, [row3])

    df = session.createDataFrame([row1, row2, row3])
    assert [field.name for field in df.schema.fields] == ["A", "B", "D", "C", "E"]
    Utils.check_answer(
        df,
        [
            Row(1, 2, None, None, None),
            Row(3, 4, None, None, None),
            Row(None, None, 5, 6, 7),
        ],
    )

    with pytest.raises(ValueError) as ex_info:
        session.createDataFrame([row1, row4])
    assert "4 fields are required by schema but 2 values are provided" in str(ex_info)


def test_create_dataframe_with_mixed_dict_namedtuple_row(session):
    d = {"a": 1, "b": 2}
    Data = namedtuple("Data", ["a", "b"])
    t = Data(3, 4)
    r = Row(a=5, b=6)
    df = session.createDataFrame([d, t, r])
    assert [field.name for field in df.schema.fields] == ["A", "B"]
    Utils.check_answer(df, [Row(1, 2), Row(3, 4), Row(5, 6)])

    r2 = Row(c=7, d=8)
    df = session.createDataFrame([d, t, r2])
    assert [field.name for field in df.schema.fields] == ["A", "B", "C", "D"]
    Utils.check_answer(
        df, [Row(1, 2, None, None), Row(3, 4, None, None), Row(None, None, 7, 8)]
    )


def test_create_dataframe_with_schema_col_names(session):
    col_names = ["a", "b", "c", "d"]
    df = session.create_dataframe([[1, 2, 3, 4]], schema=col_names)
    for field, expected_name in zip(df.schema.fields, col_names):
        assert Utils.equals_ignore_case(field.name, expected_name)

    # only give first two column names,
    # and the rest will be populated as "_#num"
    df = session.create_dataframe([[1, 2, 3, 4]], schema=col_names[:2])
    for field, expected_name in zip(df.schema.fields, col_names[:2] + ["_3", "_4"]):
        assert Utils.equals_ignore_case(field.name, expected_name)

    # the column names provided via schema keyword will overwrite other column names
    df = session.create_dataframe(
        [{"aa": 1, "bb": 2, "cc": 3, "dd": 4}], schema=col_names
    )
    for field, expected_name in zip(df.schema.fields, col_names):
        assert Utils.equals_ignore_case(field.name, expected_name)


def test_create_dataframe_with_variant(session_cnx):
    with session_cnx() as session:
        data = [
            1,
            "one",
            1.1,
            datetime.datetime.strptime(
                "2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"
            ),
            datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
            datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
            True,
            bytearray("a", "utf-8"),
            Decimal(0.5),
            [1, 2, 3],
            {"a": "foo"},
        ]
        df = session.create_dataframe(
            [data],
            schema=StructType(
                [StructField(f"col_{i+1}", VariantType()) for i in range(len(data))]
            ),
        )
        assert df.collect() == [
            Row(
                "1",
                '"one"',
                "1.1",
                '"2017-02-24T12:00:05.456000"',
                '"20:57:06"',
                '"2017-02-25"',
                "true",
                '"61"',
                "0.5",
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "a": "foo"\n}',
            )
        ]


def test_create_dataframe_with_single_value(session_cnx):
    with session_cnx() as session:
        data = [1, 2, 3]
        expected_names = ["_1"]
        expected_rows = [Row(d) for d in data]
        df = session.create_dataframe(data)
        assert [field.name for field in df.schema.fields] == expected_names
        assert df.collect() == expected_rows
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_empty(session):
    Utils.check_answer(session.create_dataframe([[]]), [Row(None)])
    Utils.check_answer(session.create_dataframe([[], []]), [Row(None), Row(None)])

    with pytest.raises(ValueError) as ex_info:
        session.createDataFrame([])
    assert "data cannot be empty" in str(ex_info)


def test_create_dataframe_from_none_data(session_cnx):
    with session_cnx() as session:
        assert session.create_dataframe([None, None]).collect() == [
            Row(None),
            Row(None),
        ]
        assert session.create_dataframe([[None, None], [1, "1"]]).collect() == [
            Row(None, None),
            Row(1, "1"),
        ]
        assert session.create_dataframe([[1, "1"], [None, None]]).collect() == [
            Row(1, "1"),
            Row(None, None),
        ]

        # large None data
        assert session.create_dataframe([None] * 20000).collect() == [Row(None)] * 20000


def test_create_dataframe_large_without_batch_insert(session):
    from snowflake.snowpark._internal import analyzer_obj

    original_value = analyzer_obj.ARRAY_BIND_THRESHOLD
    try:
        analyzer_obj.ARRAY_BIND_THRESHOLD = 40000
        with pytest.raises(ProgrammingError) as ex_info:
            session.create_dataframe([1] * 20000).collect()
        assert "SQL compilation error" in str(ex_info)
        assert "maximum number of expressions in a list exceeded" in str(ex_info)
    finally:
        analyzer_obj.ARRAY_BIND_THRESHOLD = original_value


def test_create_dataframe_with_invalid_data(session_cnx):
    with session_cnx() as session:
        # None input
        with pytest.raises(ValueError) as ex_info:
            session.create_dataframe(None)
        assert "data cannot be None" in str(ex_info)

        # input other than list and tuple
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe(1)
        assert "only accepts data as a list, tuple or a pandas DataFrame" in str(
            ex_info
        )
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe({1, 2})
        assert "only accepts data as a list, tuple or a pandas DataFrame" in str(
            ex_info
        )
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe({"a": 1, "b": 2})
        assert "only accepts data as a list, tuple or a pandas DataFrame" in str(
            ex_info
        )
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe(Row(a=1, b=2))
        assert "create_dataframe() function does not accept a Row object" in str(
            ex_info
        )

        # inconsistent type
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([1, "1"])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([1, 1.0])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([1.0, Decimal(1.0)])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe(["1", bytearray("1", "utf-8")])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([datetime.datetime.now(), datetime.date.today()])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([datetime.datetime.now(), datetime.time()])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([[[1, 2, 3], 1], [1, 1]])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.create_dataframe([[[1, 2, 3], 1], [{1: 2}, 1]])
        assert "Cannot merge type" in str(ex_info)

        # inconsistent length
        with pytest.raises(ValueError) as ex_info:
            session.create_dataframe([[1], [1, 2]])
        assert "data consists of rows with different lengths" in str(ex_info)


def test_attribute_reference_to_sql(session):
    from snowflake.snowpark.functions import sum as sum_

    df = session.create_dataframe([(3, 1), (None, 2), (1, None), (4, 5)]).to_df(
        "a", "b"
    )
    agg_results = (
        df.agg(
            [
                sum_(df["a"].is_null().cast(IntegerType())),
                sum_(df["b"].is_null().cast(IntegerType())),
            ]
        )
        .to_df("a", "b")
        .collect()
    )

    Utils.check_answer([Row(1, 1)], agg_results)


def test_dataframe_duplicated_column_names(session):
    df = session.sql("select 1 as a, 2 as a")
    # collect() works and return a row with duplicated keys,
    # which aligns with Pyspark
    res = df.collect()
    assert len(res[0]) == 2
    assert res[0].A == 1

    # however, create a table/view doesn't work because
    # Snowflake doesn't allow duplicated column names
    with pytest.raises(ProgrammingError) as ex_info:
        df.create_or_replace_view(
            Utils.random_name_for_temp_object(TempObjectType.VIEW)
        )
    assert "duplicate column name 'A'" in str(ex_info)


def test_dropna(session):
    Utils.check_answer(TestData.double3(session).dropna(), [Row(1.0, 1)])

    res = TestData.double3(session).dropna(how="all").collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    Utils.check_answer(
        TestData.double3(session).dropna(subset=["a"]), [Row(1.0, 1), Row(4.0, None)]
    )

    res = TestData.double3(session).dropna(thresh=1).collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    with pytest.raises(TypeError) as ex_info:
        TestData.double3(session).dropna(subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)


def test_fillna(session):
    Utils.check_answer(
        TestData.double3(session).fillna(11),
        [
            Row(1.0, 1),
            Row(11.0, 2),
            Row(11.0, 3),
            Row(4.0, 11),
            Row(11.0, 11),
            Row(11.0, 11),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.double3(session).fillna(11, subset=["a"]),
        [
            Row(1.0, 1),
            Row(11.0, 2),
            Row(11.0, 3),
            Row(4.0, None),
            Row(11.0, None),
            Row(11.0, None),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.double3(session).fillna(None),
        [
            Row(1.0, 1),
            Row(None, 2),
            Row(None, 3),
            Row(4.0, None),
            Row(None, None),
            Row(None, None),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.null_data1(session).fillna({}), TestData.null_data1(session).collect()
    )
    Utils.check_answer(
        TestData.null_data1(session).fillna(1, subset=[]),
        TestData.null_data1(session).collect(),
    )

    # fillna for all basic data types
    data = [
        1,
        "one",
        1.0,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        Decimal(0.5),
    ]
    none_data = [None] * len(data)
    none_data[2] = float("nan")
    col_names = ["col{}".format(idx + 1) for idx in range(len(data))]
    value_dict = {
        col_name: (
            json.dumps(value) if isinstance(value, (list, dict, tuple)) else value
        )
        for col_name, value in zip(col_names, data)
    }
    df = session.create_dataframe([data, none_data], schema=col_names)
    Utils.check_answer(df.fillna(value_dict), [Row(*data), Row(*data)])

    # Python `int` can be filled into FloatType/DoubleType,
    # but Python `float` can't be filled into IntegerType/LongType (will be ignored)
    Utils.check_answer(
        session.create_dataframe(
            [[1, 1.1], [None, None]], schema=["col1", "col2"]
        ).fillna({"col1": 1.1, "col2": 1}),
        [Row(1, 1.1), Row(None, 1)],
    )

    # negative case
    df = session.create_dataframe(
        [[[1, 2], (1, 3)], [None, None]], schema=["col1", "col2"]
    )
    with pytest.raises(TypeError) as ex_info:
        df.fillna(1, subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.fillna([1, 3])
    assert "All values in value should be in one of" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.fillna((1, 3))
    assert "All values in value should be in one of" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.fillna({1: 3})
    assert "All keys in value should be column names (str)" in str(ex_info)


def test_replace(session):
    df = session.create_dataframe(
        [[1, 1.0, "1.0"], [2, 2.0, "2.0"]], schema=["a", "b", "c"]
    )

    # empty to_replace or subset will return the original dataframe
    Utils.check_answer(df.replace({}), [Row(1, 1.0, "1.0"), Row(2, 2.0, "2.0")])
    Utils.check_answer(
        df.replace({1: 4}, subset=[]), [Row(1, 1.0, "1.0"), Row(2, 2.0, "2.0")]
    )

    # subset=None will apply the replacement to all columns
    # we can replace a float with an integer
    Utils.check_answer(df.replace(1, 3), [Row(3, 3.0, "1.0"), Row(2, 2.0, "2.0")])
    Utils.check_answer(
        df.replace([1, 2], [3, 4]), [Row(3, 3.0, "1.0"), Row(4, 4.0, "2.0")]
    )
    Utils.check_answer(df.replace([1, 2], 3), [Row(3, 3.0, "1.0"), Row(3, 3.0, "2.0")])
    # value will be ignored
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, value=5), [Row(3, 3.0, "1.0"), Row(4, 4.0, "2.0")]
    )

    # subset
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, subset=["a"]), [Row(3, 1.0, "1.0"), Row(4, 2.0, "2.0")]
    )
    Utils.check_answer(
        df.replace({1: 3, 2: 4}, subset="b"), [Row(1, 3.0, "1.0"), Row(2, 4.0, "2.0")]
    )

    # we can't replace an integer with a float
    # and replace a string with a float (will be skipped)
    Utils.check_answer(
        df.replace({1: 3.0, 2: 4.0, "1.0": 1.0, "2.0": "3.0"}),
        [Row(1, 3.0, "1.0"), Row(2, 4.0, "3.0")],
    )

    # we can replace any value with a None
    Utils.check_answer(
        df.replace({1: None, 2: None, "2.0": None}),
        [Row(None, None, "1.0"), Row(None, None, None)],
    )
    Utils.check_answer(
        df.replace(1.0, None),
        [Row(1, None, "1.0"), Row(2, 2.0, "2.0")],
    )

    # negative case
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.replace({1: 3}, subset=["d"])
    assert "The DataFrame does not contain the column named" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        df.replace({1: 2}, subset={1: "a"})
    assert "subset should be a list or tuple of column names" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace([1], [2, 3])
    assert "to_replace and value lists should be of the same length" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace(1, [2, 3])
    assert "All keys and values in value should be in one of" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace([1, (1, 2)], [2, 3])
    assert "All keys and values in value should be in one of" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.replace(1, {1: 2})
    assert "All keys and values in value should be in one of" in str(ex_info)


def test_select_case_expr(session):
    df = session.create_dataframe([1, 2, 3], schema=["a"])
    Utils.check_answer(
        df.select(when(col("a") == 1, 4).otherwise(col("a"))), [Row(4), Row(2), Row(3)]
    )


def test_select_expr(session):
    df = session.create_dataframe([-1, 2, 3], schema=["a"])
    Utils.check_answer(
        df.select_expr("abs(a)", "a + 2", "cast(a as string)"),
        [Row(1, 1, "-1"), Row(2, 4, "2"), Row(3, 5, "3")],
    )
    Utils.check_answer(
        df.select_expr(["abs(a)", "a + 2", "cast(a as string)"]),
        [Row(1, 1, "-1"), Row(2, 4, "2"), Row(3, 5, "3")],
    )


def test_describe(session):
    assert TestData.test_data2(session).describe().columns == [
        "SUMMARY",
        "A",
        "B",
    ]
    Utils.check_answer(
        TestData.test_data2(session).describe("a", "b").collect(),
        [
            Row("count", 6, 6),
            Row("mean", 2.0, 1.5),
            Row("stddev", 0.8944271909999159, 0.5477225575051661),
            Row("min", 1, 1),
            Row("max", 3, 2),
        ],
    )
    Utils.check_answer(
        TestData.test_data3(session).describe().collect(),
        [
            Row("count", 2, 1),
            Row("mean", 1.5, 2.0),
            Row("stddev", 0.7071067811865476, None),
            Row("min", 1, 2),
            Row("max", 2, 2),
        ],
    )

    Utils.check_answer(
        session.create_dataframe(["a", "a", "c", "z", "b", "a"]).describe(),
        [
            Row("count", "6"),
            Row("mean", None),
            Row("stddev", None),
            Row("min", "a"),
            Row("max", "z"),
        ],
    )

    # describe() will ignore all non-numeric and non-string columns
    data = [
        1,
        "one",
        1.0,
        Decimal(0.5),
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
    ]
    assert session.create_dataframe([data]).describe().columns == [
        "SUMMARY",
        "_1",
        "_2",
        "_3",
        "_4",
    ]

    # return an "empty" dataframe if no numeric or string column is present
    Utils.check_answer(
        TestData.timestamp1(session).describe(),
        [
            Row("count"),
            Row("mean"),
            Row("stddev"),
            Row("min"),
            Row("max"),
        ],
    )

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).describe("c")
    assert "invalid identifier" in str(ex_info)


@pytest.mark.parametrize(
    "save_mode", ["append", "overwrite", "ignore", "errorifexists"]
)
def test_write_temp_table(session, save_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, 2), (3, 4)]).toDF("a", "b")
    try:
        df.write.save_as_table(table_name, mode=save_mode, create_temp_table=True)
        Utils.check_answer(session.table(table_name), df, True)
        table_info = session.sql(f"show tables like '{table_name}'").collect()
        assert table_info[0]["kind"] == "TEMPORARY"
    finally:
        Utils.drop_table(session, table_name)


def test_write_copy_into_location_basic(session):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    try:
        df = session.create_dataframe(
            [["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df.write.copy_into_location(temp_stage, file_format_type="parquet")
        copied_files = session.sql(f"list @{temp_stage}").collect()
        assert len(copied_files) == 1
        assert ".parquet" in copied_files[0][0]
    finally:
        Utils.drop_stage(session, temp_stage)


@pytest.mark.parametrize(
    "partition_by",
    [
        col("last_name"),
        "last_name",
        concat(col("last_name"), lit("s")),
        "last_name || 's'",
    ],
)
def test_write_copy_into_location_csv(session, partition_by):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    try:
        df = session.create_dataframe(
            [["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df.write.copy_into_location(
            temp_stage,
            partition_by=partition_by,
            file_format_type="csv",
            format_type_options={"COMPRESSION": "GZIP"},
            header=True,
            overwrite=False,
        )
        copied_files = session.sql(f"list @{temp_stage}").collect()
        assert len(copied_files) == 2
        assert ".csv.gz" in copied_files[0][0]
        assert ".csv.gz" in copied_files[1][0]
    finally:
        Utils.drop_stage(session, temp_stage)


def test_queries(session):
    df = TestData.column_has_special_char(session)
    queries = df.queries
    assert len(queries["queries"]) == 1
    assert len(queries["post_actions"]) == 0
    assert df._plan.queries[0].sql.strip() in queries["queries"]

    # multiple queries and
    df = session.create_dataframe([1] * 20000)
    queries, post_actions = df.queries["queries"], df.queries["post_actions"]
    assert len(queries) == 3
    assert queries[0].startswith("CREATE")
    assert queries[1].startswith("INSERT")
    assert queries[2].startswith("SELECT")
    assert len(post_actions) == 1
    assert post_actions[0].startswith("DROP")


def test_df_columns(session):
    assert session.create_dataframe([1], schema=["a"]).columns == ["A"]

    temp_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, temp_table, '"a b" int, "a""b" int, "a" int, a int', is_temporary=True
    )
    session.sql(f"insert into {temp_table} values (1, 2, 3, 4)").collect()
    try:
        df = session.table(temp_table)
        assert df.columns == ['"a b"', '"a""b"', '"a"', "A"]
        assert df.select(df.a).collect()[0][0] == 4
        assert df.select(df.A).collect()[0][0] == 4
        assert df.select(df["a"]).collect()[0][0] == 4
        assert (
            df.select(df["A"]).collect()[0][0] == 4
        )  # Snowflake finds column a without quotes.
        assert df.select(df['"a b"']).collect()[0][0] == 1
        assert df.select(df['"a""b"']).collect()[0][0] == 2
        assert df.select(df['"a"']).collect()[0][0] == 3
        assert df.select(df['"A"']).collect()[0][0] == 4

        with pytest.raises(SnowparkColumnException) as sce:
            df.select(df['"A B"']).collect()
        assert (
            sce.value.message
            == 'The DataFrame does not contain the column named "A B".'
        )
    finally:
        Utils.drop_table(session, temp_table)


@pytest.mark.parametrize(
    "column_list",
    [["jan", "feb", "mar", "apr"], [col("jan"), col("feb"), col("mar"), col("apr")]],
)
def test_unpivot(session, column_list):
    Utils.check_answer(
        TestData.monthly_sales_flat(session)
        .unpivot("sales", "month", column_list)
        .sort("empid"),
        [
            Row(1, "electronics", "JAN", 100),
            Row(1, "electronics", "FEB", 200),
            Row(1, "electronics", "MAR", 300),
            Row(1, "electronics", "APR", 100),
            Row(2, "clothes", "JAN", 100),
            Row(2, "clothes", "FEB", 300),
            Row(2, "clothes", "MAR", 150),
            Row(2, "clothes", "APR", 200),
            Row(3, "cars", "JAN", 200),
            Row(3, "cars", "FEB", 400),
            Row(3, "cars", "MAR", 100),
            Row(3, "cars", "APR", 50),
        ],
        sort=False,
    )
