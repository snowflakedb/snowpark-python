#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
from array import array
from collections import namedtuple
from decimal import Decimal
from itertools import product
from test.utils import Utils

import pytest

from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import col
from snowflake.snowpark.internal.sp_expressions import (
    AttributeReference as SPAttributeReference,
    Star as SPStar,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
)


def test_distinct(session_cnx):
    """Tests df.distinct()."""
    with session_cnx() as session:
        df = session.createDataFrame(
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
        ).toDF("id", "v")

        res = df.distinct().sort(["id", "v"]).collect()
        assert res == [
            Row([None, None]),
            Row([None, 1]),
            Row([1, None]),
            Row([1, 1]),
            Row([2, 2]),
            Row([3, 3]),
            Row([4, 4]),
            Row([5, 5]),
        ]

        res = df.select(col("id")).distinct().sort(["id"]).collect()
        assert res == [Row([None]), Row([1]), Row([2]), Row([3]), Row([4]), Row([5])]

        res = df.select(col("v")).distinct().sort(["v"]).collect()
        assert res == [Row([None]), Row([1]), Row([2]), Row([3]), Row([4]), Row([5])]


def test_first(session_cnx):
    """Tests df.first()."""
    with session_cnx() as session:
        df = session.createDataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"]]).toDF(
            "id", "v"
        )

        # empty first, should default to 1
        res = df.first()
        assert res == Row([1, "a"])

        res = df.first(0)
        assert res == []

        res = df.first(1)
        assert res == [Row([1, "a"])]

        res = df.first(2)
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"])]

        res = df.first(3)
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"]), Row([3, "c"])]

        res = df.first(4)
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"]), Row([3, "c"]), Row([4, "d"])]

        # Negative value is equivalent to collect()
        res = df.first(-1)
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"]), Row([3, "c"]), Row([4, "d"])]

        # first-value larger than cardinality
        res = df.first(123)
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"]), Row([3, "c"]), Row([4, "d"])]

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
        expected = [Row([1]), Row([3]), Row([5]), Row([7]), Row([9])]
        assert res == expected

        # range(start, end)
        df = session.range(1, 10)
        res = df.collect()
        expected = [
            Row([1]),
            Row([2]),
            Row([3]),
            Row([4]),
            Row([5]),
            Row([6]),
            Row([7]),
            Row([8]),
            Row([9]),
        ]
        assert res == expected

        # range(end)
        df = session.range(10)
        res = df.collect()
        expected = [
            Row([0]),
            Row([1]),
            Row([2]),
            Row([3]),
            Row([4]),
            Row([5]),
            Row([6]),
            Row([7]),
            Row([8]),
            Row([9]),
        ]
        assert res == expected


def test_select_single_column(session_cnx):
    """Tests df.select() on dataframes with a single column."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).select("id").collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") < 4).select("id").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 4).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 3).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter(col("id") <= 0).collect()
        expected = []
        assert res == expected


def test_select_star(session_cnx):
    """Tests df.select('*')."""
    with session_cnx() as session:
        # Single column
        res = session.range(3, 8).select("*").collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Two columns
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df.select("*").collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected


def test_df_subscriptable(session_cnx):
    """Tests select & filter as df[...]"""
    with session_cnx() as session:
        # Star, single column
        res = session.range(3, 8)[["*"]].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Star, two columns
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[["*"]].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected
        # without double brackets should refer to a Column object
        assert type(df["*"]) == Column

        # single column, str type
        df = session.range(3, 8)
        res = df[["ID"]].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected
        assert type(df["ID"]) == Column

        # single column, int type
        df = session.range(3, 8)
        res = df[df[0] > 5].collect()
        expected = [Row([6]), Row([7])]
        assert res == expected
        assert type(df[0]) == Column

        # two columns, list type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[["ID", "ID_PRIME"]].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected

        # two columns, tuple type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[("ID", "ID_PRIME")].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected

        # two columns, int type
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df[[df[1].getName()]].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected


def test_filter(session_cnx):
    """Tests for df.filter()."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") < 4).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter(col("id") <= 4).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter(col("id") <= 3).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter(col("id") <= 0).collect()
        expected = []
        assert res == expected


def test_filter_incorrect_type(session_cnx):
    """Tests for incorrect type passed to DataFrame.filter()."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)

        with pytest.raises(TypeError) as ex_info:
            df.filter("string_type")
        assert ex_info.type == TypeError
        assert (
            "DataFrame.filter() input type must be Column. Got: <class 'str'>"
            in str(ex_info)
        )

        with pytest.raises(TypeError) as ex_info:
            df.filter(1234)
        assert ex_info.type == TypeError
        assert (
            "DataFrame.filter() input type must be Column. Got: <class 'int'>"
            in str(ex_info)
        )


def test_filter_chained(session_cnx):
    """Tests for chained DataFrame.filter() operations"""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") < 4).filter(col("id") < 4).collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = (
            session.range(1, 10, 2)
            .filter(col("id") <= 4)
            .filter(col("id") >= 0)
            .collect()
        )
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = (
            session.range(1, 10, 2)
            .filter(col("id") <= 3)
            .filter(col("id") != 5)
            .collect()
        )
        expected = [Row([1]), Row([3])]
        assert res == expected


def test_filter_chained_col_objects_int(session_cnx):
    """Tests for chained DataFrame.filter() operations."""
    with session_cnx() as session:
        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 4).filter(col("id") > 1).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") > 4).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") > 1).filter(col("id") >= 5).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") >= 1).filter(col("id") >= 5).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col("id") == 5).collect()
        expected = [Row([5])]
        assert res == expected


def test_drop(session_cnx):
    """Test for dropping columns from a dataframe."""
    with session_cnx() as session:
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        res = df.drop("id").select("id_prime").collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
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
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
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
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected


def test_join_inner(session_cnx):
    """Test for inner join of dataframes."""
    with session_cnx() as session:
        # Implicit inner join on single column
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id").collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert res == expected

        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "inner").collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id").collect()
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7])]
        assert res == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7])]
        res = df1.join(df2, "id").collect()
        assert res == expected


def test_join_left_anti(session_cnx):
    """Test for left-anti join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row([3]), Row([4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row([3, 3]), Row([4, 4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "left_anti").collect()
        expected = [Row([3, 3]), Row([4, 4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_left_outer(session_cnx):
    """Test for left-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [
            Row([3, 3, None]),
            Row([4, 4, None]),
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "left_outer").collect()
        expected = [
            Row([3, 3, None]),
            Row([4, 4, None]),
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_right_outer(session_cnx):
    """Test for right-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [Row([5]), Row([6]), Row([7]), Row([8]), Row([9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
            Row([8, None, 8]),
            Row([9, None, 9]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        res = df1.join(df2, "id", "right_outer").collect()
        expected = [
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
            Row([8, None, 8]),
            Row([9, None, 9]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_left_semi(session_cnx):
    """Test for left semi join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "left_semi").collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "left_semi").collect()
        expected = [Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row([5, 5]), Row([6, 6]), Row([7, 7])]
        res = df1.join(df2, "id", "left_semi").collect()
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_cross(session_cnx):
    """Test for cross join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.crossJoin(df2).collect()
        expected = [Row([x, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.crossJoin(df2).collect()
        expected = [Row([x, x, y, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [Row([x, x, y, y]) for x, y in product(range(3, 8), range(5, 10))]
        res = df1.crossJoin(df2).collect()
        assert sorted(res, key=lambda r: (r.get(0), r.get(2))) == expected

        with pytest.raises(Exception) as ex:
            df1.join(df2, col("id"), "cross")
        assert "Cross joins cannot take columns as input." in str(ex.value)

        # Case, join on same-name column, other columns have different name, select common column.
        this = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        other = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        df_cross = this.crossJoin(other).select([this.col("id"), other.col("id")])
        res = df_cross.collect()
        expected = [Row([x, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected


def test_join_outer(session_cnx):
    """Test for outer join of dataframes."""
    with session_cnx() as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, "id", "outer").collect()
        expected = [
            Row([3]),
            Row([4]),
            Row([5]),
            Row([6]),
            Row([7]),
            Row([8]),
            Row([9]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime")])
        res = df1.join(df2, "id", "outer").collect()
        expected = [
            Row([3, 3, None]),
            Row([4, 4, None]),
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
            Row([8, None, 8]),
            Row([9, None, 9]),
        ]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col("id"), col("id").alias("id_prime1")])
        df2 = session.range(5, 10).select([col("id"), col("id").alias("id_prime2")])
        expected = [
            Row([3, 3, None]),
            Row([4, 4, None]),
            Row([5, 5, 5]),
            Row([6, 6, 6]),
            Row([7, 7, 7]),
            Row([8, None, 8]),
            Row([9, None, 9]),
        ]
        res = df1.join(df2, "id", "outer").collect()
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_toDF(session_cnx):
    """Test df.toDF()."""
    with session_cnx() as session:
        df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])

        # calling toDF() with fewer new names than columns should fail
        with pytest.raises(Exception) as ex:
            df.toDF(["new_name"])
        assert "The number of columns doesn't match. Old column names (2):" in str(
            ex.value
        )

        res = (
            df.toDF(["rename1", "rename2"])
            .select([col("rename1"), col("rename2")])
            .collect()
        )
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        res = df.toDF(["rename1", "rename2"]).columns
        assert res == ['"RENAME1"', '"RENAME2"']

        df_prime = df.toDF(["rename1", "rename2"])
        res = df_prime.select(df_prime.RENAME1).collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


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
            datetime.datetime.now(),
            datetime.date.today(),
            datetime.time(),
            True,
            bytearray("a", "utf-8"),
            Decimal(0.5),
        ]
        data2 = [
            0,
            "",
            0.0,
            datetime.datetime.min,
            datetime.date.min,
            datetime.time.min,
            False,
            bytes(),
            Decimal(0),
        ]
        expected_names = ["_{}".format(idx + 1) for idx in range(len(data1))]
        expected_rows = [Row(data1), Row(data2)]
        df = session.createDataFrame([data1, data2])
        assert [field.name for field in df.schema.fields] == expected_names
        assert [type(field.datatype) for field in df.schema.fields] == [
            LongType,
            StringType,
            DoubleType,
            TimestampType,
            DateType,
            TimeType,
            BooleanType,
            BinaryType,
            DecimalType,
        ]
        assert df.collect() == expected_rows
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_with_semi_structured_data_types(session_cnx):
    with session_cnx() as session:
        data = [
            ["'", 2],
            ("'", 2),
            [[1, 2], [2, 1]],
            array("I", [1, 2, 3]),
            {"'": 1},
            Variant(1),
        ]
        df = session.createDataFrame([data])
        assert [type(field.datatype) for field in df.schema.fields] == [
            ArrayType,
            ArrayType,
            ArrayType,
            ArrayType,
            MapType,
            VariantType,
        ]
        assert df.collect() == [
            Row(
                [
                    '[\n  "\'",\n  2\n]',
                    '[\n  "\'",\n  2\n]',
                    "[\n  [\n    1,\n    2\n  ],\n  [\n    2,\n    1\n  ]\n]",
                    "[\n  1,\n  2,\n  3\n]",
                    '{\n  "\'": 1\n}',
                    "1",
                ]
            )
        ]


def test_create_dataframe_with_dict(session_cnx):
    with session_cnx() as session:
        data = {"snow_{}".format(idx + 1): idx ** 3 for idx in range(5)}
        expected_names = list(data.keys())
        expected_rows = [Row(list(data.values()))]
        df = session.createDataFrame([data])
        for field, expected_name in zip(df.schema.fields, expected_names):
            assert Utils.equals_ignore_case(field.name, expected_name)
        assert df.collect() == expected_rows
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_with_namedtuple(session_cnx):
    Data = namedtuple("Data", ["snow_{}".format(idx + 1) for idx in range(5)])
    with session_cnx() as session:
        data = Data(*[idx ** 3 for idx in range(5)])
        expected_names = list(data._fields)
        expected_rows = [Row(data)]
        df = session.createDataFrame([data])
        for field, expected_name in zip(df.schema.fields, expected_names):
            assert Utils.equals_ignore_case(field.name, expected_name)
        assert df.collect() == expected_rows
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_with_single_value(session_cnx):
    with session_cnx() as session:
        data = [1, 2, 3]
        expected_names = ["VALUES"]
        expected_rows = [Row(d) for d in data]
        df = session.createDataFrame(data)
        assert [field.name for field in df.schema.fields] == expected_names
        assert df.collect() == expected_rows
        assert df.select(expected_names).collect() == expected_rows


def test_create_dataframe_empty(session_cnx):
    with session_cnx() as session:
        data = [[]]
        df = session.createDataFrame(data)
        expected_rows = [Row(None)]
        assert df.collect() == expected_rows


def test_create_dataframe_from_none_data(session_cnx):
    with session_cnx() as session:
        assert session.createDataFrame([None, None]).collect() == [Row(None), Row(None)]
        assert session.createDataFrame([[None, None], [1, "1"]]).collect() == [
            Row([None, None]),
            Row([1, "1"]),
        ]
        assert session.createDataFrame([[1, "1"], [None, None]]).collect() == [
            Row([1, "1"]),
            Row([None, None]),
        ]


def test_create_dataframe_with_invalid_data(session_cnx):
    with session_cnx() as session:
        # None input
        with pytest.raises(ValueError) as ex_info:
            session.createDataFrame(None)
        assert "Data cannot be None" in str(ex_info)

        # input other than list, tuple, namedtuple and dict
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame(1)
        assert "only accepts data in List, NamedTuple, Tuple or Dict type" in str(
            ex_info
        )
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame({1, 2})
        assert "only accepts data in List, NamedTuple, Tuple or Dict type" in str(
            ex_info
        )

        # inconsistent type
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([1, "1"])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([1, 1.0])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([1.0, Decimal(1.0)])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame(["1", bytearray("1", "utf-8")])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([datetime.datetime.now(), datetime.date.today()])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([datetime.datetime.now(), datetime.time()])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([[[1, 2, 3], 1], [1, 1]])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([[[1, 2, 3], 1], [{1: 2}, 1]])
        assert "Cannot merge type" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            session.createDataFrame([[[1, 2, 3], 1], [Variant({1: 2}), 1]])
        assert "Cannot merge type" in str(ex_info)

        # inconsistent length
        with pytest.raises(ValueError) as ex_info:
            session.createDataFrame([[1], [1, 2]])
        assert "Data consists of rows with different lengths" in str(ex_info)
