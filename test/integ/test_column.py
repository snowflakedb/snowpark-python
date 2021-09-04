#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import math
from decimal import Decimal
from test.utils import TestData

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import col, lit, parse_json, when
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import StringType


def test_column_names_with_space(session_cnx):
    c1 = '"name with space"'
    c2 = '"name.with.dot"'
    with session_cnx() as session:
        df = session.createDataFrame([[1, "a"]]).toDF([c1, c2])
        assert df.select(c1).collect() == [Row(1)]
        assert df.select(col(c1)).collect() == [Row(1)]
        assert df.select(df[c1]).collect() == [Row(1)]

        assert df.select(c2).collect() == [Row("a")]
        assert df.select(col(c2)).collect() == [Row("a")]
        assert df.select(df[c2]).collect() == [Row("a")]


def test_get_column_name(session_cnx):
    with session_cnx() as session:
        assert TestData.integer1(session).col("a").getName() == '"A"'
        assert not (col("col") > 100).getName()


def test_unary_operator(session_cnx):
    with session_cnx() as session:
        test_data1 = TestData.test_data1(session)
        # unary minus
        assert test_data1.select(-test_data1["NUM"]).collect() == [Row(-1), Row(-2)]
        # not
        assert test_data1.select(~test_data1["BOOL"]).collect() == [
            Row(False),
            Row(True),
        ]


def test_equal_and_not_equal(session_cnx):
    with session_cnx() as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["BOOL"] == True).collect() == [
            Row(1, True, "a")
        ]
        assert test_data1.where(test_data1["BOOL"] == False).collect() == [
            Row(2, False, "b")
        ]


def test_gt_and_lt(session_cnx):
    with session_cnx() as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["NUM"] > 1).collect() == [Row(2, False, "b")]
        assert test_data1.where(test_data1["NUM"] < 2).collect() == [Row(1, True, "a")]


def test_ge_and_le(session_cnx):
    with session_cnx() as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["NUM"] >= 2).collect() == [
            Row(2, False, "b")
        ]
        assert test_data1.where(test_data1["NUM"] <= 1).collect() == [Row(1, True, "a")]


def test_equal_null_safe(session_cnx):
    with session_cnx() as session:
        df = session.sql("select * from values(null, 1),(2, 2),(null, null) as T(a,b)")
        assert df.select(df["A"].equal_null(df["B"])).collect() == [
            Row(False),
            Row(True),
            Row(True),
        ]


def test_nan_and_null(session_cnx):
    with session_cnx() as session:
        df = session.sql(
            "select * from values(1.1,1),(null,2),('NaN' :: Float,3) as T(a, b)"
        )
        res_row = df.where(df["A"].equal_nan()).collect()[0]
        assert math.isnan(res_row[0])
        assert res_row[1] == 3
        assert df.where(df["A"].is_null()).collect() == [Row(None, 2)]
        res_row1, res_row2 = df.where(df["A"].is_not_null()).collect()
        assert res_row1 == Row(1.1, 1)
        assert math.isnan(res_row2[0])
        assert res_row2[1] == 3


def test_and_or(session_cnx):
    with session_cnx() as session:
        df = session.sql(
            "select * from values(true,true),(true,false),(false,true),(false,false) as T(a, b)"
        )
        assert df.where(df["A"] & df["B"]).collect() == [Row(True, True)]
        assert df.where(df["A"] | df["B"]).collect() == [
            Row(True, True),
            Row(True, False),
            Row(False, True),
        ]


def test_add_subtract_multiply_divide_mod_pow(session_cnx):
    with session_cnx() as session:
        df = session.sql("select * from values(11, 13) as T(a, b)")
        assert df.select(df["A"] + df["B"]).collect() == [Row(24)]
        assert df.select(df["A"] - df["B"]).collect() == [Row(-2)]
        assert df.select(df["A"] * df["B"]).collect() == [Row(143)]
        assert df.select(df["A"] % df["B"]).collect() == [Row(11)]
        assert df.select(df["A"] ** df["B"]).collect() == [Row(11 ** 13)]
        res = df.select(df["A"] / df["B"]).collect()
        assert len(res) == 1
        assert len(res[0]) == 1
        assert res[0][0].to_eng_string() == "0.846154"

        # test reverse operator
        assert df.select(2 + df["B"]).collect() == [Row(15)]
        assert df.select(2 - df["B"]).collect() == [Row(-11)]
        assert df.select(2 * df["B"]).collect() == [Row(26)]
        assert df.select(2 % df["B"]).collect() == [Row(2)]
        assert df.select(2 ** df["B"]).collect() == [Row(2 ** 13)]
        res = df.select(2 / df["B"]).collect()
        assert len(res) == 1
        assert len(res[0]) == 1
        assert res[0][0].to_eng_string() == "0.153846"


def test_bitwise_operator(session_cnx):
    with session_cnx() as session:
        df = session.sql("select * from values(1, 2) as T(a, b)")
        assert df.select(df["A"].bitand(df["B"])).collect() == [Row(0)]
        assert df.select(df["A"].bitor(df["B"])).collect() == [Row(3)]
        assert df.select(df["A"].bitxor(df["B"])).collect() == [Row(3)]


def test_cast(session_cnx):
    with session_cnx() as session:
        test_data1 = TestData.test_data1(session)
        sc = test_data1.select(test_data1["NUM"].cast(StringType())).schema
        assert len(sc.fields) == 1
        assert sc.fields[0].column_identifier == '"CAST (""NUM"" AS STRING)"'
        assert type(sc.fields[0].datatype) == StringType
        assert not sc.fields[0].nullable


def test_order(session_cnx):
    with session_cnx() as session:
        null_data1 = TestData.null_data1(session)
        assert null_data1.sort(null_data1["A"].asc()).collect() == [
            Row(None),
            Row(None),
            Row(1),
            Row(2),
            Row(3),
        ]
        assert null_data1.sort(null_data1["A"].asc_nulls_first()).collect() == [
            Row(None),
            Row(None),
            Row(1),
            Row(2),
            Row(3),
        ]
        assert null_data1.sort(null_data1["A"].asc_nulls_last()).collect() == [
            Row(1),
            Row(2),
            Row(3),
            Row(None),
            Row(None),
        ]
        assert null_data1.sort(null_data1["A"].desc()).collect() == [
            Row(3),
            Row(2),
            Row(1),
            Row(None),
            Row(None),
        ]
        assert null_data1.sort(null_data1["A"].desc_nulls_last()).collect() == [
            Row(3),
            Row(2),
            Row(1),
            Row(None),
            Row(None),
        ]
        assert null_data1.sort(null_data1["A"].desc_nulls_first()).collect() == [
            Row(None),
            Row(None),
            Row(3),
            Row(2),
            Row(1),
        ]


def test_like(session_cnx):
    with session_cnx() as session:
        assert TestData.string4(session).where(col("A").like(lit("%p%"))).collect() == [
            Row("apple"),
            Row("peach"),
        ]
        assert TestData.string4(session).where(col("A").like("a%")).collect() == [
            Row("apple"),
        ]
        assert TestData.string4(session).where(col("A").like("%x%")).collect() == []
        assert TestData.string4(session).where(col("A").like("ap.le")).collect() == []
        assert TestData.string4(session).where(col("A").like("")).collect() == []


def test_regexp(session_cnx):
    with session_cnx() as session:
        assert TestData.string4(session).where(
            col("a").regexp(lit("ap.le"))
        ).collect() == [Row("apple")]
        assert TestData.string4(session).where(
            col("a").regexp(".*(a?a)")
        ).collect() == [Row("banana")]
        assert TestData.string4(session).where(col("A").regexp("%a%")).collect() == []

        with pytest.raises(ProgrammingError) as ex_info:
            TestData.string4(session).where(col("A").regexp("+*")).collect()
        assert "Invalid regular expression" in str(ex_info)


def test_collate(session_cnx):
    with session_cnx() as session:
        assert TestData.string3(session).where(
            col("a").collate("en_US-trim") == "abcba"
        ).collect() == [Row("  abcba  ")]


def test_subfield(session_cnx):
    with session_cnx() as session:
        assert TestData.null_json1(session).select(col("v")["a"]).collect() == [
            Row("null"),
            Row('"foo"'),
            Row(None),
        ]

        assert TestData.array2(session).select(col("arr1")[0]).collect() == [
            Row("1"),
            Row("6"),
        ]
        assert TestData.array2(session).select(
            parse_json(col("f"))[0]["a"]
        ).collect() == [Row("1"), Row("1")]

        # Row name is not case-sensitive. field name is case-sensitive
        assert TestData.variant2(session).select(
            col("src")["vehicle"][0]["make"]
        ).collect() == [Row('"Honda"')]
        assert TestData.variant2(session).select(
            col("SRC")["vehicle"][0]["make"]
        ).collect() == [Row('"Honda"')]
        assert TestData.variant2(session).select(
            col("src")["VEHICLE"][0]["make"]
        ).collect() == [Row(None)]
        assert TestData.variant2(session).select(
            col("src")["vehicle"][0]["MAKE"]
        ).collect() == [Row(None)]

        # Space and dot in key is fine. User need to escape single quote with two single quotes
        assert TestData.variant2(session).select(
            col("src")["date with '' and ."]
        ).collect() == [Row('"2017-04-28"')]

        # Path is not accepted
        assert TestData.variant2(session).select(
            col("src")["salesperson.id"]
        ).collect() == [Row(None)]


def test_when_case(session_cnx):
    with session_cnx() as session:
        assert TestData.null_data1(session).select(
            when(col("a").is_null(), lit(5))
            .when(col("a") == 1, lit(6))
            .otherwise(lit(7))
            .as_("a")
        ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]
        assert TestData.null_data1(session).select(
            when(col("a").is_null(), lit(5))
            .when(col("a") == 1, lit(6))
            .else_(lit(7))
            .as_("a")
        ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]

        # empty otherwise
        assert TestData.null_data1(session).select(
            when(col("a").is_null(), lit(5)).when(col("a") == 1, lit(6)).as_("a")
        ).collect() == [Row(5), Row(None), Row(6), Row(None), Row(5)]

        # wrong type
        with pytest.raises(ProgrammingError) as ex_info:
            TestData.null_data1(session).select(
                when(col("a").is_null(), lit("a")).when(col("a") == 1, lit(6)).as_("a")
            ).collect()
        assert "Numeric value 'a' is not recognized" in str(ex_info)
