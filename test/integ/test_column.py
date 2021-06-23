#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import math

from src.snowflake.snowpark.types.sf_types import StringType, StructType, StructField
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.functions import col
from ..utils import TestData


def test_column_names_with_space(session_cnx, db_parameters):
    c1 = '"name with space"'
    c2 = '"name.with.dot"'
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, "a"]]).toDF([c1, c2])
        assert df.select(c1).collect() == [Row(1)]
        assert df.select(col(c1)).collect() == [Row(1)]
        assert df.select(df[c1]).collect() == [Row(1)]

        assert df.select(c2).collect() == [Row('a')]
        assert df.select(col(c2)).collect() == [Row('a')]
        assert df.select(df[c2]).collect() == [Row('a')]


def test_column_alias_and_case_insensitive_name(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1]]).toDF(["a"])
        assert df.select(df["a"].as_("b")).schema.fields[0].name == "B"
        assert df.select(df["a"].alias("b")).schema.fields[0].name == "B"
        assert df.select(df["a"].name("b")).schema.fields[0].name == "B"

        assert df.select(df["a"].as_('"b"')).schema.fields[0].name == '"b"'
        assert df.select(df["a"].alias('"b"')).schema.fields[0].name == '"b"'
        assert df.select(df["a"].name('"b"')).schema.fields[0].name == '"b"'


def test_unary_operator(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data1 = TestData.test_data1(session)
        # unary minus
        assert test_data1.select(-test_data1["NUM"]).collect() == [Row([-1]), Row([-2])]
        # not
        assert test_data1.select(~test_data1["BOOL"]).collect() == [Row([False]), Row([True])]


def test_equal_and_not_equal(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["BOOL"] == True).collect() == [Row([1, True, "a"])]
        assert test_data1.where(test_data1["BOOL"] == False).collect() == [Row([2, False, "b"])]


def test_gt_and_lt(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["NUM"] > 1).collect() == [Row([2, False, "b"])]
        assert test_data1.where(test_data1["NUM"] < 2).collect() == [Row([1, True, "a"])]


def test_ge_and_le(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data1 = TestData.test_data1(session)
        assert test_data1.where(test_data1["NUM"] >= 2).collect() == [Row([2, False, "b"])]
        assert test_data1.where(test_data1["NUM"] <= 1).collect() == [Row([1, True, "a"])]


def test_equal_null_safe(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("select * from values(null, 1),(2, 2),(null, null) as T(a,b)")
        assert df.select(df["A"].equal_null(df["B"])).collect() == [Row(False), Row(True), Row(True)]


def test_nan_and_null(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("select * from values(1.1,1),(null,2),('NaN' :: Float,3) as T(a, b)")
        res_row = df.where(df["A"].equal_nan()).collect()[0]
        assert math.isnan(res_row[0])
        assert res_row[1] == 3
        assert df.where(df["A"].is_null()).collect() == [Row([None, 2])]
        res_row1, res_row2 = df.where(df["A"].is_not_null()).collect()
        assert res_row1 == Row([1.1, 1])
        assert math.isnan(res_row2[0])
        assert res_row2[1] == 3


def test_and_or(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("select * from values(true,true),(true,false),(false,true),(false,false) as T(a, b)")
        assert df.where(df["A"] & df["B"]).collect() == [Row([True, True])]
        assert df.where(df["A"] | df["B"]).collect() == [Row([True, True]), Row([True, False]), Row([False, True])]


def test_add_subtract_multiply_divide_mod_pow(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("select * from values(11, 13) as T(a, b)")
        assert df.select(df["A"] + df["B"]).collect() == [Row(24)]
        assert df.select(df["A"] - df["B"]).collect() == [Row(-2)]
        assert df.select(df["A"] * df["B"]).collect() == [Row(143)]
        assert df.select(df["A"] % df["B"]).collect() == [Row(11)]
        assert df.select(df["A"] ** df["B"]).collect() == [Row(11**13)]
        res = df.select(df["A"] / df["B"]).collect()
        assert len(res) == 1
        assert len(res[0]) == 1
        assert res[0].get_decimal(0).to_eng_string() == "0.846154"

        # test reverse operator
        assert df.select(2 + df["B"]).collect() == [Row(15)]
        assert df.select(2 - df["B"]).collect() == [Row(-11)]
        assert df.select(2 * df["B"]).collect() == [Row(26)]
        assert df.select(2 % df["B"]).collect() == [Row(2)]
        assert df.select(2 ** df["B"]).collect() == [Row(2**13)]
        res = df.select(2 / df["B"]).collect()
        assert len(res) == 1
        assert len(res[0]) == 1
        assert res[0].get_decimal(0).to_eng_string() == "0.153846"


def test_bitwise_operator(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("select * from values(1, 2) as T(a, b)")
        assert df.select(df["A"].bitand(df["B"])).collect() == [Row(0)]
        assert df.select(df["A"].bitor(df["B"])).collect() == [Row(3)]
        assert df.select(df["A"].bitxor(df["B"])).collect() == [Row(3)]


def test_cast(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data1 = TestData.test_data1(session)
        sc = test_data1.select(test_data1["NUM"].cast(StringType())).schema
        assert len(sc.fields) == 1
        assert sc.fields[0].column_identifier == "\"CAST (\"\"NUM\"\" AS STRING)\""
        assert type(sc.fields[0].datatype) == StringType
        assert not sc.fields[0].nullable


def test_order(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        null_data1 = TestData.null_data1(session)
        assert null_data1.sort(null_data1["A"].asc()).collect() == [Row(None), Row(None), Row(1), Row(2), Row(3)]
        assert null_data1.sort(null_data1["A"].asc_nulls_first()).collect() == [Row(None), Row(None), Row(1), Row(2),
                                                                                Row(3)]
        assert null_data1.sort(null_data1["A"].asc_nulls_last()).collect() == [Row(1), Row(2), Row(3), Row(None),
                                                                               Row(None)]
        assert null_data1.sort(null_data1["A"].desc()).collect() == [Row(3), Row(2), Row(1), Row(None), Row(None)]
        assert null_data1.sort(null_data1["A"].desc_nulls_last()).collect() == [Row(3), Row(2), Row(1), Row(None),
                                                                                Row(None)]
        assert null_data1.sort(null_data1["A"].desc_nulls_first()).collect() == [Row(None), Row(None), Row(3), Row(2),
                                                                                 Row(1)]
