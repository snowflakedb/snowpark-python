#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import math

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkColumnException,
    SnowparkPlanException,
    SnowparkSQLUnexpectedAliasException,
)
from snowflake.snowpark.functions import avg, col, in_, lit, parse_json, sql_expr, when
from snowflake.snowpark.types import StringType
from tests.utils import TestData, Utils


def test_column_names_with_space(session):
    c1 = '"name with space"'
    c2 = '"name.with.dot"'
    df = session.create_dataframe([[1, "a"]]).to_df([c1, c2])
    assert df.select(c1).collect() == [Row(1)]
    assert df.select(col(c1)).collect() == [Row(1)]
    assert df.select(df[c1]).collect() == [Row(1)]

    assert df.select(c2).collect() == [Row("a")]
    assert df.select(col(c2)).collect() == [Row("a")]
    assert df.select(df[c2]).collect() == [Row("a")]


def test_column_alias_and_case_insensitive_name(session):
    df = session.create_dataframe([1, 2]).to_df(["a"])
    assert df.select(df["a"].as_("b")).schema.fields[0].name == "B"
    assert df.select(df["a"].alias("b")).schema.fields[0].name == "B"
    assert df.select(df["a"].name("b")).schema.fields[0].name == "B"


def test_column_alias_and_case_sensitive_name(session):
    df = session.create_dataframe([1, 2]).to_df(["a"])
    assert df.select(df["a"].as_('"b"')).schema.fields[0].name == '"b"'
    assert df.select(df["a"].alias('"b"')).schema.fields[0].name == '"b"'
    assert df.select(df["a"].name('"b"')).schema.fields[0].name == '"b"'


def test_unary_operator(session):
    test_data1 = TestData.test_data1(session)
    # unary minus
    assert test_data1.select(-test_data1["NUM"]).collect() == [Row(-1), Row(-2)]
    # not
    assert test_data1.select(~test_data1["BOOL"]).collect() == [
        Row(False),
        Row(True),
    ]


def test_alias(session):
    test_data1 = TestData.test_data1(session)
    assert test_data1.select(test_data1["NUM"]).schema.fields[0].name == "NUM"
    assert (
        test_data1.select(test_data1["NUM"].as_("NUMBER")).schema.fields[0].name
        == "NUMBER"
    )
    assert (
        test_data1.select(test_data1["NUM"].as_("NUMBER")).schema.fields[0].name
        != '"NUM"'
    )
    assert (
        test_data1.select(test_data1["NUM"].alias("NUMBER")).schema.fields[0].name
        == "NUMBER"
    )


def test_equal_and_not_equal(session):
    test_data1 = TestData.test_data1(session)
    assert test_data1.where(test_data1["BOOL"] == True).collect() == [Row(1, True, "a")]
    assert test_data1.where(test_data1["BOOL"] == lit(True)).collect() == [
        Row(1, True, "a")
    ]

    assert test_data1.where(test_data1["BOOL"] == False).collect() == [
        Row(2, False, "b")
    ]
    assert test_data1.where(test_data1["BOOL"] != True).collect() == [
        Row(2, False, "b")
    ]
    assert test_data1.where(test_data1["BOOL"] != lit(True)).collect() == [
        Row(2, False, "b")
    ]


def test_gt_and_lt(session):
    test_data1 = TestData.test_data1(session)
    assert test_data1.where(test_data1["NUM"] > 1).collect() == [Row(2, False, "b")]
    assert test_data1.where(test_data1["NUM"] > lit(1)).collect() == [
        Row(2, False, "b")
    ]
    assert test_data1.where(test_data1["NUM"] < 2).collect() == [Row(1, True, "a")]
    assert test_data1.where(test_data1["NUM"] < lit(2)).collect() == [Row(1, True, "a")]


def test_leq_and_geq(session):
    test_data1 = TestData.test_data1(session)
    assert test_data1.where(test_data1["NUM"] >= 2).collect() == [Row(2, False, "b")]
    assert test_data1.where(test_data1["NUM"] >= lit(2)).collect() == [
        Row(2, False, "b")
    ]
    assert test_data1.where(test_data1["NUM"] <= 1).collect() == [Row(1, True, "a")]
    assert test_data1.where(test_data1["NUM"] <= lit(1)).collect() == [
        Row(1, True, "a")
    ]
    assert test_data1.where(test_data1["NUM"].between(lit(0), lit(1))).collect() == [
        Row(1, True, "a")
    ]
    assert test_data1.where(test_data1["NUM"].between(0, 1)).collect() == [
        Row(1, True, "a")
    ]


def test_null_safe_operators(session):
    df = session.sql("select * from values(null, 1),(2, 2),(null, null) as T(a,b)")
    assert df.select(df["A"].equal_null(df["B"])).collect() == [
        Row(False),
        Row(True),
        Row(True),
    ]


def test_nan_and_null(session):
    df = session.sql(
        "select * from values(1.1,1),(null,2),('NaN' :: Float,3) as T(a, b)"
    )
    res = df.where(df["A"].equal_nan()).collect()
    assert len(res) == 1
    res_row = res[0]
    assert math.isnan(res_row[0])
    assert res_row[1] == 3
    assert df.where(df["A"].is_null()).collect() == [Row(None, 2)]
    res_row1, res_row2 = df.where(df["A"].is_not_null()).collect()
    assert res_row1 == Row(1.1, 1)
    assert math.isnan(res_row2[0])
    assert res_row2[1] == 3


def test_and_or(session):
    df = session.sql(
        "select * from values(true,true),(true,false),(false,true),(false,false) as T(a, b)"
    )
    assert df.where(df["A"] & df["B"]).collect() == [Row(True, True)]
    assert df.where(df["A"] | df["B"]).collect() == [
        Row(True, True),
        Row(True, False),
        Row(False, True),
    ]


def test_add_subtract_multiply_divide_mod_pow(session):
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


def test_cast(session):
    test_data1 = TestData.test_data1(session)
    sc = test_data1.select(test_data1["NUM"].cast(StringType())).schema
    assert len(sc.fields) == 1
    assert sc.fields[0].column_identifier == '"CAST (""NUM"" AS STRING)"'
    assert type(sc.fields[0].datatype) == StringType
    assert not sc.fields[0].nullable


def test_order(session):
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


def test_bitwise_operator(session):
    df = session.sql("select * from values(1, 2) as T(a, b)")
    assert df.select(df["A"].bitand(df["B"])).collect() == [Row(0)]
    assert df.select(df["A"].bitor(df["B"])).collect() == [Row(3)]
    assert df.select(df["A"].bitxor(df["B"])).collect() == [Row(3)]


def test_withcolumn_with_special_column_names(session):
    # Ensure that One and "One" are different column names
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(['"One"']).with_column("Two", lit("two")),
        Row(1, "two"),
    )
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(['"One"']).with_column("One", lit("two")),
        Row(1, "two"),
    )
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(["One"]).with_column('"One"', lit("two")),
        Row(1, "two"),
    )

    # Ensure that One and ONE are the same
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(["one"]).with_column('"ONE"', lit("two")),
        Row("two"),
    )
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(["One"]).with_column("One", lit("two")),
        Row("two"),
    )
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(["one"]).with_column("ONE", lit("two")),
        Row("two"),
    )
    Utils.check_answer(
        session.create_dataframe([[1]]).to_df(["OnE"]).with_column("oNe", lit("two")),
        Row("two"),
    )

    # Ensure that One and ONE are the same
    Utils.check_answer(
        session.create_dataframe([[1]])
        .to_df(['"OnE"'])
        .with_column('"OnE"', lit("two")),
        Row("two"),
    )


def test_toDF_with_special_column_names(session):
    assert (
        session.create_dataframe([[1]]).to_df(["ONE"]).schema
        == session.create_dataframe([[1]]).to_df(["one"]).schema
    )
    assert (
        session.create_dataframe([[1]]).to_df(["OnE"]).schema
        == session.create_dataframe([[1]]).to_df(["oNe"]).schema
    )
    assert (
        session.create_dataframe([[1]]).to_df(["OnE"]).schema
        == session.create_dataframe([[1]]).to_df(['"ONE"']).schema
    )
    assert (
        session.create_dataframe([[1]]).to_df(["ONE"]).schema
        != session.create_dataframe([[1]]).to_df(['"oNe"']).schema
    )
    assert (
        session.create_dataframe([[1]]).to_df(['"ONe"']).schema
        != session.create_dataframe([[1]]).to_df(['"oNe"']).schema
    )
    assert (
        session.create_dataframe([[1]]).to_df(['"ONe"']).schema
        != session.create_dataframe([[1]]).to_df(["ONe"]).schema
    )


def test_column_resolution_with_different_kins_of_names(session):
    df = session.create_dataframe([[1]]).to_df(["One"])
    assert df.select(df["one"]).collect() == [Row(1)]
    assert df.select(df["oNe"]).collect() == [Row(1)]
    assert df.select(df['"ONE"']).collect() == [Row(1)]
    with pytest.raises(SnowparkColumnException):
        df.col('"One"')

    df = session.create_dataframe([[1]]).to_df(["One One"])
    assert df.select(df["One One"]).collect() == [Row(1)]
    assert df.select(df['"One One"']).collect() == [Row(1)]
    with pytest.raises(SnowparkColumnException):
        df.col('"one one"')
    with pytest.raises(SnowparkColumnException):
        df.col("one one")
    with pytest.raises(SnowparkColumnException):
        df.col('"ONE ONE"')

    df = session.create_dataframe([[1]]).to_df(['"One One"'])
    assert df.select(df['"One One"']).collect() == [Row(1)]
    with pytest.raises(SnowparkColumnException):
        df.col('"ONE ONE"')


def test_drop_columns_by_string(session):
    df = session.create_dataframe([[1, 2]]).to_df(["One", '"One"'])
    assert df.drop("one").schema.fields[0].name == '"One"'
    assert df.drop('"One"').schema.fields[0].name == "ONE"
    assert [field.name for field in df.drop([]).schema.fields] == ["ONE", '"One"']
    assert [field.name for field in df.drop('"one"').schema.fields] == [
        "ONE",
        '"One"',
    ]

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop("ONE", '"One"')
    assert "Cannot drop all columns" in str(ex_info)


def test_drop_columns_by_column(session):
    df = session.create_dataframe([[1, 2]]).to_df(["One", '"One"'])
    assert df.drop(col("one")).schema.fields[0].name == '"One"'
    assert df.drop(df['"One"']).schema.fields[0].name == "ONE"
    assert [field.name for field in df.drop(col('"one"')).schema.fields] == [
        "ONE",
        '"One"',
    ]

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop(df["ONE"], col('"One"'))
    assert "Cannot drop all columns" in str(ex_info)

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop(df["ONE"] + col('"One"'))
    assert "You must specify the column by name" in str(ex_info)

    # Note below should arguably not work, but does because the semantics is to drop by name.
    df2 = session.create_dataframe([[1, 2]]).to_df(["One", '"One"'])
    assert df.drop(df2["one"]).schema.fields[0].name == '"One"'


def test_fully_qualified_column_name(session):
    random_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    schema = "{}.{}".format(
        session.get_current_database(), session.get_current_schema()
    )
    r_name = '"r_tr#!.{}"'.format(random_name)
    s_name = '"s_tr#!.{}"'.format(random_name)
    udf_name = '"u_tr#!.{}"'.format(random_name)
    try:
        session._run_query(f'create or replace table {schema}.{r_name} ("d(" int)')
        session._run_query(f'create or replace table {schema}.{s_name} ("c(" int)')
        session._run_query(
            f"create or replace function {schema}.{udf_name} "
            f"(v integer) returns float as '3.141592654::FLOAT'"
        )
        df = session.sql(
            f'select {schema}.{r_name}."d(",'
            f' {schema}.{s_name}."c(", {schema}.{udf_name}(1 :: INT)'
            f" from {schema}.{r_name}, {schema}.{s_name}"
        )
        cols_unresolved = [col(field.name) for field in df.schema.fields]
        cols_resolved = [df[field.name] for field in df.schema.fields]
        df2 = df.select([*cols_unresolved, *cols_resolved])
        df2.collect()
    finally:
        session._run_query(f"drop table if exists {schema}.{r_name}")
        session._run_query(f"drop table if exists {schema}.{s_name}")
        session._run_query(f"drop function if exists {schema}.{udf_name}(integer)")


def test_column_names_with_quotes(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df('col"', '"col"', '"""col"')
    assert df.select(col('col"')).collect() == [Row(1)]
    assert df.select(col('"col"""')).collect() == [Row(1)]
    assert df.select(col('"col"')).collect() == [Row(2)]
    assert df.select(col('"""col"')).collect() == [Row(3)]

    with pytest.raises(SnowparkPlanException) as ex_info:
        df.select(col('"col""')).collect()
    assert "Invalid identifier" in str(ex_info)
    with pytest.raises(SnowparkPlanException) as ex_info:
        df.select(col('""col"')).collect()
    assert "Invalid identifier" in str(ex_info)
    with pytest.raises(SnowparkPlanException) as ex_info:
        df.select(col('"col""""')).collect()
    assert "Invalid identifier" in str(ex_info)


def test_column_constructors_col(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col", '"col"', "col .")
    assert df.select(col("col")).collect() == [Row(1)]
    assert df.select(col('"col"')).collect() == [Row(2)]
    assert df.select(col("col .")).collect() == [Row(3)]
    assert df.select(col("COL")).collect() == [Row(1)]
    assert df.select(col("CoL")).collect() == [Row(1)]
    assert df.select(col('"COL"')).collect() == [Row(1)]

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(col('"Col"')).collect()
    assert "invalid identifier" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(col("COL .")).collect()
    assert "invalid identifier" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(col('"CoL"')).collect()
    assert "invalid identifier" in str(ex_info)


def test_column_constructors_select(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col", '"col"', "col .")
    assert df.select("col").collect() == [Row(1)]
    assert df.select('"col"').collect() == [Row(2)]
    assert df.select("col .").collect() == [Row(3)]
    assert df.select("COL").collect() == [Row(1)]
    assert df.select("CoL").collect() == [Row(1)]
    assert df.select('"COL"').collect() == [Row(1)]

    with pytest.raises(ProgrammingError) as ex_info:
        df.select('"Col"').collect()
    assert "invalid identifier" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select("COL .").collect()
    assert "invalid identifier" in str(ex_info)


def test_sql_expr_column(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col", '"col"', "col .")
    assert df.select(sql_expr("col")).collect() == [Row(1)]
    assert df.select(sql_expr('"col"')).collect() == [Row(2)]
    assert df.select(sql_expr("COL")).collect() == [Row(1)]
    assert df.select(sql_expr("CoL")).collect() == [Row(1)]
    assert df.select(sql_expr('"COL"')).collect() == [Row(1)]
    assert df.select(sql_expr("col + 10")).collect() == [Row(11)]
    assert df.select(sql_expr('"col" + 10')).collect() == [Row(12)]
    assert df.filter(sql_expr("col < 1")).collect() == []
    assert df.filter(sql_expr('"col" = 2')).select(col("col")).collect() == [Row(1)]

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sql_expr('"Col"')).collect()
    assert "invalid identifier" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sql_expr("COL .")).collect()
    assert "syntax error" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sql_expr('"CoL"')).collect()
    assert "invalid identifier" in str(ex_info)
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sql_expr("col .")).collect()
    assert "syntax error" in str(ex_info)


def test_errors_for_aliased_columns(session):
    df = session.create_dataframe([[1]]).to_df("c")
    with pytest.raises(SnowparkSQLUnexpectedAliasException) as ex_info:
        df.select(col("a").as_("b") + 10).collect()
    assert "You can only define aliases for the root" in str(ex_info)
    with pytest.raises(SnowparkSQLUnexpectedAliasException) as ex_info:
        df.group_by(col("a")).agg(avg(col("a").as_("b"))).collect()
    assert "You can only define aliases for the root" in str(ex_info)


def test_like(session):
    assert TestData.string4(session).where(col("A").like(lit("%p%"))).collect() == [
        Row("apple"),
        Row("peach"),
    ]

    # These are additional
    assert TestData.string4(session).where(col("A").like("a%")).collect() == [
        Row("apple"),
    ]
    assert TestData.string4(session).where(col("A").like("%x%")).collect() == []
    assert TestData.string4(session).where(col("A").like("ap.le")).collect() == []
    assert TestData.string4(session).where(col("A").like("")).collect() == []


def test_subfield(session):
    assert TestData.null_json1(session).select(col("v")["a"]).collect() == [
        Row("null"),
        Row('"foo"'),
        Row(None),
    ]

    assert TestData.array2(session).select(col("arr1")[0]).collect() == [
        Row("1"),
        Row("6"),
    ]
    assert TestData.array2(session).select(parse_json(col("f"))[0]["a"]).collect() == [
        Row("1"),
        Row("1"),
    ]

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


def test_regexp(session):
    assert TestData.string4(session).where(col("a").regexp(lit("ap.le"))).collect() == [
        Row("apple")
    ]
    assert TestData.string4(session).where(col("a").regexp(".*(a?a)")).collect() == [
        Row("banana")
    ]
    assert TestData.string4(session).where(col("A").regexp("%a%")).collect() == []

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.string4(session).where(col("A").regexp("+*")).collect()
    assert "Invalid regular expression" in str(ex_info)


def test_collate(session):
    assert TestData.string3(session).where(
        col("a").collate("en_US-trim") == "abcba"
    ).collect() == [Row("  abcba  ")]


def test_get_column_name(session):
    assert TestData.integer1(session).col("a").getName() == '"A"'
    assert not (col("col") > 100).getName()


def test_when_case(session):
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


def test_lit_contains_single_quote(session):
    df = session.create_dataframe([[1, "'"], [2, "''"]]).to_df(["a", "b"])
    assert df.where(col("b") == "'").collect() == [Row(1, "'")]


def test_in_expression_1_in_with_constant_value_list(session):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df(["a", "b", "c", "d"])

    df1 = df.filter(col("a").in_(1, 2))
    Utils.check_answer([Row(1, "a", 1, 1), Row(2, "b", 2, 2)], df1, sort=False)

    df2 = df.filter(~col("a").in_(lit(1), lit(2)))
    Utils.check_answer([Row(3, "b", 33, 33)], df2, sort=False)

    df3 = df.select(col("a").in_(1, 2).as_("in_result"))
    Utils.check_answer([Row(True), Row(True), Row(False)], df3, sort=False)

    df4 = df.select(~col("a").in_(lit(1), lit(2)).as_("in_result"))
    Utils.check_answer([Row(False), Row(False), Row(True)], df4, sort=False)

    # Redo tests with list inputs
    df1 = df.filter(col("a").in_([1, 2]))
    Utils.check_answer([Row(1, "a", 1, 1), Row(2, "b", 2, 2)], df1, sort=False)

    df2 = df.filter(~col("a").in_([lit(1), lit(2)]))
    Utils.check_answer([Row(3, "b", 33, 33)], df2, sort=False)

    df3 = df.select(col("a").in_([1, 2]).as_("in_result"))
    Utils.check_answer([Row(True), Row(True), Row(False)], df3, sort=False)

    df4 = df.select(~col("a").in_([lit(1), lit(2)]).as_("in_result"))
    Utils.check_answer([Row(False), Row(False), Row(True)], df4, sort=False)


def test_in_expression_2_in_with_subquery(session):
    df0 = session.create_dataframe([[1], [2], [5]]).to_df(["a"])
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df(["a", "b", "c", "d"])

    # filter without NOT
    df1 = df.filter(col("a").in_(df0.filter(col("a") < 3)))
    Utils.check_answer(df1, [Row(1, "a", 1, 1), Row(2, "b", 2, 2)])

    # filter with NOT
    df2 = df.filter(~df["a"].in_(df0.filter(col("a") < 3)))
    Utils.check_answer(df2, [Row(3, "b", 33, 33)])

    # select without NOT
    df3 = df.select(col("a").in_(df0.filter(col("a") < 2)).as_("in_result"))
    Utils.check_answer(df3, [Row(True), Row(False), Row(False)])

    # select with NOT
    df4 = df.select(~df["a"].in_(df0.filter(col("a") < 2)).as_("in_result"))
    Utils.check_answer(df4, [Row(False), Row(True), Row(True)])


def test_in_expression_3_in_with_all_types(session):
    ...
    # TODO: check with Kris why this doesn't need to be ported here. Can be addressed separately.


def test_in_expression_4_negative_test_to_input_column_in_value_list(session):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df(["a", "b", "c", "d"])

    with pytest.raises(TypeError) as ex_info:
        df.filter(col("a").in_([col("c")]))

    assert (
        "is not supported for the values parameter of the function in(). You must either "
        "specify a sequence of literals or a DataFrame that represents a subquery."
        in str(ex_info.value)
    )

    with pytest.raises(TypeError) as ex_info:
        df.filter(col("a").in_([1, df["c"]]))

    assert (
        "is not supported for the values parameter of the function in(). You must either "
        "specify a sequence of literals or a DataFrame that represents a subquery."
        in str(ex_info.value)
    )

    with pytest.raises(TypeError) as ex_info:
        df.filter(col("a").in_([1, df.select("c").limit(1)]))

    assert (
        "is not supported for the values parameter of the function in(). You must either "
        "specify a sequence of literals or a DataFrame that represents a subquery."
        in str(ex_info.value)
    )


def test_in_expression_5_negative_test_that_sub_query_has_multiple_columns(session):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df("a", "b", "c", "d")

    with pytest.raises(ValueError) as ex_info:
        df.filter(col("a").in_(df.select("c", "d")))

    assert "does not match the number of columns" in str(ex_info)


def test_in_expression_6_multiple_columns_with_const_values(session):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df("a", "b", "c", "d")

    # filter without NOT
    df1 = df.filter(in_([col("a"), col("b")], [[1, "a"], [2, "b"], [3, "c"]]))
    Utils.check_answer(df1, [Row(1, "a", 1, 1), Row(2, "b", 2, 2)])

    # filter with NOT
    df2 = df.filter(~in_([col("a"), col("b")], [[1, "a"], [2, "b"], [3, "c"]]))
    Utils.check_answer(df2, [Row(3, "b", 33, 33)])

    # select without NOT
    df3 = df.select(
        in_([col("a"), col("c")], [[1, 1], [2, 2], [3, 3]]).as_("in_result")
    )
    Utils.check_answer(df3, [Row(True), Row(True), Row(False)])

    # select with NOT
    df4 = df.select(
        ~in_([col("a"), col("c")], [[1, 1], [2, 2], [3, 3]]).as_("in_result")
    )
    Utils.check_answer(df4, [Row(False), Row(False), Row(True)])


def test_in_expression_7_multiple_columns_with_sub_query(session):
    df0 = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"]]).to_df("a", "b")
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df("a", "b", "c", "d")

    # filter without NOT
    df1 = df.filter(in_([col("a"), col("b")], df0))
    Utils.check_answer(df1, [Row(1, "a", 1, 1), Row(2, "b", 2, 2)])

    # filter with NOT
    df2 = df.filter(~in_([col("a"), col("b")], df0))
    Utils.check_answer(df2, [Row(3, "b", 33, 33)])

    # select without NOT
    df3 = df.select(in_([col("a"), col("b")], df0).as_("in_result"))
    Utils.check_answer(df3, [Row(True), Row(True), Row(False)])

    # select with NOT
    df4 = df.select(~in_([col("a"), col("b")], df0).as_("in_result"))
    Utils.check_answer(df4, [Row(False), Row(False), Row(True)])


def test_in_expression_8_negative_test_to_input_column_in_value_list(session):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df("a", "b", "c", "d")

    with pytest.raises(TypeError) as ex_info:
        df.filter(in_([col("a"), col("b")], [[1, "a"], [col("c"), "b"]]))

    assert (
        "is not supported for the values parameter of the function in(). You must either "
        "specify a sequence of literals or a DataFrame that represents a subquery."
        in str(ex_info.value)
    )


def test_in_expression_9_negative_test_for_the_column_count_doesnt_match_the_value_list(
    session,
):
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]]
    ).to_df("a", "b", "c", "d")

    with pytest.raises(ValueError) as ex_info:
        df.filter(in_([col("a"), col("b")], [[1, "a", 2]]))

    assert "does not match the number of columns" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        df.filter(in_([col("a"), col("b")], df.select("a", "b", "c")))

    assert "does not match the number of columns" in str(ex_info)
