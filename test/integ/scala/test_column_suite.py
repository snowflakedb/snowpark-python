#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import Utils

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import avg, col, lit, sql_expr
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


def test_column_alias_and_case_insensitive_name(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1, 2]).toDF(["a"])
        assert df.select(df["a"].as_("b")).schema.fields[0].name == "B"
        assert df.select(df["a"].alias("b")).schema.fields[0].name == "B"
        assert df.select(df["a"].name("b")).schema.fields[0].name == "B"


def test_column_alias_and_case_sensitive_name(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1, 2]).toDF(["a"])
        assert df.select(df["a"].as_('"b"')).schema.fields[0].name == '"b"'
        assert df.select(df["a"].alias('"b"')).schema.fields[0].name == '"b"'
        assert df.select(df["a"].name('"b"')).schema.fields[0].name == '"b"'


def test_withcolumn_with_special_column_names(session_cnx):
    with session_cnx() as session:
        # Ensure that One and "One" are different column names
        Utils.check_answer(
            session.createDataFrame([[1]])
            .toDF(['"One"'])
            .withColumn("Two", lit("two")),
            Row(1, "two"),
        )
        Utils.check_answer(
            session.createDataFrame([[1]])
            .toDF(['"One"'])
            .withColumn("One", lit("two")),
            Row(1, "two"),
        )
        Utils.check_answer(
            session.createDataFrame([[1]])
            .toDF(["One"])
            .withColumn('"One"', lit("two")),
            Row(1, "two"),
        )

        # Ensure that One and ONE are the same
        Utils.check_answer(
            session.createDataFrame([[1]])
            .toDF(["one"])
            .withColumn('"ONE"', lit("two")),
            Row("two"),
        )
        Utils.check_answer(
            session.createDataFrame([[1]]).toDF(["One"]).withColumn("One", lit("two")),
            Row("two"),
        )
        Utils.check_answer(
            session.createDataFrame([[1]]).toDF(["one"]).withColumn("ONE", lit("two")),
            Row("two"),
        )
        Utils.check_answer(
            session.createDataFrame([[1]]).toDF(["OnE"]).withColumn("oNe", lit("two")),
            Row("two"),
        )

        # Ensure that One and ONE are the same
        Utils.check_answer(
            session.createDataFrame([[1]])
            .toDF(['"OnE"'])
            .withColumn('"OnE"', lit("two")),
            Row("two"),
        )


def test_toDF_with_special_column_names(session_cnx):
    with session_cnx() as session:
        assert (
            session.createDataFrame([[1]]).toDF(["ONE"]).schema
            == session.createDataFrame([[1]]).toDF(["one"]).schema
        )
        assert (
            session.createDataFrame([[1]]).toDF(["OnE"]).schema
            == session.createDataFrame([[1]]).toDF(["oNe"]).schema
        )
        assert (
            session.createDataFrame([[1]]).toDF(["OnE"]).schema
            == session.createDataFrame([[1]]).toDF(['"ONE"']).schema
        )
        assert (
            session.createDataFrame([[1]]).toDF(["ONE"]).schema
            != session.createDataFrame([[1]]).toDF(['"oNe"']).schema
        )
        assert (
            session.createDataFrame([[1]]).toDF(['"ONe"']).schema
            != session.createDataFrame([[1]]).toDF(['"oNe"']).schema
        )
        assert (
            session.createDataFrame([[1]]).toDF(['"ONe"']).schema
            != session.createDataFrame([[1]]).toDF(["ONe"]).schema
        )


def test_column_resolution_with_different_kins_of_names(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1]]).toDF(["One"])
        assert df.select(df["one"]).collect() == [Row(1)]
        assert df.select(df["oNe"]).collect() == [Row(1)]
        assert df.select(df['"ONE"']).collect() == [Row(1)]
        with pytest.raises(SnowparkClientException):
            df.col('"One"')

        df = session.createDataFrame([[1]]).toDF(["One One"])
        assert df.select(df["One One"]).collect() == [Row(1)]
        assert df.select(df['"One One"']).collect() == [Row(1)]
        with pytest.raises(SnowparkClientException):
            df.col('"one one"')
        with pytest.raises(SnowparkClientException):
            df.col("one one")
        with pytest.raises(SnowparkClientException):
            df.col('"ONE ONE"')

        df = session.createDataFrame([[1]]).toDF(['"One One"'])
        assert df.select(df['"One One"']).collect() == [Row(1)]
        with pytest.raises(SnowparkClientException):
            df.col('"ONE ONE"')


def test_drop_columns_by_string(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2]]).toDF(["One", '"One"'])
        assert df.drop("one").schema.fields[0].name == '"One"'
        assert df.drop('"One"').schema.fields[0].name == "ONE"
        assert [field.name for field in df.drop([]).schema.fields] == ["ONE", '"One"']
        assert [field.name for field in df.drop('"one"').schema.fields] == [
            "ONE",
            '"One"',
        ]

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop("ONE", '"One"')
        assert "Cannot drop all columns" in str(ex_info)


def test_drop_columns_by_column(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2]]).toDF(["One", '"One"'])
        assert df.drop(col("one")).schema.fields[0].name == '"One"'
        assert df.drop(df['"One"']).schema.fields[0].name == "ONE"
        assert [field.name for field in df.drop(col('"one"')).schema.fields] == [
            "ONE",
            '"One"',
        ]

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop(df["ONE"], col('"One"'))
        assert "Cannot drop all columns" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop(df["ONE"] + col('"One"'))
        assert "Can only drop columns by name" in str(ex_info)


def test_fully_qualified_column_name(session_cnx):
    with session_cnx() as session:
        random_name = Utils.random_name()
        schema = "{}.{}".format(
            session.getCurrentDatabase(), session.getCurrentSchema()
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


def test_column_names_with_quotes(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2, 3]]).toDF('col"', '"col"', '"""col"')
        assert df.select(col('col"')).collect() == [Row(1)]
        assert df.select(col('"col"""')).collect() == [Row(1)]
        assert df.select(col('"col"')).collect() == [Row(2)]
        assert df.select(col('"""col"')).collect() == [Row(3)]

        with pytest.raises(SnowparkClientException) as ex_info:
            df.select(col('"col""')).collect()
        assert "invalid identifier" in str(ex_info)
        with pytest.raises(SnowparkClientException) as ex_info:
            df.select(col('""col"')).collect()
        assert "invalid identifier" in str(ex_info)
        with pytest.raises(SnowparkClientException) as ex_info:
            df.select(col('"col""""')).collect()
        assert "invalid identifier" in str(ex_info)


def test_column_constructors_col(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2, 3]]).toDF("col", '"col"', "col .")
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


def test_column_constructors_select(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2, 3]]).toDF("col", '"col"', "col .")
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


def test_sql_expr_column(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 2, 3]]).toDF("col", '"col"', "col .")
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


def test_errors_for_aliased_columns(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1]]).toDF("c")
        with pytest.raises(ProgrammingError) as ex_info:
            df.select(col("a").as_("b") + 10).collect()
        assert "syntax error" in str(ex_info)
        with pytest.raises(ProgrammingError) as ex_info:
            df.groupBy(col("a")).agg(avg(col("a").as_("b"))).collect()
        assert "syntax error" in str(ex_info)


def test_lit_contains_single_quote(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, "'"], [2, "''"]]).toDF(["a", "b"])
        assert df.where(col("b") == "'").collect() == [Row(1, "'")]
