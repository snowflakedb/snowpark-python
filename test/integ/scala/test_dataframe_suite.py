#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import re
from test.utils import TestData, Utils

import pytest

from snowflake import connector
from snowflake.snowpark.functions import col, lit, max, sum
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
    StringType,
    StructField,
    StructType,
    VariantType,
)

SAMPLING_DEVIATION = 0.4


def test_null_data_in_tables(session_cnx):
    with session_cnx() as session:
        table_name = Utils.random_name()
        try:
            Utils.create_table(session, table_name, "num int")
            session.sql(
                f"insert into {table_name} values(null),(null),(null)"
            ).collect()
            res = session.table(table_name).collect()
            assert res == [Row(None), Row(None), Row(None)]
        finally:
            Utils.drop_table(session, table_name)


def test_null_data_in_local_relation_with_filters(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
            ["a", "b"]
        )
        assert df.collect() == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
        df2 = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
            ["a", "b"]
        )
        assert df.collect() == df2.collect()

        assert df.filter(col("b").is_null()).collect() == [
            Row(1, None),
            Row(3, None),
        ]
        assert df.filter(col("b").is_not_null()).collect() == [Row(2, "NotNull")]
        assert df.sort(col("b").asc_nulls_last()).collect() == [
            Row(2, "NotNull"),
            Row(1, None),
            Row(3, None),
        ]


def test_project_null_values(session_cnx):
    """Tests projecting null values onto different columns in a dataframe"""
    with session_cnx() as session:
        df = session.createDataFrame([1, 2]).toDF("a").withColumn("b", lit(None))
        assert df.collect() == [Row(1, None), Row(2, None)]

        df2 = session.createDataFrame([1, 2]).toDF("a").select(lit(None))
        assert len(df2.schema.fields) == 1
        assert df2.schema.fields[0].datatype == StringType()
        assert df2.collect() == [Row(None), Row(None)]


def test_write_null_data_to_table(session):
    table_name = Utils.random_name()
    df = session.createDataFrame([(1, None), (2, None), (3, None)]).toDF("a", "b")
    try:
        df.write.saveAsTable(table_name)
        Utils.check_answer(session.table(table_name), df, True)
    finally:
        Utils.drop_table(session, table_name)


def test_createOrReplaceView_with_null_data(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
            ["a", "b"]
        )
        view_name = Utils.random_name()
        try:
            df.createOrReplaceView(view_name)

            res = session.sql(f"select * from {view_name}").collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
        finally:
            Utils.drop_view(session, view_name)


def test_adjust_column_width_of_show(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, None], [2, "NotNull"]]).toDF("a", "b")
        # run show(), make sure no error is reported
        df.show(10, 4)

        res = df._DataFrame__show_string(10, 4)
        assert (
            res
            == """
--------------
|"A"  |"B"   |
--------------
|1    |NULL  |
|2    |N...  |
--------------\n""".lstrip()
        )


def test_show_with_null_data(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, None], [2, "NotNull"]]).toDF("a", "b")
        # run show(), make sure no error is reported
        df.show(10)

        res = df._DataFrame__show_string(10)
        assert (
            res
            == """
-----------------
|"A"  |"B"      |
-----------------
|1    |NULL     |
|2    |NotNull  |
-----------------\n""".lstrip()
        )


def test_show_multi_lines_row(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(
            [
                ("line1\nline2", None),
                ("single line", "NotNull\none more line\nlast line"),
            ]
        ).toDF("a", "b")

        res = df._DataFrame__show_string(10)
        assert (
            res
            == """
-------------------------------
|"A"          |"B"            |
-------------------------------
|line1        |NULL           |
|line2        |               |
|single line  |NotNull        |
|             |one more line  |
|             |last line      |
-------------------------------\n""".lstrip()
        )


def test_show(session_cnx):
    with session_cnx() as session:
        TestData.test_data1(session).show()

        res = TestData.test_data1(session)._DataFrame__show_string(10)
        assert (
            res
            == """
--------------------------
|"NUM"  |"BOOL"  |"STR"  |
--------------------------
|1      |True    |a      |
|2      |False   |b      |
--------------------------\n""".lstrip()
        )

        # make sure show runs with sql
        session.sql("show tables").show()

        session.sql("drop table if exists test_table_123").show()

        # truncate result, no more than 50 characters
        res = session.sql(
            "drop table if exists test_table_123"
        )._DataFrame__show_string(1)

        assert (
            res
            == """
------------------------------------------------------
|"status"                                            |
------------------------------------------------------
|Drop statement executed successfully (TEST_TABL...  |
------------------------------------------------------\n""".lstrip()
        )


def test_non_select_query_composition(session_cnx):
    with session_cnx() as session:
        table_name = Utils.random_name()
        try:
            session.sql(
                f"create or replace temporary table {table_name} (num int)"
            ).collect()
            df = (
                session.sql("show tables")
                .select('"name"')
                .filter(col('"name"') == table_name)
            )
            assert len(df.collect()) == 1
            schema = df.schema
            assert len(schema.fields) == 1
            assert type(schema.fields[0].datatype) is StringType
            assert schema.fields[0].name == '"name"'
        finally:
            Utils.drop_table(session, table_name)


def test_non_select_query_composition_union(session_cnx):
    with session_cnx() as session:
        table_name = Utils.random_name()
        try:
            session.sql(
                f"create or replace temporary table {table_name} (num int)"
            ).collect()
            df1 = session.sql("show tables")
            df2 = session.sql("show tables")

            df = df1.union(df2).select('"name"').filter(col('"name"') == table_name)
            res = df.collect()
            assert len(res) == 2

        finally:
            Utils.drop_table(session, table_name)


def test_non_select_query_composition_self_union(session_cnx):
    with session_cnx() as session:
        table_name = Utils.random_name()
        try:
            session.sql(
                f"create or replace temporary table {table_name} (num int)"
            ).collect()
            df = session.sql("show tables")

            union = df.union(df).select('"name"').filter(col('"name"') == table_name)

            assert len(union.collect()) == 2
            assert len(union._DataFrame__plan.queries) == 3
        finally:
            Utils.drop_table(session, table_name)


def test_only_use_result_scan_when_composing_queries(session_cnx):
    with session_cnx() as session:
        df = session.sql("show tables")
        assert len(df._DataFrame__plan.queries) == 1
        assert df._DataFrame__plan.queries[0].sql == "show tables"

        df2 = df.select('"name"')
        assert len(df2._DataFrame__plan.queries) == 2
        assert "RESULT_SCAN" in df2._DataFrame__plan.queries[-1].sql


def test_joins_on_result_scan(session_cnx):
    with session_cnx() as session:
        df1 = session.sql("show tables").select(['"name"', '"kind"'])
        df2 = session.sql("show tables").select(['"name"', '"rows"'])

        result = df1.join(df2, '"name"')
        result.collect()  # no error
        assert len(result.schema.fields) == 3


def test_select_star(session_cnx):
    with session_cnx() as session:
        double2 = TestData.double2(session)
        expected = TestData.double2(session).collect()
        assert double2.select("*").collect() == expected
        assert double2.select(double2.col("*")).collect() == expected


def test_first(session_cnx):
    with session_cnx() as session:
        assert TestData.integer1(session).first() == Row(1)
        assert TestData.null_data1(session).first() == Row(None)
        assert TestData.integer1(session).filter(col("a") < 0).first() is None

        res = TestData.integer1(session).first(2)
        assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2)]

        # return all elements
        res = TestData.integer1(session).first(3)
        assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]

        res = TestData.integer1(session).first(10)
        assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]

        res = TestData.integer1(session).first(-10)
        assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]


def test_sample_with_row_count(session_cnx):
    """Tests sample using n (row count)"""
    with session_cnx() as session:
        row_count = 10000
        df = session.range(row_count)
        assert df.sample(n=0).count() == 0
        assert len(df.sample(n=0).collect()) == 0
        row_count_10_percent = int(row_count / 10)
        assert df.sample(n=row_count_10_percent).count() == row_count_10_percent
        assert df.sample(n=row_count).count() == row_count
        assert df.sample(n=row_count + 10).count() == row_count
        assert len(df.sample(n=row_count_10_percent).collect()) == row_count_10_percent
        assert len(df.sample(n=row_count).collect()) == row_count
        assert len(df.sample(n=row_count + 10).collect()) == row_count


def test_sample_with_frac(session_cnx):
    """Tests sample using frac"""
    with session_cnx() as session:
        row_count = 10000
        df = session.range(row_count)
        assert df.sample(frac=0.0).count() == 0
        half_row_count = row_count * 0.5
        assert (
            abs(df.sample(frac=0.5).count() - half_row_count)
            < half_row_count * SAMPLING_DEVIATION
        )
        assert df.sample(frac=1.0).count() == row_count
        assert len(df.sample(frac=0.0).collect()) == 0
        half_row_count = row_count * 0.5
        assert (
            abs(len(df.sample(frac=0.5).collect()) - half_row_count)
            < half_row_count * SAMPLING_DEVIATION
        )
        assert len(df.sample(frac=1.0).collect()) == row_count


def test_sample_negative(session_cnx):
    """Tests negative test cases for sample"""
    with session_cnx() as session:
        row_count = 10000
        df = session.range(row_count)
        with pytest.raises(ValueError):
            df.sample(n=-1)
        with pytest.raises(connector.errors.ProgrammingError):
            df.sample(n=1000001).count()
        with pytest.raises(ValueError):
            df.sample(frac=-0.01)
        with pytest.raises(ValueError):
            df.sample(frac=1.01)


def test_sample_on_join(session_cnx):
    """Tests running sample on a join statement"""
    with session_cnx() as session:
        row_count = 10000
        df1 = session.range(row_count).withColumn('"name"', lit("value1"))
        df2 = session.range(row_count).withColumn('"name"', lit("value2"))

        result = df1.join(df2, '"ID"')
        sample_row_count = int(result.count() / 10)

        assert result.sample(n=sample_row_count).count() == sample_row_count
        assert (
            abs(result.sample(frac=0.1).count() - sample_row_count)
            < sample_row_count * SAMPLING_DEVIATION
        )


def test_sample_on_union(session_cnx):
    """Tests running sample on union statements"""
    with session_cnx() as session:
        row_count = 10000
        df1 = session.range(row_count).withColumn('"name"', lit("value1"))
        df2 = session.range(5000, 5000 + row_count).withColumn('"name"', lit("value2"))

        # Test union
        result = df1.union(df2)
        sample_row_count = int(result.count() / 10)
        assert result.sample(n=sample_row_count).count() == sample_row_count
        assert (
            abs(result.sample(frac=0.1).count() - sample_row_count)
            < sample_row_count * SAMPLING_DEVIATION
        )
        # Test unionAll
        result = df1.unionAll(df2)
        sample_row_count = int(result.count() / 10)
        assert result.sample(n=sample_row_count).count() == sample_row_count
        assert (
            abs(result.sample(frac=0.1).count() - sample_row_count)
            < sample_row_count * SAMPLING_DEVIATION
        )


def test_select(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ["a", "b", "c"]
        )

        # select(String, String*) with 1 column
        expected_result = [Row(1), Row(2), Row(3)]
        assert df.select("a").collect() == expected_result
        # select(Seq[String]) with 1 column
        assert df.select(["a"]).collect() == expected_result
        # select(Column, Column*) with 1 column
        assert df.select(col("a")).collect() == expected_result
        # select(Seq[Column]) with 1 column
        assert df.select([col("a")]).collect() == expected_result

        expected_result = [Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)]
        # select(String, String*) with 3 columns
        assert df.select(["a", "b", "c"]).collect() == expected_result
        # select(Seq[String]) with 3 column
        assert df.select(["a", "b", "c"]).collect() == expected_result
        # select(Column, Column*) with 3 column
        assert df.select(col("a"), col("b"), col("c")).collect() == expected_result
        # select(Seq[Column]) with 3 column
        assert df.select([col("a"), col("b"), col("c")]).collect() == expected_result

        # test col("a") + col("c")
        expected_result = [Row("a", 11), Row("b", 22), Row("c", 33)]
        # select(Column, Column*) with col("a") + col("b")
        assert df.select(col("b"), col("a") + col("c")).collect() == expected_result
        # select(Seq[Column]) with col("a") + col("b")
        assert df.select([col("b"), col("a") + col("c")]).collect() == expected_result


def test_select_negative_select(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ["a", "b", "c"]
        )

        # Select with empty sequences
        with pytest.raises(TypeError) as ex_info:
            df.select()
        assert "select() input cannot be empty" in str(ex_info)

        with pytest.raises(TypeError) as ex_info:
            df.select([])
        assert "select() input cannot be empty" in str(ex_info)

        # select columns which don't exist
        with pytest.raises(connector.errors.ProgrammingError) as ex_info:
            df.select("not_exists_column").collect()
        assert "SQL compilation error" in str(ex_info)

        with pytest.raises(connector.errors.ProgrammingError) as ex_info:
            df.select(["not_exists_column"]).collect()
        assert "SQL compilation error" in str(ex_info)

        with pytest.raises(connector.errors.ProgrammingError) as ex_info:
            df.select(col("not_exists_column")).collect()
        assert "SQL compilation error" in str(ex_info)

        with pytest.raises(connector.errors.ProgrammingError) as ex_info:
            df.select([col("not_exists_column")]).collect()
        assert "SQL compilation error" in str(ex_info)


def test_drop_and_dropcolumns(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ["a", "b", "c"]
        )

        expected_result = [Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)]

        # drop non-exist-column (do nothing)
        assert df.drop("not_exist_column").collect() == expected_result
        assert df.drop(["not_exist_column"]).collect() == expected_result
        assert df.drop(col("not_exist_column")).collect() == expected_result
        assert df.drop([col("not_exist_column")]).collect() == expected_result

        # drop 1st column
        expected_result = [Row("a", 10), Row("b", 20), Row("c", 30)]
        assert df.drop("a").collect() == expected_result
        assert df.drop(["a"]).collect() == expected_result
        assert df.drop(col("a")).collect() == expected_result
        assert df.drop([col("a")]).collect() == expected_result

        # drop 2nd column
        expected_result = [Row(1, 10), Row(2, 20), Row(3, 30)]
        assert df.drop("b").collect() == expected_result
        assert df.drop(["b"]).collect() == expected_result
        assert df.drop(col("b")).collect() == expected_result
        assert df.drop([col("b")]).collect() == expected_result

        # drop 2nd and 3rd column
        expected_result = [Row(1), Row(2), Row(3)]
        assert df.drop("b", "c").collect() == expected_result
        assert df.drop(["b", "c"]).collect() == expected_result
        assert df.drop(col("b"), col("c")).collect() == expected_result
        assert df.drop([col("b"), col("c")]).collect() == expected_result

        # drop all columns (negative test)
        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop("a", "b", "c")
        assert "Cannot drop all column" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop(["a", "b", "c"])
        assert "Cannot drop all column" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop(col("a"), col("b"), col("c"))
        assert "Cannot drop all column" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            df.drop([col("a"), col("b"), col("c")])
        assert "Cannot drop all column" in str(ex_info)


def test_groupby(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(
            [
                ("country A", "state A", 50),
                ("country A", "state A", 50),
                ("country A", "state B", 5),
                ("country A", "state B", 5),
                ("country B", "state A", 100),
                ("country B", "state A", 100),
                ("country B", "state B", 10),
                ("country B", "state B", 10),
            ]
        ).toDF(["country", "state", "value"])

        # groupBy without column
        assert df.groupBy().agg(max(col("value"))).collect() == [Row(100)]
        assert df.groupBy([]).agg(sum(col("value"))).collect() == [Row(330)]
        assert df.groupBy().agg([sum(col("value"))]).collect() == [Row(330)]

        # groupBy() on 1 column
        expected_res = [Row("country A", 110), Row("country B", 220)]
        assert df.groupBy("country").agg(sum(col("value"))).collect() == expected_res
        assert df.groupBy(["country"]).agg(sum(col("value"))).collect() == expected_res
        assert (
            df.groupBy(col("country")).agg(sum(col("value"))).collect() == expected_res
        )
        assert (
            df.groupBy([col("country")]).agg(sum(col("value"))).collect()
            == expected_res
        )

        # groupBy() on 2 columns
        expected_res = [
            Row("country A", "state B", 10),
            Row("country B", "state B", 20),
            Row("country A", "state A", 100),
            Row("country B", "state A", 200),
        ]

        res = df.groupBy(["country", "state"]).agg(sum(col("value"))).collect()
        assert sorted(res, key=lambda x: x[2]) == expected_res

        res = (
            df.groupBy([col("country"), col("state")]).agg(sum(col("value"))).collect()
        )
        assert sorted(res, key=lambda x: x[2]) == expected_res


def test_escaped_character(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(["'", "\\", "\n"]).toDF("a")
        res = df.collect()
        assert res == [Row("'"), Row("\\"), Row("\n")]


def test_create_or_replace_temporary_view(session_cnx, db_parameters):
    with session_cnx() as session:
        view_name = Utils.random_name()
        view_name1 = f'"{view_name}%^11"'
        view_name2 = f'"{view_name}"'

        try:
            df = session.createDataFrame([1, 2, 3]).toDF("a")
            df.createOrReplaceTempView(view_name)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row(1), Row(2), Row(3)]

            # test replace
            df2 = session.createDataFrame(["a", "b", "c"]).toDF("b")
            df2.createOrReplaceTempView(view_name)
            res = session.table(view_name).collect()
            assert res == [Row("a"), Row("b"), Row("c")]

            # view name has special char
            df.createOrReplaceTempView(view_name1)
            res = session.table(view_name1).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row(1), Row(2), Row(3)]

            # view name has quote
            df.createOrReplaceTempView(view_name2)
            res = session.table(view_name2).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row(1), Row(2), Row(3)]

            # Get a second session object
            session2 = Session.builder.configs(db_parameters).create()
            assert session is not session2
            with pytest.raises(connector.errors.ProgrammingError) as ex_info:
                session2.table(view_name).collect()
            assert "does not exist or not authorized" in str(ex_info)
        finally:
            Session._set_active_session(session)
            Utils.drop_view(session, view_name)
            Utils.drop_view(session, view_name1)
            Utils.drop_view(session, view_name2)
            session2.close()


def test_quoted_column_names(session_cnx):
    with session_cnx() as session:
        normalName = "NORMAL_NAME"
        lowerCaseName = '"lower_case"'
        quoteStart = '"""quote_start"'
        quoteEnd = '"quote_end"""'
        quoteMiddle = '"quote_""_mid"'
        quoteAllCases = '"""quote_""_start"""'

        table_name = Utils.random_name()
        try:
            Utils.create_table(
                session,
                table_name,
                f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
                f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int",
            )
            session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

            # test select()
            df1 = session.table(table_name).select(
                normalName,
                lowerCaseName,
                quoteStart,
                quoteEnd,
                quoteMiddle,
                quoteAllCases,
            )
            schema1 = df1.schema

            assert len(schema1.fields) == 6
            assert schema1.fields[0].name == normalName
            assert schema1.fields[1].name == lowerCaseName
            assert schema1.fields[2].name == quoteStart
            assert schema1.fields[3].name == quoteEnd
            assert schema1.fields[4].name == quoteMiddle
            assert schema1.fields[5].name == quoteAllCases

            assert df1.collect() == [Row(1, 2, 3, 4, 5, 6)]

            # test select() + cacheResult() + select()
            # TODO uncomment cacheResult when available
            df2 = session.table(table_name).select(
                normalName,
                lowerCaseName,
                quoteStart,
                quoteEnd,
                quoteMiddle,
                quoteAllCases,
            )
            # df2 = df2.cacheResult().select(normalName,
            #                               lowerCaseName, quoteStart, quoteEnd,
            #                               quoteMiddle, quoteAllCases)
            schema2 = df2.schema

            assert len(schema2.fields) == 6
            assert schema2.fields[0].name == normalName
            assert schema2.fields[1].name == lowerCaseName
            assert schema2.fields[2].name == quoteStart
            assert schema2.fields[3].name == quoteEnd
            assert schema2.fields[4].name == quoteMiddle
            assert schema2.fields[5].name == quoteAllCases

            assert df1.collect() == [Row(1, 2, 3, 4, 5, 6)]

            # Test drop()
            df3 = session.table(table_name).drop(
                lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases
            )
            schema3 = df3.schema
            assert len(schema3.fields) == 1
            assert schema3.fields[0].name == normalName
            assert df3.collect() == [Row(1)]

            # Test select() + cacheResult() + drop()
            # TODO uncomment cacheResult when available
            df4 = session.table(table_name).select(
                normalName,
                lowerCaseName,
                quoteStart,
                quoteEnd,
                quoteMiddle,
                quoteAllCases,
            )  # df4 = df4.cacheResult()
            df4 = df4.drop(
                lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases
            )

            schema4 = df4.schema
            assert len(schema4.fields) == 1
            assert schema4.fields[0].name == normalName
            assert df4.collect() == [Row(1)]

        finally:
            Utils.drop_table(session, table_name)


def test_column_names_without_surrounding_quote(session_cnx):
    with session_cnx() as session:
        normalName = "NORMAL_NAME"
        lowerCaseName = '"lower_case"'
        quoteStart = '"""quote_start"'
        quoteEnd = '"quote_end"""'
        quoteMiddle = '"quote_""_mid"'
        quoteAllCases = '"""quote_""_start"""'

        table_name = Utils.random_name()
        try:
            Utils.create_table(
                session,
                table_name,
                f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
                f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int",
            )
            session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

            quoteStart2 = '"quote_start'
            quoteEnd2 = 'quote_end"'
            quoteMiddle2 = 'quote_"_mid'

            df1 = session.table(table_name).select(quoteStart2, quoteEnd2, quoteMiddle2)

            # Even if the input format can be simplified format,
            # the returned column is the same.
            schema1 = df1.schema
            assert len(schema1.fields) == 3
            assert schema1.fields[0].name == quoteStart
            assert schema1.fields[1].name == quoteEnd
            assert schema1.fields[2].name == quoteMiddle
            assert df1.collect() == [Row(3, 4, 5)]

        finally:
            Utils.drop_table(session, table_name)


def test_negative_test_for_user_input_invalid_quoted_name(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1, 2, 3]).toDF("a")
        with pytest.raises(SnowparkClientException) as ex_info:
            df.where(col('"A" = "A" --"') == 2).collect()
        assert "invalid identifier" in str(ex_info)


def test_clone_with_union_dataframe(session_cnx):
    with session_cnx() as session:
        table_name = Utils.random_name()
        try:
            Utils.create_table(session, table_name, "c1 int, c2 int")

            session.sql(f"insert into {table_name} values(1, 1),(2, 2)").collect()
            df = session.table(table_name)

            union_df = df.union(df)
            cloned_union_df = union_df.clone()
            res = cloned_union_df.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 2)]
        finally:
            Utils.drop_table(session, table_name)


def test_negative_test_to_input_invalid_table_name_for_saveAsTable(session):
    df = session.createDataFrame([(1, None), (2, "NotNull"), (3, None)]).toDF("a", "b")
    with pytest.raises(SnowparkClientException) as ex_info:
        df.write.saveAsTable("negative test invalid table name")
    assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_negative_test_to_input_invalid_view_name_for_createOrReplaceView(
    session_cnx, db_parameters
):
    with session_cnx() as session:
        df = session.createDataFrame([[2, "NotNull"]]).toDF(["a", "b"])
        with pytest.raises(SnowparkClientException) as ex_info:
            df.createOrReplaceView("negative test invalid table name")
        assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_variant_in_array_and_dict(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(
            [Row([1, "\"'"], {"a": "\"'"})],
            schema=StructType(
                [StructField("col1", VariantType()), StructField("col2", VariantType())]
            ),
        )
        assert df.collect() == [Row('[\n  1,\n  "\\"\'"\n]', '{\n  "a": "\\"\'"\n}')]
