import re

from src.snowflake.snowpark.functions import col, max, sum
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from src.snowflake.snowpark.types.sf_types import StringType
from test.utils import Utils, TestData

from snowflake import connector

import pytest


def test_null_data_in_tables(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name = Utils.random_name()
        try:
            Utils.create_table(session, table_name, "num int")
            session.sql(f"insert into {table_name} values(null),(null),(null)").collect()
            res = session.table(table_name).collect()
            assert res == [Row([None]), Row([None]), Row([None])]
        finally:
            Utils.drop_table(session, table_name)


@pytest.mark.skip(reason='requires is_null, sort, createDataFrame type inference')
def test_null_data_in_local_relation_with_filters(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, None], [2, 'NotNull'], [3, None]]).toDF(['a', 'b'])
        assert df.collect() == [Row([1, None]), Row([2, 'NotNull']), Row([3, None])]
        df2 = session.createDataFrame([[1, None], [2, 'NotNull'], [3, None]]).toDF(['a', 'b'])
        assert df.collect() == df2.collect()

        assert df.filter(col('b').is_null()).collect() == [Row([1, None]), Row([3, None])]
        assert df.filter((col('b').is_not_null())).collect() == [Row([2, 'NotNull'])]
        assert df.sort(col('b').asc_nulls_last).collect() == \
               [Row([2, 'NotNull']), Row([1, None]), Row([3, None])]


def test_createOrReplaceView_with_null_data_modified(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[2, 'NotNull'], [1, None], [3, None]]).toDF(['a', 'b'])
        view_name = Utils.random_name()
        df.createOrReplaceView(view_name)

        res = session.sql(f"select * from {view_name}").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, None]), Row([2, 'NotNull']), Row([3, None])]


def test_non_select_query_composition(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name = Utils.random_name()
        try:
            session.sql(f"create or replace temporary table {table_name} (num int)").collect()
            df = session.sql('show tables').select('"name"').filter(col('"name"') == table_name)
            assert len(df.collect()) == 1
            schema = df.schema
            assert len(schema.fields) == 1
            assert type(schema.fields[0].datatype) is StringType
            assert schema.fields[0].name == '"name"'
        finally:
            Utils.drop_table(session, table_name)


def test_only_use_result_scan_when_composing_queries(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.sql("show tables")
        assert len(df._DataFrame__plan.queries) == 1
        assert df._DataFrame__plan.queries[0].sql == 'show tables'

        df2 = df.select('"name"')
        assert len(df2._DataFrame__plan.queries) == 2
        assert 'RESULT_SCAN' in df2._DataFrame__plan.queries[-1].sql


def test_joins_on_result_scan(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.sql('show tables').select(['"name"', '"kind"'])
        df2 = session.sql('show tables').select(['"name"', '"rows"'])

        result = df1.join(df2, '"name"')
        result.collect()  # no error
        assert len(result.schema.fields) == 3


def test_select_star(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        double2 = TestData.double2(session)
        expected = TestData.double2(session).collect()
        assert double2.select("*").collect() == expected
        assert double2.select(double2.col('*')).collect() == expected


def test_select(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ['a', 'b', 'c'])

        # select(String, String*) with 1 column
        expected_result = [Row([1]), Row([2]), Row([3])]
        assert df.select("a").collect() == expected_result
        # select(Seq[String]) with 1 column
        assert df.select(["a"]).collect() == expected_result
        # select(Column, Column*) with 1 column
        assert df.select(col("a")).collect() == expected_result
        # select(Seq[Column]) with 1 column
        assert df.select([col("a")]).collect() == expected_result

        expected_result = [Row([1, "a", 10]), Row([2, "b", 20]), Row([3, "c", 30])]
        # select(String, String*) with 3 columns
        assert df.select(["a", "b", "c"]).collect() == expected_result
        # select(Seq[String]) with 3 column
        assert df.select(["a", "b", "c"]).collect() == expected_result
        # select(Column, Column*) with 3 column
        assert df.select(col("a"), col("b"), col("c")).collect() == expected_result
        # select(Seq[Column]) with 3 column
        assert df.select([col("a"), col("b"), col("c")]).collect() == expected_result

        # test col("a") + col("c")
        expected_result = [Row(["a", 11]), Row(["b", 22]), Row(["c", 33])]
        # select(Column, Column*) with col("a") + col("b")
        assert df.select(col("b"), col("a") + col("c")).collect() == expected_result
        # select(Seq[Column]) with col("a") + col("b")
        assert df.select([col("b"), col("a") + col("c")]).collect() == expected_result


def test_select_negative_select(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ['a', 'b', 'c'])

        # Select with empty sequences
        with pytest.raises(TypeError) as ex_info:
            df.select()
        assert "Select input must be Column, str, or list" in str(ex_info)

        with pytest.raises(TypeError) as ex_info:
            df.select([])
        assert "Select input must be Column, str, or list" in str(ex_info)

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


def test_drop_and_dropcolumns(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
            ['a', 'b', 'c'])

        expected_result = [Row([1, "a", 10]), Row([2, "b", 20]), Row([3, "c", 30])]

        # drop non-exist-column (do nothing)
        assert df.drop('not_exist_column').collect() == expected_result
        assert df.drop(['not_exist_column']).collect() == expected_result
        assert df.drop(col('not_exist_column')).collect() == expected_result
        assert df.drop([col('not_exist_column')]).collect() == expected_result

        # drop 1st column
        expected_result = [Row(["a", 10]), Row(["b", 20]), Row(["c", 30])]
        assert df.drop('a').collect() == expected_result
        assert df.drop(['a']).collect() == expected_result
        assert df.drop(col('a')).collect() == expected_result
        assert df.drop([col('a')]).collect() == expected_result

        # drop 2nd column
        expected_result = [Row([1, 10]), Row([2, 20]), Row([3, 30])]
        assert df.drop('b').collect() == expected_result
        assert df.drop(['b']).collect() == expected_result
        assert df.drop(col('b')).collect() == expected_result
        assert df.drop([col('b')]).collect() == expected_result

        # drop 2nd and 3rd column
        expected_result = [Row([1]), Row([2]), Row([3])]
        assert df.drop('b', 'c').collect() == expected_result
        assert df.drop(['b', 'c']).collect() == expected_result
        assert df.drop(col('b'), col('c')).collect() == expected_result
        assert df.drop([col('b'), col('c')]).collect() == expected_result

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


def test_groupby(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([("country A", "state A", 50),
                                      ("country A", "state A", 50),
                                      ("country A", "state B", 5),
                                      ("country A", "state B", 5),
                                      ("country B", "state A", 100),
                                      ("country B", "state A", 100),
                                      ("country B", "state B", 10),
                                      ("country B", "state B", 10)]). \
            toDF(["country", "state", "value"])

        # groupBy without column
        assert df.groupBy().agg(max(col("value"))).collect() == [Row([100])]
        assert df.groupBy([]).agg(sum(col("value"))).collect() == [Row([330])]
        assert df.groupBy().agg([sum(col("value"))]).collect() == [Row([330])]

        # groupBy() on 1 column
        expected_res = [Row(["country A", 110]), Row(["country B", 220])]
        assert df.groupBy('country').agg(sum(col('value'))).collect() == expected_res
        assert df.groupBy(['country']).agg(sum(col('value'))).collect() == expected_res
        assert df.groupBy(col('country')).agg(sum(col('value'))).collect() == expected_res
        assert df.groupBy([col('country')]).agg(sum(col('value'))).collect() == expected_res

        # groupBy() on 2 columns
        expected_res = [Row(["country A", "state B", 10]),
                        Row(["country B", "state B", 20]),
                        Row(["country A", "state A", 100]),
                        Row(["country B", "state A", 200])]

        res = df.groupBy(['country', 'state']).agg(sum(col('value'))).collect()
        assert sorted(res, key=lambda x: x[2]) == expected_res

        res = df.groupBy([col('country'), col('state')]).agg(sum(col('value'))).collect()
        assert sorted(res, key=lambda x: x[2]) == expected_res


@pytest.mark.skip(reason='bug in code, needs to be fixed')
def test_escaped_character(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame(["'", "\\", "\n"]).toDF('a')
        res = df.collect()
        assert res == [Row(["'"]), Row(["\\"]), Row(["\n"])]


def test_create_or_replace_temporary_view(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        view_name = Utils.random_name()
        view_name1 = f"\"{view_name}%^11\""
        view_name2 = f"\"{view_name}\""

        try:
            df = session.createDataFrame([1, 2, 3]).toDF('a')
            df.createOrReplaceTempView(view_name)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]

            # test replace
            df2 = session.createDataFrame(["a", "b", "c"]).toDF('b')
            df2.createOrReplaceTempView(view_name)
            res = session.table(view_name).collect()
            assert res == [Row(["a"]), Row(["b"]), Row(["c"])]

            # view name has special char
            df.createOrReplaceTempView(view_name1)
            res = session.table(view_name1).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]

            # view name has quote
            df.createOrReplaceTempView(view_name2)
            res = session.table(view_name2).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]

            # Get a second session object
            with session_cnx(db_parameters) as session2:
                assert session is not session2
                with pytest.raises(connector.errors.ProgrammingError) as ex_info:
                    res = session2.table(view_name).collect()
                assert "does not exist or not authorized" in str(ex_info)
        finally:
            Utils.drop_view(session, view_name)
            Utils.drop_view(session, view_name1)
            Utils.drop_view(session, view_name2)


def test_quoted_column_names(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        normalName = "NORMAL_NAME"
        lowerCaseName = "\"lower_case\""
        quoteStart = "\"\"\"quote_start\""
        quoteEnd = "\"quote_end\"\"\""
        quoteMiddle = "\"quote_\"\"_mid\""
        quoteAllCases = "\"\"\"quote_\"\"_start\"\"\""

        table_name = Utils.random_name()
        try:
            Utils.create_table(session, table_name,
                               f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
                               f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int")
            session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

            # test select()
            df1 = session.table(table_name).select(normalName, lowerCaseName, quoteStart, quoteEnd,
                                                   quoteMiddle, quoteAllCases)
            schema1 = df1.schema

            assert len(schema1.fields) == 6
            assert schema1.fields[0].name == normalName
            assert schema1.fields[1].name == lowerCaseName
            assert schema1.fields[2].name == quoteStart
            assert schema1.fields[3].name == quoteEnd
            assert schema1.fields[4].name == quoteMiddle
            assert schema1.fields[5].name == quoteAllCases

            assert df1.collect() == [Row([1, 2, 3, 4, 5, 6])]

            # test select() + cacheResult() + select()
            # TODO uncomment cacheResult when available
            df2 = session.table(table_name).select(normalName, lowerCaseName, quoteStart, quoteEnd,
                                                   quoteMiddle, quoteAllCases)
            # df2 = df2.cacheResult().select(normalName, lowerCaseName, quoteStart, quoteEnd,
            #                               quoteMiddle, quoteAllCases)
            schema2 = df2.schema

            assert len(schema2.fields) == 6
            assert schema2.fields[0].name == normalName
            assert schema2.fields[1].name == lowerCaseName
            assert schema2.fields[2].name == quoteStart
            assert schema2.fields[3].name == quoteEnd
            assert schema2.fields[4].name == quoteMiddle
            assert schema2.fields[5].name == quoteAllCases

            assert df1.collect() == [Row([1, 2, 3, 4, 5, 6])]

            # Test drop()
            df3 = session.table(table_name).drop(lowerCaseName, quoteStart, quoteEnd, quoteMiddle,
                                                 quoteAllCases)
            schema3 = df3.schema
            assert len(schema3.fields) == 1
            assert schema3.fields[0].name == normalName
            assert df3.collect() == [Row([1])]

            # Test select() + cacheResult() + drop()
            # TODO uncomment cacheResult when available
            df4 = session.table(table_name).select(normalName, lowerCaseName, quoteStart, quoteEnd,
                                                   quoteMiddle, quoteAllCases) \
                # df4 = df4.cacheResult()
            df4 = df4.drop(lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)

            schema4 = df4.schema
            assert len(schema4.fields) == 1
            assert schema4.fields[0].name == normalName
            assert df4.collect() == [Row([1])]

        finally:
            Utils.drop_table(session, table_name)


def test_column_names_without_surrounding_quote(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        normalName = "NORMAL_NAME"
        lowerCaseName = "\"lower_case\""
        quoteStart = "\"\"\"quote_start\""
        quoteEnd = "\"quote_end\"\"\""
        quoteMiddle = "\"quote_\"\"_mid\""
        quoteAllCases = "\"\"\"quote_\"\"_start\"\"\""

        table_name = Utils.random_name()
        try:
            Utils.create_table(session, table_name,
                               f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
                               f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int")
            session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

            quoteStart2 = "\"quote_start"
            quoteEnd2 = "quote_end\""
            quoteMiddle2 = "quote_\"_mid"

            df1 = session.table(table_name).select(quoteStart2, quoteEnd2, quoteMiddle2)

            # Even if the input format can be simplified format, the returned column is the same.
            schema1 = df1.schema
            assert len(schema1.fields) == 3
            assert schema1.fields[0].name == quoteStart
            assert schema1.fields[1].name == quoteEnd
            assert schema1.fields[2].name == quoteMiddle
            assert df1.collect() == [Row([3, 4, 5])]

        finally:
            Utils.drop_table(session, table_name)

def test_negative_test_for_user_input_invalid_quoted_name(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([1, 2, 3]).toDF('a')
        with pytest.raises(SnowparkClientException) as ex_info:
            df.where(col('"A" = "A" --"') == 2).collect()
        assert "Invalid identifier" in str(ex_info)


def test_negative_test_to_input_invalid_view_name_for_createOrReplaceView(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[2, 'NotNull']]).toDF(['a', 'b'])
        with pytest.raises(SnowparkClientException) as ex_info:
            df.createOrReplaceView("negative test invalid table name")
        assert re.compile("The object name .* is invalid.").match(ex_info.value.message)
