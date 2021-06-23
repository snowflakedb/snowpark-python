from src.snowflake.snowpark.dataframe import DataFrame
from src.snowflake.snowpark.functions import count, col, lit
from src.snowflake.snowpark.row import Row

import pytest

from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from test.utils import Utils


def test_join_using(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[i, str(i)] for i in range(1, 4)]).toDF(['int', 'str'])
        df2 = session.createDataFrame([[i, str(i + 1)] for i in range(1, 4)]).toDF(['int', 'str'])

        assert df.join(df2, 'int').collect() == \
               [Row([1, '1', '2']), Row([2, '2', '3']), Row([3, '3', '4'])]


def test_join_using_multiple_columns(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[i, i + 1, str(i)] for i in range(1, 4)]). \
            toDF(['int', 'int2', 'str'])
        df2 = session.createDataFrame([[i, i + 1, str(i + 1)] for i in range(1, 4)]). \
            toDF(['int', 'int2', 'str'])

        res = df.join(df2, ['int', 'int2']).collect()
        assert sorted(res, key=lambda x: x[0]) == \
               [Row([1, 2, '1', '2']), Row([2, 3, '2', '3']), Row([3, 4, '3', '4'])]


def test_full_outer_join_followed_by_inner_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        a = session.createDataFrame([[1, 2], [2, 3]]).toDF(['a', 'b'])
        b = session.createDataFrame([[2, 5], [3, 4]]).toDF(['a', 'c'])
        c = session.createDataFrame([[3, 1]]).toDF(['a', 'd'])

        ab = a.join(b, ['a'], 'fullouter')
        abc = ab.join(c, 'a')
        assert abc.collect() == [Row([3, None, 4, 1])]


# TODO add limit as the original test
def test_limit_with_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 1, '1'], [2, 2, '3']]).toDF(['int', 'int2', 'str'])
        df2 = session.createDataFrame([[1, 1, '1'], [2, 3, '5']]).toDF(['int', 'int2', 'str'])
        inner = df.join(df2, ['int', 'int2'], 'inner').agg(count(col('int')))
        assert inner.collect() == [Row([1])]


def test_default_inner_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([1, 2]).toDF(['a'])
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])

        res = df.join(df2).collect()
        res.sort(key=lambda x: (x[0], x[1]))
        assert res == \
               [Row([1, 1, "test1"]), Row([1, 2, "test2"]), Row([2, 1, "test1"]),
                Row([2, 2, "test2"])]


def test_default_inner_join_using_column(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([1, 2]).toDF(['a'])
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])

        assert df.join(df2, 'a').collect() == [Row([1, "test1"]), Row([2, "test2"])]
        assert df.join(df2, 'a').filter(col('a') > 1).collect() == [Row([2, 'test2'])]


def test_3_way_joins(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame([1, 2]).toDF(['a'])
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])
        df3 = session.createDataFrame([[f"test{i}", f"hello{i}"] for i in range(1, 3)]) \
            .toDF(['key', 'val'])

        # 3 way join with column renaming
        res = df1.join(df2, 'a').toDF(['num', 'key']).join(df3, ['key']).collect()
        assert res == [Row(['test1', 1, 'hello1']), Row(['test2', 2, 'hello2'])]


def test_default_inner_join_with_join_conditions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])
        df2 = session.createDataFrame([[i, f"num{i}"] for i in range(1, 3)]).toDF(['num', 'val'])

        res = df1.join(df2, df1['a'] == df2['num']).collect()
        assert sorted(res, key=lambda x: x[0]) == \
               [Row([1, 'test1', 1, 'num1']), Row([2, 'test2', 2, 'num2'])]


def test_join_with_multiple_conditions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])
        df2 = session.createDataFrame([[i, f"num{i}"] for i in range(1, 3)]).toDF(['num', 'val'])

        res = df1.join(df2, df1['a'] == df2['num'] and df1['b'] == df2['val']).collect()
        assert res == []


@pytest.mark.skip(message='Requires wrapException in SnowflakePlan')
def test_join_with_ambiguous_column_in_condidtion(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([1, 2]).toDF(['a'])
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])

        with pytest.raises(SnowparkClientException) as ex_info:
            df.join(df2, col('a') == col('a')).collect()
        assert 'Possible ambiguous reference to' in str(ex_info)


def test_join_using_multiple_columns_and_specifying_join_type(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        table_name2 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "int int, int2 int, str string")
            session.sql(f"insert into {table_name1} values(1, 2, '1'),(3, 4, '3')").collect()
            Utils.create_table(session, table_name2, "int int, int2 int, str string")
            session.sql(f"insert into {table_name2} values(1, 3, '1'),(5, 6, '5')").collect()

            df = session.table(table_name1)
            df2 = session.table(table_name2)

            assert df.join(df2, ['int', 'str'], 'inner').collect() == [Row([1, '1', 2, 3])]

            res = df.join(df2, ['int', 'str'], 'left').collect()
            assert sorted(res, key=lambda x: x[0]) == [Row([1, '1', 2, 3]), Row([3, '3', 4, None])]

            res = df.join(df2, ['int', 'str'], 'right').collect()
            assert sorted(res, key=lambda x: x[0]) == [Row([1, "1", 2, 3]), Row([5, "5", None, 6])]

            res = df.join(df2, ['int', 'str'], 'outer').collect()
            res.sort(key=lambda x: x[0])
            assert res == \
                   [Row([1, "1", 2, 3]), Row([3, '3', 4, None]), Row([5, "5", None, 6])]

            assert df.join(df2, ['int', 'str'], 'left_semi').collect() == [Row([1, 2, '1'])]
            assert df.join(df2, ['int', 'str'], 'semi').collect() == [Row([1, 2, '1'])]

            assert df.join(df2, ['int', 'str'], 'left_anti').collect() == [Row([3, 4, '3'])]
            assert df.join(df2, ['int', 'str'], 'anti').collect() == [Row([3, 4, '3'])]
        finally:
            Utils.drop_table(session, table_name1)
            Utils.drop_table(session, table_name2)


def test_cross_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame([[1, '1'], [3, '3']]).toDF(['int', 'str'])
        df2 = session.createDataFrame([[2, '2'], [4, '4']]).toDF(['int', 'str'])

        res = df1.crossJoin(df2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "1", 2, "2"]), Row([1, "1", 4, "4"]), Row([3, "3", 2, "2"]),
                       Row([3, "3", 4, "4"])]

        res = df2.crossJoin(df1).collect()
        res.sort(key=lambda x: (x[0], x[2]))
        assert res == [Row([2, "2", 1, "1"]), Row([2, "2", 3, "3"]), Row([4, "4", 1, "1"]),
                       Row([4, "4", 3, "3"])]


def test_join_ambiguous_columns_with_specified_sources(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([1, 2]).toDF(['a'])
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 3)]).toDF(['a', 'b'])

        res = df.join(df2, df['a'] == df2['a']).collect()
        assert sorted(res, key=lambda x: x[0]) == [Row([1, 1, 'test1']), Row([2, 2, 'test2'])]

        res = df.join(df2, df['a'] == df2['a']).select(df['a'] * df2['a'], 'b').collect()
        assert sorted(res, key=lambda x: x[0]) == [Row([1, 'test1']), Row([4, 'test2'])]


@pytest.mark.skip(message='Requires wrapException in SnowflakePlan')
def test_join_ambiguous_columns_without_specified_sources(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 'one'], [2, 'two']]).toDF(['intcol', ' stringcol'])
        df2 = session.createDataFrame([[1, 'one'], [3, 'three']]).toDF(['intcol', ' bcol'])

        for join_type in ['inner', 'leftouter', 'rightouter', 'full_outer']:
            with pytest.raises(SnowparkClientException) as ex_info:
                df.join(df2, col('intcol') == col('intcol')).collect()
            assert 'Possible ambiguous reference to' in str(ex_info)
            assert 'INTCOL' in str(ex_info)

            with pytest.raises(SnowparkClientException) as ex_info:
                df.join(df2, df['intcol'] == df['intcol']).collect()
            assert 'Possible ambiguous reference to' in str(ex_info)
            assert 'INTCOL' in str(ex_info)


def test_join_expression_ambiguous_columns(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, -1, 'one'], [2, -2, 'two']]).toDF(
            ['intcol', 'negcol', 'lhscol'])
        rhs = session.createDataFrame([[1, -10, 'one'], [2, -20, 'two']]).toDF(
            ['intcol', 'negcol', 'rhscol'])

        df = lhs.join(rhs, lhs['intcol'] == rhs['intcol']) \
            .select(lhs['intcol'] + rhs['intcol'], lhs['negcol'], rhs['negcol'], 'lhscol', 'rhscol')

        res = sorted(df.collect(), key=lambda x: x[0])
        assert res == [Row([2, -1, -10, "one", "one"]), Row([4, -2, -20, "two", "two"])]


@pytest.mark.skip(message='Requires wrapException in SnowflakePlan')
def semi_join_expression_ambiguous_columns(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, -1, 'one'], [2, -2, 'two']]).toDF(
            ['intcol', 'negcol', 'lhscol'])
        rhs = session.createDataFrame([[1, -10, 'one'], [2, -20, 'two']]).toDF(
            ['intcol', 'negcol', 'rhscol'])

        with pytest.raises(SnowparkClientException) as ex_info:
            lhs.join(rhs, lhs['intcol'] == rhs['intcol'], 'leftsemi').select(
                rhs['intcol']).collect()
        assert 'Column referenced with df["INTCOL"]' in str(ex_info)
        assert 'not present' in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            lhs.join(rhs, lhs['intcol'] == rhs['intcol'], 'leftanti').select(
                rhs['intcol']).collect()
        assert 'Column referenced with df["INTCOL"]' in str(ex_info)
        assert 'not present' in str(ex_info)


def test_semi_join_with_columns_from_LHS(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, -1, 'one'], [2, -2, 'two']]).toDF(
            ['intcol', 'negcol', 'lhscol'])
        rhs = session.createDataFrame([[1, -10, 'one'], [2, -20, 'two']]).toDF(
            ['intcol', 'negcol', 'rhscol'])

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol'], 'leftsemi').select('intcol').collect()
        assert res == [Row([1]), Row([2])]

        res = rhs.join(lhs, lhs['intcol'] == rhs['intcol'], 'leftsemi').select('intcol').collect()
        assert res == [Row([1]), Row([2])]

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol'] and lhs['negcol'] == rhs['negcol'],
                       'leftsemi').select(lhs['intcol']).collect()
        assert res == []

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol'], 'leftanti').select('intcol').collect()
        assert res == []

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol'], 'leftanti').select(
            lhs['intcol']).collect()
        assert res == []

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol'] and lhs['negcol'] == rhs['negcol'],
                       'leftanti').select(lhs['intcol']).collect()
        assert sorted(res, key=lambda x: x[0]) == [Row([1]), Row([2])]


def test_using_joins(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, -1, 'one'], [2, -2, 'two']]).toDF(
            ['intcol', 'negcol', 'lhscol'])
        rhs = session.createDataFrame([[1, -10, 'one'], [2, -20, 'two']]).toDF(
            ['intcol', 'negcol', 'rhscol'])

        for join_type in ['inner', 'leftouter', 'rightouter', 'full_outer']:
            res = lhs.join(rhs, ['intcol'], join_type).select('*').collect()
            assert res == [Row([1, -1, "one", -10, "one"]), Row([2, -2, "two", -20, "two"])]

            res = lhs.join(rhs, ['intcol'], join_type).collect()
            assert res == [Row([1, -1, "one", -10, "one"]), Row([2, -2, "two", -20, "two"])]

            # TODO requires wrapException
            # with pytest.raises(SnowparkClientException) as ex_info:
            #     lhs.join(rhs, ['intcol'], join_type).select('negcol').collect()
            # assert 'NEGCOL' in str(ex_info)
            # assert 'ambiguous' in str(ex_info)

            res = lhs.join(rhs, ['intcol'], join_type).select('intcol').collect()
            assert res == [Row([1]), Row([2])]
            res = lhs.join(rhs, ['intcol'], join_type).select(lhs['negcol'],
                                                              rhs['negcol']).collect()
            assert sorted(res, key=lambda x: -x[0]) == [Row([-1, -10]), Row([-2, -20])]


def test_columns_with_and_without_quotes(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, 1.0]]).toDF(['intcol', 'doublecol'])
        rhs = session.createDataFrame([[1, 2.0]]).toDF(['"INTCOL"', '"DoubleCol"'])

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol']) \
            .select(lhs['"INTCOL"'], rhs['intcol'], 'doublecol', col('"DoubleCol"')).collect()
        assert res == [Row([1, 1, 1.0, 2.0])]

        res = lhs.join(rhs, lhs['doublecol'] == rhs['"DoubleCol"']) \
            .select(lhs['"INTCOL"'], rhs['intcol'], 'doublecol', col('"DoubleCol"')).collect()
        assert res == []

        # Below LHS and RHS are swapped but we still default to using the column name as is.
        res = lhs.join(rhs, col('doublecol') == col('\"DoubleCol\"')) \
            .select(lhs['"INTCOL"'], rhs['intcol'], 'doublecol', col('"DoubleCol"')).collect()
        assert res == []

        # TODO requires wrapException
        # with pytest.raises(SnowparkClientException) as ex_info:
        #     lhs.join(rhs, col('intcol') == col('"INTCOL"')).collect()
        # assert 'INTCOL' in str(ex_info)
        # assert 'ambiguous' in str(ex_info)


def test_aliases_multiple_levels_deep(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([[1, -1, 'one'], [2, -2, 'two']]).toDF(
            ['intcol', 'negcol', 'lhscol'])
        rhs = session.createDataFrame([[1, -10, 'one'], [2, -20, 'two']]).toDF(
            ['intcol', 'negcol', 'rhscol'])

        res = lhs.join(rhs, lhs['intcol'] == rhs['intcol']) \
            .select((lhs['negcol'] + rhs['negcol']).alias('newCol'), lhs['intcol'], rhs['intcol']) \
            .select((lhs['intcol'] + rhs['intcol']), 'newCol').collect()
        assert sorted(res, key=lambda x: x[0]) == [Row([2, -11]), Row([4, -22])]


def test_join_sql_as_the_backing_dataframe(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "int int, int2 int, str string")
            session.sql(f"insert into {table_name1} values(1, 2, '1'),(3, 4, '3')").collect()

            df = session.sql(f"select * from {table_name1} where int2< 10")
            df2 = session.sql(
                "select 1 as INT, 3 as INT2, '1' as STR UNION select 5 as INT, 6 as INT2, '5' as STR")

            assert df.join(df2, ['int', 'str'], 'inner').collect() == [Row([1, '1', 2, 3])]

            assert df.join(df2, ['int', 'str'], 'left').collect() == \
                   [Row([1, '1', 2, 3]), Row([3, '3', 4, None])]

            assert df.join(df2, ['int', 'str'], 'right').collect() == \
                   [Row([1, '1', 2, 3]), Row([5, '5', None, 6])]

            res = df.join(df2, ['int', 'str'], 'outer').collect()
            res.sort(key=lambda x: x[0])
            assert res == \
                   [Row([1, '1', 2, 3]), Row([3, '3', 4, None]), Row([5, '5', None, 6])]

            assert df.join(df2, ['int', 'str'], 'left_semi').collect() == [Row([1, 2, '1'])]
            assert df.join(df2, ['int', 'str'], 'semi').collect() == [Row([1, 2, '1'])]

            assert df.join(df2, ['int', 'str'], 'left_anti').collect() == [Row([3, 4, '3'])]
            assert df.join(df2, ['int', 'str'], 'anti').collect() == [Row([3, 4, '3'])]

        finally:
            Utils.drop_table(session, table_name1)


def test_negative_test_for_self_join_with_conditions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
            df = session.table(table_name1)
            self_dfs = [df, DataFrame(df.session, df._DataFrame__plan)]

            msg = 'Joining a DataFrame to itself can lead to incorrect results due to ambiguity of column references. Instead, join this DataFrame to a clone() of itself.'

            for df2 in self_dfs:
                for join_type in ["", "inner", "left", "right", "outer"]:
                    with pytest.raises(SnowparkClientException) as ex_info:
                        if not join_type:
                            df.join(df2, df['c1'] == df['c2']).collect()
                        else:
                            df.join(df2, df['c1'] == df['c2'], join_type).collect()
                    assert msg in str(ex_info)

        finally:
            Utils.drop_table(session, table_name1)


def test_clone_can_help_these_self_joins(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
            df = session.table(table_name1)
            cloned_df = df.clone()

            # inner self join
            assert df.join(cloned_df, df['c1'] == cloned_df['c2']).collect() == [Row([2, 3, 1, 2])]

            # left self join
            res = df.join(cloned_df, df['c1'] == cloned_df['c2'], 'left').collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, 2, None, None]), Row([2, 3, 1, 2])]

            # right self join
            res = df.join(cloned_df, df['c1'] == cloned_df['c2'], 'right').collect()
            res.sort(key=lambda x: x[0] or 0)
            assert res == [Row([None, None, 2, 3]), Row([2, 3, 1, 2])]

            # outer self join
            res = df.join(cloned_df, df['c1'] == cloned_df['c2'], 'outer').collect()
            res.sort(key=lambda x: x[0] or 0)
            assert res == [Row([None, None, 2, 3]), Row([1, 2, None, None]), Row([2, 3, 1, 2])]

        finally:
            Utils.drop_table(session, table_name1)


def test_natural_cross_joins(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
            df = session.table(table_name1)
            df2 = df  # Another reference of "df"
            cloned_df = df.clone()

            # "natural join" supports self join
            assert df.naturalJoin(df2).collect() == [Row([1, 2]), Row([2, 3])]
            assert df.naturalJoin(cloned_df).collect() == [Row([1, 2]), Row([2, 3])]

            # "cross join" supports self join
            res = df.crossJoin(df2).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, 2, 1, 2]), Row([1, 2, 2, 3]), Row([2, 3, 1, 2]),
                           Row([2, 3, 2, 3])]

            res = df.crossJoin(df2).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, 2, 1, 2]), Row([1, 2, 2, 3]), Row([2, 3, 1, 2]),
                           Row([2, 3, 2, 3])]

        finally:
            Utils.drop_table(session, table_name1)


def test_clone_with_join_dataframe(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 2), (2, 3)").collect()
            df = session.table(table_name1)

            assert df.collect() == [Row([1, 2]), Row([2, 3])]

            cloned_df = df.clone()
            #  Cloned DF has the same conent with original DF
            assert cloned_df.collect() == [Row([1, 2]), Row([2, 3])]

            join_df = df.join(cloned_df, df['c1'] == cloned_df['c2'])
            assert join_df.collect() == [Row([2, 3, 1, 2])]
            # Cloned join DF
            cloned_join_df = join_df.clone()
            assert cloned_join_df.collect() == [Row([2, 3, 1, 2])]

        finally:
            Utils.drop_table(session, table_name1)


def test_join_on_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 1), (2, 2)").collect()
            df_l = session.table(table_name1)
            df_r = df_l.clone()
            df_j = df_l.join(df_r, df_l['c1'] == df_r['c1'])

            assert df_j.collect() == [Row([1, 1, 1, 1]), Row([2, 2, 2, 2])]

            df_j_clone = df_j.clone()
            # Because of duplicate column name rename, we have to get a name.
            col_name = df_j.schema.fields[0].name
            df_j_j = df_j.join(df_j_clone, df_j[col_name] == df_j_clone[col_name])

            assert df_j_j.collect() == [Row([1, 1, 1, 1, 1, 1, 1, 1]),
                                        Row([2, 2, 2, 2, 2, 2, 2, 2])]

        finally:
            Utils.drop_table(session, table_name1)


@pytest.mark.skip(message='Requires wrapException in SnowflakePlan')
def test_negative_test_join_on_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name1 = Utils.random_name()
        try:
            Utils.create_table(session, table_name1, "c1 int, c2 int")
            session.sql(f"insert into {table_name1} values(1, 1), (2, 2)").collect()
            df_l = session.table(table_name1)
            df_r = df_l.clone()
            df_j = df_l.join(df_r, df_l['c1'] == df_r['c1'])
            df_j_clone = df_j.clone()

            with pytest.raises(SnowparkClientException) as ex_info:
                df_j.join(df_j_clone, df_l["c1"] == df_r["c1"]).collect()
            assert "Possible ambiguous reference to 'C1' present in both join sides. " in str(
                ex_info)

        finally:
            Utils.drop_table(session, table_name1)


def test_outer_join_conversion(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([(1, 2, "1"), (3, 4, "3")]).toDF(["int", "int2", "str"])
        df2 = session.createDataFrame([(1, 3, "1"), (5, 6, "5")]).toDF(["int", "int2", "str"])

        # outer -> left
        outer_join_2_left = df.join(df2, df['int'] == df2['int'], 'outer').where(df['int'] >= 3) \
            .collect()
        assert outer_join_2_left == [Row([3, 4, '3', None, None, None])]

        # outer -> right
        outer_join_2_right = df.join(df2, df['int'] == df2['int'], 'outer').where(df2['int'] >= 3) \
            .collect()
        assert outer_join_2_right == [Row([None, None, None, 5, 6, '5'])]

        # outer -> inner
        outer_join_2_inner = df.join(df2, df['int'] == df2['int'], 'outer') \
            .where(df['int'] == 1 and df2['int2'] == 3).collect()
        assert outer_join_2_inner == [Row([1, 2, "1", 1, 3, "1"])]

        # right -> inner
        right_join_2_inner = df.join(df2, df['int'] == df2['int'], 'right') \
            .where(df['int'] > 0).collect()
        assert right_join_2_inner == [Row([1, 2, "1", 1, 3, "1"])]

        # left -> inner
        left_join_2_inner = df.join(df2, df['int'] == df2['int'], 'left') \
            .where(df2['int'] > 0).collect()
        assert left_join_2_inner == [Row([1, 2, "1", 1, 3, "1"])]


def test_dont_throw_analysis_exception_in_check_cartesian(session_cnx, db_parameters):
    """Don't throw Analysis Exception in CheckCartesianProduct when join condition is false or null"""
    with session_cnx(db_parameters) as session:
        df = session.range(10).toDF(['id'])
        dfNull = session.range(10).select(lit(None).as_('b'))
        df.join(dfNull, col('id') == col('b'), 'left').collect()

        dfOne = df.select(lit(1).as_('a'))
        dfTwo = session.range(10).select(lit(2).as_('b'))
        dfOne.join(dfTwo, col('a') == col('b'), 'left').collect()


def test_name_alias_on_multiple_join(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_trips = Utils.random_name()
        table_stations = Utils.random_name()
        try:
            session.sql((f"create or replace temp table {table_trips} (starttime timestamp, "
                         f"start_station_id int, end_station_id int)")).collect()
            session.sql((f"create or replace temp table {table_stations} "
                         f"(station_id int, station_name string)")).collect()

            df_trips = session.table(table_trips)
            df_start_stations = session.table(table_stations)
            df_end_stations = session.table(table_stations)

            # assert no error
            df = df_trips \
                .join(df_end_stations, df_trips['end_station_id'] == df_end_stations["station_id"]) \
                .join(df_start_stations,
                      df_trips['start_station_id'] == df_start_stations['station_id']) \
                .filter(df_trips['starttime'] >= '2021-01-01') \
                .select(
                df_start_stations['station_name'],
                df_end_stations['station_name'],
                df_trips['starttime'])

            res = df.collect()
        finally:
            Utils.drop_table(session, table_trips)
            Utils.drop_table(session, table_stations)


def test_name_alias_on_multiple_join_unnormalized_name(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_trips = Utils.random_name()
        table_stations = Utils.random_name()
        try:
            session.sql((f"create or replace temp table {table_trips} (starttime timestamp, "
                         f"\"start<station>id\" int, \"end+station+id\" int)")).collect()
            session.sql((f"create or replace temp table {table_stations} "
                         f"(\"station^id\" int, \"station%name\" string)")).collect()

            df_trips = session.table(table_trips)
            df_start_stations = session.table(table_stations)
            df_end_stations = session.table(table_stations)

            # assert no error
            df = df_trips \
                .join(df_end_stations, df_trips['end+station+id'] == df_end_stations["station^id"]) \
                .join(df_start_stations,
                      df_trips['start<station>id'] == df_start_stations['station^id']) \
                .filter(df_trips['starttime'] >= '2021-01-01') \
                .select(
                df_start_stations['station%name'],
                df_end_stations['station%name'],
                df_trips['starttime'])

            res = df.collect()
        finally:
            Utils.drop_table(session, table_trips)
            Utils.drop_table(session, table_stations)


@pytest.mark.skip(message='Requires wrapException in SnowflakePlan')
def test_report_error_when_refer_common_col(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame([[1, 2]]).toDF(['a', 'b'])
        df2 = session.createDataFrame([[1, 2]]).toDF(['c', 'd'])
        df3 = session.createDataFrame([[1, 2]]).toDF(['e', 'f'])

        df4 = df1.join(df2, df1['a'] == df2['c'])
        df5 = df3.join(df2, df2['c'] == df3['e'])
        df6 = df4.join(df5, df4['a'] == df5['e'])

        with pytest.raises(Exception) as ex_info:
            df6.select('*').select(df2['c']).collect()
        assert "Possible ambiguous reference to 'C' present in both join sides." in str(ex_info)
