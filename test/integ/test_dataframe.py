#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest
from itertools import product
import datetime
from decimal import Decimal

# TODO fix 'src.' in imports
from src.snowflake.snowpark.column import Column
from src.snowflake.snowpark.internal.sp_expressions import \
    AttributeReference as SPAttributeReference, ResolvedStar as SPResolvedStar
from src.snowflake.snowpark.types.sf_types import LongType, StringType, DoubleType, TimestampType, DateType, TimeType,\
    BooleanType, BinaryType, DecimalType
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.functions import col


def test_new_df_from_range(session_cnx, db_parameters):
    """Tests df.range()."""
    with session_cnx(db_parameters) as session:
        # range(start, end, step)
        df = session.range(1, 10, 2)
        res = df.collect()
        expected = [Row([1]), Row([3]), Row([5]), Row([7]), Row([9])]
        assert res == expected

        # range(start, end)
        df = session.range(1, 10)
        res = df.collect()
        expected = [Row([1]), Row([2]), Row([3]), Row([4]), Row([5]), Row([6]), Row([7]), Row([8]),
                    Row([9])]
        assert res == expected

        # range(end)
        df = session.range(10)
        res = df.collect()
        expected = [Row([0]), Row([1]), Row([2]), Row([3]), Row([4]), Row([5]), Row([6]), Row([7]),
                    Row([8]), Row([9])]
        assert res == expected


def test_select_single_column(session_cnx, db_parameters):
    """Tests df.select() on dataframes with a single column."""
    with session_cnx(db_parameters) as session:
        df = session.range(1, 10, 2)
        res = df.filter("id > 4").select("id").collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter("id < 4").select("id").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter("id <= 4").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter("id <= 3").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).select("id").filter("id <= 0").collect()
        expected = []
        assert res == expected


def test_select_star(session_cnx, db_parameters):
    """Tests df.select('*')."""
    with session_cnx(db_parameters) as session:

        # Single column
        res = session.range(3, 8).select('*').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Two columns
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df.select('*').collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected


def test_df_subscriptable(session_cnx, db_parameters):
    """Tests select & filter as df[...]"""
    with session_cnx(db_parameters) as session:

        # Star, single column
        res = session.range(3, 8)[['*']].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Star, two columns
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df[['*']].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected
        # without double brackets should refer to a Column object
        assert type(df['*']) == Column

        # single column, str type
        df = session.range(3, 8)
        res = df[['ID']].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected
        assert type(df['ID']) == Column

        # single column, int type
        df = session.range(3, 8)
        res = df[df[0] > 5].collect()
        expected = [Row([6]), Row([7])]
        assert res == expected
        assert type(df[0]) == Column

        # two columns, list type
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df[['ID', 'ID_PRIME']].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected

        # two columns, tuple type
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df[('ID', 'ID_PRIME')].collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert res == expected

        # two columns, int type
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df[[df[1].get_name()]].collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected


def test_filter(session_cnx, db_parameters):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters) as session:
        df = session.range(1, 10, 2)
        res = df.filter("id > 4").collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter("id < 4").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter("id <= 4").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter("id <= 3").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter("id <= 0").collect()
        expected = []
        assert res == expected


def test_filter_chained(session_cnx, db_parameters):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters) as session:
        df = session.range(1, 10, 2)
        res = df.filter("id > 4").filter("id > 1").collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter("id > 1").filter("id > 4").collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter("id < 4").filter("id < 4").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter("id <= 4").filter("id >= 0").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected

        res = session.range(1, 10, 2).filter("id <= 3").filter("id != 5").collect()
        expected = [Row([1]), Row([3])]
        assert res == expected


def test_filter_chained_col_objects_int(session_cnx, db_parameters):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters) as session:
        df = session.range(1, 10, 2)
        res = df.filter(col('id') > 4).filter(col('id') > 1).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col('id') > 1).filter(col('id') > 4).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col('id') > 1).filter(col('id') >= 5).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col('id') >= 1).filter(col('id') >= 5).collect()
        expected = [Row([5]), Row([7]), Row([9])]
        assert res == expected

        df = session.range(1, 10, 2)
        res = df.filter(col('id') == 5).collect()
        expected = [Row([5])]
        assert res == expected


def test_drop(session_cnx, db_parameters):
    """Test for dropping columns from a dataframe."""
    with session_cnx(db_parameters) as session:
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        res = df.drop('id').select('id_prime').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected

        # dropping all columns should raise exception
        with pytest.raises(Exception):
            df.drop('id').drop('id_prime')

        # Drop second column renamed several times
        df2 = session.range(3, 8).select(['id', col('id').alias('id_prime')]). \
            select(['id', col('id_prime').alias('id_prime_2')]). \
            select(['id', col('id_prime_2').alias('id_prime_3')]). \
            select(['id', col('id_prime_3').alias('id_prime_4')]). \
            drop('id_prime_4')
        res = df2.select('id').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected


def test_alias(session_cnx, db_parameters):
    """Test for dropping columns from a dataframe."""
    with session_cnx(db_parameters) as session:
        # Selecting non-existing column (already renamed) should fail
        with pytest.raises(Exception):
            session.range(3, 8).select(col('id').alias('id_prime')). \
                select(col('id').alias('id_prime')).collect()

        # Rename column several times
        df = session.range(3, 8).select(col('id').alias('id_prime')). \
            select(col('id_prime').alias('id_prime_2')). \
            select(col('id_prime_2').alias('id_prime_3')). \
            select(col('id_prime_3').alias('id_prime_4'))
        res = df.select('id_prime_4').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert res == expected


def test_join_inner(session_cnx, db_parameters):
    """Test for inner join of dataframes."""
    with session_cnx(db_parameters) as session:
        # Implicit inner join on single column
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id').collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert res == expected

        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'inner').collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert res == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id').collect()
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7])]
        assert res == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7])]
        res = df1.join(df2, 'id').collect()
        assert res == expected


def test_join_left_anti(session_cnx, db_parameters):
    """Test for left-anti join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'left_anti').collect()
        expected = [Row([3]), Row([4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id', 'left_anti').collect()
        expected = [Row([3, 3]), Row([4, 4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        res = df1.join(df2, 'id', 'left_anti').collect()
        expected = [Row([3, 3]), Row([4, 4])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_left_outer(session_cnx, db_parameters):
    """Test for left-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'left_outer').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id', 'left_outer').collect()
        expected = [Row([3, 3, None]), Row([4, 4, None]), Row([5, 5, 5]), Row([6, 6, 6]),
                    Row([7, 7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        res = df1.join(df2, 'id', 'left_outer').collect()
        expected = [Row([3, 3, None]), Row([4, 4, None]), Row([5, 5, 5]), Row([6, 6, 6]),
                    Row([7, 7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_right_outer(session_cnx, db_parameters):
    """Test for right-outer join of dataframes."""
    # TODO remove sorted(res) and add df.sort() when available, as an extra step.
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'right_outer').collect()
        expected = [Row([5]), Row([6]), Row([7]), Row([8]), Row([9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id', 'right_outer').collect()
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7]), Row([8, None, 8]),
                    Row([9, None, 9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        res = df1.join(df2, 'id', 'right_outer').collect()
        expected = [Row([5, 5, 5]), Row([6, 6, 6]), Row([7, 7, 7]), Row([8, None, 8]),
                    Row([9, None, 9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_left_semi(session_cnx, db_parameters):
    """Test for left semi join of dataframes."""
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'left_semi').collect()
        expected = [Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id', 'left_semi').collect()
        expected = [Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        expected = [Row([5, 5]), Row([6, 6]), Row([7, 7])]
        res = df1.join(df2, 'id', 'left_semi').collect()
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_join_cross(session_cnx, db_parameters):
    """Test for cross join of dataframes."""
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.crossJoin(df2).collect()
        expected = [Row([x, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.crossJoin(df2).collect()
        expected = [Row([x, x, y, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        expected = [Row([x, x, y, y]) for x, y in product(range(3, 8), range(5, 10))]
        res = df1.crossJoin(df2).collect()
        assert sorted(res, key=lambda r: (r.get(0), r.get(2))) == expected

        with pytest.raises(Exception) as ex:
            df1.join(df2, col('id'), 'cross')
        assert 'Cross joins cannot take columns as input.' in str(ex.value)

        # Case, join on same-name column, other columns have different name, select common column.
        this = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        other = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        df_cross = this.crossJoin(other).select([this.col("id"), other.col("id")])
        res = df_cross.collect()
        expected = [Row([x, y]) for x, y in product(range(3, 8), range(5, 10))]
        assert sorted(res, key=lambda r: (r.get(0), r.get(1))) == expected


def test_join_outer(session_cnx, db_parameters):
    """Test for outer join of dataframes."""
    with session_cnx(db_parameters) as session:
        df1 = session.range(3, 8)
        df2 = session.range(5, 10)
        res = df1.join(df2, 'id', 'outer').collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7]), Row([8]), Row([9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Join on same-name column, other columns have same name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime')])
        res = df1.join(df2, 'id', 'outer').collect()
        expected = [Row([3, 3, None]), Row([4, 4, None]), Row([5, 5, 5]), Row([6, 6, 6]),
                    Row([7, 7, 7]), Row([8, None, 8]), Row([9, None, 9])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        # Case, join on same-name column, other columns have different name
        df1 = session.range(3, 8).select([col('id'), col('id').alias('id_prime1')])
        df2 = session.range(5, 10).select([col('id'), col('id').alias('id_prime2')])
        expected = [Row([3, 3, None]), Row([4, 4, None]), Row([5, 5, 5]), Row([6, 6, 6]),
                    Row([7, 7, 7]), Row([8, None, 8]), Row([9, None, 9])]
        res = df1.join(df2, 'id', 'outer').collect()
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_toDF(session_cnx, db_parameters):
    """Test df.toDF()."""
    with session_cnx(db_parameters) as session:
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])

        # calling toDF() with fewer new names than columns should fail
        with pytest.raises(Exception) as ex:
            df.toDF(['new_name'])
        assert "The number of columns doesn't match. Old column names (2):" in str(ex.value)

        res = df.toDF(['rename1', 'rename2']).select([col('rename1'), col('rename2')]).collect()
        expected = [Row([3, 3]), Row([4, 4]), Row([5, 5]), Row([6, 6]), Row([7, 7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected

        res = df.toDF(['rename1', 'rename2']).columns
        assert res == ['"RENAME1"', '"RENAME2"']

        df_prime = df.toDF(['rename1', 'rename2'])
        res = df_prime.select(df_prime.RENAME1).collect()
        expected = [Row([3]), Row([4]), Row([5]), Row([6]), Row([7])]
        assert sorted(res, key=lambda r: r.get(0)) == expected


def test_df_col(session_cnx, db_parameters):
    """Test df.col()"""
    with session_cnx(db_parameters) as session:
        df = session.range(3, 8).select([col('id'), col('id').alias('id_prime')])
        c = df.col('id')
        assert type(c) == Column
        assert type(c.expression) == SPAttributeReference

        c = df.col('*')
        assert type(c) == Column
        assert type(c.expression) == SPResolvedStar


def test_create_dataframe(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        # different data types
        # TODO: SNOW-366940 test array, dict, variant and geo type after we support Variant
        data = [0, "one", 1.0, datetime.datetime.now(), datetime.date.today(), datetime.time(), True,
                bytearray('a', 'utf-8'), Decimal(0.5)]
        none_data = [None] * len(data)
        df = session.create_dataframe([data, none_data])
        assert [field.name for field in df.schema.fields] == ["_{}".format(idx + 1) for idx in range(len(data))]
        assert [type(field.data_type) for field in df.schema.fields] == \
               [LongType, StringType, DoubleType, TimestampType, DateType, TimeType, BooleanType,
                BinaryType, DecimalType]
        assert df.collect() == [Row(data), Row(none_data)]

        # multiple rows
        df = session.create_dataframe([range(100)])
        assert df.collect() == [Row(range(100))]
