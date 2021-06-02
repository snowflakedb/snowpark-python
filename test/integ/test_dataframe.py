#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest

# TODO fix 'src.' in imports
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.functions import col


def test_new_df_from_range(session_cnx, db_parameters):
    """Tests retrieving a negative number of results."""
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
    """Tests retrieving a negative number of results."""
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
