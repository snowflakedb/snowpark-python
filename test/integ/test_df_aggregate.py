#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.row import Row


def test_df_agg_tuples_basic(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )

        # Aggregations on 'first' column
        res = df.agg([("first", "min")]).collect()
        assert res == [Row([1])]

        res = df.agg([("first", "count")]).collect()
        assert res == [Row([4])]

        res = df.agg([("first", "max")]).collect()
        assert res == [Row([2])]

        res = df.agg([("first", "avg")]).collect()
        assert res == [Row([1.5])]

        res = df.agg([("first", "std")]).collect()
        assert res == [Row([0.577349980514419])]

        # combine those together
        res = df.agg(
            [
                ("first", "min"),
                ("first", "count"),
                ("first", "max"),
                ("first", "avg"),
                ("first", "std"),
            ]
        ).collect()
        assert res == [Row([1, 4, 2, 1.5, 0.577349980514419])]

        # Aggregations on 'second' column
        res = df.agg([("second", "min")]).collect()
        assert res == [Row([4])]

        res = df.agg([("second", "count")]).collect()
        assert res == [Row([4])]

        res = df.agg([("second", "max")]).collect()
        assert res == [Row([6])]

        res = df.agg([("second", "avg")]).collect()
        assert res == [Row([4.75])]

        res = df.agg([("second", "std")]).collect()
        assert res == [Row([0.9574272818339783])]

        # combine those together
        res = df.agg(
            [
                ("second", "min"),
                ("second", "count"),
                ("second", "max"),
                ("second", "avg"),
                ("second", "std"),
            ]
        ).collect()
        assert res == [Row([4, 4, 6, 4.75, 0.9574272818339783])]

        # Get aggregations for both columns
        res = df.agg(
            [
                ("first", "min"),
                ("second", "count"),
                ("first", "max"),
                ("second", "avg"),
                ("first", "std"),
            ]
        ).collect()
        assert res == [Row([1, 4, 2, 4.75, 0.577349980514419])]


def test_df_agg_tuples_avg_basic(session_cnx, db_parameters):
    """Test for making sure all avg word-variations work as expected"""
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "avg")]).collect()
        assert res == [Row([1.5])]

        res = df.agg([("first", "average")]).collect()
        assert res == [Row([1.5])]

        res = df.agg([("first", "mean")]).collect()
        assert res == [Row([1.5])]


def test_df_agg_tuples_std_basic(session_cnx, db_parameters):
    """Test for making sure all stddev variations work as expected"""
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "stddev")]).collect()
        assert res == [Row([0.577349980514419])]

        res = df.agg([("first", "std")]).collect()
        assert res == [Row([0.577349980514419])]


def test_df_agg_tuples_count_basic(session_cnx, db_parameters):
    """Test for making sure all count variations work as expected"""
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "count")]).collect()
        assert res == [Row([4])]

        res = df.agg([("second", "size")]).collect()
        assert res == [Row([4])]


def test_df_groupBy_invalid_input(session_cnx, db_parameters):
    """Test for check invalid input for groupBy function"""
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        with pytest.raises(ValueError) as ex_info:
            df.groupBy([], []).count().collect()
        assert "groupBy() only accepts one list, but got 2" in str(ex_info)
        with pytest.raises(TypeError) as ex_info:
            df.groupBy(1).count().collect()
        assert (
            "groupBy() only accepts str and Column objects,"
            " or the list containing str and Column objects" in str(ex_info)
        )
