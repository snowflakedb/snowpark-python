#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import math

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col


def assert_rows(res, expected):
    for row_index in range(0, len(res)):
        for tuple_index in range(0, len(res[row_index])):
            if isinstance(res[row_index][tuple_index], float):
                assert math.isclose(
                    res[row_index][tuple_index], expected[row_index][tuple_index]
                )
            else:
                assert res[row_index][tuple_index] == expected[row_index][tuple_index]


def test_df_agg_tuples_basic(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )

        # Aggregations on 'first' column
        res = df.agg([("first", "min")]).collect()
        assert_rows(res, [Row(1)])

        res = df.agg([("first", "count")]).collect()
        assert_rows(res, [Row(4)])

        res = df.agg([("first", "max")]).collect()
        assert_rows(res, [Row(2)])

        res = df.agg([("first", "avg")]).collect()
        assert_rows(res, [Row(1.5)])

        res = df.agg([("first", "std")]).collect()
        assert_rows(res, [Row(0.577349980514419)])

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
        assert_rows(res, [Row(1, 4, 2, 1.5, 0.577349980514419)])

        # Aggregations on 'second' column
        res = df.agg([("second", "min")]).collect()
        assert_rows(res, [Row(4)])

        res = df.agg([("second", "count")]).collect()
        assert_rows(res, [Row(4)])

        res = df.agg([("second", "max")]).collect()
        assert_rows(res, [Row(6)])

        res = df.agg([("second", "avg")]).collect()
        assert_rows(res, [Row(4.75)])

        res = df.agg([("second", "std")]).collect()
        assert_rows(res, [Row(0.9574272818339783)])

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
        assert_rows(res, [Row(4, 4, 6, 4.75, 0.9574272818339783)])

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
        assert_rows(res, [Row(1, 4, 2, 4.75, 0.577349980514419)])


def test_df_agg_tuples_avg_basic(session_cnx):
    """Test for making sure all avg word-variations work as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "avg")]).collect()
        assert_rows(res, [Row(1.5)])

        res = df.agg([("first", "average")]).collect()
        assert_rows(res, [Row(1.5)])

        res = df.agg([("first", "mean")]).collect()
        assert_rows(res, [Row(1.5)])


def test_df_agg_tuples_std_basic(session_cnx):
    """Test for making sure all stddev variations work as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "stddev")]).collect()
        assert_rows(res, [Row(0.577349980514419)])

        res = df.agg([("first", "std")]).collect()
        assert_rows(res, [Row(0.577349980514419)])


def test_df_agg_tuples_count_basic(session_cnx):
    """Test for making sure all count variations work as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "count")]).collect()
        assert_rows(res, [Row(4)])

        res = df.agg([("second", "size")]).collect()
        assert_rows(res, [Row(4)])


def test_df_groupBy_invalid_input(session_cnx):
    """Test for check invalid input for groupBy function"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        with pytest.raises(TypeError) as ex_info:
            df.groupBy([], []).count().collect()
        assert (
            "groupBy() only accepts str and Column objects,"
            " or a list containing str and Column objects" in str(ex_info)
        )
        with pytest.raises(TypeError) as ex_info:
            df.groupBy(1).count().collect()
        assert (
            "groupBy() only accepts str and Column objects,"
            " or a list containing str and Column objects" in str(ex_info)
        )


def test_df_agg_tuples_sum_basic(session_cnx):
    """Test for making sure sum works as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        # Aggregations on 'first' column
        res = df.agg([("first", "sum")]).collect()
        assert_rows(res, [Row(6)])

        res = df.agg([("second", "sum")]).collect()
        assert_rows(res, [Row(19)])

        res = df.agg([("second", "sum"), ("first", "sum")]).collect()
        assert_rows(res, [Row(19, 6)])

        res = df.groupBy("first").sum("second").collect()
        res.sort(key=lambda x: x[0])
        assert_rows(res, [Row(1, 8), Row(2, 11)])

        # same as above, but pass Column object to groupBy() and sum()
        res = df.groupBy(col("first")).sum(col("second")).collect()
        res.sort(key=lambda x: x[0])
        assert_rows(res, [Row(1, 8), Row(2, 11)])


def test_df_agg_dict_arg(session_cnx):
    """Test for making sure dict when passed to agg() works as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )
        res = df.agg({"first": "sum"}).collect()
        assert_rows(res, [Row(6)])

        res = df.agg({"second": "sum"}).collect()
        assert_rows(res, [Row(19)])

        res = df.agg({"second": "sum", "first": "sum"}).collect()
        assert_rows(res, [Row(19, 6)])

        res = df.agg({"first": "count", "second": "size"}).collect()
        assert_rows(res, [Row(4, 4)])

        # negative tests
        with pytest.raises(TypeError) as ex_info:
            df.agg({"second": 1, "first": "sum"})
        assert (
            "Dictionary passed to DataFrame.agg() should contain only strings: got key-value pair with types (<class 'str'>, <class 'int'>)"
            in str(ex_info)
        )

        with pytest.raises(TypeError) as ex_info:
            df.agg({"second": "sum", 1: "sum"})
        assert (
            "Dictionary passed to DataFrame.agg() should contain only strings: got key-value pair with types (<class 'int'>, <class 'str'>)"
            in str(ex_info)
        )

        with pytest.raises(TypeError) as ex_info:
            df.agg({"second": "sum", 1: 1})
        assert (
            "Dictionary passed to DataFrame.agg() should contain only strings: got key-value pair with types (<class 'int'>, <class 'int'>)"
            in str(ex_info)
        )


def test_df_agg_invalid_args_in_list(session_cnx):
    """Test for making sure when a list passed to agg() produces correct errors."""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )

        assert df.agg([("first", "count")]).collect() == [Row(4)]

        # invalid type
        with pytest.raises(TypeError) as ex_info:
            df.agg([int])
        assert "Lists passed to DataFrame.agg() should only contain" in str(ex_info)

        with pytest.raises(TypeError) as ex_info:
            df.agg(["first"])
        assert "Lists passed to DataFrame.agg() should only contain" in str(ex_info)

        # not a pair
        with pytest.raises(TypeError) as ex_info:
            df.agg([("first", "count", "invalid_arg")])
        assert "Lists passed to DataFrame.agg() should only contain" in str(ex_info)

        # pairs with invalid type
        with pytest.raises(TypeError) as ex_info:
            df.agg([("first", 123)])
        assert "Lists passed to DataFrame.agg() should only contain" in str(ex_info)

        with pytest.raises(TypeError) as ex_info:
            df.agg([(123, "sum")])
        assert "Lists passed to DataFrame.agg() should only contain" in str(ex_info)


def test_df_agg_empty_args(session_cnx):
    """Test for making sure dict when passed to agg() works as expected"""
    with session_cnx() as session:
        df = session.createDataFrame([[1, 4], [1, 4], [2, 5], [2, 6]]).toDF(
            ["first", "second"]
        )

        assert_rows(df.agg({}).collect(), [Row(1, 4)])
