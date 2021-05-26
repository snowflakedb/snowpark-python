#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest

# TODO fix 'src.' in imports
from src.snowflake.snowpark.row import Row


def test_something():
    assert True == True


@pytest.mark.parametrize("use_jvm_for_network", [True, False])
@pytest.mark.parametrize("use_jvm_for_plans", [True, False])
def test_new_df_from_range(session_cnx, db_parameters, use_jvm_for_network, use_jvm_for_plans):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, use_jvm_for_plans) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

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


@pytest.mark.parametrize("use_jvm_for_network", [True])
def test_select_single_column(session_cnx, db_parameters, use_jvm_for_network):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, True) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

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


@pytest.mark.parametrize("use_jvm_for_network", [True])
def test_filter(session_cnx, db_parameters, use_jvm_for_network):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, True) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

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


@pytest.mark.parametrize("use_jvm_for_network", [True])
def test_filter_chained(session_cnx, db_parameters, use_jvm_for_network):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, True) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

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
