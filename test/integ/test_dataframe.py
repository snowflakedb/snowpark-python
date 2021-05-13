#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest

# TODO fix 'src.' in imports
from src.snowflake.snowpark.Row import Row


def test_something():
    assert True == True


@pytest.mark.parametrize("use_jvm_for_network", [True, False])
def test_new_df_from_range(session_cnx, db_parameters, use_jvm_for_network):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, True) as session:
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
