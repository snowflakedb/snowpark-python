#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
# TODO fix 'src.' in imports
from src.snowflake.snowpark.row import Row


def test_select_1(session_cnx, db_parameters):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters) as session:
        res = session.sql('select 1').collect()
        assert res == [Row([1])]
