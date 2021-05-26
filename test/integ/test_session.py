#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest

# TODO fix 'src.' in imports
from src.snowflake.snowpark.row import Row


@pytest.mark.parametrize("use_jvm_for_network", [True, False])
@pytest.mark.parametrize("use_jvm_for_use_jvm_for_plans", [True, False])
def test_select_1(session_cnx, db_parameters, use_jvm_for_network, use_jvm_for_use_jvm_for_plans):
    """Tests retrieving a negative number of results."""
    with session_cnx(db_parameters, use_jvm_for_network, use_jvm_for_use_jvm_for_plans) as session:
        session.sql('use warehouse tests_pysnowpark').collect()
        res = session.sql('select 1').collect()
        assert res == [Row([1])]