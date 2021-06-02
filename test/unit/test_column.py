#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from src.snowflake.snowpark.functions import col


def test_getName():
    """Tests retrieving a negative number of results."""
    name = col("id").getName()
    assert name == 'id'

    name = col("*").getName()
    assert name == "*"