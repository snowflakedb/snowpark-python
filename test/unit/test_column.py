#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.sp_expressions import (
    LeafExpression as SPLeafExpression,
)
from snowflake.snowpark.functions import col


def test_getName():
    """Test getName() of Column."""
    name = col("id").getName()
    assert name == '"ID"'

    # LeafExpression is not named Expression, so should not return a name
    name = col(SPLeafExpression()).getName()
    assert not name
