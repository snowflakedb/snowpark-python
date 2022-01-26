#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column
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


def test_aliases():
    assert Column.isin == Column.in_
    assert Column.astype == Column.cast
    assert Column.rlike == Column.regexp
    assert Column.substring == Column.substr
    assert Column.bitwiseAnd == Column.bitand
    assert Column.bitwiseOR == Column.bitor
    assert Column.bitwiseXOR == Column.bitxor
    assert Column.isNotNull == Column.is_not_null
    assert Column.isNull == Column.is_null
    assert Column.eqNullSafe == Column.equal_null
    assert Column.getName == Column.get_name
