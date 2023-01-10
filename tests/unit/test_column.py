#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column
from snowflake.snowpark.functions import col


def test_getName():
    """Test getName() of Column."""
    name = col("id").getName()
    assert name == '"ID"'


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
    assert Column.getItem == Column.__getitem__
