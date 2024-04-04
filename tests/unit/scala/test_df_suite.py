#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.relational_grouped_dataframe import (
    _CubeType,
    _GroupByType,
    _PivotType,
    _RollupType,
)


def test_to_string_of_relational_grouped_dataframe():
    assert _GroupByType().to_string() == "GroupBy"
    assert _CubeType().to_string() == "Cube"
    assert _RollupType().to_string() == "Rollup"
    assert _PivotType(None, []).to_string() == "Pivot"
