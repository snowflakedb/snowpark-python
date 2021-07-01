#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from snowflake.snowpark.relational_grouped_dataframe import (
    CubeType,
    GroupByType,
    PivotType,
    RollupType,
)


def test_to_string_of_relational_grouped_dataframe():
    assert GroupByType().to_string() == "GroupBy"
    assert CubeType().to_string() == "Cube"
    assert RollupType().to_string() == "Rollup"
    assert PivotType(None, []).to_string() == "Pivot"
