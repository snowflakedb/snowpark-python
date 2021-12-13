#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from unittest.mock import MagicMock

from snowflake.snowpark import DataFrame
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan


def test_get_unaliased():
    # Basic single-aliased column
    aliased = "l_gdyf_A"
    unaliased = "A"
    values = DataFrame.get_unaliased(aliased)
    assert len(values) == 1
    assert values[0] == unaliased

    # Double-aliased column
    aliased = "l_gdyf_l_yuif_A"
    unaliased = "l_yuif_A"
    unaliased2 = "A"
    values = DataFrame.get_unaliased(aliased)
    assert len(values) == 2
    assert values[0] == unaliased
    assert values[1] == unaliased2

    # Column that isn't aliased
    aliased = "l_hfdjishafud_A"
    unaliased = "l_hfdjishafud_A"
    values = DataFrame.get_unaliased(aliased)
    assert len(values) == 0


def test_dataframe_method_alias():
    df = DataFrame(session=MagicMock(), plan=MagicMock())
    assert df.rename == df.withColumnRenamed
    assert df.drop_duplicates == df.dropDuplicates
