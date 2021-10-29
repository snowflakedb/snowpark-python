#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformers.string_indexer import StringIndexer

TABLENAME = "hayu.stringindexer.table_temp"


def test_transform(session):
    df = session.createDataFrame([["Northwest Territories"], ["Alberta"]]).toDF(
        ["value"]
    )
    indexer = StringIndexer()
    expected_df = session.createDataFrame([Row(2), Row(11)]).toDF("expected").collect()
    actual_df = df.select(indexer.transform(df.col("value"))).collect()

    assert expected_df == actual_df
