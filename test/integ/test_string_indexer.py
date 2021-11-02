#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from test.utils import Utils

from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformers.string_indexer import StringIndexer

TABLENAME = "table_temp"


def test_fit(session):
    input_df = session.createDataFrame(["a", "b", "c", "b", "d", "a"]).toDF(
        "input_value"
    )
    expected_df = session.createDataFrame(
        [
            Row("a", 1),
            Row("b", 2),
            Row("c", 3),
            Row("d", 4),
        ]
    ).toDF("distinct_value", "index")

    indexer = StringIndexer(session=session, input_col="input_value")
    indexer.fit(input_df)
    actual_df = session.table(TABLENAME)

    Utils.check_answer(expected_df, actual_df)


def test_transform(session):
    input_df = session.createDataFrame(["a", "b", "c", "b", "d", "a"]).toDF(
        "input_value"
    )
    indexer = StringIndexer(session=session, input_col="input_value")
    indexer.fit(input_df)

    df = session.createDataFrame(["b", "d", "z", "a"]).toDF("value")
    expected_df = session.createDataFrame([2, 4, -1, 1]).toDF("expected")
    actual_df = df.select(indexer.transform(df["value"]))

    Utils.check_answer(expected_df, actual_df)
