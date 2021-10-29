#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformers.standard_scaler import StandardScaler


def test_transform(session):
    mean = 5.492480
    stddev = 2.884792887

    df = session.createDataFrame([2.0, 5.0]).toDF("value")
    expected_df = (
        session.createDataFrame(
            [
                Row((2.0 - mean) / stddev),
                Row((5.0 - mean) / stddev),
            ]
        )
        .toDF(["expected"])
        .collect()
    )
    scaler = StandardScaler()
    actual_df = df.select(scaler.transform(df.col("value"))).collect()

    for value1, value2 in zip(expected_df, actual_df):
        assert "{:.5f}".format(value1[0]) == "{:.5f}".format(value2[0])
