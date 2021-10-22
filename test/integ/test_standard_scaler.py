#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.ml.transformers.standard_scaler import StandardScaler


def test_transform(session):
    mean = 3
    stddev = 0.32
    temp_df = session.createDataFrame([[mean, stddev]]).toDF("mean", "stddev")
    test_df = session.createDataFrame([2.0, 3.0, 5.0]).toDF("value")
    test_col = test_df.col("value")
    standard_score = (test_col - temp_df.col("mean")) / temp_df.col("stddev")

    scaler = StandardScaler()
    assert scaler.transform(test_col) == standard_score
