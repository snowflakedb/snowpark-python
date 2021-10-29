#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformers.standard_scaler import StandardScaler

TABLENAME = "hayu.standardscaler.table_temp"


def test_transform(session):
    mean = float(session.table(TABLENAME).collect()[0][0])
    stddev = float(session.table(TABLENAME).collect()[0][1])

    df = session.createDataFrame([-1.0, 5.0]).toDF("value")
    expected_df = (
        session.createDataFrame(
            [
                Row((-1.0 - mean) / stddev),
                Row((5.0 - mean) / stddev),
            ]
        )
        .toDF(["expected"])
        .collect()
    )
    scaler = StandardScaler()
    actual_df = df.select(scaler.transform(df.col("value"))).collect()

    for value1, value2 in zip(expected_df, actual_df):
        assert "{:.6f}".format(value1[0]) == "{:.6f}".format(value2[0])
