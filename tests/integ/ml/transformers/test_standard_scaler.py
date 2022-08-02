#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import json

from snowflake.snowpark import Row
from snowflake.snowpark.functions import avg, col, stddev
from snowflake.snowpark.ml.transformers.feature import StandardScaler
from snowflake.snowpark.ml.transformers.utils import (
    ColumnsMetadataColumn,
    NumericStatistics,
    StateTable,
)

_TEMP_TABLE = "table_temp"


def test_fit(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).to_df("id", "str", "float")

    input_col = "float"
    expected_mean = input_df.select(avg(input_df[input_col])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df[input_col])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col=input_col)
    scaler.fit(input_df)

    fitted_col = f"{input_col}_standard_scaler_fitted"
    columns_metadata = session.table(StateTable.COLUMNS_METADATA)
    metadata_df = columns_metadata.filter(
        col(ColumnsMetadataColumn.COLUMN_NAME) == fitted_col
    )
    stats_df = metadata_df.select(col(ColumnsMetadataColumn.NUMERIC_STATISTICS))
    original_numeric_stats = stats_df.collect()[0][0]
    numeric_stats = json.loads(original_numeric_stats)

    actual_mean = float(numeric_stats[NumericStatistics.MEAN])
    actual_stddev = float(numeric_stats[NumericStatistics.STDDEV])

    assert f"{actual_mean:.4f}" == f"{expected_mean:.4f}"
    assert f"{actual_stddev:.4f}" == f"{expected_stddev:.4f}"


def test_transform(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "b", 2.0),
            Row(3, "c", 3.5),
            Row(4, "b", 4.0),
            Row(5, "d", 2.5),
            Row(6, "a", 8.3),
        ]
    ).to_df("id", "str", "float")

    input_col = "float"
    expected_mean = input_df.select(avg(input_df[input_col])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df[input_col])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col=input_col, output_col="output")
    scaler.fit(input_df)

    values = [5.0, 10.2, 0.1, -0.6]
    df = session.createDataFrame(values).to_df("value")

    expected_df = session.createDataFrame(
        [Row(v, (v - expected_mean) / expected_stddev) for v in values]
    ).to_df(["value", "expected"])
    expected_rows = expected_df.collect()

    actual_df = scaler.transform(df.select(col("value")))
    actual_rows = actual_df.collect()

    for actual_val, expected_val in zip(actual_rows, expected_rows):
        assert f"{actual_val[1]:.4f}" == f"{expected_val[1]:.4f}"
