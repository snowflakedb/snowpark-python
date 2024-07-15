#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Window
from snowflake.snowpark.functions import col, row_number


def test_this(session):
    df = session.createDataFrame([[1, 2, 3], [4, 5, 6]], schema=["A", "B", "C"])

    df.select(row_number().over(Window.partitionBy(col("A"))))
