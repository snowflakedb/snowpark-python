#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session


def test_example():
    session = Session.builder.getOrCreate()
    # df = session.createDataFrame([1, 2, 3], schema=["A"])
    df = session.table("test_table")

    variadic = df.to_df("one", "two")

    df = df.select(col("A").regexp("test"))

    assert df and variadic
    #
    # df = df.select(col("A").startswith("test"))
    #
    # df = df.select(col("A").endswith("test"))
    #
    # df = df.select(col("A").substr(col("B"), col("C")))
    #
    # df = df.select(col("A").collate("test"))
    #
    # df = df.select(col("A").contains("test"))

    # .select(col("*")).collect())
