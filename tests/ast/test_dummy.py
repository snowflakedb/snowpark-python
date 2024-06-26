#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session


def test_example():

    session = Session.builder.getOrCreate()

    session.createDataFrame([1, 2, 3]).select(col("*")).collect()
