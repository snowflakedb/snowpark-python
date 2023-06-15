#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.mock.mock_connection import MockServerConnection
from snowflake.snowpark.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

session = Session(MockServerConnection())


def test_put_file():
    local_file_path = "~/tmp/test_data.csv"
    stage_file = "@mystage/prefix/test_data.csv"
    session.file.put(local_file_path, "@mystage/prefix")
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", FloatType()),
        ]
    )
    df = session.read.schema(user_schema).csv(stage_file)
    df.show()
