#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.mock.mock_connection import MockServerConnection

session = Session(MockServerConnection())


def test_put_file():
    local_file_path = "~/tmp/test_data.csv"
    session.file.put(local_file_path, "@mystage/prefix")
