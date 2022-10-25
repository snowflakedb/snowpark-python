#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pandas
import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.exceptions import SnowparkPandasException


def test_df_pandas_general_exception():
    fake_snowflake_connection = mock.create_autospec(SnowflakeConnection)
    fake_snowflake_connection._telemetry = None
    fake_snowflake_connection._session_parameters = {}
    fake_snowflake_connection.is_closed.return_value = False
    fake_server_connection = ServerConnection({}, fake_snowflake_connection)
    fake_session = Session(fake_server_connection)
    with mock.patch("snowflake.snowpark.session.write_pandas") as mock_write_pandas:
        mock_write_pandas.return_value = (False, 0, 0, [])
        with pytest.raises(SnowparkPandasException):
            fake_session.write_pandas(pandas.DataFrame(), "fake_table")
