#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pandas
import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkPandasException


def test_df_pandas_general_exception(mock_server_connection):
    fake_session = Session(mock_server_connection)
    with mock.patch("snowflake.snowpark.session.write_pandas") as mock_write_pandas:
        mock_write_pandas.return_value = (False, 0, 0, [])
        with pytest.raises(SnowparkPandasException):
            fake_session.write_pandas(pandas.DataFrame(), "fake_table")
