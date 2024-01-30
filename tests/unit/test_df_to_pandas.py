#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

try:
    import pandas
except ImportError:
    pytest.skip("Pandas is not available", allow_module_level=True)


from snowflake.connector.cursor import SnowflakeCursor
from snowflake.snowpark import Session


def test_df_to_pandas_kwargs_passing(mock_server_connection):
    # setup
    fake_session = Session(mock_server_connection)
    query = "select 1 as A"
    mock_result_cursor = mock.create_autospec(SnowflakeCursor)
    mock_result_cursor.query = query
    mock_result_cursor.sfqid = ""
    mock_result_cursor.fetch_pandas_all = mock.Mock(
        return_value=pandas.DataFrame({"A": [1]})
    )
    mock_result_cursor.fetch_pandas_batches = mock.Mock(
        return_value=pandas.DataFrame({"A": [1]})
    )
    mock_execute = mock.Mock(return_value=mock_result_cursor)
    fake_session._conn._conn.cursor().execute = mock_execute

    # test cases - to_pandas()
    fake_session.sql(query).to_pandas()
    mock_result_cursor.fetch_pandas_all.assert_called_with()

    fake_session.sql(query).to_pandas(statement_params={"SF_PARTNER": "FAKE_PARTNER"})
    mock_result_cursor.fetch_pandas_all.assert_called_with()

    fake_session.sql(query).to_pandas(deduplicate_objects=True, split_blocks=True)
    mock_result_cursor.fetch_pandas_all.assert_called_with(
        deduplicate_objects=True, split_blocks=True
    )

    fake_session.sql(query).to_pandas(
        deduplicate_objects=True,
        split_blocks=True,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    mock_result_cursor.fetch_pandas_all.assert_called_with(
        deduplicate_objects=True, split_blocks=True
    )

    # test cases - to_pandas_batches()
    fake_session.sql(query).to_pandas_batches()
    mock_result_cursor.fetch_pandas_batches.assert_called_with()

    fake_session.sql(query).to_pandas_batches(
        statement_params={"SF_PARTNER": "FAKE_PARTNER"}
    )
    mock_result_cursor.fetch_pandas_batches.assert_called_with()

    fake_session.sql(query).to_pandas_batches(
        deduplicate_objects=True, split_blocks=True
    )
    mock_result_cursor.fetch_pandas_batches.assert_called_with(
        deduplicate_objects=True, split_blocks=True
    )

    fake_session.sql(query).to_pandas_batches(
        deduplicate_objects=True,
        split_blocks=True,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    mock_result_cursor.fetch_pandas_batches.assert_called_with(
        deduplicate_objects=True, split_blocks=True
    )
