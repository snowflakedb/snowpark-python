#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.mock._connection import MockServerConnection


def test_connection(session):
    # test coverage for not implemented error
    with pytest.raises(NotImplementedError):
        session.create_dataframe([1])._execute_and_get_query_id()

    with pytest.raises(NotImplementedError):
        # this would call connection.run_query
        session.query_tag = "tag"

    with pytest.raises(NotImplementedError):
        MockServerConnection().upload_file("a", "b")

    with pytest.raises(NotImplementedError):
        MockServerConnection().get_result_set(None)
