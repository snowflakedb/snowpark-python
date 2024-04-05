#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udf import UDFRegistration


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
def test_do_register_udf_negative(cleanup_registration_patch):
    fake_session = mock.create_autospec(Session)
    fake_session._runtime_version_from_requirement = None
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session._import_paths = {}
    fake_session.udf = UDFRegistration(fake_session)
    with pytest.raises(SnowparkSQLException) as ex_info:
        udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(
        side_effect=BaseException("Test BaseException code path")
    )
    fake_session.udf = UDFRegistration(fake_session)
    with pytest.raises(BaseException, match="Test BaseException code path"):
        udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    cleanup_registration_patch.assert_called()


# @mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
# @mock.patch("snowflake.snowpark.session._is_execution_environment_sandboxed", return_value=True)
# @mock.patch("snowflake.snowpark._internal.udf_utils._is_execution_environment_sandboxed", return_value=True)
# @mock.patch("snowflake.snowpark._internal.udf_utils._should_continue_registration")
# @pytest.mark.parametrize("callback_return_val", [True, False])
# def test_do_register_udf_sandbox(session_sandbox, utils_sandbox, mock_callback, cleanup_registration_patch, callback_return_val):
#     mock_callback.return_value = callback_return_val
#     fake_session = mock.create_autospec(Session)
#     fake_session._runtime_version_from_requirement = None
#     fake_session.get_fully_qualified_name_if_possible = mock.Mock(
#         return_value="database.schema"
#     )
#     fake_session._run_query.return_val = []
#     fake_session._import_paths = {}
#     fake_session.udf = UDFRegistration(fake_session)
#     test_udf = udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
#     cleanup_registration_patch.assert_not_called()
