#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sysconfig

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc, udf
from tests.utils import TempObjectType, Utils

pytestmark = [
    pytest.mark.skipif(
        not sysconfig.get_config_var("Py_GIL_DISABLED"),
        reason="requires a free-threaded Python client",
    ),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="runtime versions are not available in local testing",
    ),
]


def test_udf_from_free_threaded_client_runs_free_threaded(session):
    # Registration silently falls back to the GIL runtime when the server rejects 3.14t, so check the runtime
    # the function actually ran on.
    @udf(name=Utils.random_name_for_temp_object(TempObjectType.FUNCTION))
    def is_free_threaded() -> int:
        import sysconfig

        return int(bool(sysconfig.get_config_var("Py_GIL_DISABLED")))

    assert session.range(1).select(is_free_threaded()).collect()[0][0] == 1


def test_sproc_from_free_threaded_client_runs_free_threaded(session):
    @sproc(name=Utils.random_name_for_temp_object(TempObjectType.PROCEDURE))
    def is_free_threaded(session_: Session) -> int:
        import sysconfig

        return int(bool(sysconfig.get_config_var("Py_GIL_DISABLED")))

    assert is_free_threaded() == 1
