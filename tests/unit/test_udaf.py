#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udaf
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udaf import UDAFRegistration, UserDefinedAggregateFunction


def test_register_udaf_negative():
    fake_session = mock.create_autospec(Session)
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(TypeError, match="Invalid handler: expecting a class type"):
        fake_session.udaf.register(1)

    class FakeClass:
        pass

    sum_udaf = UserDefinedAggregateFunction(
        FakeClass, "fake_name", IntegerType(), [IntegerType()]
    )

    with pytest.raises(TypeError, match="must be Column or column name"):
        sum_udaf(1)

    with pytest.raises(
        ValueError, match="Incorrect number of arguments passed to the UDAF"
    ):
        sum_udaf("a", "b")


@mock.patch("snowflake.snowpark.udaf.cleanup_failed_permanent_registration")
def test_do_register_udaf_negative(cleanup_registration_patch):
    fake_session = mock.create_autospec(Session)
    fake_session.get_fully_qualified_current_schema = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session._runtime_version_from_requirement = None
    fake_session._packages = []
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(SnowparkSQLException) as ex_info:

        @udaf(session=fake_session)
        class SnowparkSQLExceptionTestHandler:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self) -> int:
                return self._sum

            def accumulate(self, input_value: int) -> None:
                self._sum += input_value

            def merge(self, other_sum: int) -> None:
                self._sum += other_sum

            def finish(self) -> int:
                return self._sum

    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(
        side_effect=BaseException("Test BaseException code path")
    )
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(BaseException, match="Test BaseException code path"):

        @udaf(session=fake_session)
        class BaseExceptionTestHandler:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self) -> int:
                return self._sum

            def accumulate(self, input_value: int) -> None:
                self._sum += input_value

            def merge(self, other_sum: int) -> None:
                self._sum += other_sum

            def finish(self) -> int:
                return self._sum

    cleanup_registration_patch.assert_called()
