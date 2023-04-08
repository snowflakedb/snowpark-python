#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark.context import TaskContext, get_active_session
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkSQLException,
)


def test_get_active_session(session):
    assert session == get_active_session()


def test_task_context(session):
    # we are not able to test it in a real task, so just call functions here
    task_context = TaskContext(session)

    with pytest.raises(SnowparkSQLException, match="must be called from within a task"):
        task_context.set_return_value("1")

    with pytest.raises(SnowparkSQLException, match="must be called from within a task"):
        task_context.get_predecessor_return_value()

    with pytest.raises(SnowparkInvalidObjectNameException):
        task_context.get_predecessor_return_value("invalid name")

    # this function can actually be called outside of a task...
    assert not task_context.get_current_task_name()
