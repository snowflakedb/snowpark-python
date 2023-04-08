#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from typing import Any, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import single_quote
from snowflake.snowpark._internal.utils import validate_object_name

_use_scoped_temp_objects = True


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()


class TaskContext:
    """
    Represents the context in a `Snowflake Task <https://docs.snowflake.com/en/user-guide/tasks-intro>`_.
    """

    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        """
        Initializes a :class:`TaskContext` object.

        Args:
            session: A Snowpark session
        """
        self._session = session

    def set_return_value(self, value: Any) -> None:
        """
        Explicitly sets the return value for a task. This value can be retrieved
        in a child task using :meth:`get_predecessor_return_value`.
        This method can only be called in a Snowflake task.

        See `SYSTEM$SET_RETURN_VALUE <https://docs.snowflake.com/en/sql-reference/functions/system_set_return_value>`_ for details.

        Args:
            value: The return value for a task. It will be converted to a ``str``
                when the underlying SQL system function is called.
        """
        self._session.call("system$set_return_value", str(value))

    def get_predecessor_return_value(self, task_name: Optional[str] = None) -> str:
        """
        Retrieves the return value for the predecessor task in a DAG of tasks.
        The return value is explicitly set by the predecessor task using :meth:`set_return_value`.
        This method can only be called in a Snowflake task.

        See `SYSTEM$GET_PREDECESSOR_RETURN_VALUE <https://docs.snowflake.com/en/sql-reference/functions/system_get_predecessor_return_value>`_ for details.

        Args:
            task_name: The task name of the predecessor task that sets the return value to be retrieved.

                - If the task has only one predecessor task that is enabled, the argument is optional.
                  If this argument is omitted, the function retrieves the return value for the only enabled predecessor task.
                - If the task has multiple predecessor tasks that are enabled, this argument is required.
        """
        if task_name:
            validate_object_name(task_name)
            return self._session.call(
                "system$get_predecessor_return_value", single_quote(task_name)
            )
        else:
            return self._session.call("system$get_predecessor_return_value")

    def get_current_task_name(self) -> str:
        """
        Returns the name of the task currently executing.
        This method can only be called in a Snowflake task.
        """
        return str(self._session.call("system$current_user_task_name"))
