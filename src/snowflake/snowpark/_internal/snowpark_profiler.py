#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import threading
from typing import List, Literal, Optional

import snowflake.snowpark

logger = logging.getLogger(__name__)


class SnowparkProfiler:
    """
    Base class for stored procedure profiler and UDF profiler
    """

    def __init__(
        self,
        session: "snowflake.snowpark.Session",
    ) -> None:
        self._session = session
        self._query_history = None
        self._lock = threading.RLock()
        self._active_profiler_number = 0
        self._has_target_stage = False
        self._is_enabled = False

        self._active_profiler_name = "ACTIVE_PYTHON_PROFILER"
        self._output_sql = ""
        self._profiler_module_name = ""

    def register_modules(self, modules: Optional[List[str]] = None) -> None:
        """
        Register modules to generate profiles for them.

        Args:
            modules: List of names of stored procedures. Registered modules will be overwritten by this input.
            Input None or an empty list will remove registered modules.
        """
        module_string = ",".join(modules) if modules is not None else ""
        sql_statement = (
            f"alter session set {self._profiler_module_name}='{module_string}'"
        )
        self._session.sql(sql_statement)._internal_collect_with_tag_no_telemetry()

    def set_active_profiler(
        self, active_profiler_type: Literal["LINE", "MEMORY"] = "LINE"
    ) -> None:
        """
        Set active profiler.

        Args:
            active_profiler_type: String that represent active_profiler, must be either 'LINE' or 'MEMORY'
            (case-insensitive). Active profiler is 'LINE' by default.

        """
        if active_profiler_type.upper() not in ["LINE", "MEMORY"]:
            raise ValueError(
                f"active_profiler expect 'LINE', 'MEMORY', got {active_profiler_type} instead"
            )
        sql_statement = f"alter session set {self._active_profiler_name} = '{active_profiler_type.upper()}'"
        try:
            self._session.sql(sql_statement)._internal_collect_with_tag_no_telemetry()
        except Exception as e:
            logger.warning(
                f"Set active profiler failed because of {e}. Active profiler is previously set value or default 'LINE' now."
            )
        with self._lock:
            self._active_profiler_number += 1
            if self._query_history is None:
                self._query_history = self._session.query_history(
                    include_thread_id=True, include_error=True
                )
            self._is_enabled = True

    def disable(self) -> None:
        """
        Disable profiler.
        """
        with self._lock:
            self._active_profiler_number -= 1
            if self._active_profiler_number == 0:
                self._session._conn.remove_query_listener(self._query_history)  # type: ignore
                self._query_history = None
            self._is_enabled = False
        sql_statement = f"alter session set {self._active_profiler_name} = ''"
        self._session.sql(sql_statement)._internal_collect_with_tag_no_telemetry()

    @staticmethod
    def _is_procedure_or_function_call(query: str) -> bool:
        pass

    def _get_last_query_id(self) -> Optional[str]:
        current_thread = threading.get_ident()
        for query in self._query_history.queries[::-1]:  # type: ignore
            query_thread = getattr(query, "thread_id", None)
            if query_thread == current_thread and self._is_procedure_or_function_call(
                query.sql_text
            ):
                return query.query_id
        return None

    def get_output(self) -> str:
        """
        Return the profiles of last executed stored procedure or UDF in current thread. If there is no previous
        stored procedure or UDF call, an error will be raised.

        Note:
            Please call this function right after the stored procedure or UDF you want to profile to avoid any error.

        """
        # return empty string when profiler is not enabled to not interrupt user's code
        if not self._is_enabled:
            logger.warning(
                "You are seeing this warning because you try to get profiler output while profiler is disabled. Please use profiler.set_active_profiler() to enable profiler."
            )
            return ""
        query_id = self._get_last_query_id()
        if query_id is None:
            logger.warning(
                "You are seeing this warning because last executed stored procedure or UDF does not exist. Please run the store procedure or UDF before get profiler output."
            )
            return ""
        sql = self._output_sql.format(query_id=query_id)
        return self._session.sql(sql)._internal_collect_with_tag_no_telemetry()[0][0]  # type: ignore
