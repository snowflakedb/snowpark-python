#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import threading
from typing import List, Literal, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.utils import (
    SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN,
    parse_table_name,
    strip_double_quotes_in_like_statement_in_table_name,
)

logger = logging.getLogger(__name__)


class StoredProcedureProfiler:
    """
    Set up profiler to receive profiles of stored procedures. This feature cannot be used in owner's right stored
    procedure because owner's right stored procedure will not be able to set session-level parameters.
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

    def register_modules(self, stored_procedures: Optional[List[str]] = None) -> None:
        """
        Register stored procedures to generate profiles for them.

        Args:
            stored_procedures: List of names of stored procedures. Registered modules will be overwritten by this input.
            Input None or an empty list will remove registered modules.
        """
        sp_string = ",".join(stored_procedures) if stored_procedures is not None else ""
        sql_statement = f"alter session set python_profiler_modules='{sp_string}'"
        self._session.sql(sql_statement)._internal_collect_with_tag_no_telemetry()

    def set_target_stage(self, stage: str) -> None:
        """
        Set targeted stage for profiler output.

        Args:
            stage: String of fully qualified name of targeted stage
        """
        with self._lock:
            self._has_target_stage = True
        names = parse_table_name(stage)
        if len(names) != 3:
            raise ValueError(
                f"stage name must be fully qualified name, got {stage} instead"
            )
        existing_stages = self._session.sql(
            f"show stages like '{strip_double_quotes_in_like_statement_in_table_name(names[2])}' in schema {names[0]}.{names[1]}"
        )._internal_collect_with_tag_no_telemetry()
        if len(existing_stages) == 0:  # type: ignore
            self._session.sql(
                f"create temp stage if not exists {stage} FILE_FORMAT = (RECORD_DELIMITER = NONE FIELD_DELIMITER = NONE )"
            )._internal_collect_with_tag_no_telemetry()
        sql_statement = f"alter session set PYTHON_PROFILER_TARGET_STAGE ='{stage}'"
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
        if not self._has_target_stage:
            self.set_target_stage(self._session.get_session_stage().lstrip("@"))
            logger.info(
                "Target stage for profiler not found, using default stage of current session."
            )
        if active_profiler_type.upper() not in ["LINE", "MEMORY"]:
            raise ValueError(
                f"active_profiler expect 'LINE', 'MEMORY', got {active_profiler_type} instead"
            )
        sql_statement = f"alter session set ACTIVE_PYTHON_PROFILER = '{active_profiler_type.upper()}'"
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
        sql_statement = "alter session set ACTIVE_PYTHON_PROFILER = ''"
        self._session.sql(sql_statement)._internal_collect_with_tag_no_telemetry()

    @staticmethod
    def _is_sp_call(query: str) -> bool:
        query = query.upper().strip(" ")
        return SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN.match(
            query
        ) is not None or query.startswith("CALL")

    def _get_last_query_id(self) -> Optional[str]:
        current_thread = threading.get_ident()
        for query in self._query_history.queries[::-1]:  # type: ignore
            query_thread = getattr(query, "thread_id", None)
            if query_thread == current_thread and self._is_sp_call(query.sql_text):
                return query.query_id
        return None

    def get_output(self) -> str:
        """
        Return the profiles of last executed stored procedure in current thread. If there is no previous
        stored procedure call, an error will be raised.
        Please call this function right after the stored procedure you want to profile to avoid any error.

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
                "You are seeing this warning because last executed stored procedure does not exist. Please run the store procedure before get profiler output."
            )
            return ""
        sql = f"select snowflake.core.get_python_profiler_output('{query_id}')"
        return self._session.sql(sql)._internal_collect_with_tag_no_telemetry()[0][0]  # type: ignore
