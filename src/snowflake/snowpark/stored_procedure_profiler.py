#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
from typing import Literal

import snowflake.snowpark
from snowflake.snowpark._internal.snowpark_profiler import SnowparkProfiler
from snowflake.snowpark._internal.utils import (
    parse_table_name,
    strip_double_quotes_in_like_statement_in_table_name,
    SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN,
)

logger = logging.getLogger(__name__)


class StoredProcedureProfiler(SnowparkProfiler):
    """
    Set up profiler to receive profiles of stored procedures. This feature cannot be used in owner's right stored
    procedure because owner's right stored procedure will not be able to set session-level parameters.
    """

    def __init__(
        self,
        session: "snowflake.snowpark.Session",
    ) -> None:
        super().__init__(session)
        self._output_sql = (
            "select snowflake.core.get_python_profiler_output('{query_id}')"
        )
        self._profiler_module_name = "python_profiler_modules"

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
        super().set_active_profiler(active_profiler_type)

    @staticmethod
    def _is_procedure_or_function_call(query: str) -> bool:
        query = query.upper().strip(" ")
        return SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN.match(
            query
        ) is not None or query.startswith("CALL")
