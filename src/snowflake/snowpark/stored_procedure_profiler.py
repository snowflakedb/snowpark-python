#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re
import threading
from typing import List, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.utils import validate_object_name


class StoredProcedureProfiler:
    """
    Set up profiler to receive profiles of stored procedures.

    Note:
        This feature cannot be used in owner's right stored procedure because owner's right stored procedure will not be able to set session-level parameters.
    """

    def __init__(
        self,
        session: "snowflake.snowpark.Session" = None,
    ) -> None:
        self.stage = ""
        self.active_profiler_type = ""
        self.registered_stored_procedures = []
        self.session = session
        self._pattern = r"WITH\s+.*?\s+AS\s+PROCEDURE\s+.*?\s+CALL\s+.*"
        self._query_history = session.query_history(include_thread_id=True)

    def register_modules(self, stored_procedures: List[str]):
        """
        Register stored procedures to generate profiles for them.

        Note:
            Registered modules will be overwritten by this function. Use this function with an empty string will remove registered modules.
        Args:
            stored_procedures: List of names of stored procedures.
        """
        self.registered_stored_procedures = stored_procedures
        sql_statement = (
            f"alter session set python_profiler_modules='{','.join(stored_procedures)}'"
        )
        self.session.sql(sql_statement).collect()

    def set_targeted_stage(self, stage: str):
        """
        Set targeted stage for profiler output.

        Note:
            The stage name must be a fully qualified name.

        Args:
            stage: String of fully qualified name of targeted stage
        """
        validate_object_name(stage)
        self.stage = stage
        if (
            len(self.session.sql(f"show stages like '{self.stage}'").collect()) == 0
            and len(
                self.session.sql(
                    f"show stages like '{self.stage.split('.')[-1]}'"
                ).collect()
            )
            == 0
        ):
            self.session.sql(
                f"create temp stage if not exists {self.stage} FILE_FORMAT = (RECORD_DELIMITER = NONE FIELD_DELIMITER = NONE )"
            ).collect()
        sql_statement = f'alter session set PYTHON_PROFILER_TARGET_STAGE ="{stage}"'
        self.session.sql(sql_statement).collect()

    def set_active_profiler(self, active_profiler_type: Optional[str] = None):
        """
        Set active profiler.

        Note:
            Active profiler must be either 'LINE' or 'MEMORY' (case-sensitive). Active profiler is 'LINE' by default.
        Args:
            active_profiler_type: String that represent active_profiler, must be either 'LINE' or 'MEMORY' (case-sensitive).

        """
        if active_profiler_type is None:
            active_profiler_type = ""

        if active_profiler_type not in ["LINE", "MEMORY", ""]:
            raise ValueError(
                f"active_profiler expect 'LINE', 'MEMORY' or empty string '', got {active_profiler_type} instead"
            )
        self.active_profiler_type = active_profiler_type
        sql_statement = f"alter session set ACTIVE_PYTHON_PROFILER = '{self.active_profiler_type.upper()}'"
        self.session.sql(sql_statement).collect()

    def disable(self):
        """
        Disable profiler.
        """
        self.active_profiler_type = ""
        sql_statement = "alter session set ACTIVE_PYTHON_PROFILER = ''"
        self.session.sql(sql_statement).collect()

    def _is_sp_call(self, query):
        return re.match(self._pattern, query, re.DOTALL) is not None

    def _get_last_query_id(self):
        current_thread = threading.get_ident()
        for query in self._query_history.queries[::-1]:
            query_thread = getattr(query, "thread_id", None)
            if query_thread is None or query_thread == current_thread:
                if query.sql_text.upper().startswith("CALL") or self._is_sp_call(
                    query.sql_text
                ):
                    return query.query_id
        return None

    def get_output(self) -> str:
        """
        Return the profiles of last executed stored procedure in current thread.

        Note:
            This function must be called right after the execution of stored procedure you want to profile.
        """
        query_id = self._get_last_query_id()
        sql = f"select snowflake.core.get_python_profiler_output('{query_id}')"
        return self.session.sql(sql).collect()[0][0]
