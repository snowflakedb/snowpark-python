#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re
from contextlib import contextmanager
from typing import List, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.utils import validate_object_name


class Profiler:
    """
    Setup profiler to receive profiles of stored procedures.

    Note:
        This feature cannot be used in owner's right SP because owner's right SP will not be able to set session-level parameters.
    """

    def __init__(
        self,
        stage: Optional[str] = "",
        active_profiler: Optional[str] = "LINE",
        session: Optional["snowflake.snowpark.Session"] = None,
    ) -> None:
        self.stage = stage
        self.active_profiler = active_profiler
        self.modules_to_register = []
        self.register_modules_sql = ""
        self.set_targeted_stage_sql = ""
        self.enable_profiler_sql = ""
        self.disable_profiler_sql = ""
        self.set_active_profiler_sql = ""
        self.pattern = r"WITH\s+.*?\s+AS\s+PROCEDURE\s+.*?\s+CALL\s+.*"
        self.session = session
        self._prepare_sql()
        self.query_history = None

    def _prepare_sql(self):
        self.register_modules_sql = f"alter session set python_profiler_modules='{','.join(self.modules_to_register)}'"
        self.set_targeted_stage_sql = (
            f'alter session set PYTHON_PROFILER_TARGET_STAGE ="{self.stage}"'
        )
        self.enable_profiler_sql = "alter session set ENABLE_PYTHON_PROFILER = true"
        self.disable_profiler_sql = "alter session set ENABLE_PYTHON_PROFILER = false"
        self.set_active_profiler_sql = f"alter session set ACTIVE_PYTHON_PROFILER = '{self.active_profiler.upper()}'"

    def register_profiler_modules(self, modules: List[str]):
        """
        Register stored procedures to generate profiles for them.

        Note:
            Registered nodules will be overwritten by this function,
            use this function with an empty string will remove registered modules.
        Args:
            modules: List of names of stored procedures.
        """
        self.modules_to_register = modules
        self._prepare_sql()
        if self.session is not None:
            self._register_modules()

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
        self._prepare_sql()
        if self.session is not None:
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
                    f"create temp stage if not exist {self.stage} FILE_FORMAT = (RECORD_DELIMITER = NONE FIELD_DELIMITER = NONE )"
                ).collect()
            self._set_targeted_stage()

    def set_active_profiler(self, active_profiler: str):
        """
        Set active profiler.

        Note:
            Active profiler must be either 'LINE' or 'MEMORY' (case-sensitive),
            active profiler is set to 'LINE' by default.
        Args:
            active_profiler: String that represent active_profiler, must be either 'LINE' or 'MEMORY' (case-sensitive).

        """
        if active_profiler not in ["LINE", "MEMORY"]:
            raise ValueError(
                f"active_profiler expect 'LINE' or 'MEMORY', got {active_profiler} instead"
            )
        self.active_profiler = active_profiler
        self._prepare_sql()
        if self.session is not None:
            self._set_active_profiler()

    def _register_modules(self):
        self.session.sql(self.register_modules_sql).collect()

    def _set_targeted_stage(self):
        self.session.sql(self.set_targeted_stage_sql).collect()

    def _set_active_profiler(self):
        self.session.sql(self.set_active_profiler_sql).collect()

    def enable_profiler(self):
        """
        Enable profiler. Profiles will be generated until profiler is disabled.
        """
        self.session.sql(self.enable_profiler_sql).collect()

    def disable_profiler(self):
        """
        Disable profiler.
        """
        self.session.sql(self.disable_profiler_sql).collect()

    def _is_sp_call(self, query):
        return re.match(self.pattern, query, re.DOTALL) is not None

    def _get_last_query_id(self):
        for query in self.query_history.queries[::-1]:
            if query.sql_text.upper().startswith("CALL") or self._is_sp_call(
                query.sql_text
            ):
                return query.query_id
        return None

    def show_profiles(self) -> str:
        """
        Return and show the profiles of last executed stored procedure.

        Note:
            This function must be called right after the execution of stored procedure you want to profile.
        """
        query_id = self._get_last_query_id()
        sql = f"select snowflake.core.get_python_profiler_output('{query_id}')"
        res = self.session.sql(sql).collect()
        print(res[0][0])  # noqa: T201: we need to print here.
        return res[0][0]

    def dump_profiles(self, dst_file: str):
        """
        Write the profiles of last executed stored procedure to given file.

        Note:
            This function must be called right after the execution of stored procedure you want to profile.

        Args:
            dst_file: String of file name that you want to store the profiles.
        """
        query_id = self._get_last_query_id()
        sql = f"select snowflake.core.get_python_profiler_output('{query_id}')"
        res = self.session.sql(sql).collect()
        with open(dst_file, "w") as f:
            f.write(str(res[0][0]))


@contextmanager
def profiler(
    stage: str,
    active_profiler: str,
    session: "snowflake.snowpark.Session",
    modules: Optional[List[str]] = None,
):
    internal_profiler = Profiler(stage, active_profiler, session)
    session.profiler = internal_profiler
    internal_profiler.query_history = session.query_history()
    modules = modules or []
    try:
        # create stage if not exist
        if (
            len(session.sql(f"show stages like '{internal_profiler.stage}'").collect())
            == 0
            and len(
                session.sql(
                    f"show stages like '{internal_profiler.stage.split('.')[-1]}'"
                ).collect()
            )
            == 0
        ):
            session.sql(
                f"create temp stage if not exist {internal_profiler.stage} FILE_FORMAT = (RECORD_DELIMITER = NONE FIELD_DELIMITER = NONE )"
            ).collect()
        # set up phase
        internal_profiler._set_targeted_stage()
        internal_profiler._set_active_profiler()

        internal_profiler.register_profiler_modules(modules)
        internal_profiler._register_modules()
        internal_profiler.enable_profiler()
    finally:
        yield
        internal_profiler.register_profiler_modules([])
        internal_profiler._register_modules()
        internal_profiler.disable_profiler()
