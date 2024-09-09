#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from contextlib import contextmanager
from typing import List, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.utils import validate_object_name


class Profiler:
    def __init__(
        self,
        stage: str,
        active_profiler: str = "LINE",
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
        self.session = session
        self.stage_and_profiler_name_validation()
        self.prepare_sql()
        self.query_history = None

    def stage_and_profiler_name_validation(self):
        if self.active_profiler not in ["LINE", "MEMORY"]:
            raise ValueError(
                f"active_profiler expect 'LINE' or 'MEMORY', got {self.active_profiler} instead"
            )
        validate_object_name(self.stage)

    def prepare_sql(self):
        self.register_modules_sql = f"alter session set python_profiler_modules='{','.join(self.modules_to_register)}'"
        self.set_targeted_stage_sql = (
            f'alter session set PYTHON_PROFILER_TARGET_STAGE ="{self.stage}"'
        )
        self.enable_profiler_sql = "alter session set ENABLE_PYTHON_PROFILER = true"
        self.disable_profiler_sql = "alter session set ENABLE_PYTHON_PROFILER = false"
        self.set_active_profiler_sql = f"alter session set ACTIVE_PYTHON_PROFILER = '{self.active_profiler.upper()}'"

    def register_modules(self, modules: List[str]):
        self.modules_to_register = modules
        self.prepare_sql()

    def set_targeted_stage(self, stage: str):
        self.stage = stage
        self.prepare_sql()

    def set_active_profiler(self, active_profiler: str):
        self.active_profiler = active_profiler
        self.prepare_sql()

    def _register_modules(self):
        self.session.sql(self.register_modules_sql).collect()

    def _set_targeted_stage(self):
        self.session.sql(self.set_targeted_stage_sql).collect()

    def _set_active_profiler(self):
        self.session.sql(self.set_active_profiler_sql).collect()

    def enable_profiler(self):
        self.session.sql(self.enable_profiler_sql).collect()

    def disable_profiler(self):
        self.session.sql(self.disable_profiler_sql).collect()

    def _get_last_query_id(self):
        sps = self.session.sql("show procedures").collect()
        names = [r.name for r in sps]
        for query in self.query_history.queries[::-1]:
            if query.sql_text.startswith("CALL"):
                sp_name = query.sql_text.split(" ")[1].split("(")[0]
                if sp_name.upper() in names:
                    return query.query_id
        return None

    def show_profiles(self):
        query_id = self._get_last_query_id()
        sql = f"select snowflake.core.get_python_profiler_output({query_id});"
        res = self.session.sql(sql).collect()
        return res

    def dump_profiles(self, dst_file: str):
        query_id = self._get_last_query_id()
        sql = f"select snowflake.core.get_python_profiler_output({query_id});"
        res = self.session.sql(sql).collect()
        with open(dst_file, "w") as f:
            for row in res:
                f.write(str(row))


@contextmanager
def profiler(
    stage: str,
    active_profiler: str,
    session: "snowflake.snowpark.Session",
    modules: Optional[List[str]] = None,
):
    internal_profiler = Profiler(stage, active_profiler, session)
    modules = [] if modules is None else modules
    try:
        # set up phase
        internal_profiler._set_targeted_stage()
        internal_profiler._set_active_profiler()

        internal_profiler.register_modules(modules)
        internal_profiler._register_modules()
        internal_profiler.enable_profiler()
    finally:
        yield
        internal_profiler.register_modules([])
        internal_profiler._register_modules()
        internal_profiler.disable_profiler()
