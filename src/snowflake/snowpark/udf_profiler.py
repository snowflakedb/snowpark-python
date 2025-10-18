#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import snowflake.snowpark
from snowflake.snowpark._internal.snowpark_profiler import SnowparkProfiler


logger = logging.getLogger(__name__)

SNOWFLAKE_ANONYMOUS_FUNCTION_PATTERN = re.compile(
    r"^\s*WITH\s+\w+\s+AS\s+FUNCTION", re.IGNORECASE
)


class UDFProfiler(SnowparkProfiler):
    """
    Set up profiler to receive profiles of UDFs.
    """

    def __init__(
        self,
        session: "snowflake.snowpark.Session",
    ) -> None:
        super().__init__(session)

        self._output_sql = "select * from table(SNOWFLAKE.LOCAL.GET_PYTHON_UDF_PROFILER_OUTPUT('{query_id}'));"
        self._profiler_module_name = "PYTHON_UDF_PROFILER_MODULES"

    @staticmethod
    def _is_procedure_or_function_call(query: str) -> bool:
        query = query.upper().strip(" ")
        return SNOWFLAKE_ANONYMOUS_FUNCTION_PATTERN.match(
            query
        ) is not None or query.startswith("SELECT")
