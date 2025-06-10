#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Contains core classes of Snowpark.
"""

# types, udf, functions, exceptions still use its own modules

__all__ = [
    "Column",
    "CaseExpr",
    "Row",
    "Session",
    "FileOperation",
    "PutResult",
    "GetResult",
    "DataFrame",
    "DataFrameStatFunctions",
    "DataFrameAnalyticsFunctions",
    "DataFrameNaFunctions",
    "DataFrameWriter",
    "DataFrameReader",
    "GroupingSets",
    "RelationalGroupedDataFrame",
    "Window",
    "WindowSpec",
    "Table",
    "UpdateResult",
    "DeleteResult",
    "MergeResult",
    "WhenMatchedClause",
    "WhenNotMatchedClause",
    "QueryRecord",
    "QueryHistory",
    "QueryListener",
    "AsyncJob",
    "StoredProcedureProfiler",
]


import sys
import warnings

from snowflake.snowpark.version import VERSION

__version__ = ".".join(str(x) for x in VERSION if x is not None)


from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.stored_procedure_profiler import StoredProcedureProfiler
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_analytics_functions import DataFrameAnalyticsFunctions
from snowflake.snowpark.dataframe_na_functions import DataFrameNaFunctions
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.dataframe_stat_functions import DataFrameStatFunctions
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.file_operation import FileOperation, GetResult, PutResult
from snowflake.snowpark.query_history import QueryHistory, QueryListener, QueryRecord
from snowflake.snowpark.relational_grouped_dataframe import (
    GroupingSets,
    RelationalGroupedDataFrame,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import (
    DeleteResult,
    MergeResult,
    Table,
    UpdateResult,
    WhenMatchedClause,
    WhenNotMatchedClause,
)
from snowflake.snowpark.window import Window, WindowSpec

_deprecation_warning_msg = (
    "Python Runtime 3.8 reached its End-Of-Life (EOL) on October 14, 2024, there will be no further bug fixes "
    "or security updates for this runtime. We recommend that you upgrade your existing Python 3.8 objects to "
    "Python 3.9, 3.10 or 3.11 before March 31, 2025. Please note that end of support does not impact execution, "
    "and you will still be able to update and invoke existing objects. "
    "However, they will be running on an unsupported runtime which will no longer be maintained or patched by "
    "the Snowflake team. For more details, please refer "
    "to https://docs.snowflake.com/en/developer-guide/python-runtime-support-policy."
)
warnings.filterwarnings(
    "once",  # ensure the warning is only shown once to avoid warning explosion
    message=_deprecation_warning_msg,
)

if sys.version_info.major == 3 and sys.version_info.minor == 8:
    warnings.warn(
        _deprecation_warning_msg,
        category=DeprecationWarning,
        stacklevel=2,
    )
