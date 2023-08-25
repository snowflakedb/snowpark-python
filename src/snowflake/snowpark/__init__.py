#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
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
    "AsyncJob",
]


from snowflake.snowpark.version import VERSION

__version__ = ".".join(str(x) for x in VERSION if x is not None)


from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_na_functions import DataFrameNaFunctions
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.dataframe_stat_functions import DataFrameStatFunctions
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.file_operation import FileOperation, GetResult, PutResult
from snowflake.snowpark.query_history import QueryHistory, QueryRecord
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


def register_sql_magic():
    try:
        import IPython
        from IPython.core.magic import register_cell_magic

        def sql(line, cell):
            user_ns = IPython.get_ipython().user_ns
            if "session" in user_ns:
                session = user_ns["session"]
                # the following will allow using variables
                # for example session.sql("select {variable1}")
                formatted_sql = cell.format(**user_ns)
                name = None
                line = (line or "").strip()
                if line:
                    name = line.strip().split(" ")[0]
                df = session.sql(formatted_sql)
                if name:
                    user_ns[name] = df
                else:
                    user_ns["__df"] = df
                    return df
            else:
                return "No session was found. Define a Snowpark Session object with the name 'session'"

        register_cell_magic(sql)
    except Exception:
        pass


register_sql_magic()
