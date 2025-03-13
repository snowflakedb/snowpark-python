#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import queue
import traceback
from enum import Enum
from typing import Any, Tuple, Optional, Callable, Dict

from snowflake.snowpark._internal.data_source.dbms_dialects.sqlite3_dialect import (
    Sqlite3Dialect,
)
from snowflake.snowpark._internal.data_source.drivers.sqlite_driver import SqliteDriver
import snowflake
from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.dbms_dialects.oracledb_dialect import (
    OracledbDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.sqlserver_dialect import (
    SqlServerDialect,
)
from snowflake.snowpark._internal.data_source.drivers.oracledb_driver import (
    OracledbDriver,
)
from snowflake.snowpark._internal.data_source.drivers.pyodbc_driver import PyodbcDriver
from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
import logging

logger = logging.getLogger(__name__)

_MAX_RETRY_TIME = 3
STATEMENT_PARAMS_DATA_SOURCE = "SNOWPARK_PYTHON_DATASOURCE"
DATA_SOURCE_DBAPI_SIGNATURE = "DataFrameReader.dbapi"
DATA_SOURCE_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_DBAPI_SIGNATURE} */"
)


class DBMS_TYPE(Enum):
    SQL_SERVER_DB = "SQL_SERVER_DB"
    ORACLE_DB = "ORACLE_DB"
    SQLITE_DB = "SQLITE3_DB"
    UNKNOWN = "UNKNOWN"


DBMS_MAP = {
    DBMS_TYPE.SQL_SERVER_DB: SqlServerDialect,
    DBMS_TYPE.ORACLE_DB: OracledbDialect,
    DBMS_TYPE.SQLITE_DB: Sqlite3Dialect,
}

DRIVER_MAP = {
    "pyodbc": PyodbcDriver,
    "oracledb": OracledbDriver,
    "sqlite3": SqliteDriver,
}


def detect_dbms(dbapi2_conn) -> Tuple[DBMS_TYPE, str]:
    """Detects the DBMS type from a DBAPI2 connection."""

    # Get the Python driver name
    python_driver_name = type(dbapi2_conn).__module__.lower()

    # Dictionary-based lookup for known DBMS
    dbms_mapping = {
        "pyodbc": detect_dbms_pyodbc,
        "oracledb": lambda conn: DBMS_TYPE.ORACLE_DB,
        "sqlite3": lambda conn: DBMS_TYPE.SQLITE_DB,
    }

    if python_driver_name in dbms_mapping:
        return dbms_mapping[python_driver_name](dbapi2_conn), python_driver_name

    logger.debug(f"Unsupported database driver: {python_driver_name}")
    return DBMS_TYPE.UNKNOWN, python_driver_name


def detect_dbms_pyodbc(dbapi2_conn):
    """Detects the DBMS type for a pyodbc connection."""
    # pyodbc.SQL_DBMS_NAME is a constant used to get the DBMS name by calling dbapi2_conn.getinfo(pyodbc.SQL_DBMS_NAME)
    # and according to the ODBC spec, SQL_DBMS_NAME is an integer value 17
    # https://github.com/microsoft/ODBC-Specification/blob/4dda95986bda5d3b55d7749315d3e5a0951c1e50/Windows/inc/sql.h#L467
    # here we are using pyodbc_conn.getinfo(17) to get the DBMS name to avoid importing pyodbc
    # which helps our test while achieving the same goal
    dbms_name = dbapi2_conn.getinfo(17).lower()  # pyodbc.SQL_DBMS_NAME = 17

    # Set-based lookup for SQL Server
    sqlserver_keywords = {"sql server", "mssql", "sqlserver"}
    if any(keyword in dbms_name for keyword in sqlserver_keywords):
        return DBMS_TYPE.SQL_SERVER_DB

    logger.debug(f"Unsupported DBMS for pyodbc: {dbms_name}")
    return DBMS_TYPE.UNKNOWN


def _task_fetch_data_from_source(
    worker: DataSourceReader,
    parquet_file_queue: queue.Queue,
    partition: str,
    partition_idx: int,
    tmp_dir: str,
    query_timeout: int = 0,
    session_init_statement: Optional[str] = None,
):

    conn = worker.driver.prepare_connection(
        worker.driver.create_connection(), query_timeout
    )
    cursor = conn.cursor()

    def convert_to_parquet(fetched_data, fetch_idx):
        df = DataSourceReader.data_source_data_to_pandas_df(fetched_data, worker.schema)
        if df.empty:
            logger.debug(
                f"The DataFrame is empty, no parquet file is generated for partition {partition_idx} fetch {fetch_idx}."
            )
            return None
        path = os.path.join(
            tmp_dir, f"data_partition{partition_idx}_fetch{fetch_idx}.parquet"
        )
        df.to_parquet(path)
        return path

    if session_init_statement:
        cursor.execute(session_init_statement)
    for i, result in enumerate(worker.read(partition, cursor)):
        parquet_file_location = convert_to_parquet(result, i)
        if parquet_file_location:
            parquet_file_queue.put(parquet_file_location)


def _task_fetch_data_from_source_with_retry(
    worker: DataSourceReader,
    parquet_file_queue: queue.Queue,
    partition: str,
    partition_idx: int,
    tmp_dir: str,
    query_timeout: int = 0,
    session_init_statement: Optional[str] = None,
):
    _retry_run(
        _task_fetch_data_from_source,
        worker,
        parquet_file_queue,
        partition,
        partition_idx,
        tmp_dir,
        query_timeout,
        session_init_statement,
    )


def _upload_and_copy_into_table(
    session: "snowflake.snowpark.Session",
    local_file: str,
    snowflake_stage_name: str,
    snowflake_table_name: Optional[str] = None,
    on_error: Optional[str] = "abort_statement",
    statements_params: Optional[Dict[str, str]] = None,
):
    file_name = os.path.basename(local_file)
    put_query = (
        f"PUT {normalize_local_file(local_file)} "
        f"@{snowflake_stage_name} OVERWRITE=TRUE {DATA_SOURCE_SQL_COMMENT}"
    )
    copy_into_table_query = f"""
    COPY INTO {snowflake_table_name} FROM @{snowflake_stage_name}/{file_name}
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    PURGE=TRUE
    ON_ERROR={on_error}
    {DATA_SOURCE_SQL_COMMENT}
    """
    session.sql(put_query).collect(statement_params=statements_params)
    session.sql(copy_into_table_query).collect(statement_params=statements_params)


def _upload_and_copy_into_table_with_retry(
    session: "snowflake.snowpark.Session",
    local_file: str,
    snowflake_stage_name: str,
    snowflake_table_name: Optional[str] = None,
    on_error: Optional[str] = "abort_statement",
    statements_params: Optional[Dict[str, str]] = None,
):
    _retry_run(
        _upload_and_copy_into_table,
        session,
        local_file,
        snowflake_stage_name,
        snowflake_table_name,
        on_error,
        statements_params,
    )


def _retry_run(func: Callable, *args, **kwargs) -> Any:
    retry_count = 0
    last_error = None
    error_trace = ""
    func_name = func.__name__
    while retry_count < _MAX_RETRY_TIME:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            error_trace = traceback.format_exc()
            retry_count += 1
            logger.debug(
                f"[{func_name}] Attempt {retry_count}/{_MAX_RETRY_TIME} failed with {type(last_error).__name__}: {str(last_error)}. Retrying..."
            )
    error_message = (
        f"Function `{func_name}` failed after {_MAX_RETRY_TIME} attempts.\n"
        f"Last error: [{type(last_error).__name__}] {str(last_error)}\n"
        f"Traceback:\n{error_trace}"
    )
    final_error = SnowparkDataframeReaderException(message=error_message)
    raise final_error
