#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import os
import queue
import traceback
from enum import Enum
from typing import Any, Tuple, Optional, Callable, Dict, Set, Union, List
import logging

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.data_source.dbms_dialects import (
    Sqlite3Dialect,
    OracledbDialect,
    SqlServerDialect,
    DatabricksDialect,
    PostgresDialect,
    MysqlDialect,
)
from snowflake.snowpark._internal.data_source.drivers import (
    SqliteDriver,
    OracledbDriver,
    PyodbcDriver,
    DatabricksDriver,
    Psycopg2Driver,
    PymysqlDriver,
)
import snowflake
from snowflake.snowpark._internal.data_source import DataSourceReader
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    IntegerType,
    TimestampType,
    DateType,
    BinaryType,
)
from snowflake.connector.options import pandas as pd

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
    DATABRICKS_DB = "DATABRICKS_DB"
    POSTGRES_DB = "POSTGRES_DB"
    MYSQL_DB = "MYSQL_DB"
    UNKNOWN = "UNKNOWN"


class DRIVER_TYPE(str, Enum):
    PYODBC = "pyodbc"
    ORACLEDB = "oracledb"
    SQLITE3 = "sqlite3"
    DATABRICKS = "databricks.sql.client"
    PSYCOPG2 = "psycopg2.extensions"
    PYMYSQL = "pymysql.connections"
    UNKNOWN = "unknown"


DBMS_MAP = {
    DBMS_TYPE.SQL_SERVER_DB: SqlServerDialect,
    DBMS_TYPE.ORACLE_DB: OracledbDialect,
    DBMS_TYPE.SQLITE_DB: Sqlite3Dialect,
    DBMS_TYPE.DATABRICKS_DB: DatabricksDialect,
    DBMS_TYPE.POSTGRES_DB: PostgresDialect,
    DBMS_TYPE.MYSQL_DB: MysqlDialect,
}

DRIVER_MAP = {
    DRIVER_TYPE.PYODBC: PyodbcDriver,
    DRIVER_TYPE.ORACLEDB: OracledbDriver,
    DRIVER_TYPE.SQLITE3: SqliteDriver,
    DRIVER_TYPE.DATABRICKS: DatabricksDriver,
    DRIVER_TYPE.PSYCOPG2: Psycopg2Driver,
    DRIVER_TYPE.PYMYSQL: PymysqlDriver,
}

UDTF_PACKAGE_MAP = {
    DBMS_TYPE.ORACLE_DB: ["oracledb>=2.0.0,<4.0.0", "snowflake-snowpark-python"],
    DBMS_TYPE.SQLITE_DB: ["snowflake-snowpark-python"],
    DBMS_TYPE.SQL_SERVER_DB: [
        "pyodbc>=4.0.26,<6.0.0",
        "msodbcsql",
        "snowflake-snowpark-python",
    ],
    DBMS_TYPE.POSTGRES_DB: ["psycopg2>=2.0.0,<3.0.0", "snowflake-snowpark-python"],
    DBMS_TYPE.DATABRICKS_DB: [
        "snowflake-snowpark-python",
        "databricks-sql-connector>=4.0.0,<5.0.0",
    ],
    DBMS_TYPE.MYSQL_DB: ["pymysql>=1.0.0,<2.0.0", "snowflake-snowpark-python"],
}


def detect_dbms(dbapi2_conn) -> Tuple[DBMS_TYPE, DRIVER_TYPE]:
    """Detects the DBMS type from a DBAPI2 connection."""

    # Get the Python driver name
    python_driver_name = type(dbapi2_conn).__module__.lower()
    driver_type = DRIVER_TYPE.UNKNOWN
    try:
        driver_type = DRIVER_TYPE(python_driver_name)
    except ValueError:
        pass

    if driver_type in DBMS_MAPPING:
        return DBMS_MAPPING[python_driver_name](dbapi2_conn), driver_type

    logger.debug(f"Unsupported database driver: {python_driver_name}")
    return DBMS_TYPE.UNKNOWN, driver_type


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


DBMS_MAPPING = {
    DRIVER_TYPE.PYODBC: detect_dbms_pyodbc,
    DRIVER_TYPE.ORACLEDB: lambda conn: DBMS_TYPE.ORACLE_DB,
    DRIVER_TYPE.SQLITE3: lambda conn: DBMS_TYPE.SQLITE_DB,
    DRIVER_TYPE.DATABRICKS: lambda conn: DBMS_TYPE.DATABRICKS_DB,
    DRIVER_TYPE.PSYCOPG2: lambda conn: DBMS_TYPE.POSTGRES_DB,
    DRIVER_TYPE.PYMYSQL: lambda conn: DBMS_TYPE.MYSQL_DB,
}


def _task_fetch_data_from_source(
    worker: DataSourceReader,
    partition: str,
    partition_idx: int,
    tmp_dir: str,
):
    def convert_to_parquet(fetched_data, fetch_idx):
        df = data_source_data_to_pandas_df(fetched_data, worker.schema)
        if df.empty:
            logger.debug(
                f"The DataFrame is empty, no parquet file is generated for partition {partition_idx} fetch {fetch_idx}."
            )
            return
        path = os.path.join(
            tmp_dir, f"data_partition{partition_idx}_fetch{fetch_idx}.parquet"
        )
        df.to_parquet(path)

    for i, result in enumerate(worker.read(partition)):
        if isinstance(result, list):
            convert_to_parquet(result, i)
        else:
            convert_to_parquet(list(worker.read(partition)), 0)
            break


def _task_fetch_data_from_source_with_retry(
    worker: DataSourceReader,
    partition: str,
    partition_idx: int,
    tmp_dir: str,
):
    _retry_run(
        _task_fetch_data_from_source,
        worker,
        partition,
        partition_idx,
        tmp_dir,
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
    session.file.put(
        normalize_local_file(local_file),
        f"{snowflake_stage_name}",
        overwrite=True,
        statement_params=statements_params,
    )
    copy_into_table_query = f"""
    COPY INTO {snowflake_table_name} FROM @{snowflake_stage_name}/{file_name}
    FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER=TRUE)
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    PURGE=TRUE
    ON_ERROR={on_error}
    {DATA_SOURCE_SQL_COMMENT}
    """
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
        except SnowparkDataframeReaderException:
            # SnowparkDataframeReaderException is a non-retryable exception
            raise
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


def add_unseen_files_to_process_queue(
    work_dir: str, set_of_files_already_added_in_queue: Set[str], queue: queue.Queue
):
    """Add unseen files in the work_dir to the queue for processing."""
    # all files in the work_dir are parquet files, no subdirectory
    all_files = set(os.listdir(work_dir))
    unseen = all_files - set_of_files_already_added_in_queue
    for file in unseen:
        queue.put(os.path.join(work_dir, file))
        set_of_files_already_added_in_queue.add(file)


def convert_custom_schema_to_structtype(
    custom_schema: Union[str, StructType]
) -> StructType:
    if isinstance(custom_schema, str):
        schema = type_string_to_type_object(custom_schema)
        if not isinstance(schema, StructType):
            raise ValueError(
                f"Invalid schema string: {custom_schema}. "
                f"You should provide a valid schema string representing a struct type."
                'For example: "id INTEGER, int_col INTEGER, text_col STRING".'
            )
        return schema
    elif isinstance(custom_schema, StructType):
        return custom_schema
    else:
        raise ValueError(
            f"Invalid schema type: {type(custom_schema)}."
            'The schema should be either a valid schema string, for example: "id INTEGER, int_col INTEGER, text_col STRING".'
            'or a valid StructType, for example: StructType([StructField("ID", IntegerType(), False)])'
        )


def data_source_data_to_pandas_df(
    data: List[Any], schema: StructType
) -> "pd.DataFrame":
    # unquote column name because double quotes stored in parquet file create column mismatch during copy into table
    columns = [unquote_if_quoted(col.name) for col in schema.fields]
    # this way handles both list of object and list of tuples and avoid implicit pandas type conversion
    df = pd.DataFrame([list(row) for row in data], columns=columns, dtype=object)

    for field in schema.fields:
        name = unquote_if_quoted(field.name)
        if isinstance(field.datatype, IntegerType):
            # 'Int64' is a pandas dtype while 'int64' is a numpy dtype, as stated here:
            # https://github.com/pandas-dev/pandas/issues/27731
            # https://pandas.pydata.org/docs/reference/api/pandas.Int64Dtype.html
            # https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.int64
            df[name] = df[name].astype("Int64")
        elif isinstance(field.datatype, (TimestampType, DateType)):
            df[name] = df[name].map(
                lambda x: x.isoformat()
                if isinstance(x, (datetime.datetime, datetime.date))
                else x
            )
        elif isinstance(field.datatype, BinaryType):
            df[name] = df[name].map(
                lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x
            )
    return df
