#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import queue
import traceback
import multiprocessing as mp
from io import BytesIO
from enum import Enum
from typing import Any, Tuple, Optional, Callable, Dict
import logging
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
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException


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
    parquet_queue: mp.Queue,
):
    """
    Fetch data from source and convert to parquet BytesIO objects.
    Put BytesIO objects into the multiprocessing queue.
    """

    def convert_to_parquet_bytesio(fetched_data, fetch_idx):
        df = worker.data_source_data_to_pandas_df(fetched_data)
        if df.empty:
            logger.debug(
                f"The DataFrame is empty, no parquet BytesIO is generated for partition {partition_idx} fetch {fetch_idx}."
            )
            return

        # Create BytesIO object and write parquet data to it
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)  # Reset position to beginning

        # Create a unique identifier for this parquet data
        parquet_id = f"data_partition{partition_idx}_fetch{fetch_idx}.parquet"

        # Put the BytesIO object and its identifier into the queue
        parquet_queue.put((parquet_id, parquet_buffer))
        logger.debug(f"Added parquet BytesIO to queue: {parquet_id}")

    for i, result in enumerate(worker.read(partition)):
        convert_to_parquet_bytesio(result, i)

    parquet_queue.put((f"PARTITION_COMPLETE_{partition_idx}", None))


def _task_fetch_data_from_source_with_retry(
    worker: DataSourceReader,
    partition: str,
    partition_idx: int,
    parquet_queue: mp.Queue,
):
    _retry_run(
        _task_fetch_data_from_source,
        worker,
        partition,
        partition_idx,
        parquet_queue,
    )


def _upload_and_copy_into_table(
    session: "snowflake.snowpark.Session",
    parquet_id: str,
    parquet_buffer: BytesIO,
    snowflake_stage_name: str,
    snowflake_table_name: Optional[str] = None,
    on_error: Optional[str] = "abort_statement",
    statements_params: Optional[Dict[str, str]] = None,
):
    """
    Upload BytesIO parquet data to Snowflake stage and copy into table.
    """
    # Reset buffer position to beginning
    parquet_buffer.seek(0)

    # Upload BytesIO directly to stage using put_stream
    stage_file_path = f"@{snowflake_stage_name}/{parquet_id}"
    session.file.put_stream(
        parquet_buffer,
        stage_file_path,
        overwrite=True,
    )

    # Copy into table
    copy_into_table_query = f"""
    COPY INTO {snowflake_table_name} FROM @{snowflake_stage_name}/{parquet_id}
    FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER=TRUE)
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    PURGE=TRUE
    ON_ERROR={on_error}
    {DATA_SOURCE_SQL_COMMENT}
    """
    session.sql(copy_into_table_query).collect(statement_params=statements_params)
    logger.debug(f"Successfully uploaded and copied BytesIO parquet: {parquet_id}")


def _upload_and_copy_into_table_with_retry(
    session: "snowflake.snowpark.Session",
    parquet_id: str,
    parquet_buffer: BytesIO,
    snowflake_stage_name: str,
    snowflake_table_name: Optional[str] = None,
    on_error: Optional[str] = "abort_statement",
    statements_params: Optional[Dict[str, str]] = None,
):
    _retry_run(
        _upload_and_copy_into_table,
        session,
        parquet_id,
        parquet_buffer,
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


# DBAPI worker function that processes multiple partitions
def worker_process(partition_queue: mp.Queue, parquet_queue: mp.Queue, reader):
    """Worker process that fetches data from multiple partitions"""
    while True:
        try:
            partition_idx, query = partition_queue.get(block=False)

            _task_fetch_data_from_source_with_retry(
                reader,
                query,
                partition_idx,
                parquet_queue,
            )
        except queue.Empty:
            # No more work available, exit gracefully
            break
        except Exception as e:
            # Put error information in queue to signal failure
            parquet_queue.put(("ERROR", e))
            break


def process_parquet_queue_with_threads(
    session: "snowflake.snowpark.Session",
    parquet_queue: mp.Queue,
    processes: list,
    total_partitions: int,
    snowflake_stage_name: str,
    snowflake_table_name: str,
    max_workers: int,
    statements_params: Optional[Dict[str, str]] = None,
    on_error: str = "abort_statement",
) -> None:
    """
    Process parquet data from a multiprocessing queue using a thread pool.

    This utility method handles the common pattern of:
    1. Reading parquet data from a multiprocessing queue
    2. Uploading and copying the data to Snowflake using multiple threads
    3. Tracking completion of partitions
    4. Handling errors and process monitoring

    Args:
        session: Snowflake session for database operations
        parquet_queue: Multiprocessing queue containing parquet data
        processes: List of worker processes to monitor
        total_partitions: Total number of partitions expected
        snowflake_stage_name: Name of the Snowflake stage for uploads
        snowflake_table_name: Name of the target Snowflake table
        max_workers: Maximum number of threads for parallel uploads
        statements_params: Optional parameters for SQL statements
        on_error: Error handling strategy for COPY INTO operations

    Raises:
        RuntimeError: If any worker process fails
        Exception: Any exception raised during parquet processing
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import queue

    completed_partitions = set()

    with ThreadPoolExecutor(max_workers=max_workers) as thread_executor:
        thread_futures = []

        while len(completed_partitions) < total_partitions:
            try:
                # Get item from queue with timeout
                item = parquet_queue.get(block=False)
                parquet_id, parquet_buffer = item

                # Check for completion signals
                if parquet_id.startswith("PARTITION_COMPLETE_"):
                    partition_idx = int(parquet_id.split("_")[-1])
                    completed_partitions.add(partition_idx)
                    logger.debug(f"Partition {partition_idx} completed.")
                    continue

                # Check for errors
                if parquet_id == "ERROR":
                    logger.error(f"Error in data fetching process: {parquet_buffer}")
                    raise parquet_buffer

                # Process valid BytesIO parquet data
                logger.debug(f"Retrieved BytesIO parquet from queue: {parquet_id}")
                thread_future = thread_executor.submit(
                    _upload_and_copy_into_table_with_retry,
                    session,
                    parquet_id,
                    parquet_buffer,
                    snowflake_stage_name,
                    snowflake_table_name,
                    on_error,
                    statements_params,
                )
                thread_futures.append(thread_future)
                logger.debug(
                    f"Submitted BytesIO parquet {parquet_id} to thread executor for ingestion."
                )

            except queue.Empty:
                # Check if any processes have failed
                for i, process in enumerate(processes):
                    if not process.is_alive() and process.exitcode != 0:
                        raise RuntimeError(
                            f"Process {i} failed with exit code {process.exitcode}"
                        )
                continue

        # Wait for all upload threads to complete
        for future in as_completed(thread_futures):
            future.result()  # Throw error if the thread failed
            logger.debug("A thread future completed successfully.")

    # Wait for all processes to complete
    for process in processes:
        process.join()
        if process.exitcode != 0:
            raise RuntimeError(f"Process failed with exit code {process.exitcode}")
