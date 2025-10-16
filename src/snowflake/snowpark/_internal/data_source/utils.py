#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import math
import os
import queue
import time
import traceback
import threading
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from io import BytesIO
from enum import Enum
from typing import Any, Tuple, Optional, Callable, Dict, Union, Set
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
_MAX_WORKER_SCALE = 2  # 2 * max_workers
STATEMENT_PARAMS_DATA_SOURCE = "SNOWPARK_PYTHON_DATASOURCE"
DATA_SOURCE_DBAPI_SIGNATURE = "DataFrameReader.dbapi"
DATA_SOURCE_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_DBAPI_SIGNATURE} */"
)
PARTITION_TASK_COMPLETE_SIGNAL_PREFIX = "PARTITION_COMPLETE_"
PARTITION_TASK_ERROR_SIGNAL = "ERROR"


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
    python_driver_name = (
        "oracledb"
        if python_driver_name == "oracledb.connection"
        else python_driver_name
    )
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
    parquet_queue: Union[mp.Queue, queue.Queue],
    stop_event: threading.Event = None,
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
        if stop_event and stop_event.is_set():
            return
        convert_to_parquet_bytesio(result, i)

    parquet_queue.put((f"{PARTITION_TASK_COMPLETE_SIGNAL_PREFIX}{partition_idx}", None))


def _task_fetch_data_from_source_with_retry(
    worker: DataSourceReader,
    partition: str,
    partition_idx: int,
    parquet_queue: Union[mp.Queue, queue.Queue],
    stop_event: threading.Event = None,
):
    start = time.perf_counter()
    logger.debug(f"Partition {partition_idx} fetch start")
    _retry_run(
        _task_fetch_data_from_source,
        worker,
        partition,
        partition_idx,
        parquet_queue,
        stop_event,
    )
    end = time.perf_counter()
    logger.debug(
        f"Partition {partition_idx} fetch finished, used {end - start} seconds"
    )


def _upload_and_copy_into_table(
    session: "snowflake.snowpark.Session",
    parquet_id: str,
    parquet_buffer: BytesIO,
    snowflake_stage_name: str,
    backpressure_semaphore: BoundedSemaphore,
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
    MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
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
    backpressure_semaphore: BoundedSemaphore,
    snowflake_table_name: Optional[str] = None,
    on_error: Optional[str] = "abort_statement",
    statements_params: Optional[Dict[str, str]] = None,
):
    start = time.perf_counter()
    logger.debug(f"Parquet file {parquet_id} upload and copy into table start")
    try:
        _retry_run(
            _upload_and_copy_into_table,
            session,
            parquet_id,
            parquet_buffer,
            snowflake_stage_name,
            backpressure_semaphore,
            snowflake_table_name,
            on_error,
            statements_params,
        )
    finally:
        # proactively close the buffer to release memory
        parquet_buffer.close()
        backpressure_semaphore.release()
    end = time.perf_counter()
    logger.debug(
        f"Parquet file {parquet_id} upload and copy into table finished, used {end - start} seconds"
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
def worker_process(
    partition_queue: Union[mp.Queue, queue.Queue],
    parquet_queue: Union[mp.Queue, queue.Queue],
    process_or_thread_error_indicator: Union[mp.Queue, queue.Queue],
    reader,
    stop_event: threading.Event = None,
):
    """Worker process that fetches data from multiple partitions"""
    while True:
        if stop_event and stop_event.is_set():
            # other worker has set the stop event signalling me to stop, exit gracefully
            break
        try:
            # Get item from queue with timeout
            partition_idx, query = partition_queue.get(timeout=1.0)

            _task_fetch_data_from_source_with_retry(
                reader,
                query,
                partition_idx,
                parquet_queue,
                stop_event,
            )
        except queue.Empty:
            # indicate whether a process is exit gracefully
            process_or_thread_error_indicator.put(os.getpid())
            # No more work available, exit gracefully
            break
        except Exception as e:
            # Put error information in queue to signal failure
            parquet_queue.put((PARTITION_TASK_ERROR_SIGNAL, e))
            break


def process_completed_futures(thread_futures) -> float:
    """Process completed futures with simplified error handling."""
    for parquet_id, future in list(thread_futures):  # Iterate over a copy of the set
        if future.done():
            thread_futures.discard((parquet_id, future))
            try:
                future.result()
                logger.debug(
                    f"Thread future for parquet {parquet_id} completed successfully."
                )
            except BaseException:
                # Cancel all remaining futures when one fails
                for remaining_parquet_id, remaining_future in list(
                    thread_futures
                ):  # Also iterate over copy here
                    if not remaining_future.done():
                        remaining_future.cancel()
                        logger.debug(
                            f"Cancelled a remaining future {remaining_parquet_id} due to error in another thread."
                        )
                thread_futures.clear()  # Clear the set since all are cancelled
                raise
    return time.perf_counter()


def _drain_process_status_queue(
    process_or_thread_error_indicator: Union[mp.Queue, queue.Queue],
) -> Set:
    result = set()
    while True:
        try:
            result.add(process_or_thread_error_indicator.get(block=False))
        except queue.Empty:
            break
    return result


def process_parquet_queue_with_threads(
    session: "snowflake.snowpark.Session",
    parquet_queue: Union[mp.Queue, queue.Queue],
    process_or_thread_error_indicator: Union[mp.Queue, queue.Queue],
    workers: list,
    total_partitions: int,
    snowflake_stage_name: str,
    snowflake_table_name: str,
    max_workers: int,
    statements_params: Optional[Dict[str, str]] = None,
    on_error: str = "abort_statement",
    fetch_with_process: bool = False,
) -> Tuple[float, float, float]:
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
        process_or_thread_error_indicator: Multiprocessing queue containing process exit information
        workers: List of worker processes or thread futures to monitor
        total_partitions: Total number of partitions expected
        snowflake_stage_name: Name of the Snowflake stage for uploads
        snowflake_table_name: Name of the target Snowflake table
        max_workers: Maximum number of threads for parallel uploads
        statements_params: Optional parameters for SQL statements
        on_error: Error handling strategy for COPY INTO operations

    Raises:
        SnowparkDataframeReaderException: If any worker process fails
    """
    fetch_to_local_end_time = time.perf_counter()
    upload_to_sf_start_time = math.inf
    upload_to_sf_end_time = -math.inf

    completed_partitions = set()
    gracefully_exited_processes = set()
    # process parquet_queue may produce more data than the threads can handle,
    # so we use semaphore to limit the number of threads
    backpressure_semaphore = BoundedSemaphore(value=_MAX_WORKER_SCALE * max_workers)
    logger.debug(
        f"Initialized backpressure semaphore with value: {_MAX_WORKER_SCALE * max_workers}"
    )
    with ThreadPoolExecutor(max_workers=max_workers) as thread_executor:
        thread_futures = set()  # stores tuples of (parquet_id, thread_future)
        while len(completed_partitions) < total_partitions or thread_futures:
            # Process any completed futures and handle errors
            upload_to_sf_end_time = process_completed_futures(thread_futures)

            try:
                backpressure_semaphore.acquire()
                parquet_id, parquet_buffer = parquet_queue.get(block=False)

                # Check for completion signals
                if parquet_id.startswith(PARTITION_TASK_COMPLETE_SIGNAL_PREFIX):
                    partition_idx = int(parquet_id.split("_")[-1])
                    completed_partitions.add(partition_idx)
                    logger.debug(f"Partition {partition_idx} completed.")
                    backpressure_semaphore.release()  # Release semaphore since no thread was created
                    fetch_to_local_end_time = time.perf_counter()
                    continue
                # Check for errors
                elif parquet_id == PARTITION_TASK_ERROR_SIGNAL:
                    logger.error(f"Error in data fetching process: {parquet_buffer}")
                    backpressure_semaphore.release()  # Release semaphore since no thread was created
                    raise parquet_buffer

                # Process valid BytesIO parquet data
                logger.debug(f"Retrieved BytesIO parquet from queue: {parquet_id}")

                upload_to_sf_start_time = (
                    time.perf_counter()
                    if upload_to_sf_start_time == math.inf
                    else upload_to_sf_start_time
                )
                thread_future = thread_executor.submit(
                    _upload_and_copy_into_table_with_retry,
                    session,
                    parquet_id,
                    parquet_buffer,
                    snowflake_stage_name,
                    backpressure_semaphore,
                    snowflake_table_name,
                    on_error,
                    statements_params,
                )
                thread_futures.add((parquet_id, thread_future))
                logger.debug(
                    f"Submitted BytesIO parquet {parquet_id} to thread executor for ingestion. Active threads: {len(thread_futures)}"
                )

            except queue.Empty:
                backpressure_semaphore.release()  # Release semaphore if no data was fetched
                if fetch_with_process:
                    # Check if any processes have failed
                    for i, process in enumerate(workers):
                        if not process.is_alive():
                            gracefully_exited_processes = (
                                gracefully_exited_processes.union(
                                    _drain_process_status_queue(
                                        process_or_thread_error_indicator
                                    )
                                )
                            )
                            if process.pid not in gracefully_exited_processes:
                                raise SnowparkDataframeReaderException(
                                    f"Partition {i} data fetching process failed with exit code {process.exitcode} or failed silently"
                                )
                else:
                    # Check if any threads have failed
                    for i, future in enumerate(workers):
                        if future.done():
                            try:
                                future.result()
                            except BaseException as e:
                                if isinstance(e, SnowparkDataframeReaderException):
                                    raise e
                                raise SnowparkDataframeReaderException(
                                    f"Partition {i} data fetching thread failed with error: {e}"
                                )
                time.sleep(0.1)
                continue

    if fetch_with_process:
        # Wait for all processes to complete
        for process in workers:
            process.join()
        # empty parquet queue to get all signals after each process ends
        gracefully_exited_processes = gracefully_exited_processes.union(
            _drain_process_status_queue(process_or_thread_error_indicator)
        )

        # check if any process fails
        for idx, process in enumerate(workers):
            if process.pid not in gracefully_exited_processes:
                raise SnowparkDataframeReaderException(
                    f"Partition {idx} data fetching process failed with exit code {process.exitcode} or failed silently"
                )
    else:
        # Wait for all threads to complete
        for idx, future in enumerate(workers):
            try:
                future.result()
            except BaseException as e:
                if isinstance(e, SnowparkDataframeReaderException):
                    raise e
                raise SnowparkDataframeReaderException(
                    f"Partition {idx} data fetching thread failed with error: {e}"
                )
    logger.debug(f"fetch to local end at {fetch_to_local_end_time}")
    logger.debug(f"upload and copy into end at {upload_to_sf_end_time}")
    logger.debug(
        f"upload and copy into total time: {upload_to_sf_end_time - upload_to_sf_start_time}"
    )

    return fetch_to_local_end_time, upload_to_sf_start_time, upload_to_sf_end_time


def track_data_source_statement_params(
    dataframe, statement_params: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Helper method to initialize and update data source tracking statement_params based on dataframe attributes.
    """
    statement_params = statement_params or {}
    if (
        dataframe._plan
        and dataframe._plan.api_calls
        and dataframe._plan.api_calls[0].get("name") == DATA_SOURCE_DBAPI_SIGNATURE
    ):
        # Track data source ingestion
        statement_params[STATEMENT_PARAMS_DATA_SOURCE] = "1"

    return statement_params if statement_params else None
