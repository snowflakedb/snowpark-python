#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import argparse
import datetime
import json
import logging
import os
import sys
import time
from pathlib import Path
from threading import Thread
from typing import List

from snowflake.snowpark import Session, Window
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import avg, col, random as snowflake_random

connection_parameters_path = str(Path(__file__).absolute().parent.parent)
sys.path.append(connection_parameters_path)
from parameters import CONNECTION_PARAMETERS  # noqa: E402

logger = logging.getLogger("long-running")
logger.setLevel(logging.INFO)


def generate_columns(n: int) -> List[str]:
    return [f'{"a" * 50}{i}' for i in range(n)]


def run(session: Session, number_of_columns: int):
    """
    These APIs are called:
    simplifier or no simplifier
    select, sort, and filter
    with_column
    aggregate
    window
    join
    union
    collect
    to_pandas
    cache_result (includes save_as_table)
    to_local_iterator
    to_pandas_batch
    collect_nowait
    write_pandas
    """
    new_columns = generate_columns(number_of_columns)
    df = session.create_dataframe([[1]] * 100, schema=["a"])
    for c in new_columns:
        df = df.with_column(c, snowflake_random())
    for c in new_columns:
        df = df.drop(c)
    for c in new_columns:
        df = df.with_column(c, snowflake_random())
    df = df.select("a", *new_columns).filter(col("a") == 1).sort("a")
    df = df.with_column(
        "window_value", avg(df[1]).over(Window.partition_by(df[2]).order_by(df[3]))
    )
    df.collect_nowait().result()
    with df.cache_result() as cached_df:
        union_df = cached_df.union(df)
        join_df = union_df.join(df, union_df[1] == df[1])
        join_df.collect()
    group_by_df = df.group_by("a").avg(df[1])
    group_by_df.collect()
    for _ in df.to_local_iterator():
        pass
    for pandas_df in df.to_pandas_batches():
        with session.write_pandas(
            pandas_df,
            table_name=random_name_for_temp_object(TempObjectType.TABLE),
            auto_create_table=True,
        ):
            pass


def start_cpu_mem_daemon(interval):
    import psutil  # put it here to avoid it's being loaded by pytest in merge gate.

    def gather_cpu_memory():
        process = psutil.Process(os.getpid())
        os.makedirs(log_folder, exist_ok=True)
        start_time = time.time()
        while True:
            info = {
                "test_duration": time.time() - start_time,
                "process_used_memory": process.memory_info().rss,
                "process_cpu_percentage": process.cpu_percent(),
                "os_used_mem": psutil.virtual_memory()[3],
                "os_cpu": psutil.cpu_percent(),
            }
            logger.info(json.dumps(info))
            time.sleep(interval)

    resource_thread = Thread(target=gather_cpu_memory, daemon=True)
    resource_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snowpark Python Long Running test")
    parser.add_argument(
        "--duration", default=1, type=int, help="Duration of the test in seconds."
    )
    parser.add_argument(
        "--res_interval",
        default=10,
        type=int,
        help="Interval of outputing CPU and Memory usage.",
    )
    parser.add_argument(
        "--log_folder",
        default=None,
        type=str,
        help="Log file path. Log to console if not set.",
    )
    parser.add_argument(
        "--columns",
        type=int,
        default=50,
        help="number of columns, default ncalls.",
    )
    parser.add_argument(
        "--simplify",
        action="store_true",
        default=False,
        help="Whether use sql simplifier",
    )
    args = parser.parse_args()
    log_folder = args.log_folder
    logger = logging.getLogger("long-running")
    logger.setLevel(logging.INFO)
    if log_folder:
        log_file_path = f"{log_folder if log_folder[-1] == '/' else log_folder + '/'}long_running_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        logger.addHandler(logging.FileHandler(log_file_path))
    else:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    log_header = {
        "Test": "Snowpark Python Long Running Test",
        "Parameters": str(args),
    }
    logger.info(json.dumps(log_header))
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    session.sql_simplifier_enabled = args.simplify
    start_cpu_mem_daemon(args.res_interval)
    try:
        start_time = time.time()
        end_time = start_time + args.duration
        while time.time() < end_time:
            run(session, args.columns)
        print("Test stopped.")
    except KeyboardInterrupt:
        print("Test Interrupted.")
    finally:
        session.close()
