#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import argparse
import logging
import random
import sys
import time
from pathlib import Path
from typing import Iterable

import memory_profiler

from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session

connection_parameters_path = str(Path(__file__).absolute().parent.parent)
sys.path.append(connection_parameters_path)
from parameters import CONNECTION_PARAMETERS  # noqa: E402

logger = logging.getLogger("snowflake.snowpark")
logger.setLevel(logging.ERROR)
logger.addHandler(logging.StreamHandler())


def generate_columns(n: int):
    return [f'{i} as {"a" * 50}{i}' for i in range(n)]


def to_projection(columns: Iterable[str]):
    return ",".join(columns)


def generate_projection(n: int):
    return to_projection(generate_columns(n))


def with_column(session: Session, ncalls: int):
    df = session.sql("select 1 as a")
    for i in range(ncalls):
        df = df.with_column(f"{'a' * 50}{i}", (col("a") + 1))
    return df


def drop(session: Session, ncalls: int):
    projection = generate_projection(ncalls)
    df = session.sql(f"select 1 as a, {projection}")
    for i in range(ncalls):
        df = df.drop(f"{'a' * 50}{i}")
    return df


def union(session: Session, ncalls: int):
    projection = generate_projection(ncalls)
    df = session.sql(f"select {projection}")
    for _ in range(1, ncalls):
        df = df.union(session.sql(f"select {projection}"))
    return df


def union_by_name(session: Session, ncalls: int):
    columns = generate_columns(ncalls)
    projection = to_projection(columns)
    df = session.sql(f"select {projection}")
    for _ in range(ncalls):
        random.shuffle(columns)
        projection = to_projection(columns)
        df = df.union(session.sql(f"select {projection}"))
    return df


def join(session: Session, ncalls: int):
    columns = ",".join(f'{i} as {"a" * 50}{i}' for i in range(ncalls))
    df = session.sql(f"select {columns}")
    for _ in range(1, ncalls):
        temp_df = session.sql(f"select {columns}")
        df = df.natural_join(temp_df)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snowpark Python API performance test")
    parser.add_argument(
        "api", help="the API to test: with_column, drop, union, union_by_name, join"
    )
    parser.add_argument("ncalls", type=int, help="number of calls")
    parser.add_argument(
        "-s",
        "--simplify",
        action="store_true",
        default=False,
        help="Whether use sql simplifier",
    )
    parser.add_argument(
        "-m", "--memory", action="store_true", default=False, help="Do memory profiling"
    )
    args = parser.parse_args()

    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    session.sql_simplifier_enabled = args.simplify
    print("Snowpark Python API Performance Test")
    print("Parameters: ", args)
    try:
        api = args.api
        func = eval(api)
        if args.memory:
            func = memory_profiler.profile(func)
        t0 = time.time()
        dataframe = func(session, args.ncalls)
        t1 = time.time()
        print("Client side time elapsed: ", t1 - t0)
        dataframe.collect()
        t2 = time.time()
        print("SQL execution time elapsed: ", t2 - t1)
    finally:
        session.close()
