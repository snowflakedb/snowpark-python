#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import argparse

from snowflake.snowpark import Session
from snowflake.snowpark.types import IntegerType, StructField, StructType

parser = argparse.ArgumentParser()
parser.add_argument("--protocol", type=str, default="http", help="http or https")
parser.add_argument("--host", type=str)
parser.add_argument("--port", type=int)
parser.add_argument("--account", type=str)
parser.add_argument("--user", type=str)
parser.add_argument("--password", type=str)
parser.add_argument("--warehouse", type=str)
parser.add_argument("--database", type=str)
parser.add_argument("--schema", type=str)
parser.add_argument("--table", type=str, default="t10", help="save data to this table")
parser.add_argument("--rows", type=int, help="number of rows")
parser.add_argument("--cols", type=int, help="number of columns")
parser.add_argument("--infer-schema", action="store_true")

args = parser.parse_args()

try:
    from connection_parameters import CONNECTION_PARAMETERS
except:
    CONNECTION_PARAMETERS = {
        "host": args.host,
        "port": args.port,
        "account": args.account,
        "user": args.user,
        "warehouse": args.warehouse,
        "database": args.database,
        "schema": args.schema,
        "password": args.password,
        "protocol": args.protocol,
    }

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
try:
    row = list(range(args.cols))
    rows = [row] * args.rows
    df = session.createDataFrame(
        rows,
        schema=StructType(
            [StructField(f"_{i}", IntegerType()) for i in range(args.cols)]
        )
        if not args.infer_schema
        else None,
    )
    df.write.mode("overwrite").saveAsTable(args.table)
finally:
    session.close()
