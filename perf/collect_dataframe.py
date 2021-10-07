#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import argparse

from snowflake.snowpark import Session

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
parser.add_argument(
    "--table", type=str, default="t10", help="retrieve data from this table"
)
parser.add_argument(
    "--pandas",
    action="store_true",
    help="call DataFrame.toPandas() instead of .collect()",
)

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
    df = session.table(args.table)
    if args.pandas:
        pandas_df = df.toPandas()
        print(pandas_df.shape)
    else:
        local_data = df.collect()
        print(len(local_data))
finally:
    session.close()
