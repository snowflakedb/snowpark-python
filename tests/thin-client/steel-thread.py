#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session

CONNECTION_PARAMETERS = {
    "host": "snowflake.dev.local",
    "protocol": "http",
    "port": 53200,
    "account": "snowflake",
    "user": "admin",
    "password": "test",
    "schema": "TSCHEMA",
    "database": "TDB",
    "warehouse": "REGRESS",
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
df = session.table("ttab")
df = df.filter("C1 < 20")
df.show()
