#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, contains

session = Session.builder.create()
df = session.table("test_table").filter("STR LIKE '%e%'")
df2 = df.select("A", df.col("STR"), col("B"), df.STR, df.STR.regexp("test"), col("C") + col("D"))
print(session._ast_batch._request)
