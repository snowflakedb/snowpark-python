#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, contains

session = Session.builder.create()
df = session.table("test_table")
df = df.filter("STR LIKE '%e%'")
df.show()
